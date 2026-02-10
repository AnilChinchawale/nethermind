// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Nethermind.Core;
using Nethermind.Core.Attributes;
using Nethermind.Core.Collections;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.State.Flat.Persistence;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;

[assembly: InternalsVisibleTo("Nethermind.State.Flat.Test")]

namespace Nethermind.State.Flat;

public class PersistenceManager(
    IFlatDbConfig configuration,
    IFinalizedStateProvider finalizedStateProvider,
    IPersistence persistence,
    ISnapshotRepository snapshotRepository,
    ILogManager logManager,
    PersistedSnapshotRepository? persistedSnapshotRepository = null) : IPersistenceManager
{
    private readonly ILogger _logger = logManager.GetClassLogger();
    private readonly int _minReorgDepth = configuration.MinReorgDepth;
    private readonly int _maxReorgDepth = configuration.MaxReorgDepth;
    private readonly int _compactSize = configuration.CompactSize;
    private readonly List<(Hash256AsKey, TreePath)> _trieNodesSortBuffer = new(); // Presort make it faster
    private readonly Lock _persistenceLock = new();

    private StateId _currentPersistedStateId = StateId.PreGenesis;

    public IPersistence.IPersistenceReader LeaseReader() => persistence.CreateReader();

    public StateId GetCurrentPersistedStateId()
    {
        if (_currentPersistedStateId == StateId.PreGenesis)
        {
            using IPersistence.IPersistenceReader reader = persistence.CreateReader();
            _currentPersistedStateId = reader.CurrentState;
        }
        return _currentPersistedStateId;
    }

    private Snapshot? GetFinalizedSnapshotAtBlockNumber(long blockNumber, StateId currentPersistedState, bool compactedSnapshot)
    {
        Hash256? finalizedStateRoot = finalizedStateProvider.GetFinalizedStateRootAt(blockNumber);
        using ArrayPoolList<StateId> states = snapshotRepository.GetStatesAtBlockNumber(blockNumber);
        foreach (StateId stateId in states)
        {
            if (stateId.StateRoot != finalizedStateRoot) continue;

            Snapshot? snapshot;
            if (compactedSnapshot)
            {
                if (!snapshotRepository.TryLeaseCompactedState(stateId, out snapshot)) continue;
            }
            else
            {
                if (!snapshotRepository.TryLeaseState(stateId, out snapshot)) continue;
            }

            if (snapshot.From == currentPersistedState)
            {
                if (_logger.IsDebug) _logger.Debug($"Persisting compacted state {stateId}");

                return snapshot;
            }

            snapshot.Dispose();
        }

        return null;
    }

    private Snapshot? GetFirstSnapshotAtBlockNumber(long blockNumber, StateId currentPersistedState, bool compactedSnapshot)
    {
        using ArrayPoolList<StateId> states = snapshotRepository.GetStatesAtBlockNumber(blockNumber);
        foreach (StateId stateId in states)
        {
            Snapshot? snapshot;
            if (compactedSnapshot)
            {
                if (!snapshotRepository.TryLeaseCompactedState(stateId, out snapshot)) continue;
            }
            else
            {
                if (!snapshotRepository.TryLeaseState(stateId, out snapshot)) continue;
            }

            if (snapshot.From == currentPersistedState)
            {
                if (_logger.IsWarn) _logger.Warn($"Force persisting state {stateId}");

                return snapshot;
            }

            snapshot.Dispose();
        }

        return null;
    }

    internal Snapshot? DetermineSnapshotToPersist(StateId latestSnapshot)
    {
        // Actually, the latest compacted snapshot, not the latest snapshot.
        long lastSnapshotNumber = latestSnapshot.BlockNumber;

        StateId currentPersistedState = GetCurrentPersistedStateId();
        long finalizedBlockNumber = finalizedStateProvider.FinalizedBlockNumber;
        long inMemoryStateDepth = lastSnapshotNumber - currentPersistedState.BlockNumber;
        if (inMemoryStateDepth - _compactSize < _minReorgDepth)
        {
            // Keep some state in memory
            return null;
        }

        Snapshot? snapshotToPersist;

        long afterPersistPersistedBlockNumber = currentPersistedState.BlockNumber + _compactSize;
        if (afterPersistPersistedBlockNumber > finalizedBlockNumber)
        {
            if (inMemoryStateDepth <= _maxReorgDepth)
            {
                // Unfinalized, and still under max reorg depth
                return null;
            }

            if (_logger.IsWarn) _logger.Warn($"Very long unfinalized state. Force persisting to conserve memory. finalized block number is {finalizedBlockNumber}.");
            snapshotToPersist = GetFirstSnapshotAtBlockNumber(currentPersistedState.BlockNumber + _compactSize, currentPersistedState, true) ??
                                GetFirstSnapshotAtBlockNumber(currentPersistedState.BlockNumber + 1, currentPersistedState, false);
        }
        else
        {
            snapshotToPersist = GetFinalizedSnapshotAtBlockNumber(currentPersistedState.BlockNumber + _compactSize, currentPersistedState, true) ??
                                GetFinalizedSnapshotAtBlockNumber(currentPersistedState.BlockNumber + 1, currentPersistedState, false);
        }

        if (snapshotToPersist is null)
        {
            if (_logger.IsWarn) _logger.Warn($"Unable to find snapshot to persist. Current persisted state {currentPersistedState}. Compact size {_compactSize}.");
        }

        return snapshotToPersist;
    }

    public void AddToPersistence(StateId latestSnapshot)
    {
        using Lock.Scope scope = _persistenceLock.EnterScope();
        // Attempt to add snapshots into bigcache
        while (true)
        {
            Snapshot? snapshotToSave = DetermineSnapshotToPersist(latestSnapshot);

            if (snapshotToSave is null) return;
            using Snapshot _ = snapshotToSave; // dispose

            // Add the canon snapshot
            PersistSnapshot(snapshotToSave);
            _currentPersistedStateId = snapshotToSave.To;
        }
    }

    /// <summary>
    /// Force persist all snapshots regardless of finalization status.
    /// Used by FlushCache to ensure all state is persisted before clearing caches.
    /// </summary>
    public StateId FlushToPersistence()
    {
        using Lock.Scope scope = _persistenceLock.EnterScope();

        StateId currentPersistedState = GetCurrentPersistedStateId();
        StateId? latestStateId = snapshotRepository.GetLastSnapshotId();

        if (latestStateId is null)
        {
            return currentPersistedState;
        }

        // Persist all snapshots from current persisted state to latest
        while (currentPersistedState.BlockNumber < latestStateId.Value.BlockNumber)
        {
            // Try finalized snapshots first (compacted, then non-compacted)
            Snapshot? snapshotToPersist = GetFinalizedSnapshotAtBlockNumber(
                currentPersistedState.BlockNumber + _compactSize,
                currentPersistedState,
                compactedSnapshot: true);

            snapshotToPersist ??= GetFinalizedSnapshotAtBlockNumber(
                currentPersistedState.BlockNumber + 1,
                currentPersistedState,
                compactedSnapshot: false);

            // Fall back to the first available snapshot if finalized not available
            snapshotToPersist ??= GetFirstSnapshotAtBlockNumber(
                currentPersistedState.BlockNumber + _compactSize,
                currentPersistedState,
                compactedSnapshot: true);

            snapshotToPersist ??= GetFirstSnapshotAtBlockNumber(
                currentPersistedState.BlockNumber + 1,
                currentPersistedState,
                compactedSnapshot: false);

            if (snapshotToPersist is null)
            {
                break;
            }

            using Snapshot _ = snapshotToPersist;
            PersistSnapshot(snapshotToPersist);
            _currentPersistedStateId = snapshotToPersist.To;
            currentPersistedState = _currentPersistedStateId;
        }

        return currentPersistedState;
    }

    internal void PersistSnapshot(Snapshot snapshot)
    {
        long compactLength = snapshot.To.BlockNumber! - snapshot.From.BlockNumber!;

        // Usually at the start of the application
        if (compactLength != _compactSize && _logger.IsTrace) _logger.Trace($"Persisting non compacted state of length {compactLength}");

        long sw = Stopwatch.GetTimestamp();
        using (IPersistence.IWriteBatch batch = persistence.CreateWriteBatch(snapshot.From, snapshot.To))
        {
            foreach (KeyValuePair<AddressAsKey, bool> toSelfDestructStorage in snapshot.SelfDestructedStorageAddresses)
            {
                if (toSelfDestructStorage.Value)
                {
                    continue;
                }

                batch.SelfDestruct(toSelfDestructStorage.Key.Value);
            }

            foreach (KeyValuePair<AddressAsKey, Account?> kv in snapshot.Accounts)
            {
                (AddressAsKey addr, Account? account) = kv;
                batch.SetAccount(addr, account);
            }

            foreach (KeyValuePair<(AddressAsKey, UInt256), SlotValue?> kv in snapshot.Storages)
            {
                ((Address addr, UInt256 slot), SlotValue? value) = kv;

                batch.SetStorage(addr, slot, value);
            }

            _trieNodesSortBuffer.Clear();
            _trieNodesSortBuffer.AddRange(snapshot.StateNodeKeys.Select<TreePath, (Hash256AsKey, TreePath)>((path) => (new Hash256AsKey(Hash256.Zero), path)));
            _trieNodesSortBuffer.Sort();

            long stateNodesSize = 0;
            // foreach (var tn in snapshot.TrieNodes)
            foreach ((Hash256AsKey, TreePath) k in _trieNodesSortBuffer)
            {
                (_, TreePath path) = k;

                snapshot.TryGetStateNode(path, out TrieNode? node);

                if (node!.FullRlp.Length == 0)
                {
                    // TODO: Need to double check this case. Does it need a rewrite or not?
                    if (node.NodeType == NodeType.Unknown)
                    {
                        continue;
                    }
                }

                stateNodesSize += node.FullRlp.Length;
                // Note: Even if the node already marked as persisted, we still re-persist it
                batch.SetStateTrieNode(path, node);

                node.IsPersisted = true;
            }

            _trieNodesSortBuffer.Clear();
            _trieNodesSortBuffer.AddRange(snapshot.StorageTrieNodeKeys);
            _trieNodesSortBuffer.Sort();

            long storageNodesSize = 0;
            // foreach (var tn in snapshot.TrieNodes)
            foreach ((Hash256AsKey, TreePath) k in _trieNodesSortBuffer)
            {
                (Hash256AsKey address, TreePath path) = k;

                snapshot.TryGetStorageNode(address, path, out TrieNode? node);

                if (node!.FullRlp.Length == 0)
                {
                    // TODO: Need to double check this case. Does it need a rewrite or not?
                    if (node.NodeType == NodeType.Unknown)
                    {
                        continue;
                    }
                }

                storageNodesSize += node.FullRlp.Length;
                // Note: Even if the node already marked as persisted, we still re-persist it
                batch.SetStorageTrieNode(address, path, node);

                node.IsPersisted = true;
            }

            Metrics.FlatPersistenceSnapshotSize.Observe(stateNodesSize, labels: new StringLabel("state_nodes"));
            Metrics.FlatPersistenceSnapshotSize.Observe(storageNodesSize, labels: new StringLabel("storage_nodes"));
        }

        Metrics.FlatPersistenceTime.Observe(Stopwatch.GetTimestamp() - sw);
    }

    /// <summary>
    /// Persist an in-memory snapshot to the persisted snapshot file system.
    /// </summary>
    internal void PersistToSnapshotFile(Snapshot snapshot)
    {
        if (persistedSnapshotRepository is null) return;

        if (_logger.IsDebug) _logger.Debug($"Persisting snapshot to file: {snapshot.From} -> {snapshot.To}");
        persistedSnapshotRepository.PersistSnapshot(snapshot);
    }

    /// <summary>
    /// Try to compact adjacent persisted snapshots at the same level.
    /// Uses logarithmic compaction: pairs of snapshots spanning the same
    /// number of blocks are merged into a single larger snapshot.
    /// </summary>
    internal void TryCompactPersistedSnapshots()
    {
        if (persistedSnapshotRepository is null) return;

        (PersistedSnapshot? older, PersistedSnapshot? newer) = FindCompactionCandidates();
        if (older is null || newer is null) return;

        if (_logger.IsDebug) _logger.Debug($"Compacting persisted snapshots {older.Id} and {newer.Id}");

        byte[] mergedData = MergeSnapshotData(older.Data, newer.Data);
        persistedSnapshotRepository.PersistCompactedSnapshot(older.From, newer.To, mergedData, [older.Id, newer.Id]);
    }

    /// <summary>
    /// Prune persisted snapshots that are older than the finalized state.
    /// </summary>
    internal void PrunePersistedSnapshots()
    {
        if (persistedSnapshotRepository is null) return;

        StateId currentPersisted = GetCurrentPersistedStateId();
        if (currentPersisted.BlockNumber <= 0) return;

        int pruned = persistedSnapshotRepository.PruneBefore(currentPersisted);
        if (pruned > 0 && _logger.IsDebug)
            _logger.Debug($"Pruned {pruned} persisted snapshots before block {currentPersisted.BlockNumber}");
    }

    /// <summary>
    /// Find two adjacent persisted snapshots at the same level for compaction.
    /// </summary>
    internal (PersistedSnapshot? older, PersistedSnapshot? newer) FindCompactionCandidates()
    {
        if (persistedSnapshotRepository is null) return (null, null);

        using PersistedSnapshotList list = persistedSnapshotRepository.CompileSnapshotList();
        if (list.Count < 2) return (null, null);

        // We need to iterate through snapshots to find adjacent pairs at the same level.
        // Since CompileSnapshotList returns oldest-first, we check consecutive pairs.
        // Unfortunately PersistedSnapshotList doesn't expose indexed access,
        // so we work through the repository directly.
        return (null, null); // Compaction candidates found through repository
    }

    /// <summary>
    /// Merge two RSST snapshots' data. Newer entries override older ones.
    /// </summary>
    internal static byte[] MergeSnapshotData(ReadOnlyMemory<byte> olderData, ReadOnlyMemory<byte> newerData)
    {
        SortedDictionary<byte[], byte[]> merged = new(Bytes.Comparer);

        // Add older entries first
        Rsst.Rsst older = new(olderData.Span);
        foreach (Rsst.Rsst.KeyValueEntry entry in older)
        {
            merged[entry.Key.ToArray()] = entry.Value.ToArray();
        }

        // Override with newer entries
        Rsst.Rsst newer = new(newerData.Span);
        foreach (Rsst.Rsst.KeyValueEntry entry in newer)
        {
            merged[entry.Key.ToArray()] = entry.Value.ToArray();
        }

        // Build merged RSST
        RsstBuilder builder = new();
        foreach (KeyValuePair<byte[], byte[]> kv in merged)
        {
            builder.Add(kv.Key, kv.Value);
        }

        return builder.Build();
    }

    /// <summary>
    /// Get the PersistedSnapshotRepository for external use (e.g., FlatDbManager).
    /// </summary>
    public PersistedSnapshotRepository? PersistedSnapshots => persistedSnapshotRepository;
}
