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
using Nethermind.Serialization.Rlp;
using Nethermind.State.Flat.Persistence;
using Nethermind.State.Flat.PersistedSnapshots;
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
    IPersistedSnapshotManager persistedSnapshotManager,
    IPersistedSnapshotRepository persistedSnapshotRepository) : IPersistenceManager
{
    private readonly ILogger _logger = logManager.GetClassLogger();
    private readonly int _minReorgDepth = configuration.MinReorgDepth;
    private readonly int _maxInMemoryReorgDepth = configuration.MaxInMemoryReorgDepth;
    private readonly int _longFinalityReorgDepth = configuration.LongFinalityReorgDepth;
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

    private (PersistedSnapshot? Persisted, Snapshot? InMemory) GetFinalizedSnapshotAtBlockNumber(long blockNumber, StateId currentPersistedState, bool compactedSnapshot)
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

                return (null, snapshot);
            }

            snapshot.Dispose();
        }

        // No in-memory snapshot found — try persisted snapshot at same block/root
        if (finalizedStateRoot is not null)
        {
            StateId targetStateId = new StateId(blockNumber, finalizedStateRoot);
            bool found = compactedSnapshot
                ? persistedSnapshotRepository.TryLeaseCompactedSnapshotTo(targetStateId, out PersistedSnapshot? persisted)
                : persistedSnapshotRepository.TryLeaseSnapshotTo(targetStateId, out persisted);
            if (found)
            {
                if (persisted!.From == currentPersistedState)
                    return (persisted, null);
                persisted.Dispose();
            }
        }

        return (null, null);
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

    /// <summary>
    /// Collect non-compacted snapshots starting from currentPersistedState that can be converted
    /// to persisted snapshots (RSST files) instead of being force-persisted to RocksDB.
    /// </summary>
    private Snapshot[]? CollectSnapshotsToConvert(StateId currentPersistedState)
    {
        List<Snapshot> toConvert = new();
        StateId current = currentPersistedState;

        // Walk forward from persisted state, collecting non-compacted snapshots
        for (long bn = current.BlockNumber + 1; bn <= current.BlockNumber + _compactSize; bn++)
        {
            using ArrayPoolList<StateId> states = snapshotRepository.GetStatesAtBlockNumber(bn);
            bool found = false;
            foreach (StateId stateId in states)
            {
                if (snapshotRepository.TryLeaseState(stateId, out Snapshot? snapshot))
                {
                    if (snapshot.From == current)
                    {
                        toConvert.Add(snapshot);
                        current = snapshot.To;
                        found = true;
                        break;
                    }
                    snapshot.Dispose();
                }
            }
            if (!found) break;
        }

        if (toConvert.Count == 0) return null;
        return toConvert.ToArray();
    }

    internal (PersistedSnapshot? ToPersistPersistedSnapshot, Snapshot? ToPersist, Snapshot[]? ToConvert) DetermineSnapshotAction(StateId latestSnapshot)
    {
        long lastSnapshotNumber = latestSnapshot.BlockNumber;

        StateId currentPersistedState = GetCurrentPersistedStateId();
        long finalizedBlockNumber = finalizedStateProvider.FinalizedBlockNumber;
        long snapshotsDepth = lastSnapshotNumber - currentPersistedState.BlockNumber;
        if (snapshotsDepth - _compactSize < _minReorgDepth)
        {
            // Keep some state in memory
            return (null, null, null);
        }

        long afterPersistPersistedBlockNumber = currentPersistedState.BlockNumber + _compactSize;
        if (afterPersistPersistedBlockNumber > finalizedBlockNumber)
        {
            if (snapshotsDepth <= _maxInMemoryReorgDepth)
            {
                // No action needed
                return (null, null, null);
            }

            if (snapshotsDepth > _longFinalityReorgDepth)
            {
                // Need to force persisted snapshot
                return (TryGetForcePersistedSnapshot(currentPersistedState, snapshotsDepth), null, null);
            }

            // Memory pressure with unfinalized state: convert to persisted snapshots instead of force-persisting to RocksDB
            if (_logger.IsWarn) _logger.Warn($"Very long unfinalized state. Converting to persisted snapshots. finalized block number is {finalizedBlockNumber}.");
            Snapshot[]? toConvert = CollectSnapshotsToConvert(currentPersistedState);
            if (toConvert is not null)
                return (null, null, toConvert);

            // Nothing to do. No in memory snapshot, and under long finality reorg depth
            return (null, null, null);
        }

        (PersistedSnapshot? persistedSnapshot, Snapshot? snapshotToPersist) =
            GetFinalizedSnapshotAtBlockNumber(currentPersistedState.BlockNumber + _compactSize, currentPersistedState, true);

        if (snapshotToPersist is null && persistedSnapshot is null)
        {
            (persistedSnapshot, snapshotToPersist) =
                GetFinalizedSnapshotAtBlockNumber(currentPersistedState.BlockNumber + 1, currentPersistedState, false);
        }

        if (snapshotToPersist is not null)
            return (null, snapshotToPersist, null);

        if (persistedSnapshot is not null)
            return (persistedSnapshot, null, null);

        if (_logger.IsWarn) _logger.Warn($"Unable to find snapshot to persist. Current persisted state {currentPersistedState}. Compact size {_compactSize}.");
        return (null, null, null);
    }

    public void AddToPersistence(StateId latestSnapshot)
    {
        using Lock.Scope scope = _persistenceLock.EnterScope();
        while (true)
        {
            (PersistedSnapshot? persistedToPersist, Snapshot? toPersist, Snapshot[]? toConvert) = DetermineSnapshotAction(latestSnapshot);

            if (toPersist is not null)
            {
                using Snapshot _ = toPersist;
                PersistSnapshot(toPersist);
                _currentPersistedStateId = toPersist.To;
                persistedSnapshotManager.PrunePersistedSnapshots(_currentPersistedStateId);
            }
            else if (toConvert is not null)
            {
                foreach (Snapshot snapshot in toConvert)
                {
                    using Snapshot _ = snapshot;
                    persistedSnapshotManager.ConvertToPersistedSnapshot(snapshot);
                }
                snapshotRepository.RemoveStatesUntil(toConvert[^1].To);
                // Continue loop — DetermineSnapshotAction will check for force-persist next iteration
            }
            else if (persistedToPersist is not null)
            {
                using PersistedSnapshot _ = persistedToPersist;
                PersistPersistedSnapshot(persistedToPersist);
                _currentPersistedStateId = persistedToPersist.To;
                persistedSnapshotManager.PrunePersistedSnapshots(_currentPersistedStateId);
            }
            else
            {
                break;
            }
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
            (PersistedSnapshot? persisted, Snapshot? snapshotToPersist) = GetFinalizedSnapshotAtBlockNumber(
                currentPersistedState.BlockNumber + _compactSize,
                currentPersistedState,
                compactedSnapshot: true);
            persisted?.Dispose();

            if (snapshotToPersist is null)
            {
                (persisted, snapshotToPersist) = GetFinalizedSnapshotAtBlockNumber(
                    currentPersistedState.BlockNumber + 1,
                    currentPersistedState,
                    compactedSnapshot: false);
                persisted?.Dispose();
            }

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

        persistedSnapshotManager.PrunePersistedSnapshots(currentPersistedState);
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

    private PersistedSnapshot? TryGetForcePersistedSnapshot(StateId currentPersistedState, long totalDepth)
    {
        if (totalDepth <= _longFinalityReorgDepth) return null;
        PersistedSnapshot? oldest = persistedSnapshotManager.TryGetOldestSnapshot(currentPersistedState);
        if (oldest is not null && _logger.IsWarn)
            _logger.Warn($"Total reorg depth {totalDepth} exceeds LongFinalityReorgDepth {_longFinalityReorgDepth}. Force persisting persisted snapshot {oldest.From} -> {oldest.To}.");
        return oldest;
    }

    internal void PersistPersistedSnapshot(PersistedSnapshot snapshot)
    {
        long sw = Stopwatch.GetTimestamp();
        Rsst.Rsst outer = new(snapshot.Data.Span);

        using (IPersistence.IWriteBatch batch = persistence.CreateWriteBatch(snapshot.From, snapshot.To))
        {
            // SelfDestruct column (0x02)
            if (outer.TryGet(PersistedSnapshot.SelfDestructTag, out ReadOnlySpan<byte> sdColumn))
            {
                Rsst.Rsst sdRsst = new(sdColumn);
                using Rsst.Rsst.Enumerator sdEnum = sdRsst.GetEnumerator();
                while (sdEnum.MoveNext())
                {
                    if (sdEnum.Current.Value.IsEmpty) // destructed
                        batch.SelfDestruct(new Address(sdEnum.Current.Key.ToArray()));
                }
            }

            // Account column (0x00)
            if (outer.TryGet(PersistedSnapshot.AccountTag, out ReadOnlySpan<byte> accountColumn))
            {
                Rsst.Rsst accountRsst = new(accountColumn);
                using Rsst.Rsst.Enumerator accountEnum = accountRsst.GetEnumerator();
                while (accountEnum.MoveNext())
                {
                    Address addr = new(accountEnum.Current.Key.ToArray());
                    if (accountEnum.Current.Value.IsEmpty)
                    {
                        batch.SetAccount(addr, null);
                    }
                    else
                    {
                        Account? account = AccountDecoder.Slim.Decode(new RlpStream(accountEnum.Current.Value.ToArray()));
                        batch.SetAccount(addr, account);
                    }
                }
            }

            // Storage column (0x01) - nested: Address(20) → inner RSST(Slot(32) → SlotValue)
            if (outer.TryGet(PersistedSnapshot.StorageTag, out ReadOnlySpan<byte> storageColumn))
            {
                Rsst.Rsst addressLevel = new(storageColumn);
                using Rsst.Rsst.Enumerator addrEnum = addressLevel.GetEnumerator();
                while (addrEnum.MoveNext())
                {
                    Address addr = new(addrEnum.Current.Key.ToArray());
                    Rsst.Rsst innerRsst = new(addrEnum.Current.Value);
                    using Rsst.Rsst.Enumerator slotEnum = innerRsst.GetEnumerator();
                    while (slotEnum.MoveNext())
                    {
                        UInt256 slot = new(slotEnum.Current.Key, isBigEndian: true);
                        if (slotEnum.Current.Value.IsEmpty)
                        {
                            batch.SetStorage(addr, slot, null);
                        }
                        else
                        {
                            SlotValue value = SlotValue.FromSpanWithoutLeadingZero(slotEnum.Current.Value);
                            batch.SetStorage(addr, slot, value);
                        }
                    }
                }
            }

            // StateNode column (0x03): TreePath(32) + PathLength(1) → RLP
            if (outer.TryGet(PersistedSnapshot.StateNodeTag, out ReadOnlySpan<byte> stateNodeColumn))
            {
                Rsst.Rsst stateNodeRsst = new(stateNodeColumn);
                using Rsst.Rsst.Enumerator nodeEnum = stateNodeRsst.GetEnumerator();
                while (nodeEnum.MoveNext())
                {
                    ReadOnlySpan<byte> key = nodeEnum.Current.Key;
                    TreePath path = new(new ValueHash256(key[..32]), key[32]);
                    TrieNode node = new(NodeType.Unknown, nodeEnum.Current.Value.ToArray());
                    batch.SetStateTrieNode(path, node);
                }
            }

            // StorageNode column (0x04) - nested: AddressHash(32) → inner RSST(TreePath(33) → RLP)
            if (outer.TryGet(PersistedSnapshot.StorageNodeTag, out ReadOnlySpan<byte> storageNodeColumn))
            {
                Rsst.Rsst hashLevel = new(storageNodeColumn);
                using Rsst.Rsst.Enumerator hashEnum = hashLevel.GetEnumerator();
                while (hashEnum.MoveNext())
                {
                    Hash256 addressHash = new(hashEnum.Current.Key.ToArray());
                    Rsst.Rsst innerRsst = new(hashEnum.Current.Value);
                    using Rsst.Rsst.Enumerator pathEnum = innerRsst.GetEnumerator();
                    while (pathEnum.MoveNext())
                    {
                        ReadOnlySpan<byte> pathKey = pathEnum.Current.Key;
                        TreePath path = new(new ValueHash256(pathKey[..32]), pathKey[32]);
                        TrieNode node = new(NodeType.Unknown, pathEnum.Current.Value.ToArray());
                        batch.SetStorageTrieNode(addressHash, path, node);
                    }
                }
            }
        }

        Metrics.FlatPersistenceTime.Observe(Stopwatch.GetTimestamp() - sw);
    }

}
