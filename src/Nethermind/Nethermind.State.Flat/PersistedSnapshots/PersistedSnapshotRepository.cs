// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Nethermind.State.Flat.Storage;

namespace Nethermind.State.Flat.PersistedSnapshots;

/// <summary>
/// Manages persisted snapshots on disk with a two-layer design (base + compacted),
/// mirroring <see cref="SnapshotRepository"/>'s pattern.
/// </summary>
public sealed class PersistedSnapshotRepository : IPersistedSnapshotRepository
{
    private readonly ArenaManager _arenaManager;
    private readonly SnapshotCatalog _catalog;
    private readonly ConcurrentDictionary<StateId, PersistedSnapshot> _baseSnapshots = new();
    private readonly ConcurrentDictionary<StateId, PersistedSnapshot> _compactedSnapshots = new();
    private readonly object _catalogLock = new();
    private int _nextId;

    public PersistedSnapshotRepository(string basePath, long maxArenaSize = 4L * 1024 * 1024 * 1024)
    {
        string arenaDir = Path.Combine(basePath, "arenas");
        _arenaManager = new ArenaManager(arenaDir, maxArenaSize);
        _catalog = new SnapshotCatalog(Path.Combine(basePath, "catalog.bin"));
    }

    public int SnapshotCount => _baseSnapshots.Count + _compactedSnapshots.Count;

    public int CompactedSnapshotCount => _compactedSnapshots.Count;

    /// <summary>
    /// Load all persisted snapshots from catalog and arena files.
    /// </summary>
    public void LoadFromCatalog()
    {
        lock (_catalogLock)
        {
            _catalog.Load();
            _arenaManager.Initialize(_catalog.Entries);

            // Load base snapshots first
            foreach (SnapshotCatalog.CatalogEntry entry in _catalog.Entries)
            {
                if (entry.Type != PersistedSnapshotType.Base) continue;
                LoadSnapshot(entry);
            }

            // Then compacted
            foreach (SnapshotCatalog.CatalogEntry entry in _catalog.Entries)
            {
                if (entry.Type != PersistedSnapshotType.Compacted) continue;
                LoadSnapshot(entry);
            }

            _nextId = _catalog.NextId();
        }
    }

    private void LoadSnapshot(SnapshotCatalog.CatalogEntry entry)
    {
        byte[] data = _arenaManager.Read(entry.Location);
        PersistedSnapshot snapshot = new(entry.Id, entry.From, entry.To, entry.Type, data);

        if (entry.Type == PersistedSnapshotType.Base)
            _baseSnapshots[entry.To] = snapshot;
        else
            _compactedSnapshots[entry.To] = snapshot;
    }

    /// <summary>
    /// Persist an in-memory snapshot to disk as a base snapshot (keyed by To StateId).
    /// </summary>
    public PersistedSnapshot AddBaseSnapshot(Snapshot snapshot)
    {
        byte[] rsstData = PersistedSnapshotBuilder.Build(snapshot);

        lock (_catalogLock)
        {
            int id = _nextId++;
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, location));
            _catalog.Save();

            PersistedSnapshot persisted = new(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, (byte[])rsstData.Clone());
            _baseSnapshots[snapshot.To] = persisted;
            return persisted;
        }
    }

    /// <summary>
    /// Store a compacted snapshot (keyed by To StateId).
    /// </summary>
    public PersistedSnapshot AddCompactedSnapshot(StateId from, StateId to, ReadOnlySpan<byte> rsstData)
    {
        lock (_catalogLock)
        {
            int id = _nextId++;
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, from, to, PersistedSnapshotType.Compacted, location));
            _catalog.Save();

            PersistedSnapshot persisted = new(id, from, to, PersistedSnapshotType.Compacted, rsstData.ToArray());
            _compactedSnapshots[to] = persisted;
            return persisted;
        }
    }

    /// <summary>
    /// Assemble an ordered list of persisted snapshots between persistedState and targetFrom.
    /// Mirrors <see cref="SnapshotRepository.AssembleSnapshotsUntil"/>.
    /// Returns oldest-first list. Empty if chain is broken.
    /// </summary>
    public PersistedSnapshotList AssembleSnapshots(StateId targetFrom, StateId persistedState)
    {
        List<PersistedSnapshot> result = new();
        StateId current = targetFrom;

        while (current != persistedState)
        {
            PersistedSnapshot? snapshot = TryGetSnapshot(current);
            if (snapshot is null)
            {
                // Chain broken
                DisposeList(result);
                return PersistedSnapshotList.Empty;
            }

            if (!snapshot.TryAcquire())
            {
                DisposeList(result);
                return PersistedSnapshotList.Empty;
            }

            result.Add(snapshot);

            if (snapshot.From == current)
                break; // Prevent infinite loop on same-state

            if (snapshot.From == persistedState)
                break;

            current = snapshot.From;
        }

        result.Reverse(); // oldest first
        return result.Count == 0
            ? PersistedSnapshotList.Empty
            : new PersistedSnapshotList(result.ToArray());
    }

    /// <summary>
    /// Try to get a snapshot by its To StateId. Tries compacted first, then base.
    /// </summary>
    private PersistedSnapshot? TryGetSnapshot(StateId toStateId)
    {
        if (_compactedSnapshots.TryGetValue(toStateId, out PersistedSnapshot? compacted))
            return compacted;
        if (_baseSnapshots.TryGetValue(toStateId, out PersistedSnapshot? baseSnapshot))
            return baseSnapshot;
        return null;
    }

    private static void DisposeList(List<PersistedSnapshot> list)
    {
        foreach (PersistedSnapshot s in list)
            s.Dispose();
    }

    /// <summary>
    /// Assemble persisted snapshots for compaction, walking backward from toStateId.
    /// If a compacted snapshot spans too far back (below minBlockNumber), fall back to base.
    /// Returns oldest-first list, or empty if fewer than 2 snapshots found.
    /// Mirrors <see cref="SnapshotRepository.AssembleSnapshotsUntil"/>.
    /// </summary>
    public PersistedSnapshotList AssembleSnapshotsForCompaction(StateId toStateId, long minBlockNumber)
    {
        List<PersistedSnapshot> result = new();
        StateId current = toStateId;

        while (true)
        {
            PersistedSnapshot? snapshot = null;

            // Try compacted first
            if (_compactedSnapshots.TryGetValue(current, out PersistedSnapshot? compacted))
            {
                if (compacted.From.BlockNumber < minBlockNumber)
                {
                    // Compacted spans too far back, try base
                    if (_baseSnapshots.TryGetValue(current, out PersistedSnapshot? baseSnap))
                    {
                        if (baseSnap.From.BlockNumber < minBlockNumber)
                            break; // Base also spans too far
                        snapshot = baseSnap;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    snapshot = compacted;
                }
            }
            else if (_baseSnapshots.TryGetValue(current, out PersistedSnapshot? baseSnap))
            {
                if (baseSnap.From.BlockNumber < minBlockNumber)
                    break;
                snapshot = baseSnap;
            }
            else
            {
                break;
            }

            if (!snapshot.TryAcquire())
            {
                DisposeList(result);
                return PersistedSnapshotList.Empty;
            }

            result.Add(snapshot);

            if (snapshot.From == current)
                break; // Prevent infinite loop

            if (snapshot.From.BlockNumber == minBlockNumber)
                break;

            current = snapshot.From;
        }

        if (result.Count < 2)
        {
            DisposeList(result);
            return PersistedSnapshotList.Empty;
        }

        result.Reverse(); // oldest-first
        return new PersistedSnapshotList(result.ToArray());
    }

    /// <summary>
    /// Remove compacted snapshots whose To.BlockNumber matches the given block number.
    /// </summary>
    public int RemoveCompactedSnapshotsAtBlock(long blockNumber)
    {
        lock (_catalogLock)
        {
            List<StateId> toRemove = new();
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _compactedSnapshots)
            {
                if (kv.Value.To.BlockNumber == blockNumber)
                    toRemove.Add(kv.Key);
            }

            int removed = 0;
            foreach (StateId key in toRemove)
            {
                if (_compactedSnapshots.TryRemove(key, out PersistedSnapshot? snapshot))
                {
                    RemoveFromCatalog(snapshot.Id);
                    snapshot.Dispose();
                    removed++;
                }
            }

            if (removed > 0) _catalog.Save();
            return removed;
        }
    }

    public bool TryLeaseSnapshotTo(StateId toState, [NotNullWhen(true)] out PersistedSnapshot? snapshot)
    {
        if (_baseSnapshots.TryGetValue(toState, out snapshot) && snapshot.TryAcquire())
            return true;
        snapshot = null;
        return false;
    }

    public bool TryLeaseCompactedSnapshotTo(StateId toState, [NotNullWhen(true)] out PersistedSnapshot? snapshot)
    {
        if (_compactedSnapshots.TryGetValue(toState, out snapshot) && snapshot.TryAcquire())
            return true;
        snapshot = null;
        return false;
    }

    /// <summary>
    /// Find the snapshot whose From matches the given state. Tries compacted first (larger range = faster catch-up), then base.
    /// </summary>
    public PersistedSnapshot? TryGetSnapshotFrom(StateId fromState)
    {
        foreach (PersistedSnapshot snapshot in _compactedSnapshots.Values)
        {
            if (snapshot.From == fromState && snapshot.TryAcquire())
                return snapshot;
        }

        foreach (PersistedSnapshot snapshot in _baseSnapshots.Values)
        {
            if (snapshot.From == fromState && snapshot.TryAcquire())
                return snapshot;
        }

        return null;
    }

    /// <summary>
    /// Prune snapshots with To.BlockNumber before the given state.
    /// </summary>
    public int PruneBefore(StateId stateId)
    {
        lock (_catalogLock)
        {
            int pruned = 0;

            // Prune base snapshots
            List<StateId> baseToRemove = new();
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _baseSnapshots)
            {
                if (kv.Value.To.BlockNumber < stateId.BlockNumber)
                    baseToRemove.Add(kv.Key);
            }
            foreach (StateId key in baseToRemove)
            {
                if (_baseSnapshots.TryRemove(key, out PersistedSnapshot? snapshot))
                {
                    RemoveFromCatalog(snapshot.Id);
                    snapshot.Dispose();
                    pruned++;
                }
            }

            // Prune compacted snapshots
            List<StateId> compactedToRemove = new();
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _compactedSnapshots)
            {
                if (kv.Value.To.BlockNumber < stateId.BlockNumber)
                    compactedToRemove.Add(kv.Key);
            }
            foreach (StateId key in compactedToRemove)
            {
                if (_compactedSnapshots.TryRemove(key, out PersistedSnapshot? snapshot))
                {
                    RemoveFromCatalog(snapshot.Id);
                    snapshot.Dispose();
                    pruned++;
                }
            }

            if (pruned > 0) _catalog.Save();
            return pruned;
        }
    }

    private void RemoveFromCatalog(int snapshotId)
    {
        SnapshotCatalog.CatalogEntry? entry = _catalog.Find(snapshotId);
        if (entry is not null)
        {
            _arenaManager.MarkDead(entry.Location);
            _catalog.Remove(snapshotId);
        }
    }

    public void Dispose()
    {
        lock (_catalogLock)
        {
            foreach (PersistedSnapshot snapshot in _baseSnapshots.Values)
                snapshot.Dispose();
            foreach (PersistedSnapshot snapshot in _compactedSnapshots.Values)
                snapshot.Dispose();
            _baseSnapshots.Clear();
            _compactedSnapshots.Clear();
            _arenaManager.Dispose();
        }
    }
}
