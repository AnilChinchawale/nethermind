// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Collections.Concurrent;
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
    private readonly Dictionary<int, HashSet<int>> _referencedBy = []; // baseId -> set of compacted IDs
    private readonly object _lock = new();
    private int _nextId;

    public PersistedSnapshotRepository(string basePath, long maxArenaSize = 4L * 1024 * 1024 * 1024)
    {
        string arenaDir = Path.Combine(basePath, "arenas");
        _arenaManager = new ArenaManager(arenaDir, maxArenaSize);
        _catalog = new SnapshotCatalog(Path.Combine(basePath, "catalog.bin"));
    }

    public int SnapshotCount
    {
        get { lock (_lock) return _baseSnapshots.Count + _compactedSnapshots.Count; }
    }

    /// <summary>
    /// Load all persisted snapshots from catalog and arena files.
    /// </summary>
    public void LoadFromCatalog()
    {
        lock (_lock)
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
        snapshot.BuildBloom();

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

        lock (_lock)
        {
            int id = _nextId++;
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, location));
            _catalog.Save();

            PersistedSnapshot persisted = new(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, (byte[])rsstData.Clone());
            persisted.BuildBloom();
            _baseSnapshots[snapshot.To] = persisted;
            return persisted;
        }
    }

    /// <summary>
    /// Store a compacted snapshot (keyed by To StateId).
    /// </summary>
    public PersistedSnapshot AddCompactedSnapshot(StateId from, StateId to, byte[] rsstData)
    {
        lock (_lock)
        {
            int id = _nextId++;
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, from, to, PersistedSnapshotType.Compacted, location));
            _catalog.Save();

            PersistedSnapshot persisted = new(id, from, to, PersistedSnapshotType.Compacted, (byte[])rsstData.Clone());
            persisted.BuildBloom();
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
        lock (_lock)
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
    /// Compile an ordered snapshot list for queries. Snapshots are ordered
    /// oldest-first in the array; PersistedSnapshotList queries newest-first.
    /// Acquires leases on all included snapshots.
    /// </summary>
    public PersistedSnapshotList CompileSnapshotList()
    {
        lock (_lock)
        {
            int total = _baseSnapshots.Count + _compactedSnapshots.Count;
            if (total == 0) return PersistedSnapshotList.Empty;

            List<PersistedSnapshot> ordered = new(total);
            foreach (PersistedSnapshot snapshot in _baseSnapshots.Values)
                ordered.Add(snapshot);
            foreach (PersistedSnapshot snapshot in _compactedSnapshots.Values)
                ordered.Add(snapshot);

            ordered.Sort((a, b) => a.From.BlockNumber.CompareTo(b.From.BlockNumber));

            List<PersistedSnapshot> leased = new(ordered.Count);
            foreach (PersistedSnapshot snapshot in ordered)
            {
                if (snapshot.TryAcquire())
                    leased.Add(snapshot);
            }

            return leased.Count == 0
                ? PersistedSnapshotList.Empty
                : new PersistedSnapshotList(leased.ToArray());
        }
    }

    /// <summary>
    /// Prune snapshots with To.BlockNumber before the given state.
    /// </summary>
    public int PruneBefore(StateId stateId)
    {
        lock (_lock)
        {
            int pruned = 0;

            // Prune base snapshots (skip referenced ones)
            List<StateId> baseToRemove = new();
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _baseSnapshots)
            {
                if (kv.Value.To.BlockNumber < stateId.BlockNumber && !IsReferenced(kv.Value.Id))
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

    private bool IsReferenced(int snapshotId) =>
        _referencedBy.TryGetValue(snapshotId, out HashSet<int>? refs) && refs.Count > 0;

    private void RemoveFromCatalog(int snapshotId)
    {
        SnapshotCatalog.CatalogEntry? entry = _catalog.Find(snapshotId);
        if (entry is not null)
        {
            _arenaManager.MarkDead(entry.Location);
            _catalog.Remove(snapshotId);
        }
        _referencedBy.Remove(snapshotId);
        foreach (HashSet<int> refs in _referencedBy.Values)
            refs.Remove(snapshotId);
    }

    // Legacy methods for backward compatibility with existing callers

    /// <summary>
    /// Legacy: Persist an in-memory snapshot to disk as a base snapshot.
    /// </summary>
    public PersistedSnapshot PersistSnapshot(Snapshot snapshot) => AddBaseSnapshot(snapshot);

    /// <summary>
    /// Legacy: Persist compacted snapshot data.
    /// </summary>
    public PersistedSnapshot PersistCompactedSnapshot(StateId from, StateId to, byte[] rsstData, IReadOnlyList<int> referencedBaseIds)
    {
        PersistedSnapshot result = AddCompactedSnapshot(from, to, rsstData);

        lock (_lock)
        {
            foreach (int baseId in referencedBaseIds)
            {
                if (!_referencedBy.TryGetValue(baseId, out HashSet<int>? refs))
                {
                    refs = [];
                    _referencedBy[baseId] = refs;
                }
                refs.Add(result.Id);
            }
        }

        return result;
    }

    public PersistedSnapshot? FindById(int id)
    {
        lock (_lock)
        {
            foreach (PersistedSnapshot snapshot in _baseSnapshots.Values)
                if (snapshot.Id == id) return snapshot;
            foreach (PersistedSnapshot snapshot in _compactedSnapshots.Values)
                if (snapshot.Id == id) return snapshot;
            return null;
        }
    }

    public bool RemoveSnapshot(int snapshotId)
    {
        lock (_lock)
        {
            // Try base
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _baseSnapshots)
            {
                if (kv.Value.Id == snapshotId)
                {
                    _baseSnapshots.TryRemove(kv.Key, out _);
                    RemoveFromCatalog(snapshotId);
                    kv.Value.Dispose();
                    return true;
                }
            }

            // Try compacted
            foreach (KeyValuePair<StateId, PersistedSnapshot> kv in _compactedSnapshots)
            {
                if (kv.Value.Id == snapshotId)
                {
                    _compactedSnapshots.TryRemove(kv.Key, out _);
                    RemoveFromCatalog(snapshotId);
                    kv.Value.Dispose();
                    return true;
                }
            }

            return false;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            foreach (PersistedSnapshot snapshot in _baseSnapshots.Values)
                snapshot.Dispose();
            foreach (PersistedSnapshot snapshot in _compactedSnapshots.Values)
                snapshot.Dispose();
            _baseSnapshots.Clear();
            _compactedSnapshots.Clear();
            _referencedBy.Clear();
            _arenaManager.Dispose();
        }
    }
}
