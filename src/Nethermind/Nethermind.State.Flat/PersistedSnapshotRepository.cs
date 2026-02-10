// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.State.Flat.Storage;

namespace Nethermind.State.Flat;

/// <summary>
/// Manages persisted snapshots on disk. Encapsulates the arena manager
/// and catalog, providing high-level operations for persistence,
/// querying, compaction reference tracking, and pruning.
/// </summary>
public sealed class PersistedSnapshotRepository : IDisposable
{
    private readonly ArenaManager _arenaManager;
    private readonly SnapshotCatalog _catalog;
    private readonly Dictionary<int, PersistedSnapshot> _snapshots = [];
    private readonly Dictionary<int, HashSet<int>> _referencedBy = []; // baseId -> set of compacted IDs
    private readonly object _lock = new();

    public PersistedSnapshotRepository(string basePath, long maxArenaSize = 4L * 1024 * 1024 * 1024)
    {
        string arenaDir = Path.Combine(basePath, "arenas");
        _arenaManager = new ArenaManager(arenaDir, maxArenaSize);
        _catalog = new SnapshotCatalog(Path.Combine(basePath, "catalog.bin"));
    }

    public int SnapshotCount
    {
        get { lock (_lock) return _snapshots.Count; }
    }

    /// <summary>
    /// Load all persisted snapshots from catalog and arena files.
    /// Base snapshots are loaded first, then compacted.
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

            // Then compacted (they may reference base snapshots)
            foreach (SnapshotCatalog.CatalogEntry entry in _catalog.Entries)
            {
                if (entry.Type != PersistedSnapshotType.Compacted) continue;
                LoadSnapshot(entry);
            }
        }
    }

    private void LoadSnapshot(SnapshotCatalog.CatalogEntry entry)
    {
        byte[] data = _arenaManager.Read(entry.Location);
        PersistedSnapshot snapshot = new(entry.Id, entry.From, entry.To, entry.Type, data);
        _snapshots[entry.Id] = snapshot;
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
            if (_snapshots.Count == 0) return PersistedSnapshotList.Empty;

            // Collect and sort by From.BlockNumber (oldest first)
            List<PersistedSnapshot> ordered = new(_snapshots.Count);
            foreach (PersistedSnapshot snapshot in _snapshots.Values)
            {
                ordered.Add(snapshot);
            }
            ordered.Sort((a, b) => a.From.BlockNumber.CompareTo(b.From.BlockNumber));

            // Acquire leases
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
    /// Persist an in-memory snapshot to disk as a base snapshot.
    /// </summary>
    public PersistedSnapshot PersistSnapshot(Snapshot snapshot)
    {
        byte[] rsstData = PersistedSnapshotBuilder.Build(snapshot);

        lock (_lock)
        {
            int id = _catalog.NextId();
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, location));
            _catalog.Save();

            PersistedSnapshot persisted = new(id, snapshot.From, snapshot.To, PersistedSnapshotType.Base, (byte[])rsstData.Clone());
            _snapshots[id] = persisted;
            return persisted;
        }
    }

    /// <summary>
    /// Persist compacted snapshot data with references to base snapshots.
    /// </summary>
    public PersistedSnapshot PersistCompactedSnapshot(StateId from, StateId to, byte[] rsstData, IReadOnlyList<int> referencedBaseIds)
    {
        lock (_lock)
        {
            int id = _catalog.NextId();
            SnapshotLocation location = _arenaManager.Allocate(rsstData);
            _catalog.Add(new SnapshotCatalog.CatalogEntry(id, from, to, PersistedSnapshotType.Compacted, location));
            _catalog.Save();

            // Track references: this compacted snapshot depends on the base snapshots
            foreach (int baseId in referencedBaseIds)
            {
                if (!_referencedBy.TryGetValue(baseId, out HashSet<int>? refs))
                {
                    refs = [];
                    _referencedBy[baseId] = refs;
                }
                refs.Add(id);
            }

            PersistedSnapshot persisted = new(id, from, to, PersistedSnapshotType.Compacted, (byte[])rsstData.Clone());
            _snapshots[id] = persisted;
            return persisted;
        }
    }

    /// <summary>
    /// Prune snapshots with To.BlockNumber before the given state.
    /// Skips snapshots that are still referenced by compacted snapshots.
    /// </summary>
    public int PruneBefore(StateId stateId)
    {
        lock (_lock)
        {
            List<int> toRemove = [];
            foreach (KeyValuePair<int, PersistedSnapshot> kv in _snapshots)
            {
                if (kv.Value.To.BlockNumber >= stateId.BlockNumber) continue;
                if (IsReferenced(kv.Key)) continue;
                toRemove.Add(kv.Key);
            }

            foreach (int id in toRemove)
            {
                RemoveSnapshot(id);
            }

            if (toRemove.Count > 0) _catalog.Save();
            return toRemove.Count;
        }
    }

    /// <summary>
    /// Remove a specific snapshot by ID.
    /// </summary>
    public bool RemoveSnapshot(int snapshotId)
    {
        if (!_snapshots.TryGetValue(snapshotId, out PersistedSnapshot? snapshot)) return false;

        SnapshotCatalog.CatalogEntry? entry = _catalog.Find(snapshotId);
        if (entry is not null)
        {
            _arenaManager.MarkDead(entry.Location);
            _catalog.Remove(snapshotId);
        }

        // Remove from reference tracking
        _referencedBy.Remove(snapshotId);
        foreach (HashSet<int> refs in _referencedBy.Values)
        {
            refs.Remove(snapshotId);
        }

        _snapshots.Remove(snapshotId);
        snapshot.Dispose();
        return true;
    }

    public PersistedSnapshot? FindById(int id)
    {
        lock (_lock)
        {
            return _snapshots.GetValueOrDefault(id);
        }
    }

    private bool IsReferenced(int snapshotId) =>
        _referencedBy.TryGetValue(snapshotId, out HashSet<int>? refs) && refs.Count > 0;

    public void Dispose()
    {
        lock (_lock)
        {
            foreach (PersistedSnapshot snapshot in _snapshots.Values)
            {
                snapshot.Dispose();
            }
            _snapshots.Clear();
            _referencedBy.Clear();
            _arenaManager.Dispose();
        }
    }
}
