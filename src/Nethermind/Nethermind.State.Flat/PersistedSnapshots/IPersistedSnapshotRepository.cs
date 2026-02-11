// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

namespace Nethermind.State.Flat.PersistedSnapshots;

public interface IPersistedSnapshotRepository : IDisposable
{
    int SnapshotCount { get; }
    void LoadFromCatalog();

    // Two-layer storage
    PersistedSnapshot AddBaseSnapshot(Snapshot snapshot);
    PersistedSnapshot AddCompactedSnapshot(StateId from, StateId to, ReadOnlySpan<byte> rsstData);

    // Assembly (mirrors SnapshotRepository.AssembleSnapshots)
    PersistedSnapshotList AssembleSnapshots(StateId targetFrom, StateId persistedState);

    // Compaction assembly (mirrors SnapshotRepository.AssembleSnapshotsUntil)
    PersistedSnapshotList AssembleSnapshotsForCompaction(StateId toStateId, long minBlockNumber);
    int RemoveCompactedSnapshotsAtBlock(long blockNumber);

    // Lifecycle
    int PruneBefore(StateId stateId);
}
