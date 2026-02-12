// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Diagnostics.CodeAnalysis;

namespace Nethermind.State.Flat.PersistedSnapshots;

public interface IPersistedSnapshotRepository : IDisposable
{
    int SnapshotCount { get; }
    int CompactedSnapshotCount { get; }
    void LoadFromCatalog();

    // Two-layer storage
    PersistedSnapshot AddBaseSnapshot(Snapshot snapshot);
    PersistedSnapshot AddCompactedSnapshot(StateId from, StateId to, ReadOnlySpan<byte> rsstData);

    // Assembly (mirrors SnapshotRepository.AssembleSnapshots)
    PersistedSnapshotList AssembleSnapshots(StateId targetFrom, StateId persistedState);

    // Compaction assembly (mirrors SnapshotRepository.AssembleSnapshotsUntil)
    PersistedSnapshotList AssembleSnapshotsForCompaction(StateId toStateId, long minBlockNumber);
    int RemoveCompactedSnapshotsAtBlock(long blockNumber);

    // Lookup
    PersistedSnapshot? TryGetSnapshotFrom(StateId fromState);
    bool TryLeaseSnapshotTo(StateId toState, [NotNullWhen(true)] out PersistedSnapshot? snapshot);
    bool TryLeaseCompactedSnapshotTo(StateId toState, [NotNullWhen(true)] out PersistedSnapshot? snapshot);

    // Lifecycle
    int PruneBefore(StateId stateId);
}
