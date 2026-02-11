// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

namespace Nethermind.State.Flat.PersistedSnapshots;

public interface IPersistedSnapshotManager
{
    void ConvertToPersistedSnapshot(Snapshot snapshot);
    void PrunePersistedSnapshots(StateId currentPersistedState);
}
