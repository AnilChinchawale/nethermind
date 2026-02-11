// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

namespace Nethermind.State.Flat.PersistedSnapshots;

/// <summary>
/// A simple disposable list of persisted snapshots, ordered oldest-first.
/// Domain-specific query logic lives in <see cref="ReadOnlySnapshotBundle"/>.
/// </summary>
public sealed class PersistedSnapshotList : IDisposable
{
    private readonly PersistedSnapshot[] _snapshots;
    private bool _isDisposed;

    public static readonly PersistedSnapshotList Empty = new([]);

    public PersistedSnapshotList(PersistedSnapshot[] snapshots) => _snapshots = snapshots;

    public int Count => _snapshots.Length;

    public PersistedSnapshot this[int index] => _snapshots[index];

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        foreach (PersistedSnapshot snapshot in _snapshots)
        {
            snapshot.Dispose();
        }
    }
}
