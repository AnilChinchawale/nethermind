// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Runtime.CompilerServices;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.Trie;

namespace Nethermind.State.Flat;

/// <summary>
/// An ordered list of persisted snapshots for layered querying.
/// Snapshots are ordered newest-first for lookup priority.
/// </summary>
public sealed class PersistedSnapshotList : IDisposable
{
    private readonly PersistedSnapshot[] _snapshots;
    private bool _isDisposed;

    public static readonly PersistedSnapshotList Empty = new([]);

    public PersistedSnapshotList(PersistedSnapshot[] snapshots) => _snapshots = snapshots;

    public int Count => _snapshots.Length;

    public byte[]? TryLoadStateNodeRlp(in TreePath path)
    {
        for (int i = _snapshots.Length - 1; i >= 0; i--)
        {
            byte[]? rlp = _snapshots[i].TryLoadStateNodeRlp(path);
            if (rlp is not null) return rlp;
        }

        return null;
    }

    public byte[]? TryLoadStorageNodeRlp(Hash256 address, in TreePath path)
    {
        for (int i = _snapshots.Length - 1; i >= 0; i--)
        {
            byte[]? rlp = _snapshots[i].TryLoadStorageNodeRlp(address, path);
            if (rlp is not null) return rlp;
        }

        return null;
    }

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
