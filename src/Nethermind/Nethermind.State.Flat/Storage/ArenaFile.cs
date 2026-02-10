// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Microsoft.Win32.SafeHandles;

namespace Nethermind.State.Flat.Storage;

/// <summary>
/// A single append-only arena file for storing persisted snapshot RSST data.
/// </summary>
public sealed class ArenaFile(int id, string path) : IDisposable
{
    private readonly SafeFileHandle _handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

    public int Id => id;
    public string Path => path;
    public long Length => RandomAccess.GetLength(_handle);

    public void Write(long offset, ReadOnlySpan<byte> data) =>
        RandomAccess.Write(_handle, data, offset);

    public byte[] Read(long offset, int size)
    {
        byte[] buffer = new byte[size];
        RandomAccess.Read(_handle, buffer, offset);
        return buffer;
    }

    public void Dispose() => _handle.Dispose();
}
