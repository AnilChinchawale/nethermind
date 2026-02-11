// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Runtime.CompilerServices;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Utils;
using Nethermind.Int256;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;

namespace Nethermind.State.Flat;

/// <summary>
/// A persisted snapshot backed by RSST data on disk (or in memory).
/// Provides typed access to accounts, storage, self-destruct markers, and trie nodes.
///
/// Key encoding uses a tag prefix for sorting and grouping:
///   Tag 0x00 + Address (20 bytes) → Account RLP
///   Tag 0x01 + Address (20 bytes) + UInt256 slot (32 bytes big-endian) → Slot value bytes
///   Tag 0x02 + Address (20 bytes) → Self-destruct marker (empty value)
///   Tag 0x03 + TreePath.Path (32 bytes) + PathLength (1 byte) → State trie node RLP
///   Tag 0x04 + AddressHash (32 bytes) + TreePath.Path (32 bytes) + PathLength (1 byte) → Storage trie node RLP
/// </summary>
public sealed class PersistedSnapshot : RefCountingDisposable
{
    // Tag prefixes for RSST key encoding
    internal const byte AccountTag = 0x00;
    internal const byte StorageTag = 0x01;
    internal const byte SelfDestructTag = 0x02;
    internal const byte StateNodeTag = 0x03;
    internal const byte StorageNodeTag = 0x04;

    private readonly Memory<byte> _data;
    private readonly IDisposable? _dataOwner;
    private SnapshotBloomFilter? _bloom;

    public int Id { get; }
    public StateId From { get; }
    public StateId To { get; }
    public PersistedSnapshotType Type { get; }

    public ReadOnlyMemory<byte> Data => _data;
    public SnapshotBloomFilter? Bloom => _bloom;

    public PersistedSnapshot(int id, StateId from, StateId to, PersistedSnapshotType type, Memory<byte> data, IDisposable? dataOwner = null)
    {
        Id = id;
        From = from;
        To = to;
        Type = type;
        _data = data;
        _dataOwner = dataOwner;
    }

    /// <summary>
    /// Build and attach a bloom filter from the RSST data for fast negative lookups.
    /// </summary>
    public void BuildBloom(double bitsPerKey = 10.0) =>
        _bloom = SnapshotBloomFilter.BuildFromRsst(_data.Span, bitsPerKey);

    public byte[]? TryGetAccount(Address address)
    {
        Span<byte> key = stackalloc byte[1 + Address.Size];
        key[0] = AccountTag;
        address.Bytes.CopyTo(key[1..]);

        if (!BloomCheck(key)) return null;

        Rsst.Rsst rsst = new(_data.Span);
        return rsst.TryGet(key, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }

    public byte[]? TryGetSlot(Address address, in UInt256 index)
    {
        Span<byte> key = stackalloc byte[1 + Address.Size + 32];
        key[0] = StorageTag;
        address.Bytes.CopyTo(key[1..]);
        index.ToBigEndian(key.Slice(1 + Address.Size, 32));

        if (!BloomCheck(key)) return null;

        Rsst.Rsst rsst = new(_data.Span);
        return rsst.TryGet(key, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }

    public bool IsSelfDestructed(Address address)
    {
        Span<byte> key = stackalloc byte[1 + Address.Size];
        key[0] = SelfDestructTag;
        address.Bytes.CopyTo(key[1..]);

        if (!BloomCheck(key)) return false;

        Rsst.Rsst rsst = new(_data.Span);
        return rsst.TryGet(key, out _);
    }

    /// <summary>
    /// Get the self-destruct flag with boolean distinction.
    /// Returns null if no self-destruct entry exists for this address.
    /// Returns true if this is a new account (value = 0x01), false if destructed (value = empty).
    /// </summary>
    public bool? TryGetSelfDestructFlag(Address address)
    {
        Span<byte> key = stackalloc byte[1 + Address.Size];
        key[0] = SelfDestructTag;
        address.Bytes.CopyTo(key[1..]);

        if (!BloomCheck(key)) return null;

        Rsst.Rsst rsst = new(_data.Span);
        if (!rsst.TryGet(key, out ReadOnlySpan<byte> value)) return null;
        return value.Length > 0 && value[0] == 0x01;
    }

    public byte[]? TryLoadStateNodeRlp(in TreePath path)
    {
        Span<byte> key = stackalloc byte[1 + 32 + 1];
        key[0] = StateNodeTag;
        path.Path.Bytes.CopyTo(key[1..]);
        key[33] = (byte)path.Length;

        if (!BloomCheck(key)) return null;

        Rsst.Rsst rsst = new(_data.Span);
        return rsst.TryGet(key, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }

    public byte[]? TryLoadStorageNodeRlp(Hash256 address, in TreePath path)
    {
        Span<byte> key = stackalloc byte[1 + 32 + 32 + 1];
        key[0] = StorageNodeTag;
        address.Bytes.CopyTo(key[1..]);
        path.Path.Bytes.CopyTo(key[33..]);
        key[65] = (byte)path.Length;

        if (!BloomCheck(key)) return null;

        Rsst.Rsst rsst = new(_data.Span);
        return rsst.TryGet(key, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool BloomCheck(ReadOnlySpan<byte> key)
    {
        if (_bloom is null) return true;
        if (_bloom.MightContain(key))
        {
            Metrics.BloomFilterPositives++;
            return true;
        }

        Metrics.BloomFilterNegatives++;
        return false;
    }

    /// <summary>
    /// Resolve a NodeRef by reading the entry value from the referenced snapshot.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] ResolveValue(ReadOnlySpan<byte> snapshotData, int entryOffset)
    {
        Rsst.Rsst.ReadEntry(snapshotData, entryOffset, out _, out ReadOnlySpan<byte> value);
        return value.ToArray();
    }

    /// <summary>
    /// Read the raw entry value at a given offset in this snapshot's data.
    /// </summary>
    public byte[] ReadEntryValue(int entryOffset)
    {
        Rsst.Rsst.ReadEntry(_data.Span, entryOffset, out _, out ReadOnlySpan<byte> value);
        return value.ToArray();
    }

    public bool TryAcquire() => TryAcquireLease();

    protected override void CleanUp() => _dataOwner?.Dispose();
}
