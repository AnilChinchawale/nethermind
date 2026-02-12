// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Runtime.CompilerServices;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Utils;
using Nethermind.Int256;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;

namespace Nethermind.State.Flat.PersistedSnapshots;

/// <summary>
/// A persisted snapshot backed by columnar RSST data on disk (or in memory).
/// The outer RSST has 5 column entries (tags 0x00-0x04), each containing an inner RSST.
/// Inner RSST keys are the entity keys without the tag prefix:
///   Column 0x00: Address (20 bytes) → Account RLP
///   Column 0x01: Address (20 bytes) + UInt256 slot (32 bytes big-endian) → Slot value bytes
///   Column 0x02: Address (20 bytes) → Self-destruct marker
///   Column 0x03: TreePath.Path (32 bytes) + PathLength (1 byte) → State trie node RLP
///   Column 0x04: AddressHash (32 bytes) + TreePath.Path (32 bytes) + PathLength (1 byte) → Storage trie node RLP
/// </summary>
public sealed class PersistedSnapshot : RefCountingDisposable
{
    // Tag prefixes for RSST key encoding
    internal static readonly byte[] AccountTag = [0x00];
    internal static readonly byte[] StorageTag = [0x01];
    internal static readonly byte[] SelfDestructTag = [0x02];
    internal static readonly byte[] StateNodeTag = [0x03];
    internal static readonly byte[] StorageNodeTag = [0x04];

    private readonly Memory<byte> _data;
    private readonly IDisposable? _dataOwner;

    public int Id { get; }
    public StateId From { get; }
    public StateId To { get; }
    public PersistedSnapshotType Type { get; }

    public ReadOnlyMemory<byte> Data => _data;

    public PersistedSnapshot(int id, StateId from, StateId to, PersistedSnapshotType type, Memory<byte> data, IDisposable? dataOwner = null)
    {
        Id = id;
        From = from;
        To = to;
        Type = type;
        _data = data;
        _dataOwner = dataOwner;
    }


    public byte[]? TryGetAccount(Address address) =>
        TryGetFromColumn(AccountTag, address.Bytes);

    public byte[]? TryGetSlot(Address address, in UInt256 index)
    {
        Span<byte> slotKey = stackalloc byte[32];
        index.ToBigEndian(slotKey);
        return TryGetNestedValue(StorageTag, address.Bytes, slotKey);
    }

    public bool IsSelfDestructed(Address address) =>
        TryGetFromColumn(SelfDestructTag, address.Bytes) is not null;

    /// <summary>
    /// Get the self-destruct flag with boolean distinction.
    /// Returns null if no self-destruct entry exists for this address.
    /// Returns true if this is a new account (value = 0x01), false if destructed (value = empty).
    /// </summary>
    public bool? TryGetSelfDestructFlag(Address address)
    {
        byte[]? result = TryGetFromColumn(SelfDestructTag, address.Bytes);
        if (result is null) return null;
        return result.Length > 0 && result[0] == 0x01;
    }

    public byte[]? TryLoadStateNodeRlp(in TreePath path)
    {
        Span<byte> key = stackalloc byte[32 + 1];
        path.Path.Bytes.CopyTo(key);
        key[32] = (byte)path.Length;
        return TryGetFromColumn(StateNodeTag, key);
    }

    public byte[]? TryLoadStorageNodeRlp(Hash256 address, in TreePath path)
    {
        Span<byte> pathKey = stackalloc byte[33];
        path.Path.Bytes.CopyTo(pathKey);
        pathKey[32] = (byte)path.Length;
        return TryGetNestedValue(StorageNodeTag, address.Bytes, pathKey);
    }

    private byte[]? TryGetFromColumn(ReadOnlySpan<byte> tag, ReadOnlySpan<byte> entityKey)
    {
        Rsst.Rsst outer = new(_data.Span);
        if (!outer.TryGet(tag, out ReadOnlySpan<byte> columnData))
            return null;

        Rsst.Rsst inner = new(columnData);
        return inner.TryGet(entityKey, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }

    private byte[]? TryGetNestedValue(ReadOnlySpan<byte> tag, ReadOnlySpan<byte> addressKey, ReadOnlySpan<byte> entityKey)
    {
        Rsst.Rsst outer = new(_data.Span);
        if (!outer.TryGet(tag, out ReadOnlySpan<byte> columnData)) return null;

        Rsst.Rsst addressLevel = new(columnData);
        if (!addressLevel.TryGet(addressKey, out ReadOnlySpan<byte> innerData)) return null;

        Rsst.Rsst inner = new(innerData);
        return inner.TryGet(entityKey, out ReadOnlySpan<byte> value) ? value.ToArray() : null;
    }


    /// <summary>
    /// Resolve a NodeRef by reading the entry value from the referenced snapshot.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[] ResolveValue(ReadOnlySpan<byte> snapshotData, int valueLengthOffset)
    {
        Rsst.Rsst.ReadEntry(snapshotData, valueLengthOffset, out _, out ReadOnlySpan<byte> value);
        return value.ToArray();
    }

    /// <summary>
    /// Read the raw entry value at a given ValueLengthOffset in this snapshot's data.
    /// </summary>
    public byte[] ReadEntryValue(int valueLengthOffset)
    {
        Rsst.Rsst.ReadEntry(_data.Span, valueLengthOffset, out _, out ReadOnlySpan<byte> value);
        return value.ToArray();
    }

    public bool TryAcquire() => TryAcquireLease();

    protected override void CleanUp() => _dataOwner?.Dispose();
}
