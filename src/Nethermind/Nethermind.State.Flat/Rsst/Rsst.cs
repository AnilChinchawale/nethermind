// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// Recursive Static Sorted Table. A compact binary format for persisted snapshots.
/// Layout: [Data Region][Index Region (B-tree)][Trailer: IndexSize LEB128, LEB128ByteCount 1 byte]
///
/// Entry format (value first, lengths forward-readable from ValueLengthOffset):
///   [Value][ValueLength: LEB128][KeyLength: LEB128][RemainingKey]
/// </summary>
public readonly ref struct Rsst
{
    public const int MaxLeafEntries = 64;

    private readonly ReadOnlySpan<byte> _data;
    private readonly int _indexStart;
    private readonly int _indexEnd;
    private readonly int _entryCount;

    public int EntryCount => _entryCount;
    public ReadOnlySpan<byte> Data => _data;

    public Rsst(ReadOnlySpan<byte> data)
    {
        _data = data;
        if (data.Length < 2)
        {
            _indexStart = 0;
            _indexEnd = 0;
            _entryCount = 0;
            return;
        }

        // Read trailer: last byte = N (LEB128 byte count), read IndexSize forward from end-1-N
        int n = data[^1];
        int trailerSize = n + 1;
        int lebStart = data.Length - 1 - n;
        int pos = lebStart;
        int indexSize = Leb128.Read(data, ref pos);

        _indexEnd = data.Length - trailerSize;
        _indexStart = _indexEnd - indexSize;

        if (indexSize == 0)
        {
            _entryCount = 0;
        }
        else
        {
            _entryCount = CountEntriesFromTree(data[_indexStart.._indexEnd]);
        }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_entryCount == 0 || _data.Length < 2)
        {
            value = default;
            return false;
        }

        ReadOnlySpan<byte> indexRegion = _data[_indexStart.._indexEnd];
        return SearchTree(indexRegion, key, out value);
    }

    private bool SearchTree(ReadOnlySpan<byte> indexRegion, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        NodeHeader root = ReadNodeHeaderFromEnd(indexRegion);
        return SearchNode(indexRegion, indexRegion.Length - root.NodeSize, key, out value);
    }

    private bool SearchNode(ReadOnlySpan<byte> indexRegion, int nodeOffset, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        ReadOnlySpan<byte> node = indexRegion[nodeOffset..];
        NodeHeader header = NodeHeader.Read(node);

        if (header.IsLeaf)
            return SearchLeaf(node, header, key, out value);

        return SearchInternal(indexRegion, node, header, key, out value);
    }

    private bool SearchLeaf(ReadOnlySpan<byte> node, NodeHeader header, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        int lo = 0, hi = header.EntryCount - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) / 2;
            ReadOnlySpan<byte> separator = GetSeparator(node, mid, header);

            int cmpLen = Math.Min(key.Length, separator.Length);
            int cmp = key[..cmpLen].SequenceCompareTo(separator[..cmpLen]);

            if (cmp == 0 && key.Length < separator.Length)
                cmp = -1;
            else if (cmp == 0)
            {
                if (VerifyAndReadEntry(mid, node, header, key, out value))
                    return true;
                // Separator is a prefix of key but full key didn't match — search right
                lo = mid + 1;
                continue;
            }

            if (cmp < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }

        value = default;
        return false;
    }

    private bool VerifyAndReadEntry(int entryIndex, ReadOnlySpan<byte> node, NodeHeader header, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        ReadOnlySpan<byte> separator = GetSeparator(node, entryIndex, header);
        int vlOffset = GetValueLengthOffset(node, entryIndex, header);

        // Read entry from data region starting at vlOffset
        ReadEntry(_data, vlOffset, out ReadOnlySpan<byte> remainingKey, out value);

        if (key.Length != separator.Length + remainingKey.Length)
        {
            value = default;
            return false;
        }

        if (!key[..separator.Length].SequenceEqual(separator))
        {
            value = default;
            return false;
        }

        if (remainingKey.Length > 0 && !key[separator.Length..].SequenceEqual(remainingKey))
        {
            value = default;
            return false;
        }

        return true;
    }

    private bool SearchInternal(ReadOnlySpan<byte> indexRegion, ReadOnlySpan<byte> node, NodeHeader header, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        int childIdx = 0;
        for (int i = 1; i < header.EntryCount; i++)
        {
            ReadOnlySpan<byte> separator = GetSeparator(node, i, header);
            if (key.SequenceCompareTo(separator) >= 0)
                childIdx = i;
            else
                break;
        }

        int childOffset = GetChildOffset(node, childIdx, header);
        return SearchNode(indexRegion, childOffset, key, out value);
    }

    private static ReadOnlySpan<byte> GetSeparator(ReadOnlySpan<byte> node, int index, NodeHeader header) =>
        header.Format switch
        {
            0 => GetVariableSeparator(node, index, header),
            1 => GetUniformSeparator(node, index, header, strideExtra: 3),
            2 => GetUniformSeparator(node, index, header, strideExtra: 2),
            _ => throw new InvalidDataException($"Unknown node format: {header.Format}")
        };

    private static ReadOnlySpan<byte> GetVariableSeparator(ReadOnlySpan<byte> node, int index, NodeHeader header)
    {
        int offsetTableStart = header.HeaderSize;
        int entryDataStart = offsetTableStart + header.EntryCount * 2;
        int relativeOffset = BinaryPrimitives.ReadUInt16LittleEndian(node[(offsetTableStart + index * 2)..]);
        int entryOffset = entryDataStart + relativeOffset;

        if (!header.IsLeaf && index == 0)
            return ReadOnlySpan<byte>.Empty;

        int pos = entryOffset;
        int sepLen = Leb128.Read(node, ref pos);
        return node.Slice(pos, sepLen);
    }

    private static ReadOnlySpan<byte> GetUniformSeparator(ReadOnlySpan<byte> node, int index, NodeHeader header, int strideExtra)
    {
        int stride = header.SepLen + strideExtra;
        int entryStart = header.HeaderSize + index * stride;
        return node.Slice(entryStart, header.SepLen);
    }

    private static int GetValueLengthOffset(ReadOnlySpan<byte> node, int index, NodeHeader header) =>
        header.Format switch
        {
            0 => GetVariableValueLengthOffset(node, index, header),
            1 => GetUniformOffset(node, index, header, strideExtra: 3, useUint24: true),
            2 => GetUniformOffset(node, index, header, strideExtra: 2, useUint24: false),
            _ => throw new InvalidDataException($"Unknown node format: {header.Format}")
        };

    private static int GetVariableValueLengthOffset(ReadOnlySpan<byte> node, int index, NodeHeader header)
    {
        int offsetTableStart = header.HeaderSize;
        int entryDataStart = offsetTableStart + header.EntryCount * 2;
        int relativeOffset = BinaryPrimitives.ReadUInt16LittleEndian(node[(offsetTableStart + index * 2)..]);
        int entryOffset = entryDataStart + relativeOffset;

        int pos = entryOffset;
        int sepLen = Leb128.Read(node, ref pos);
        pos += sepLen;
        return Leb128.Read(node, ref pos); // ValueLengthOffset (absolute)
    }

    private static int GetChildOffset(ReadOnlySpan<byte> node, int index, NodeHeader header) =>
        header.Format switch
        {
            0 => GetVariableChildOffset(node, index, header),
            1 => GetUniformOffset(node, index, header, strideExtra: 3, useUint24: true),
            2 => GetUniformOffset(node, index, header, strideExtra: 2, useUint24: false),
            _ => throw new InvalidDataException($"Unknown node format: {header.Format}")
        };

    private static int GetVariableChildOffset(ReadOnlySpan<byte> node, int index, NodeHeader header)
    {
        int offsetTableStart = header.HeaderSize;
        int entryDataStart = offsetTableStart + header.EntryCount * 2;
        int relativeOffset = BinaryPrimitives.ReadUInt16LittleEndian(node[(offsetTableStart + index * 2)..]);
        int entryOffset = entryDataStart + relativeOffset;

        int pos = entryOffset;
        if (index == 0)
            return Leb128.Read(node, ref pos);

        int sepLen = Leb128.Read(node, ref pos);
        pos += sepLen;
        return Leb128.Read(node, ref pos);
    }

    /// <summary>
    /// Read the offset value (ValueLengthOffset or ChildOffset) from a uniform-format node entry.
    /// </summary>
    private static int GetUniformOffset(ReadOnlySpan<byte> node, int index, NodeHeader header, int strideExtra, bool useUint24)
    {
        int stride = header.SepLen + strideExtra;
        int offsetPos = header.HeaderSize + index * stride + header.SepLen;

        int relative;
        if (useUint24)
        {
            relative = node[offsetPos] | (node[offsetPos + 1] << 8) | (node[offsetPos + 2] << 16);
        }
        else
        {
            relative = BinaryPrimitives.ReadUInt16LittleEndian(node[offsetPos..]);
        }

        return header.BaseOffset + relative;
    }

    private static int CountEntriesFromTree(ReadOnlySpan<byte> indexRegion)
    {
        if (indexRegion.Length == 0) return 0;

        NodeHeader root = ReadNodeHeaderFromEnd(indexRegion);
        return CountEntriesInNode(indexRegion, indexRegion.Length - root.NodeSize, root);
    }

    private static int CountEntriesInNode(ReadOnlySpan<byte> indexRegion, int nodeOffset, NodeHeader header)
    {
        if (header.IsLeaf) return header.EntryCount;

        int total = 0;
        for (int i = 0; i < header.EntryCount; i++)
        {
            ReadOnlySpan<byte> node = indexRegion[nodeOffset..];
            int childOffset = GetChildOffset(node, i, header);
            NodeHeader childHeader = NodeHeader.Read(indexRegion[childOffset..]);
            total += CountEntriesInNode(indexRegion, childOffset, childHeader);
        }
        return total;
    }

    private static NodeHeader ReadNodeHeaderFromEnd(ReadOnlySpan<byte> indexRegion)
    {
        for (int tryStart = indexRegion.Length - 6; tryStart >= 0; tryStart--)
        {
            ushort possibleSize = BinaryPrimitives.ReadUInt16LittleEndian(indexRegion[tryStart..]);
            if (tryStart + possibleSize == indexRegion.Length && possibleSize >= 6)
            {
                NodeHeader header = NodeHeader.Read(indexRegion[tryStart..]);
                if (!ValidateNodeSize(header, possibleSize)) continue;
                return header;
            }
        }

        return NodeHeader.Read(indexRegion);
    }

    /// <summary>
    /// Validate that the declared nodeSize matches the expected size computed from header fields.
    /// For uniform formats, the size is deterministic. For variable format, apply minimum size check.
    /// </summary>
    private static bool ValidateNodeSize(NodeHeader header, int declaredSize)
    {
        if (header.Format != 0)
        {
            int offsetBytes = header.Format == 1 ? 3 : 2;
            int expectedSize = header.HeaderSize + header.EntryCount * (header.SepLen + offsetBytes);
            return expectedSize == declaredSize;
        }

        // Variable format: minimum is header + offset table
        int minSize = header.HeaderSize + header.EntryCount * 2;
        return minSize <= declaredSize;
    }

    /// <summary>
    /// Read a key-value entry given the ValueLengthOffset in the data region.
    /// Entry format: [Value: V bytes][ValueLength: LEB128][KeyLength: LEB128][RemainingKey: K bytes]
    /// ValueLengthOffset points to the start of the ValueLength LEB128.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadEntry(ReadOnlySpan<byte> data, int valueLengthOffset, out ReadOnlySpan<byte> remainingKey, out ReadOnlySpan<byte> value)
    {
        int pos = valueLengthOffset;
        int valueLength = Leb128.Read(data, ref pos);
        int keyLength = Leb128.Read(data, ref pos);
        remainingKey = data.Slice(pos, keyLength);
        value = data.Slice(valueLengthOffset - valueLength, valueLength);
    }

    public Enumerator GetEnumerator() => new(_data, _indexStart, _indexEnd, _entryCount);

    public ref struct Enumerator : IDisposable
    {
        private readonly ReadOnlySpan<byte> _data;
        private readonly (byte[] Separator, int ValueLengthOffset)[] _leafEntries;
        private int _currentIndex;

        public Enumerator(ReadOnlySpan<byte> data, int indexStart, int indexEnd, int entryCount)
        {
            _data = data;
            _currentIndex = -1;

            if (entryCount <= 0 || indexStart == indexEnd)
            {
                _leafEntries = [];
                return;
            }

            ReadOnlySpan<byte> indexRegion = data[indexStart..indexEnd];
            List<(byte[] Separator, int ValueLengthOffset)> entries = new(entryCount > 0 ? entryCount : 16);
            NodeHeader root = ReadNodeHeaderFromEnd(indexRegion);
            CollectLeafEntries(indexRegion, indexRegion.Length - root.NodeSize, entries);
            _leafEntries = entries.ToArray();
        }

        private static void CollectLeafEntries(ReadOnlySpan<byte> indexRegion, int nodeOffset, List<(byte[] Separator, int ValueLengthOffset)> entries)
        {
            ReadOnlySpan<byte> node = indexRegion[nodeOffset..];
            NodeHeader header = NodeHeader.Read(node);

            if (header.IsLeaf)
            {
                for (int i = 0; i < header.EntryCount; i++)
                {
                    byte[] sep = GetSeparator(node, i, header).ToArray();
                    int vlOffset = GetValueLengthOffset(node, i, header);
                    entries.Add((sep, vlOffset));
                }
            }
            else
            {
                for (int i = 0; i < header.EntryCount; i++)
                {
                    int childOffset = GetChildOffset(node, i, header);
                    CollectLeafEntries(indexRegion, childOffset, entries);
                }
            }
        }

        public bool MoveNext()
        {
            _currentIndex++;
            return _currentIndex < _leafEntries.Length;
        }

        public readonly KeyValueEntry Current
        {
            get
            {
                (byte[] separator, int vlOffset) = _leafEntries[_currentIndex];

                ReadEntry(_data, vlOffset, out ReadOnlySpan<byte> remainingKey, out ReadOnlySpan<byte> value);

                byte[] fullKey = new byte[separator.Length + remainingKey.Length];
                separator.CopyTo(fullKey.AsSpan());
                remainingKey.CopyTo(fullKey.AsSpan(separator.Length));

                return new KeyValueEntry(fullKey, value);
            }
        }

        public void Dispose() { }
    }

    public readonly ref struct KeyValueEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        public ReadOnlySpan<byte> Key { get; } = key;
        public ReadOnlySpan<byte> Value { get; } = value;

        public void Deconstruct(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            key = Key;
            value = Value;
        }
    }

    internal readonly struct NodeHeader
    {
        public ushort NodeSize { get; init; }
        public byte Flags { get; init; }
        public ushort EntryCount { get; init; }
        public byte SepLen { get; init; }
        public int Format => (Flags >> 1) & 0x03;
        public bool IsLeaf => (Flags & 0x01) != 0;
        public int BaseOffset { get; init; }
        public int HeaderSize { get; init; }

        public static NodeHeader Read(ReadOnlySpan<byte> node)
        {
            ushort nodeSize = BinaryPrimitives.ReadUInt16LittleEndian(node);
            byte flags = node[2];
            ushort entryCount = BinaryPrimitives.ReadUInt16LittleEndian(node[3..]);
            byte sepLen = node[5];
            int format = (flags >> 1) & 0x03;

            int baseOffset = 0;
            int headerSize = 6;

            // BaseOffset only for uniform formats (format != 0)
            if (format != 0)
            {
                int pos = 6;
                baseOffset = Leb128.Read(node, ref pos);
                headerSize = pos;
            }

            return new NodeHeader
            {
                NodeSize = nodeSize,
                Flags = flags,
                EntryCount = entryCount,
                SepLen = sepLen,
                BaseOffset = baseOffset,
                HeaderSize = headerSize
            };
        }
    }
}
