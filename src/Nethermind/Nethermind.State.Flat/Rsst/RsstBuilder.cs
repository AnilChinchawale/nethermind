// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// Builds an RSST (Recursive Static Sorted Table) from key-value entries.
/// Entries MUST be added in sorted key order. No internal sorting is performed.
///
/// Binary layout:
///   [Data Region: entries...][Index Region: B-tree nodes...][IndexSize LEB128][LEB128ByteCount: 1 byte]
///
/// Entry format (value first, lengths forward-readable from ValueLengthOffset):
///   [Value][ValueLength: LEB128][KeyLength: LEB128][RemainingKey]
/// </summary>
public sealed class RsstBuilder
{
    private readonly MemoryStream _valueStream = new();
    private readonly List<(byte[] Key, long ValueOffset, int ValueLength)> _entries = new();

    public int EntryCount => _entries.Count;

    /// <summary>
    /// Add a key-value pair. Key must be in sorted order.
    /// </summary>
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        long offset = _valueStream.Position;
        _valueStream.Write(value);
        _entries.Add((key.ToArray(), offset, value.Length));
    }

    /// <summary>
    /// Add an entry with pre-allocated value space. Returns a span where the caller writes the value directly.
    /// Key must be greater than the previously added key (sorted order required).
    /// </summary>
    public Span<byte> AddEntry(ReadOnlySpan<byte> key, int valueLength)
    {
        long offset = _valueStream.Position;
        _valueStream.SetLength(Math.Max(_valueStream.Length, offset + valueLength));
        _valueStream.Position = offset + valueLength;
        _entries.Add((key.ToArray(), offset, valueLength));
        return _valueStream.GetBuffer().AsSpan((int)offset, valueLength);
    }

    public byte[] Build()
    {
        if (_entries.Count == 0) return BuildEmpty();

        // Step 1: Compute separators for all entries
        byte[][] separators = ComputeAllSeparators();

        // Step 2: Write data region - for each entry:
        //   [Value][ValueLength LEB128][KeyLength LEB128][RemainingKey]
        MemoryStream dataStream = new();
        int[] valueLengthOffsets = new int[_entries.Count];
        byte[] valueBuffer = _valueStream.GetBuffer();

        Span<byte> leb = stackalloc byte[10];
        for (int i = 0; i < _entries.Count; i++)
        {
            (byte[] key, long valueOffset, int valueLength) = _entries[i];
            byte[] separator = separators[i];
            int remainingKeyLength = key.Length - separator.Length;

            // Value
            dataStream.Write(valueBuffer, (int)valueOffset, valueLength);

            // Record ValueLengthOffset (this is what the index stores)
            valueLengthOffsets[i] = (int)dataStream.Position;

            // ValueLength (LEB128)
            int lebLen = Leb128.Write(leb, 0, valueLength);
            dataStream.Write(leb[..lebLen]);

            // KeyLength (LEB128) = remaining key length
            lebLen = Leb128.Write(leb, 0, remainingKeyLength);
            dataStream.Write(leb[..lebLen]);

            // RemainingKey
            if (remainingKeyLength > 0)
                dataStream.Write(key, separator.Length, remainingKeyLength);
        }

        int dataRegionSize = (int)dataStream.Position;

        // Step 3: Build leaf nodes bottom-up
        List<byte[]> nodeBytes = new();
        List<int> currentLevelOffsets = new();
        List<byte[]> currentLevelFirstSeps = new();
        List<byte[]> currentLevelLastSeps = new();

        int entryIdx = 0;
        int indexOffset = 0;
        while (entryIdx < _entries.Count)
        {
            int count = Math.Min(Rsst.MaxLeafEntries, _entries.Count - entryIdx);

            List<(byte[] Separator, int ValueLengthOffset)> leafEntries = new(count);
            for (int i = 0; i < count; i++)
            {
                leafEntries.Add((separators[entryIdx + i], valueLengthOffsets[entryIdx + i]));
            }

            byte[] leaf = BuildLeafNode(leafEntries);
            currentLevelOffsets.Add(indexOffset);
            currentLevelFirstSeps.Add(separators[entryIdx]);
            currentLevelLastSeps.Add(separators[entryIdx + count - 1]);
            indexOffset += leaf.Length;
            nodeBytes.Add(leaf);

            entryIdx += count;
        }

        // Step 4: Build internal nodes bottom-up until we have a single root
        while (currentLevelOffsets.Count > 1)
        {
            List<int> nextLevelOffsets = new();
            List<byte[]> nextLevelFirstSeps = new();
            List<byte[]> nextLevelLastSeps = new();

            int childIdx = 0;
            while (childIdx < currentLevelOffsets.Count)
            {
                int childCount = Math.Min(Rsst.MaxLeafEntries, currentLevelOffsets.Count - childIdx);

                List<(byte[] Separator, int ChildOffset)> internalEntries = new(childCount);
                internalEntries.Add(([], currentLevelOffsets[childIdx]));

                for (int i = 1; i < childCount; i++)
                {
                    byte[] sep = ComputeSeparatorKey(
                        currentLevelLastSeps[childIdx + i - 1],
                        currentLevelFirstSeps[childIdx + i]);
                    internalEntries.Add((sep, currentLevelOffsets[childIdx + i]));
                }

                byte[] node = BuildInternalNode(internalEntries);
                nextLevelOffsets.Add(indexOffset);
                nextLevelFirstSeps.Add(currentLevelFirstSeps[childIdx]);
                nextLevelLastSeps.Add(currentLevelLastSeps[childIdx + childCount - 1]);
                indexOffset += node.Length;
                nodeBytes.Add(node);

                childIdx += childCount;
            }

            currentLevelOffsets = nextLevelOffsets;
            currentLevelFirstSeps = nextLevelFirstSeps;
            currentLevelLastSeps = nextLevelLastSeps;
        }

        int indexSize = indexOffset;

        // Step 5: Write trailer: [IndexSize LEB128][LEB128ByteCount: 1 byte]
        Span<byte> trailerLeb = stackalloc byte[5];
        int trailerLebLen = Leb128.Write(trailerLeb, 0, indexSize);
        int trailerSize = trailerLebLen + 1; // LEB128 bytes + 1 byte for the count

        int totalSize = dataRegionSize + indexSize + trailerSize;
        byte[] result = new byte[totalSize];

        // Copy data region
        dataStream.GetBuffer().AsSpan(0, dataRegionSize).CopyTo(result);

        // Copy index nodes
        int writePos = dataRegionSize;
        foreach (byte[] node in nodeBytes)
        {
            node.CopyTo(result.AsSpan(writePos));
            writePos += node.Length;
        }

        // IndexSize LEB128
        trailerLeb[..trailerLebLen].CopyTo(result.AsSpan(writePos));
        writePos += trailerLebLen;

        // LEB128ByteCount (1 byte)
        result[writePos] = (byte)trailerLebLen;

        return result;
    }

    private static byte[] BuildEmpty()
    {
        // Empty RSST: [IndexSize=0 LEB128][LEB128ByteCount=1]
        return [0x00, 0x01];
    }

    /// <summary>
    /// Build a leaf node, picking the smallest format (Variable/Uniform/UniformCompact).
    /// Leaf entries store (Separator, ValueLengthOffset).
    /// </summary>
    private static byte[] BuildLeafNode(List<(byte[] Separator, int ValueLengthOffset)> entries)
    {
        int count = entries.Count;
        bool uniform = true;
        int sepLen = entries[0].Separator.Length;
        int minOffset = entries[0].ValueLengthOffset;
        int maxOffset = entries[0].ValueLengthOffset;

        for (int i = 0; i < count; i++)
        {
            if (entries[i].Separator.Length != sepLen)
                uniform = false;
            if (entries[i].ValueLengthOffset < minOffset) minOffset = entries[i].ValueLengthOffset;
            if (entries[i].ValueLengthOffset > maxOffset) maxOffset = entries[i].ValueLengthOffset;
        }

        int range = maxOffset - minOffset;

        int variableSize = ComputeVariableLeafSize(entries);
        int bestFormat = 0;
        int bestSize = variableSize;

        if (uniform)
        {
            int baseOffsetLebSize = Leb128.EncodedSize(minOffset);

            // Format 2: UniformCompact uint16
            if (range <= 0xFFFF)
            {
                int compactSize = 6 + baseOffsetLebSize + count * (sepLen + 2);
                if (compactSize < bestSize)
                {
                    bestFormat = 2;
                    bestSize = compactSize;
                }
            }

            // Format 1: Uniform uint24
            if (range <= 0xFFFFFF)
            {
                int uniformSize = 6 + baseOffsetLebSize + count * (sepLen + 3);
                if (uniformSize < bestSize)
                {
                    bestFormat = 1;
                    bestSize = uniformSize;
                }
            }
        }

        return bestFormat switch
        {
            0 => WriteVariableLeafNode(entries, (byte)sepLen),
            1 => WriteUniformLeafNode(entries, (byte)sepLen, minOffset, useUint24: true),
            2 => WriteUniformLeafNode(entries, (byte)sepLen, minOffset, useUint24: false),
            _ => throw new InvalidOperationException()
        };
    }

    private static int ComputeVariableLeafSize(List<(byte[] Separator, int ValueLengthOffset)> entries)
    {
        int size = 6; // header only (no BaseOffset for variable format)
        size += entries.Count * 2; // offset table
        for (int i = 0; i < entries.Count; i++)
        {
            size += Leb128.EncodedSize(entries[i].Separator.Length)
                    + entries[i].Separator.Length
                    + Leb128.EncodedSize(entries[i].ValueLengthOffset);
        }
        return size;
    }

    private static byte[] WriteVariableLeafNode(List<(byte[] Separator, int ValueLengthOffset)> entries, byte sepLen)
    {
        int count = entries.Count;
        int headerSize = 6;
        int offsetTableSize = count * 2;

        int dataSize = 0;
        for (int i = 0; i < count; i++)
        {
            dataSize += Leb128.EncodedSize(entries[i].Separator.Length)
                        + entries[i].Separator.Length
                        + Leb128.EncodedSize(entries[i].ValueLengthOffset);
        }

        int totalSize = headerSize + offsetTableSize + dataSize;
        byte[] node = new byte[totalSize];

        // Write 6-byte header
        BinaryPrimitives.WriteUInt16LittleEndian(node, (ushort)totalSize);
        node[2] = 0x01; // IsLeaf=1, Format=0
        BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(3), (ushort)count);
        node[5] = sepLen;

        // Write offset table and entries: [SepLen LEB128][Separator][ValueLengthOffset LEB128]
        int entryDataStart = headerSize + offsetTableSize;
        int entryPos = entryDataStart;
        for (int i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(headerSize + i * 2), (ushort)(entryPos - entryDataStart));
            entryPos = Leb128.Write(node, entryPos, entries[i].Separator.Length);
            entries[i].Separator.CopyTo(node.AsSpan(entryPos));
            entryPos += entries[i].Separator.Length;
            entryPos = Leb128.Write(node, entryPos, entries[i].ValueLengthOffset);
        }

        return node;
    }

    private static byte[] WriteUniformLeafNode(List<(byte[] Separator, int ValueLengthOffset)> entries, byte sepLen, int baseOffset, bool useUint24)
    {
        int count = entries.Count;
        int baseOffsetLebSize = Leb128.EncodedSize(baseOffset);
        int offsetBytes = useUint24 ? 3 : 2;
        int stride = sepLen + offsetBytes;
        int totalSize = 6 + baseOffsetLebSize + count * stride;
        byte[] node = new byte[totalSize];

        // Write header
        BinaryPrimitives.WriteUInt16LittleEndian(node, (ushort)totalSize);
        byte format = useUint24 ? (byte)1 : (byte)2;
        node[2] = (byte)(0x01 | (format << 1)); // IsLeaf=1, Format
        BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(3), (ushort)count);
        node[5] = sepLen;

        // Write BaseOffset
        int pos = Leb128.Write(node, 6, baseOffset);

        // Write entries: [Separator][RelativeValueLengthOffset]
        for (int i = 0; i < count; i++)
        {
            entries[i].Separator.CopyTo(node.AsSpan(pos));
            pos += sepLen;

            int relative = entries[i].ValueLengthOffset - baseOffset;
            if (useUint24)
            {
                node[pos] = (byte)(relative & 0xFF);
                node[pos + 1] = (byte)((relative >> 8) & 0xFF);
                node[pos + 2] = (byte)((relative >> 16) & 0xFF);
                pos += 3;
            }
            else
            {
                BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(pos), (ushort)relative);
                pos += 2;
            }
        }

        return node;
    }

    /// <summary>
    /// Build an internal node. Entry 0 has empty separator (first child).
    /// </summary>
    private static byte[] BuildInternalNode(List<(byte[] Separator, int ChildOffset)> entries)
    {
        int count = entries.Count;
        bool uniform = true;
        int sepLen = entries.Count > 1 ? entries[1].Separator.Length : 0;
        int minOffset = entries[0].ChildOffset;
        int maxOffset = entries[0].ChildOffset;

        for (int i = 1; i < count; i++)
        {
            if (entries[i].Separator.Length != sepLen) uniform = false;
            if (entries[i].ChildOffset < minOffset) minOffset = entries[i].ChildOffset;
            if (entries[i].ChildOffset > maxOffset) maxOffset = entries[i].ChildOffset;
        }
        if (entries[0].ChildOffset < minOffset) minOffset = entries[0].ChildOffset;
        if (entries[0].ChildOffset > maxOffset) maxOffset = entries[0].ChildOffset;

        int range = maxOffset - minOffset;

        // Variable format size
        int variableSize = 6 + count * 2;
        variableSize += Leb128.EncodedSize(entries[0].ChildOffset);
        for (int i = 1; i < count; i++)
        {
            variableSize += Leb128.EncodedSize(entries[i].Separator.Length) + entries[i].Separator.Length + Leb128.EncodedSize(entries[i].ChildOffset);
        }

        int bestFormat = 0;
        int bestSize = variableSize;
        int bestBaseOffset = 0;

        if (uniform && count > 1)
        {
            int baseOffsetLebSize = Leb128.EncodedSize(minOffset);

            if (range <= 0xFFFF)
            {
                int compactSize = 6 + baseOffsetLebSize + count * (sepLen + 2);
                if (compactSize < bestSize)
                {
                    bestFormat = 2;
                    bestSize = compactSize;
                    bestBaseOffset = minOffset;
                }
            }

            if (range <= 0xFFFFFF)
            {
                int uniformSize = 6 + baseOffsetLebSize + count * (sepLen + 3);
                if (uniformSize < bestSize)
                {
                    bestFormat = 1;
                    bestSize = uniformSize;
                    bestBaseOffset = minOffset;
                }
            }
        }

        return bestFormat switch
        {
            0 => WriteVariableInternalNode(entries, (byte)sepLen),
            1 => WriteUniformInternalNode(entries, (byte)sepLen, bestBaseOffset, useUint24: true),
            2 => WriteUniformInternalNode(entries, (byte)sepLen, bestBaseOffset, useUint24: false),
            _ => throw new InvalidOperationException()
        };
    }

    private static byte[] WriteVariableInternalNode(List<(byte[] Separator, int ChildOffset)> entries, byte sepLen)
    {
        int count = entries.Count;
        int headerSize = 6;
        int offsetTableSize = count * 2;

        int dataSize = Leb128.EncodedSize(entries[0].ChildOffset);
        for (int i = 1; i < count; i++)
        {
            dataSize += Leb128.EncodedSize(entries[i].Separator.Length) + entries[i].Separator.Length + Leb128.EncodedSize(entries[i].ChildOffset);
        }

        int totalSize = headerSize + offsetTableSize + dataSize;
        byte[] node = new byte[totalSize];

        BinaryPrimitives.WriteUInt16LittleEndian(node, (ushort)totalSize);
        node[2] = 0x00; // IsLeaf=0, Format=0
        BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(3), (ushort)count);
        node[5] = sepLen;

        int entryDataStart = headerSize + offsetTableSize;
        int entryPos = entryDataStart;

        BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(headerSize), (ushort)(entryPos - entryDataStart));
        entryPos = Leb128.Write(node, entryPos, entries[0].ChildOffset);

        for (int i = 1; i < count; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(headerSize + i * 2), (ushort)(entryPos - entryDataStart));
            entryPos = Leb128.Write(node, entryPos, entries[i].Separator.Length);
            entries[i].Separator.CopyTo(node.AsSpan(entryPos));
            entryPos += entries[i].Separator.Length;
            entryPos = Leb128.Write(node, entryPos, entries[i].ChildOffset);
        }

        return node;
    }

    private static byte[] WriteUniformInternalNode(List<(byte[] Separator, int ChildOffset)> entries, byte sepLen, int baseOffset, bool useUint24)
    {
        int count = entries.Count;
        int baseOffsetLebSize = Leb128.EncodedSize(baseOffset);
        int offsetBytes = useUint24 ? 3 : 2;
        int stride = sepLen + offsetBytes;
        int totalSize = 6 + baseOffsetLebSize + count * stride;
        byte[] node = new byte[totalSize];

        BinaryPrimitives.WriteUInt16LittleEndian(node, (ushort)totalSize);
        byte format = useUint24 ? (byte)1 : (byte)2;
        node[2] = (byte)(format << 1); // IsLeaf=0, Format
        BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(3), (ushort)count);
        node[5] = sepLen;

        int pos = Leb128.Write(node, 6, baseOffset);

        for (int i = 0; i < count; i++)
        {
            if (i == 0)
                pos += sepLen; // zero-filled padding
            else
            {
                entries[i].Separator.CopyTo(node.AsSpan(pos));
                pos += sepLen;
            }

            int relative = entries[i].ChildOffset - baseOffset;
            if (useUint24)
            {
                node[pos] = (byte)(relative & 0xFF);
                node[pos + 1] = (byte)((relative >> 8) & 0xFF);
                node[pos + 2] = (byte)((relative >> 16) & 0xFF);
                pos += 3;
            }
            else
            {
                BinaryPrimitives.WriteUInt16LittleEndian(node.AsSpan(pos), (ushort)relative);
                pos += 2;
            }
        }

        return node;
    }

    private byte[][] ComputeAllSeparators()
    {
        byte[][] separators = new byte[_entries.Count][];
        for (int i = 0; i < _entries.Count; i++)
        {
            byte[]? prevKey = i > 0 ? _entries[i - 1].Key : null;
            byte[] currKey = _entries[i].Key;
            byte[]? nextKey = i < _entries.Count - 1 ? _entries[i + 1].Key : null;
            separators[i] = ComputeSeparator(prevKey, currKey, nextKey);
        }
        return separators;
    }

    private static byte[] ComputeSeparator(byte[]? prevKey, byte[] currKey, byte[]? nextKey)
    {
        int minVsPrev = 0;
        if (prevKey is not null)
        {
            int common = CommonPrefixLength(prevKey, currKey);
            minVsPrev = common + 1;
        }

        int minVsNext = 0;
        if (nextKey is not null)
        {
            int common = CommonPrefixLength(currKey, nextKey);
            minVsNext = common + 1;
        }

        int len = Math.Max(minVsPrev, minVsNext);
        len = Math.Min(len, currKey.Length);
        if (len == 0) len = Math.Min(1, currKey.Length);

        return currKey[..len];
    }

    private static int CommonPrefixLength(byte[] a, byte[] b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            if (a[i] != b[i]) return i;
        }
        return minLen;
    }

    internal static byte[] ComputeSeparatorKey(byte[] left, byte[] right)
    {
        int minLen = Math.Min(left.Length, right.Length);

        for (int i = 0; i < minLen; i++)
        {
            if (left[i] != right[i])
                return right[..(i + 1)];
        }

        if (right.Length > minLen)
            return right[..(minLen + 1)];

        return right.ToArray();
    }

    public static byte[] StreamingMerge(ReadOnlySpan<byte> olderData, ReadOnlySpan<byte> newerData)
    {
        Rsst older = new(olderData);
        Rsst newer = new(newerData);

        if (older.EntryCount == 0 && newer.EntryCount == 0) return BuildEmpty();

        RsstBuilder builder = new();

        using Rsst.Enumerator olderEnum = older.GetEnumerator();
        using Rsst.Enumerator newerEnum = newer.GetEnumerator();

        bool hasOlder = olderEnum.MoveNext();
        bool hasNewer = newerEnum.MoveNext();

        while (hasOlder && hasNewer)
        {
            ReadOnlySpan<byte> olderKey = olderEnum.Current.Key;
            ReadOnlySpan<byte> newerKey = newerEnum.Current.Key;

            int cmp = olderKey.SequenceCompareTo(newerKey);
            if (cmp < 0)
            {
                builder.Add(olderKey, olderEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
            }
            else if (cmp > 0)
            {
                builder.Add(newerKey, newerEnum.Current.Value);
                hasNewer = newerEnum.MoveNext();
            }
            else
            {
                builder.Add(newerKey, newerEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
                hasNewer = newerEnum.MoveNext();
            }
        }

        while (hasOlder)
        {
            builder.Add(olderEnum.Current.Key, olderEnum.Current.Value);
            hasOlder = olderEnum.MoveNext();
        }

        while (hasNewer)
        {
            builder.Add(newerEnum.Current.Key, newerEnum.Current.Value);
            hasNewer = newerEnum.MoveNext();
        }

        return builder.Build();
    }
}
