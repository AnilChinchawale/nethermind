// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
public ref struct RsstBuilder
{
    private Span<byte> _output;
    private int _position;

    private readonly int _extraSeparatorLength;

    // Working buffers allocated from ArrayPool
    private readonly byte[] _separatorBuffer;
    private int _separatorBufferPos;
    private readonly RsstEntry[] _entriesBuffer;
    private int _entryCount;

    // Previous key buffer for streaming separator computation
    private byte[] _prevKeyBuffer;
    private int _prevKeyLength;

    public readonly struct RsstEntry(int sepOffset, int sepLen, int valueLengthOffset)
    {
        public readonly int SepOffset = sepOffset;
        public readonly int SepLen = sepLen;
        public readonly int ValueLengthOffset = valueLengthOffset;
    }

    /// <summary>
    /// Create builder writing into output span.
    /// Allocates working buffers from ArrayPool - call Dispose() to return them.
    /// </summary>
    public RsstBuilder(Span<byte> output, int extraSeparatorLength = 0)
    {
        _output = output;
        _position = 0;
        _extraSeparatorLength = extraSeparatorLength;
        _separatorBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(65536);
        _separatorBufferPos = 0;
        _entriesBuffer = System.Buffers.ArrayPool<RsstEntry>.Shared.Rent(10000);
        _entryCount = 0;
        _prevKeyBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(256);
        _prevKeyLength = 0;
    }

    /// <summary>
    /// Return pooled buffers to ArrayPool.
    /// </summary>
    public void Dispose()
    {
        System.Buffers.ArrayPool<byte>.Shared.Return(_separatorBuffer);
        System.Buffers.ArrayPool<RsstEntry>.Shared.Return(_entriesBuffer);
        System.Buffers.ArrayPool<byte>.Shared.Return(_prevKeyBuffer);
    }

    /// <summary>
    /// Begin writing a value. Returns a span where the caller should write the value.
    /// After writing, call FinishValueWrite with the actual length and key.
    /// </summary>
    public Span<byte> BeginValueWrite(int maxSize)
    {
        int available = _output.Length - _position;
        return _output.Slice(_position, Math.Min(maxSize, available));
    }

    /// <summary>
    /// Finish value write after writing actualLen bytes to the span from BeginValueWrite.
    /// Computes separator and writes metadata.
    /// Key must be greater than previous key (sorted order).
    /// </summary>
    public void FinishValueWrite(int actualLen, ReadOnlySpan<byte> key)
    {
        int valueLengthOffset = _position + actualLen;

        // Compute separator eagerly (only need prevKey, currKey - no nextKey in streaming)
        int sepLen = ComputeSeparatorLength(
            _prevKeyBuffer.AsSpan(0, _prevKeyLength),
            key,
            nextKey: default,
            _extraSeparatorLength);

        // Store separator in contiguous buffer
        if (_separatorBufferPos + sepLen > _separatorBuffer.Length)
            throw new InvalidOperationException("Separator buffer overflow");

        key[..sepLen].CopyTo(_separatorBuffer.AsSpan(_separatorBufferPos));
        int sepOffset = _separatorBufferPos;
        _separatorBufferPos += sepLen;

        // Compute remaining key
        ReadOnlySpan<byte> remainingKey = key[sepLen..];
        int remainingKeyLength = remainingKey.Length;

        // Write metadata at valueLengthOffset: [ValueLength LEB128][RemainingKeyLength LEB128][RemainingKey]
        Span<byte> leb = stackalloc byte[10];
        int lebLen = Leb128.Write(leb, 0, actualLen);
        leb[..lebLen].CopyTo(_output[valueLengthOffset..]);
        int pos = valueLengthOffset + lebLen;

        lebLen = Leb128.Write(leb, 0, remainingKeyLength);
        leb[..lebLen].CopyTo(_output[pos..]);
        pos += lebLen;

        if (remainingKeyLength > 0)
        {
            remainingKey.CopyTo(_output[pos..]);
            pos += remainingKeyLength;
        }

        // Record entry
        if (_entryCount >= _entriesBuffer.Length)
            throw new InvalidOperationException("Entries buffer overflow");

        _entriesBuffer[_entryCount++] = new RsstEntry(sepOffset, sepLen, valueLengthOffset);

        // Update position past metadata
        _position = pos;

        // Update prevKey for next entry — grow buffer if needed
        if (key.Length > _prevKeyBuffer.Length)
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(_prevKeyBuffer);
            _prevKeyBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(key.Length);
        }

        key.CopyTo(_prevKeyBuffer.AsSpan());
        _prevKeyLength = key.Length;
    }

    /// <summary>
    /// Convenience: add key-value pair in one call.
    /// </summary>
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Span<byte> valueSpan = BeginValueWrite(value.Length);
        value.CopyTo(valueSpan);
        FinishValueWrite(value.Length, key);
    }

    /// <summary>
    /// Build index and trailer, returning final end position.
    /// </summary>
    public int Build(int maxLeafEntries = Rsst.MaxLeafEntries)
    {
        if (_entryCount == 0)
        {
            // Empty RSST: [IndexSize=0 LEB128][LEB128ByteCount=1]
            _output[_position] = 0x00;
            _output[_position + 1] = 0x01;
            return _position + 2;
        }

        int dataRegionEnd = _position;
        int indexStart = dataRegionEnd;

        // Build leaf nodes bottom-up using stackalloc for current level
        int maxLevelSize = (_entryCount + maxLeafEntries - 1) / maxLeafEntries;
        Span<NodeInfo> currentLevel = stackalloc NodeInfo[maxLevelSize];
        Span<NodeInfo> nextLevel = stackalloc NodeInfo[maxLevelSize];  // Allocate outside loop
        int currentLevelCount = 0;

        int entryIdx = 0;
        while (entryIdx < _entryCount)
        {
            int count = Math.Min(maxLeafEntries, _entryCount - entryIdx);
            RsstEntry first = _entriesBuffer[entryIdx];
            RsstEntry last = _entriesBuffer[entryIdx + count - 1];

            ReadOnlySpan<RsstEntry> leafEntries = _entriesBuffer.AsSpan(entryIdx, count);
            int nodeLen = WriteLeafNode(_output[_position..], leafEntries);

            currentLevel[currentLevelCount++] = new NodeInfo(
                _position - indexStart,
                first.SepOffset,
                first.SepLen,
                last.SepOffset,
                last.SepLen
            );

            _position += nodeLen;
            entryIdx += count;
        }

        // Build internal nodes bottom-up until single root
        while (currentLevelCount > 1)
        {
            int nextLevelCount = 0;

            int childIdx = 0;
            while (childIdx < currentLevelCount)
            {
                int childCount = Math.Min(maxLeafEntries, currentLevelCount - childIdx);
                NodeInfo first = currentLevel[childIdx];
                NodeInfo last = currentLevel[childIdx + childCount - 1];

                int nodeLen = WriteInternalNode(_output[_position..], currentLevel.Slice(childIdx, childCount), indexStart);

                nextLevel[nextLevelCount++] = new NodeInfo(
                    _position - indexStart,
                    first.FirstSepOffset,
                    first.FirstSepLen,
                    last.LastSepOffset,
                    last.LastSepLen
                );

                _position += nodeLen;
                childIdx += childCount;
            }

            // Swap levels for next iteration
            nextLevel[..nextLevelCount].CopyTo(currentLevel);
            currentLevelCount = nextLevelCount;
        }

        int indexSize = _position - indexStart;

        // Write trailer: [IndexSize LEB128][LEB128ByteCount: 1 byte]
        Span<byte> trailerLeb = stackalloc byte[5];
        int trailerLebLen = Leb128.Write(trailerLeb, 0, indexSize);
        trailerLeb[..trailerLebLen].CopyTo(_output[_position..]);
        _position += trailerLebLen;
        _output[_position] = (byte)trailerLebLen;
        _position++;

        return _position;
    }

    private readonly struct NodeInfo(int offset, int firstSepOffset, int firstSepLen, int lastSepOffset, int lastSepLen)
    {
        public readonly int Offset = offset;
        public readonly int FirstSepOffset = firstSepOffset;
        public readonly int FirstSepLen = firstSepLen;
        public readonly int LastSepOffset = lastSepOffset;
        public readonly int LastSepLen = lastSepLen;
    }

    private readonly int WriteLeafNode(Span<byte> output, ReadOnlySpan<RsstEntry> entries)
    {
        int count = entries.Length;
        bool uniform = true;
        int sepLen = entries[0].SepLen;
        int minOffset = entries[0].ValueLengthOffset;
        int maxOffset = entries[0].ValueLengthOffset;

        for (int i = 0; i < count; i++)
        {
            if (entries[i].SepLen != sepLen)
                uniform = false;
            if (entries[i].ValueLengthOffset < minOffset) minOffset = entries[i].ValueLengthOffset;
            if (entries[i].ValueLengthOffset > maxOffset) maxOffset = entries[i].ValueLengthOffset;
        }

        int range = maxOffset - minOffset;

        // Choose best format
        int bestFormat = 0;
        int bestSize = ComputeVariableLeafSize(entries);

        if (uniform)
        {
            int baseOffsetLebSize = Leb128.EncodedSize(minOffset);

            if (range <= 0xFFFF)
            {
                int compactSize = 6 + baseOffsetLebSize + count * (sepLen + 2);
                if (compactSize < bestSize)
                {
                    bestFormat = 2;
                    bestSize = compactSize;
                }
            }

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
            0 => WriteVariableLeafNodeInto(output, entries, (byte)sepLen),
            1 => WriteUniformLeafNodeInto(output, entries, (byte)sepLen, minOffset, useUint24: true),
            2 => WriteUniformLeafNodeInto(output, entries, (byte)sepLen, minOffset, useUint24: false),
            _ => throw new InvalidOperationException()
        };
    }

    private readonly int ComputeVariableLeafSize(ReadOnlySpan<RsstEntry> entries)
    {
        int size = 6 + entries.Length * 2;
        for (int i = 0; i < entries.Length; i++)
        {
            size += Leb128.EncodedSize(entries[i].SepLen)
                    + entries[i].SepLen
                    + Leb128.EncodedSize(entries[i].ValueLengthOffset);
        }
        return size;
    }

    private readonly int WriteVariableLeafNodeInto(Span<byte> output, ReadOnlySpan<RsstEntry> entries, byte sepLen)
    {
        int count = entries.Length;
        int headerSize = 6;
        int offsetTableSize = count * 2;

        int dataSize = 0;
        for (int i = 0; i < count; i++)
        {
            dataSize += Leb128.EncodedSize(entries[i].SepLen)
                        + entries[i].SepLen
                        + Leb128.EncodedSize(entries[i].ValueLengthOffset);
        }

        int totalSize = headerSize + offsetTableSize + dataSize;

        // Write header
        BinaryPrimitives.WriteUInt16LittleEndian(output, (ushort)totalSize);
        output[2] = 0x01; // IsLeaf=1, Format=0
        BinaryPrimitives.WriteUInt16LittleEndian(output[3..], (ushort)count);
        output[5] = sepLen;

        // Write entries
        int entryDataStart = headerSize + offsetTableSize;
        int entryPos = entryDataStart;
        for (int i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(output[(headerSize + i * 2)..], (ushort)(entryPos - entryDataStart));
            entryPos = Leb128.Write(output, entryPos, entries[i].SepLen);
            _separatorBuffer.AsSpan(entries[i].SepOffset, entries[i].SepLen).CopyTo(output[entryPos..]);
            entryPos += entries[i].SepLen;
            entryPos = Leb128.Write(output, entryPos, entries[i].ValueLengthOffset);
        }

        return totalSize;
    }

    private readonly int WriteUniformLeafNodeInto(Span<byte> output, ReadOnlySpan<RsstEntry> entries, byte sepLen, int baseOffset, bool useUint24)
    {
        int count = entries.Length;
        int baseOffsetLebSize = Leb128.EncodedSize(baseOffset);
        int offsetBytes = useUint24 ? 3 : 2;
        int stride = sepLen + offsetBytes;
        int totalSize = 6 + baseOffsetLebSize + count * stride;

        // Write header
        BinaryPrimitives.WriteUInt16LittleEndian(output, (ushort)totalSize);
        byte format = useUint24 ? (byte)1 : (byte)2;
        output[2] = (byte)(0x01 | (format << 1));
        BinaryPrimitives.WriteUInt16LittleEndian(output[3..], (ushort)count);
        output[5] = sepLen;

        int pos = Leb128.Write(output, 6, baseOffset);

        for (int i = 0; i < count; i++)
        {
            _separatorBuffer.AsSpan(entries[i].SepOffset, sepLen).CopyTo(output[pos..]);
            pos += sepLen;

            int relative = entries[i].ValueLengthOffset - baseOffset;
            if (useUint24)
            {
                output[pos] = (byte)(relative & 0xFF);
                output[pos + 1] = (byte)((relative >> 8) & 0xFF);
                output[pos + 2] = (byte)((relative >> 16) & 0xFF);
                pos += 3;
            }
            else
            {
                BinaryPrimitives.WriteUInt16LittleEndian(output[pos..], (ushort)relative);
                pos += 2;
            }
        }

        return totalSize;
    }

    private readonly int WriteInternalNode(Span<byte> output, ReadOnlySpan<NodeInfo> nodes, int indexStart)
    {
        int childCount = nodes.Length;

        // First child has empty separator, rest need separator between prev.last and curr.first
        bool uniform = true;
        int sepLen = 0;
        if (childCount > 1)
        {
            sepLen = ComputeSeparatorLengthBetween(
                _separatorBuffer.AsSpan(nodes[0].LastSepOffset, nodes[0].LastSepLen),
                _separatorBuffer.AsSpan(nodes[1].FirstSepOffset, nodes[1].FirstSepLen));

            for (int i = 2; i < childCount; i++)
            {
                int s = ComputeSeparatorLengthBetween(
                    _separatorBuffer.AsSpan(nodes[i - 1].LastSepOffset, nodes[i - 1].LastSepLen),
                    _separatorBuffer.AsSpan(nodes[i].FirstSepOffset, nodes[i].FirstSepLen));
                if (s != sepLen) uniform = false;
            }
        }

        int minOffset = nodes[0].Offset;
        int maxOffset = nodes[0].Offset;
        for (int i = 1; i < childCount; i++)
        {
            if (nodes[i].Offset < minOffset) minOffset = nodes[i].Offset;
            if (nodes[i].Offset > maxOffset) maxOffset = nodes[i].Offset;
        }

        int range = maxOffset - minOffset;

        // Choose format
        int bestFormat = 0;
        int bestSize = ComputeVariableInternalSize(nodes, sepLen);

        if (uniform && childCount > 1)
        {
            int baseOffsetLebSize = Leb128.EncodedSize(minOffset);

            if (range <= 0xFFFF)
            {
                int compactSize = 6 + baseOffsetLebSize + childCount * (sepLen + 2);
                if (compactSize < bestSize)
                {
                    bestFormat = 2;
                    bestSize = compactSize;
                }
            }

            if (range <= 0xFFFFFF)
            {
                int uniformSize = 6 + baseOffsetLebSize + childCount * (sepLen + 3);
                if (uniformSize < bestSize)
                {
                    bestFormat = 1;
                    bestSize = uniformSize;
                }
            }
        }

        return bestFormat switch
        {
            0 => WriteVariableInternalNodeInto(output, nodes, indexStart, (byte)sepLen),
            1 => WriteUniformInternalNodeInto(output, nodes, indexStart, (byte)sepLen, minOffset, useUint24: true),
            2 => WriteUniformInternalNodeInto(output, nodes, indexStart, (byte)sepLen, minOffset, useUint24: false),
            _ => throw new InvalidOperationException()
        };
    }

    private readonly int ComputeVariableInternalSize(ReadOnlySpan<NodeInfo> nodes, int sepLen)
    {
        int size = 6 + nodes.Length * 2;
        size += Leb128.EncodedSize(nodes[0].Offset);
        for (int i = 1; i < nodes.Length; i++)
        {
            int s = ComputeSeparatorLengthBetween(
                _separatorBuffer.AsSpan(nodes[i - 1].LastSepOffset, nodes[i - 1].LastSepLen),
                _separatorBuffer.AsSpan(nodes[i].FirstSepOffset, nodes[i].FirstSepLen));
            size += Leb128.EncodedSize(s) + s + Leb128.EncodedSize(nodes[i].Offset);
        }
        return size;
    }

    private readonly int WriteVariableInternalNodeInto(Span<byte> output, ReadOnlySpan<NodeInfo> nodes, int indexStart, byte sepLen)
    {
        int childCount = nodes.Length;
        int headerSize = 6;
        int offsetTableSize = childCount * 2;

        int dataSize = Leb128.EncodedSize(nodes[0].Offset);
        Span<byte> tempSep = stackalloc byte[256];
        for (int i = 1; i < childCount; i++)
        {
            int s = WriteSeparatorBetween(tempSep,
                _separatorBuffer.AsSpan(nodes[i - 1].LastSepOffset, nodes[i - 1].LastSepLen),
                _separatorBuffer.AsSpan(nodes[i].FirstSepOffset, nodes[i].FirstSepLen));
            dataSize += Leb128.EncodedSize(s) + s + Leb128.EncodedSize(nodes[i].Offset);
        }

        int totalSize = headerSize + offsetTableSize + dataSize;

        BinaryPrimitives.WriteUInt16LittleEndian(output, (ushort)totalSize);
        output[2] = 0x00; // IsLeaf=0, Format=0
        BinaryPrimitives.WriteUInt16LittleEndian(output[3..], (ushort)childCount);
        output[5] = sepLen;

        int entryDataStart = headerSize + offsetTableSize;
        int entryPos = entryDataStart;

        BinaryPrimitives.WriteUInt16LittleEndian(output[headerSize..], (ushort)(entryPos - entryDataStart));
        entryPos = Leb128.Write(output, entryPos, nodes[0].Offset);

        for (int i = 1; i < childCount; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(output[(headerSize + i * 2)..], (ushort)(entryPos - entryDataStart));
            int s = WriteSeparatorBetween(tempSep,
                _separatorBuffer.AsSpan(nodes[i - 1].LastSepOffset, nodes[i - 1].LastSepLen),
                _separatorBuffer.AsSpan(nodes[i].FirstSepOffset, nodes[i].FirstSepLen));
            entryPos = Leb128.Write(output, entryPos, s);
            tempSep[..s].CopyTo(output[entryPos..]);
            entryPos += s;
            entryPos = Leb128.Write(output, entryPos, nodes[i].Offset);
        }

        return totalSize;
    }

    private readonly int WriteUniformInternalNodeInto(Span<byte> output, ReadOnlySpan<NodeInfo> nodes, int indexStart, byte sepLen, int baseOffset, bool useUint24)
    {
        int childCount = nodes.Length;
        int baseOffsetLebSize = Leb128.EncodedSize(baseOffset);
        int offsetBytes = useUint24 ? 3 : 2;
        int stride = sepLen + offsetBytes;
        int totalSize = 6 + baseOffsetLebSize + childCount * stride;

        BinaryPrimitives.WriteUInt16LittleEndian(output, (ushort)totalSize);
        byte format = useUint24 ? (byte)1 : (byte)2;
        output[2] = (byte)(format << 1); // IsLeaf=0, Format
        BinaryPrimitives.WriteUInt16LittleEndian(output[3..], (ushort)childCount);
        output[5] = sepLen;

        int pos = Leb128.Write(output, 6, baseOffset);

        Span<byte> tempSep = stackalloc byte[256];
        for (int i = 0; i < childCount; i++)
        {
            if (i == 0)
            {
                // First child: zero-filled separator
                output.Slice(pos, sepLen).Clear();
                pos += sepLen;
            }
            else
            {
                int s = WriteSeparatorBetween(tempSep,
                    _separatorBuffer.AsSpan(nodes[i - 1].LastSepOffset, nodes[i - 1].LastSepLen),
                    _separatorBuffer.AsSpan(nodes[i].FirstSepOffset, nodes[i].FirstSepLen));
                tempSep[..sepLen].CopyTo(output[pos..]);
                pos += sepLen;
            }

            int relative = nodes[i].Offset - baseOffset;
            if (useUint24)
            {
                output[pos] = (byte)(relative & 0xFF);
                output[pos + 1] = (byte)((relative >> 8) & 0xFF);
                output[pos + 2] = (byte)((relative >> 16) & 0xFF);
                pos += 3;
            }
            else
            {
                BinaryPrimitives.WriteUInt16LittleEndian(output[pos..], (ushort)relative);
                pos += 2;
            }
        }

        return totalSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ComputeSeparatorLength(ReadOnlySpan<byte> prevKey, ReadOnlySpan<byte> currKey, ReadOnlySpan<byte> nextKey, int extraSeparatorLength = 0)
    {
        int minVsPrev = 0;
        if (!prevKey.IsEmpty)
        {
            int common = CommonPrefixLength(prevKey, currKey);
            minVsPrev = common + 1;
        }

        int minVsNext = 0;
        if (!nextKey.IsEmpty)
        {
            int common = CommonPrefixLength(currKey, nextKey);
            minVsNext = common + 1;
        }

        int len = Math.Max(minVsPrev, minVsNext);
        len = Math.Min(len, currKey.Length);
        if (len == 0) len = Math.Min(1, currKey.Length);

        return Math.Min(len + extraSeparatorLength, currKey.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ComputeSeparatorLengthBetween(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int minLen = Math.Min(left.Length, right.Length);

        for (int i = 0; i < minLen; i++)
        {
            if (left[i] != right[i])
                return i + 1;
        }

        if (right.Length > minLen)
            return right.Length;

        return right.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int WriteSeparatorBetween(Span<byte> output, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int len = ComputeSeparatorLengthBetween(left, right);
        right[..len].CopyTo(output);
        return len;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CommonPrefixLength(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            if (a[i] != b[i]) return i;
        }
        return minLen;
    }

    public static int StreamingMerge(ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer, Span<byte> output, int startOffset = 0, int extraSeparatorLength = 0)
    {
        Rsst olderRsst = new(older);
        Rsst newerRsst = new(newer);

        if (olderRsst.EntryCount == 0 && newerRsst.EntryCount == 0)
        {
            output[startOffset] = 0x00;
            output[startOffset + 1] = 0x01;
            return startOffset + 2;
        }

        using (RsstBuilder builder = new(output[startOffset..], extraSeparatorLength))
        {

            using Rsst.Enumerator olderEnum = olderRsst.GetEnumerator();
            using Rsst.Enumerator newerEnum = newerRsst.GetEnumerator();

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

            return startOffset + builder.Build();
        }
    }
}
