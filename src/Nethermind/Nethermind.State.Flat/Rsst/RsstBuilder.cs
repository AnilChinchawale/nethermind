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
///
/// Span-based API: accepts preallocated buffer, writes directly, returns final size.
/// </summary>
public sealed class RsstBuilder
{
    private readonly byte[] _output;
    private readonly int _startOffset;
    private int _position;

    // Separator storage: contiguous byte buffer + per-entry metadata
    private byte[] _separatorBuffer = new byte[256];
    private int _separatorBufferPos;
    private readonly List<(int SepOffset, int SepLength, int ValueLengthOffset)> _entries = new();

    // For streaming separator computation (only need previous key)
    private byte[] _prevKey = Array.Empty<byte>();
    private int _prevKeyLength;

    public int EntryCount => _entries.Count;
    public byte[] OutputBuffer => _output;
    public int CurrentPosition => _position;

    /// <summary>
    /// Create builder writing into preallocated output buffer.
    /// </summary>
    public RsstBuilder(byte[] output, int startOffset = 0)
    {
        _output = output;
        _startOffset = startOffset;
        _position = startOffset;
    }

    /// <summary>
    /// Backward-compat: allocate large buffer for tests.
    /// </summary>
    public RsstBuilder() : this(new byte[4 * 1024 * 1024], 0) { }

    /// <summary>
    /// Begin writing a value. Returns current position where value should be written.
    /// Call FinishValueWrite after writing the value.
    /// </summary>
    public void BeginValueWrite()
    {
        // Position is already set for value write
    }

    /// <summary>
    /// Finish value write after writing actualLen bytes. Computes separator and writes metadata.
    /// Key must be greater than previous key (sorted order).
    /// </summary>
    public void FinishValueWrite(int actualLen, ReadOnlySpan<byte> key)
    {
        int valueLengthOffset = _position + actualLen;

        // Compute separator eagerly (only need prevKey, currKey - no nextKey in streaming)
        int sepLen = ComputeSeparatorLength(
            _prevKey.AsSpan(0, _prevKeyLength),
            key,
            nextKey: default);

        // Store separator in contiguous buffer
        EnsureSeparatorBufferCapacity(_separatorBufferPos + sepLen);
        key[..sepLen].CopyTo(_separatorBuffer.AsSpan(_separatorBufferPos));
        int sepOffset = _separatorBufferPos;
        _separatorBufferPos += sepLen;

        // Compute remaining key
        ReadOnlySpan<byte> remainingKey = key[sepLen..];
        int remainingKeyLength = remainingKey.Length;

        // Write metadata at valueLengthOffset: [ValueLength LEB128][RemainingKeyLength LEB128][RemainingKey]
        Span<byte> leb = stackalloc byte[10];
        int lebLen = Leb128.Write(leb, 0, actualLen);
        leb[..lebLen].CopyTo(_output.AsSpan(valueLengthOffset));
        int pos = valueLengthOffset + lebLen;

        lebLen = Leb128.Write(leb, 0, remainingKeyLength);
        leb[..lebLen].CopyTo(_output.AsSpan(pos));
        pos += lebLen;

        if (remainingKeyLength > 0)
        {
            remainingKey.CopyTo(_output.AsSpan(pos));
            pos += remainingKeyLength;
        }

        // Record entry
        _entries.Add((sepOffset, sepLen, valueLengthOffset));

        // Update position past metadata
        _position = pos;

        // Update prevKey for next entry
        if (_prevKey.Length < key.Length)
            Array.Resize(ref _prevKey, key.Length);
        key.CopyTo(_prevKey);
        _prevKeyLength = key.Length;
    }

    /// <summary>
    /// Convenience: add key-value pair in one call.
    /// </summary>
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        BeginValueWrite();
        value.CopyTo(_output.AsSpan(_position));
        FinishValueWrite(value.Length, key);
    }

    /// <summary>
    /// Build index and trailer, returning final end position.
    /// </summary>
    public int Build()
    {
        if (_entries.Count == 0)
        {
            // Empty RSST: [IndexSize=0 LEB128][LEB128ByteCount=1]
            _output[_position] = 0x00;
            _output[_position + 1] = 0x01;
            return _position + 2;
        }

        int dataRegionEnd = _position;
        int indexStart = dataRegionEnd;

        // Build leaf nodes bottom-up
        List<(int Offset, int FirstSepOffset, int FirstSepLen, int LastSepOffset, int LastSepLen)> currentLevel = new();
        int entryIdx = 0;
        int indexOffset = 0;

        while (entryIdx < _entries.Count)
        {
            int count = Math.Min(Rsst.MaxLeafEntries, _entries.Count - entryIdx);

            (int sepOffset, int sepLen, int vlOffset) first = _entries[entryIdx];

            ReadOnlySpan<(int, int, int)> leafEntries = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_entries).Slice(entryIdx, count);
            int nodeLen = WriteLeafNode(_output.AsSpan(_position), leafEntries);

            currentLevel.Add((_position - indexStart, first.sepOffset, first.sepLen, _entries[entryIdx + count - 1].Item1, _entries[entryIdx + count - 1].Item2));
            _position += nodeLen;
            indexOffset += nodeLen;

            entryIdx += count;
        }

        // Build internal nodes bottom-up until single root
        while (currentLevel.Count > 1)
        {
            List<(int, int, int, int, int)> nextLevel = new();
            int childIdx = 0;

            while (childIdx < currentLevel.Count)
            {
                int childCount = Math.Min(Rsst.MaxLeafEntries, currentLevel.Count - childIdx);

                var first = currentLevel[childIdx];
                var last = currentLevel[childIdx + childCount - 1];

                int nodeLen = WriteInternalNode(_output.AsSpan(_position), currentLevel, childIdx, childCount, indexStart);

                nextLevel.Add((_position - indexStart, first.Item2, first.Item3, last.Item4, last.Item5));
                _position += nodeLen;
                indexOffset += nodeLen;

                childIdx += childCount;
            }

            currentLevel = nextLevel;
        }

        int indexSize = indexOffset;

        // Write trailer: [IndexSize LEB128][LEB128ByteCount: 1 byte]
        Span<byte> trailerLeb = stackalloc byte[5];
        int trailerLebLen = Leb128.Write(trailerLeb, 0, indexSize);
        trailerLeb[..trailerLebLen].CopyTo(_output.AsSpan(_position));
        _position += trailerLebLen;
        _output[_position] = (byte)trailerLebLen;
        _position++;

        return _position;
    }

    /// <summary>
    /// Convenience: allocate + Build() + slice to exact size.
    /// </summary>
    public byte[] BuildToArray()
    {
        int endPos = Build();
        int length = endPos - _startOffset;
        byte[] result = new byte[length];
        _output.AsSpan(_startOffset, length).CopyTo(result);
        return result;
    }

    private int WriteLeafNode(Span<byte> output, ReadOnlySpan<(int SepOffset, int SepLen, int ValueLengthOffset)> entries)
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

    private int ComputeVariableLeafSize(ReadOnlySpan<(int SepOffset, int SepLen, int ValueLengthOffset)> entries)
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

    private int WriteVariableLeafNodeInto(Span<byte> output, ReadOnlySpan<(int SepOffset, int SepLen, int ValueLengthOffset)> entries, byte sepLen)
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

    private int WriteUniformLeafNodeInto(Span<byte> output, ReadOnlySpan<(int SepOffset, int SepLen, int ValueLengthOffset)> entries, byte sepLen, int baseOffset, bool useUint24)
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

    private int WriteInternalNode(Span<byte> output, List<(int Offset, int FirstSepOffset, int FirstSepLen, int LastSepOffset, int LastSepLen)> level, int childIdx, int childCount, int indexStart)
    {
        // First child has empty separator, rest need separator between prev.last and curr.first
        bool uniform = true;
        int sepLen = 0;
        if (childCount > 1)
        {
            sepLen = ComputeSeparatorLengthBetween(
                _separatorBuffer.AsSpan(level[childIdx].LastSepOffset, level[childIdx].LastSepLen),
                _separatorBuffer.AsSpan(level[childIdx + 1].FirstSepOffset, level[childIdx + 1].FirstSepLen));

            for (int i = 2; i < childCount; i++)
            {
                int s = ComputeSeparatorLengthBetween(
                    _separatorBuffer.AsSpan(level[childIdx + i - 1].LastSepOffset, level[childIdx + i - 1].LastSepLen),
                    _separatorBuffer.AsSpan(level[childIdx + i].FirstSepOffset, level[childIdx + i].FirstSepLen));
                if (s != sepLen) uniform = false;
            }
        }

        int minOffset = level[childIdx].Offset;
        int maxOffset = level[childIdx].Offset;
        for (int i = 1; i < childCount; i++)
        {
            if (level[childIdx + i].Offset < minOffset) minOffset = level[childIdx + i].Offset;
            if (level[childIdx + i].Offset > maxOffset) maxOffset = level[childIdx + i].Offset;
        }

        int range = maxOffset - minOffset;

        // Choose format
        int bestFormat = 0;
        int bestSize = ComputeVariableInternalSize(level, childIdx, childCount, sepLen);

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
            0 => WriteVariableInternalNodeInto(output, level, childIdx, childCount, indexStart, (byte)sepLen),
            1 => WriteUniformInternalNodeInto(output, level, childIdx, childCount, indexStart, (byte)sepLen, minOffset, useUint24: true),
            2 => WriteUniformInternalNodeInto(output, level, childIdx, childCount, indexStart, (byte)sepLen, minOffset, useUint24: false),
            _ => throw new InvalidOperationException()
        };
    }

    private int ComputeVariableInternalSize(List<(int Offset, int FirstSepOffset, int FirstSepLen, int LastSepOffset, int LastSepLen)> level, int childIdx, int childCount, int sepLen)
    {
        int size = 6 + childCount * 2;
        size += Leb128.EncodedSize(level[childIdx].Offset);
        for (int i = 1; i < childCount; i++)
        {
            int s = ComputeSeparatorLengthBetween(
                _separatorBuffer.AsSpan(level[childIdx + i - 1].LastSepOffset, level[childIdx + i - 1].LastSepLen),
                _separatorBuffer.AsSpan(level[childIdx + i].FirstSepOffset, level[childIdx + i].FirstSepLen));
            size += Leb128.EncodedSize(s) + s + Leb128.EncodedSize(level[childIdx + i].Offset);
        }
        return size;
    }

    private int WriteVariableInternalNodeInto(Span<byte> output, List<(int Offset, int FirstSepOffset, int FirstSepLen, int LastSepOffset, int LastSepLen)> level, int childIdx, int childCount, int indexStart, byte sepLen)
    {
        int headerSize = 6;
        int offsetTableSize = childCount * 2;

        int dataSize = Leb128.EncodedSize(level[childIdx].Offset);
        Span<byte> tempSep = stackalloc byte[256];
        for (int i = 1; i < childCount; i++)
        {
            int s = WriteSeparatorBetween(tempSep,
                _separatorBuffer.AsSpan(level[childIdx + i - 1].LastSepOffset, level[childIdx + i - 1].LastSepLen),
                _separatorBuffer.AsSpan(level[childIdx + i].FirstSepOffset, level[childIdx + i].FirstSepLen));
            dataSize += Leb128.EncodedSize(s) + s + Leb128.EncodedSize(level[childIdx + i].Offset);
        }

        int totalSize = headerSize + offsetTableSize + dataSize;

        BinaryPrimitives.WriteUInt16LittleEndian(output, (ushort)totalSize);
        output[2] = 0x00; // IsLeaf=0, Format=0
        BinaryPrimitives.WriteUInt16LittleEndian(output[3..], (ushort)childCount);
        output[5] = sepLen;

        int entryDataStart = headerSize + offsetTableSize;
        int entryPos = entryDataStart;

        BinaryPrimitives.WriteUInt16LittleEndian(output[headerSize..], (ushort)(entryPos - entryDataStart));
        entryPos = Leb128.Write(output, entryPos, level[childIdx].Offset);

        for (int i = 1; i < childCount; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(output[(headerSize + i * 2)..], (ushort)(entryPos - entryDataStart));
            int s = WriteSeparatorBetween(tempSep,
                _separatorBuffer.AsSpan(level[childIdx + i - 1].LastSepOffset, level[childIdx + i - 1].LastSepLen),
                _separatorBuffer.AsSpan(level[childIdx + i].FirstSepOffset, level[childIdx + i].FirstSepLen));
            entryPos = Leb128.Write(output, entryPos, s);
            tempSep[..s].CopyTo(output[entryPos..]);
            entryPos += s;
            entryPos = Leb128.Write(output, entryPos, level[childIdx + i].Offset);
        }

        return totalSize;
    }

    private int WriteUniformInternalNodeInto(Span<byte> output, List<(int Offset, int FirstSepOffset, int FirstSepLen, int LastSepOffset, int LastSepLen)> level, int childIdx, int childCount, int indexStart, byte sepLen, int baseOffset, bool useUint24)
    {
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
                pos += sepLen;
            }
            else
            {
                int s = WriteSeparatorBetween(tempSep,
                    _separatorBuffer.AsSpan(level[childIdx + i - 1].LastSepOffset, level[childIdx + i - 1].LastSepLen),
                    _separatorBuffer.AsSpan(level[childIdx + i].FirstSepOffset, level[childIdx + i].FirstSepLen));
                tempSep[..sepLen].CopyTo(output[pos..]);
                pos += sepLen;
            }

            int relative = level[childIdx + i].Offset - baseOffset;
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

    /// <summary>
    /// Compute separator length (streaming mode: no nextKey).
    /// Returns separator length. Separator = key[..separatorLen].
    /// </summary>
    private static int ComputeSeparatorLength(ReadOnlySpan<byte> prevKey, ReadOnlySpan<byte> currKey, ReadOnlySpan<byte> nextKey)
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

        return len;
    }

    /// <summary>
    /// Compute separator length between two keys (for internal nodes).
    /// </summary>
    private static int ComputeSeparatorLengthBetween(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int minLen = Math.Min(left.Length, right.Length);

        for (int i = 0; i < minLen; i++)
        {
            if (left[i] != right[i])
                return i + 1;
        }

        if (right.Length > minLen)
            return minLen + 1;

        return right.Length;
    }

    /// <summary>
    /// Write separator between two keys into output buffer. Returns length written.
    /// </summary>
    private static int WriteSeparatorBetween(Span<byte> output, ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int len = ComputeSeparatorLengthBetween(left, right);
        right[..len].CopyTo(output);
        return len;
    }

    private static int CommonPrefixLength(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            if (a[i] != b[i]) return i;
        }
        return minLen;
    }

    private void EnsureSeparatorBufferCapacity(int required)
    {
        if (_separatorBuffer.Length < required)
        {
            int newSize = Math.Max(_separatorBuffer.Length * 2, required);
            Array.Resize(ref _separatorBuffer, newSize);
        }
    }

    public static int StreamingMerge(ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer, byte[] output, int startOffset = 0)
    {
        Rsst olderRsst = new(older);
        Rsst newerRsst = new(newer);

        if (olderRsst.EntryCount == 0 && newerRsst.EntryCount == 0)
        {
            output[startOffset] = 0x00;
            output[startOffset + 1] = 0x01;
            return startOffset + 2;
        }

        RsstBuilder builder = new(output, startOffset);

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

        return builder.Build();
    }

    public static byte[] StreamingMerge(ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer)
    {
        byte[] buffer = new byte[older.Length + newer.Length + 1024];
        int endPos = StreamingMerge(older, newer, buffer, 0);
        return buffer.AsSpan(0, endPos).ToArray();
    }

    /// <summary>
    /// Compute separator key between two keys (for test compatibility).
    /// </summary>
    internal static byte[] ComputeSeparatorKey(byte[] left, byte[] right)
    {
        int len = ComputeSeparatorLengthBetween(left, right);
        return right[..len];
    }
}
