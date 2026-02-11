// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// Builds an RSST (Read-only Sorted String Table) from key-value entries.
/// Entries MUST be added in sorted key order. No internal sorting is performed.
/// Writes entries directly to a growable buffer to avoid buffering all entries in memory.
/// </summary>
public sealed class RsstBuilder
{
    private readonly MemoryStream _dataStream = new();
    private readonly List<int> _entryOffsets = new();
    private readonly List<byte[]> _firstKeys = new(); // first key per leaf
    private readonly List<byte[]> _lastKeys = new();  // last key per leaf

    private byte[]? _lastKey;
    private int _leafEntryCount;

    // State for BeginLargeEntry/FinishEntry
    private int _pendingValueLebPosition = -1;
    private int _pendingValueStart = -1;
    private int _pendingLebLength;

    public int EntryCount => _entryOffsets.Count;

    private void TrackLeafBoundary(ReadOnlySpan<byte> key)
    {
        if (_leafEntryCount == 0)
            _firstKeys.Add(key.ToArray());
        _leafEntryCount++;

        byte[] currentKey = key.ToArray();
        if (_leafEntryCount >= Rsst.MaxLeafEntries)
        {
            _lastKeys.Add(currentKey);
            _leafEntryCount = 0;
        }
        _lastKey = currentKey;
    }

    /// <summary>
    /// Add an entry with pre-allocated value space. Returns a span where the caller writes the value directly.
    /// Key must be greater than the previously added key (sorted order required).
    /// </summary>
    public Span<byte> AddEntry(ReadOnlySpan<byte> key, int valueLength)
    {
        int offset = (int)_dataStream.Position;
        _entryOffsets.Add(offset);

        TrackLeafBoundary(key);

        // Write key
        Span<byte> leb = stackalloc byte[5];
        int lebLen = Leb128.Write(leb, 0, key.Length);
        _dataStream.Write(leb[..lebLen]);
        _dataStream.Write(key);

        // Write value length
        lebLen = Leb128.Write(leb, 0, valueLength);
        _dataStream.Write(leb[..lebLen]);

        // Reserve space for value and return span into it
        long valueStart = _dataStream.Position;
        _dataStream.SetLength(_dataStream.Length + valueLength);
        _dataStream.Position = valueStart + valueLength;

        return _dataStream.GetBuffer().AsSpan((int)valueStart, valueLength);
    }

    /// <summary>
    /// Convenience wrapper: add a key-value pair. Key must be in sorted order.
    /// </summary>
    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Span<byte> dest = AddEntry(key, value.Length);
        value.CopyTo(dest);
    }

    /// <summary>
    /// Begin a large entry where the exact value size is not known upfront.
    /// Returns a span of <paramref name="maxValueLength"/> bytes for the caller to write into.
    /// Call <see cref="FinishEntry"/> with the actual size after writing.
    /// The value LEB128 is padded so FinishEntry can rewrite it in-place without shifting data.
    /// </summary>
    public Span<byte> BeginLargeEntry(ReadOnlySpan<byte> key, int maxValueLength)
    {
        int offset = (int)_dataStream.Position;
        _entryOffsets.Add(offset);

        TrackLeafBoundary(key);

        // Write key
        Span<byte> leb = stackalloc byte[5];
        int lebLen = Leb128.Write(leb, 0, key.Length);
        _dataStream.Write(leb[..lebLen]);
        _dataStream.Write(key);

        // Write value LEB128 padded to max size so FinishEntry can rewrite in-place
        _pendingLebLength = Leb128.EncodedSize(maxValueLength);
        _pendingValueLebPosition = (int)_dataStream.Position;
        Span<byte> paddedLeb = stackalloc byte[5];
        Leb128.WritePadded(paddedLeb, 0, maxValueLength, _pendingLebLength);
        _dataStream.Write(paddedLeb[.._pendingLebLength]);

        // Reserve space for value and return span
        _pendingValueStart = (int)_dataStream.Position;
        _dataStream.SetLength(Math.Max(_dataStream.Length, _pendingValueStart + maxValueLength));
        _dataStream.Position = _pendingValueStart + maxValueLength;

        return _dataStream.GetBuffer().AsSpan(_pendingValueStart, maxValueLength);
    }

    /// <summary>
    /// Finish a large entry started with <see cref="BeginLargeEntry"/>.
    /// Rewrites the value LEB128 with the actual size (padded to same byte count).
    /// </summary>
    public void FinishEntry(int actualSize)
    {
        // Rewrite LEB128 in-place with actual size, padded to same byte count
        Leb128.WritePadded(_dataStream.GetBuffer().AsSpan(), _pendingValueLebPosition, actualSize, _pendingLebLength);

        // Adjust stream position to end of actual data
        _dataStream.Position = _pendingValueStart + actualSize;
        _pendingValueLebPosition = -1;
    }

    public byte[] Build()
    {
        if (_entryOffsets.Count == 0) return BuildEmpty();

        // Close last leaf if partial
        if (_leafEntryCount > 0)
        {
            _lastKeys.Add(_lastKey!);
        }

        int dataSize = (int)_dataStream.Position;
        byte[] dataBytes = _dataStream.GetBuffer();

        // Phase 2: Build B-tree index
        List<byte[]> nodeBytes = new();
        List<int> currentLevelOffsets = new();
        List<byte[]> currentLevelFirstKeys = new();
        List<byte[]> currentLevelLastKeys = new();

        // Build leaf nodes
        int entryIdx = 0;
        int leafIdx = 0;
        while (entryIdx < _entryOffsets.Count)
        {
            int count = Math.Min(Rsst.MaxLeafEntries, _entryOffsets.Count - entryIdx);

            int leafSize = 1 + Leb128.EncodedSize(count) + count * 4;
            byte[] leaf = new byte[leafSize];
            int pos = 0;
            leaf[pos++] = Rsst.LeafNodeType;
            pos = Leb128.Write(leaf, pos, count);
            for (int i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(leaf.AsSpan(pos), _entryOffsets[entryIdx + i]);
                pos += 4;
            }

            currentLevelOffsets.Add(dataSize);
            currentLevelFirstKeys.Add(_firstKeys[leafIdx]);
            currentLevelLastKeys.Add(_lastKeys[leafIdx]);
            dataSize += leaf.Length;
            nodeBytes.Add(leaf);

            entryIdx += count;
            leafIdx++;
        }

        // Build internal nodes bottom-up until we have a single root
        while (currentLevelOffsets.Count > 1)
        {
            List<int> nextLevelOffsets = new();
            List<byte[]> nextLevelFirstKeys = new();
            List<byte[]> nextLevelLastKeys = new();

            int childIdx = 0;
            while (childIdx < currentLevelOffsets.Count)
            {
                int childCount = Math.Min(Rsst.MaxLeafEntries, currentLevelOffsets.Count - childIdx);
                byte[] internalFirstKey = currentLevelFirstKeys[childIdx];
                byte[] internalLastKey = currentLevelLastKeys[childIdx + childCount - 1];

                int nodeSize = 1 + Leb128.EncodedSize(childCount) + 4;
                for (int i = 1; i < childCount; i++)
                {
                    byte[] sepKey = ComputeSeparatorKey(currentLevelLastKeys[childIdx + i - 1], currentLevelFirstKeys[childIdx + i]);
                    nodeSize += Leb128.EncodedSize(sepKey.Length) + sepKey.Length + 4;
                }

                byte[] node = new byte[nodeSize];
                int pos = 0;
                node[pos++] = Rsst.InternalNodeType;
                pos = Leb128.Write(node, pos, childCount);

                // First child offset
                BinaryPrimitives.WriteInt32LittleEndian(node.AsSpan(pos), currentLevelOffsets[childIdx]);
                pos += 4;

                // Subsequent children with separator keys
                for (int i = 1; i < childCount; i++)
                {
                    byte[] sepKey = ComputeSeparatorKey(currentLevelLastKeys[childIdx + i - 1], currentLevelFirstKeys[childIdx + i]);
                    pos = Leb128.Write(node, pos, sepKey.Length);
                    sepKey.CopyTo(node.AsSpan(pos));
                    pos += sepKey.Length;
                    BinaryPrimitives.WriteInt32LittleEndian(node.AsSpan(pos), currentLevelOffsets[childIdx + i]);
                    pos += 4;
                }

                nextLevelOffsets.Add(dataSize);
                nextLevelFirstKeys.Add(internalFirstKey);
                nextLevelLastKeys.Add(internalLastKey);
                dataSize += node.Length;
                nodeBytes.Add(node);

                childIdx += childCount;
            }

            currentLevelOffsets = nextLevelOffsets;
            currentLevelFirstKeys = nextLevelFirstKeys;
            currentLevelLastKeys = nextLevelLastKeys;
        }

        int rootOffset = currentLevelOffsets[0];

        // Phase 3: Assemble final output
        int totalSize = dataSize + Rsst.FooterSize;
        byte[] result = new byte[totalSize];

        // Copy data region
        _dataStream.GetBuffer().AsSpan(0, (int)_dataStream.Position).CopyTo(result);
        int writePos = (int)_dataStream.Position;

        // Write nodes
        foreach (byte[] node in nodeBytes)
        {
            node.CopyTo(result.AsSpan(writePos));
            writePos += node.Length;
        }

        // Write footer
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(writePos), rootOffset);
        writePos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(writePos), _entryOffsets.Count);

        return result;
    }

    private static byte[] BuildEmpty()
    {
        byte[] result = new byte[Rsst.FooterSize];
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(0), 0);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(4), 0);
        return result;
    }

    /// <summary>
    /// Compute the shortest distinguishing prefix that separates left from right.
    /// The result satisfies: left &lt; separator &lt;= right.
    /// The search uses: key &lt; separator → go left, key >= separator → go right.
    /// </summary>
    internal static byte[] ComputeSeparatorKey(byte[] left, byte[] right)
    {
        int minLen = Math.Min(left.Length, right.Length);

        for (int i = 0; i < minLen; i++)
        {
            if (left[i] != right[i])
            {
                return right[..(i + 1)];
            }
        }

        if (right.Length > minLen)
        {
            return right[..(minLen + 1)];
        }

        // They are equal (shouldn't happen with unique sorted keys), just return right
        return right.ToArray();
    }

    /// <summary>
    /// Merge two sorted RSST snapshots into one using streaming merge-sort.
    /// Newer entries override older ones when keys match.
    /// Both source RSSTs must be sorted (which they always are).
    /// </summary>
    public static byte[] StreamingMerge(ReadOnlySpan<byte> olderData, ReadOnlySpan<byte> newerData)
    {
        Rsst older = new(olderData);
        Rsst newer = new(newerData);

        if (older.EntryCount == 0 && newer.EntryCount == 0) return BuildEmpty();

        // Collect offsets from both for merge iteration
        int[] olderOffsets = CollectEntryOffsets(olderData, older);
        int[] newerOffsets = CollectEntryOffsets(newerData, newer);

        RsstBuilder builder = new();
        int oi = 0, ni = 0;

        while (oi < olderOffsets.Length && ni < newerOffsets.Length)
        {
            Rsst.ReadEntry(olderData, olderOffsets[oi], out ReadOnlySpan<byte> olderKey, out ReadOnlySpan<byte> olderValue);
            Rsst.ReadEntry(newerData, newerOffsets[ni], out ReadOnlySpan<byte> newerKey, out ReadOnlySpan<byte> newerValue);

            int cmp = olderKey.SequenceCompareTo(newerKey);
            if (cmp < 0)
            {
                builder.Add(olderKey, olderValue);
                oi++;
            }
            else if (cmp > 0)
            {
                builder.Add(newerKey, newerValue);
                ni++;
            }
            else
            {
                // Same key - newer wins
                builder.Add(newerKey, newerValue);
                oi++;
                ni++;
            }
        }

        // Drain remaining
        while (oi < olderOffsets.Length)
        {
            Rsst.ReadEntry(olderData, olderOffsets[oi], out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
            builder.Add(key, value);
            oi++;
        }

        while (ni < newerOffsets.Length)
        {
            Rsst.ReadEntry(newerData, newerOffsets[ni], out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
            builder.Add(key, value);
            ni++;
        }

        return builder.Build();
    }

    private static int[] CollectEntryOffsets(ReadOnlySpan<byte> data, Rsst rsst)
    {
        if (rsst.EntryCount == 0) return [];

        int[] offsets = new int[rsst.EntryCount];
        int idx = 0;
        int rootOffset = BinaryPrimitives.ReadInt32LittleEndian(data[^Rsst.FooterSize..]);
        CollectOffsetsFromNode(data, rootOffset, offsets, ref idx);
        return offsets;
    }

    private static void CollectOffsetsFromNode(ReadOnlySpan<byte> data, int nodeOffset, int[] offsets, ref int idx)
    {
        byte nodeType = data[nodeOffset];
        int pos = nodeOffset + 1;

        if (nodeType == Rsst.LeafNodeType)
        {
            int numEntries = Leb128.Read(data, ref pos);
            for (int i = 0; i < numEntries; i++)
            {
                offsets[idx++] = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
                pos += 4;
            }
        }
        else
        {
            int numChildren = Leb128.Read(data, ref pos);
            int firstChild = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
            pos += 4;

            CollectOffsetsFromNode(data, firstChild, offsets, ref idx);

            for (int i = 1; i < numChildren; i++)
            {
                int sepKeyLen = Leb128.Read(data, ref pos);
                pos += sepKeyLen;
                int childOffset = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
                pos += 4;
                CollectOffsetsFromNode(data, childOffset, offsets, ref idx);
            }
        }
    }
}
