// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// Builds an RSST (Read-only Sorted String Table) from key-value entries.
/// Entries must be added in sorted key order or will be sorted internally.
/// </summary>
public sealed class RsstBuilder
{
    private readonly List<(byte[] Key, byte[] Value)> _entries = new();

    public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        _entries.Add((key.ToArray(), value.ToArray()));
    }

    public byte[] Build()
    {
        if (_entries.Count == 0) return BuildEmpty();

        _entries.Sort((a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        // Phase 1: Write entries to data region
        int dataSize = 0;
        int[] entryOffsets = new int[_entries.Count];
        for (int i = 0; i < _entries.Count; i++)
        {
            entryOffsets[i] = dataSize;
            dataSize += Leb128.EncodedSize(_entries[i].Key.Length) + _entries[i].Key.Length
                        + Leb128.EncodedSize(_entries[i].Value.Length) + _entries[i].Value.Length;
        }

        // Phase 2: Build B-tree index
        // Leaf nodes: groups of up to MaxLeafEntries entry offsets
        List<byte[]> nodeBytes = new();
        List<int> currentLevelOffsets = new();
        List<byte[]> currentLevelFirstKeys = new();
        List<byte[]> currentLevelLastKeys = new();

        // Build leaf nodes
        int entryIdx = 0;
        while (entryIdx < _entries.Count)
        {
            int count = Math.Min(Rsst.MaxLeafEntries, _entries.Count - entryIdx);
            byte[] leafFirstKey = _entries[entryIdx].Key;
            byte[] leafLastKey = _entries[entryIdx + count - 1].Key;

            int leafSize = 1 + Leb128.EncodedSize(count) + count * 4;
            byte[] leaf = new byte[leafSize];
            int pos = 0;
            leaf[pos++] = Rsst.LeafNodeType;
            pos = Leb128.Write(leaf, pos, count);
            for (int i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(leaf.AsSpan(pos), entryOffsets[entryIdx + i]);
                pos += 4;
            }

            currentLevelOffsets.Add(dataSize);
            currentLevelFirstKeys.Add(leafFirstKey);
            currentLevelLastKeys.Add(leafLastKey);
            dataSize += leaf.Length;
            nodeBytes.Add(leaf);

            entryIdx += count;
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

                // Separator is between last key of left child and first key of right child
                // Compute size: type(1) + numChildren(leb128) + first_child_offset(4)
                // + for each subsequent child: sep_key_len(leb128) + sep_key + child_offset(4)
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
        int writePos = 0;

        // Write entries
        for (int i = 0; i < _entries.Count; i++)
        {
            writePos = Leb128.Write(result, writePos, _entries[i].Key.Length);
            _entries[i].Key.CopyTo(result.AsSpan(writePos));
            writePos += _entries[i].Key.Length;
            writePos = Leb128.Write(result, writePos, _entries[i].Value.Length);
            _entries[i].Value.CopyTo(result.AsSpan(writePos));
            writePos += _entries[i].Value.Length;
        }

        // Write nodes
        foreach (byte[] node in nodeBytes)
        {
            node.CopyTo(result.AsSpan(writePos));
            writePos += node.Length;
        }

        // Write footer
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(writePos), rootOffset);
        writePos += 4;
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(writePos), _entries.Count);

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
                // right[0..i+1] is a prefix of right that is > left (since right[i] > left[i])
                // and <= right (since it's a prefix of right)
                return right[..(i + 1)];
            }
        }

        // One is a prefix of the other. Since left < right and they share minLen bytes,
        // right must be longer. Use right up to minLen+1.
        if (right.Length > minLen)
        {
            return right[..(minLen + 1)];
        }

        // They are equal (shouldn't happen with unique sorted keys), just return right
        return right.ToArray();
    }
}
