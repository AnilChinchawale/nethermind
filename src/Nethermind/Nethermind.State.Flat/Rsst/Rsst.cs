// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// Read-only sorted string table. A compact binary format for persisted snapshots.
/// Layout: [entries...][B-tree index nodes...][footer: root_offset(4) | entry_count(4)]
/// </summary>
public readonly ref struct Rsst
{
    public const int FooterSize = 8; // root_offset(4) + entry_count(4)
    public const int MaxLeafEntries = 64;
    public const byte LeafNodeType = 0;
    public const byte InternalNodeType = 1;

    private readonly ReadOnlySpan<byte> _data;
    private readonly int _rootOffset;
    private readonly int _entryCount;

    public int EntryCount => _entryCount;
    public ReadOnlySpan<byte> Data => _data;

    public Rsst(ReadOnlySpan<byte> data)
    {
        _data = data;
        if (data.Length < FooterSize)
        {
            _rootOffset = 0;
            _entryCount = 0;
            return;
        }

        _rootOffset = BinaryPrimitives.ReadInt32LittleEndian(data[^FooterSize..]);
        _entryCount = BinaryPrimitives.ReadInt32LittleEndian(data[^4..]);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_entryCount == 0)
        {
            value = default;
            return false;
        }

        return SearchNode(_rootOffset, key, out value);
    }

    private bool SearchNode(int nodeOffset, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        byte nodeType = _data[nodeOffset];
        int pos = nodeOffset + 1;

        if (nodeType == LeafNodeType)
        {
            return SearchLeaf(pos, key, out value);
        }

        return SearchInternal(pos, key, out value);
    }

    private bool SearchLeaf(int pos, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        int numEntries = Leb128.Read(_data, ref pos);

        for (int i = 0; i < numEntries; i++)
        {
            int entryOffset = BinaryPrimitives.ReadInt32LittleEndian(_data[pos..]);
            pos += 4;

            int entryPos = entryOffset;
            int keyLen = Leb128.Read(_data, ref entryPos);
            ReadOnlySpan<byte> entryKey = _data.Slice(entryPos, keyLen);

            int cmp = key.SequenceCompareTo(entryKey);
            if (cmp == 0)
            {
                entryPos += keyLen;
                int valueLen = Leb128.Read(_data, ref entryPos);
                value = _data.Slice(entryPos, valueLen);
                return true;
            }

            if (cmp < 0)
            {
                // Keys are sorted, so if we've passed it, it's not here
                value = default;
                return false;
            }
        }

        value = default;
        return false;
    }

    private bool SearchInternal(int pos, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        int numChildren = Leb128.Read(_data, ref pos);

        // Read first child offset
        int childOffset = BinaryPrimitives.ReadInt32LittleEndian(_data[pos..]);
        pos += 4;

        // For each separator key, decide which child to descend into
        for (int i = 1; i < numChildren; i++)
        {
            int sepKeyLen = Leb128.Read(_data, ref pos);
            ReadOnlySpan<byte> sepKey = _data.Slice(pos, sepKeyLen);
            pos += sepKeyLen;

            int nextChildOffset = BinaryPrimitives.ReadInt32LittleEndian(_data[pos..]);
            pos += 4;

            if (key.SequenceCompareTo(sepKey) < 0)
            {
                return SearchNode(childOffset, key, out value);
            }

            childOffset = nextChildOffset;
        }

        return SearchNode(childOffset, key, out value);
    }

    /// <summary>
    /// Read a key-value entry at a given data offset.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadEntry(ReadOnlySpan<byte> data, int entryOffset, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        int pos = entryOffset;
        int keyLen = Leb128.Read(data, ref pos);
        key = data.Slice(pos, keyLen);
        pos += keyLen;
        int valueLen = Leb128.Read(data, ref pos);
        value = data.Slice(pos, valueLen);
    }

    public Enumerator GetEnumerator() => new(_data, _rootOffset, _entryCount);

    public ref struct Enumerator
    {
        private readonly ReadOnlySpan<byte> _data;
        private readonly int _entryCount;

        // We collect all leaf entry offsets during initialization by walking the tree
        private readonly int[] _entryOffsets;
        private int _currentIndex;

        public Enumerator(ReadOnlySpan<byte> data, int rootOffset, int entryCount)
        {
            _data = data;
            _entryCount = entryCount;
            _entryOffsets = new int[entryCount];
            _currentIndex = -1;

            if (entryCount > 0)
            {
                int idx = 0;
                CollectEntryOffsets(data, rootOffset, _entryOffsets, ref idx);
            }
        }

        private static void CollectEntryOffsets(ReadOnlySpan<byte> data, int nodeOffset, int[] offsets, ref int idx)
        {
            byte nodeType = data[nodeOffset];
            int pos = nodeOffset + 1;

            if (nodeType == LeafNodeType)
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

                CollectEntryOffsets(data, firstChild, offsets, ref idx);

                for (int i = 1; i < numChildren; i++)
                {
                    int sepKeyLen = Leb128.Read(data, ref pos);
                    pos += sepKeyLen;
                    int childOffset = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
                    pos += 4;
                    CollectEntryOffsets(data, childOffset, offsets, ref idx);
                }
            }
        }

        public bool MoveNext()
        {
            _currentIndex++;
            return _currentIndex < _entryCount;
        }

        public readonly KeyValueEntry Current
        {
            get
            {
                ReadEntry(_data, _entryOffsets[_currentIndex], out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);
                return new KeyValueEntry(key, value);
            }
        }
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
}
