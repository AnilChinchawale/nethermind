// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Runtime.CompilerServices;
using Nethermind.State.Flat.PersistedSnapshots;

namespace Nethermind.State.Flat;

/// <summary>
/// Lightweight managed-memory bloom filter for persisted snapshot key lookups.
/// Uses double hashing with MurmurHash3-inspired finalization.
/// </summary>
public sealed class SnapshotBloomFilter
{
    private const double DefaultBitsPerKey = 10.0;
    private const double Ln2 = 0.6931471805599453;

    private readonly byte[] _data;
    private readonly int _numHashFunctions;
    private readonly int _numBits;

    public int NumBits => _numBits;
    public int NumHashFunctions => _numHashFunctions;
    public ReadOnlySpan<byte> Data => _data;

    public SnapshotBloomFilter(int numEntries, double bitsPerKey = DefaultBitsPerKey)
    {
        _numBits = Math.Max(64, (int)(numEntries * bitsPerKey));
        _numHashFunctions = Math.Max(1, (int)(bitsPerKey * Ln2));
        _data = new byte[(_numBits + 7) / 8];
    }

    public SnapshotBloomFilter(byte[] data, int numHashFunctions, int numBits)
    {
        _data = data;
        _numHashFunctions = numHashFunctions;
        _numBits = numBits;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = Hash(key);
        uint bits = (uint)_numBits;
        for (int i = 0; i < _numHashFunctions; i++)
        {
            int bit = (int)((h1 + (uint)i * h2) % bits);
            _data[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MightContain(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = Hash(key);
        uint bits = (uint)_numBits;
        for (int i = 0; i < _numHashFunctions; i++)
        {
            int bit = (int)((h1 + (uint)i * h2) % bits);
            if ((_data[bit >> 3] & (1 << (bit & 7))) == 0) return false;
        }

        return true;
    }

    /// <summary>
    /// Build a bloom filter from columnar RSST data by enumerating all inner RSST keys.
    /// Bloom keys are [column tag byte] + [entity key] for uniqueness across columns.
    /// </summary>
    public static SnapshotBloomFilter BuildFromRsst(ReadOnlySpan<byte> rsstData, double bitsPerKey = DefaultBitsPerKey)
    {
        Rsst.Rsst outer = new(rsstData);

        // Count total entries across all inner RSSTs
        int totalEntries = 0;
        foreach (Rsst.Rsst.KeyValueEntry column in outer)
        {
            Rsst.Rsst inner = new(column.Value);
            totalEntries += inner.EntryCount;
        }

        SnapshotBloomFilter bloom = new(totalEntries, bitsPerKey);

        // Add all inner keys with column tag prefix for uniqueness
        // Max key: 1 (tag) + 65 (storage node: 32 addr + 32 path + 1 len) = 66 bytes
        Span<byte> bloomKey = stackalloc byte[66];
        foreach (Rsst.Rsst.KeyValueEntry column in outer)
        {
            Rsst.Rsst inner = new(column.Value);
            foreach (Rsst.Rsst.KeyValueEntry entry in inner)
            {
                int bloomKeyLen = column.Key.Length + entry.Key.Length;
                column.Key.CopyTo(bloomKey);
                entry.Key.CopyTo(bloomKey[column.Key.Length..]);
                bloom.Add(bloomKey[..bloomKeyLen]);
            }
        }

        return bloom;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (uint h1, uint h2) Hash(ReadOnlySpan<byte> key)
    {
        uint h1 = 0x971e137bu;
        uint h2 = 0x9747b28cu;
        foreach (byte b in key)
        {
            h1 = (h1 ^ b) * 0x01000193u;
            h2 = (h2 ^ b) * 0x01000193u;
        }

        // Finalize
        h1 ^= h1 >> 16;
        h1 *= 0x85ebca6bu;
        h1 ^= h1 >> 13;

        h2 ^= h2 >> 16;
        h2 *= 0xcc9e2d51u;
        h2 ^= h2 >> 13;

        return (h1, h2);
    }
}
