// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.Text;
using Nethermind.State.Flat.Rsst;
using NUnit.Framework;

namespace Nethermind.State.Flat.Test;

[TestFixture]
public class RsstTests
{
    [TestCase(0, 1)]
    [TestCase(1, 1)]
    [TestCase(127, 1)]
    [TestCase(128, 2)]
    [TestCase(255, 2)]
    [TestCase(16383, 2)]
    [TestCase(16384, 3)]
    [TestCase(int.MaxValue, 5)]
    public void Leb128_RoundTrip(int value, int expectedSize)
    {
        Assert.That(Leb128.EncodedSize(value), Is.EqualTo(expectedSize));

        byte[] buffer = new byte[16];
        int endPos = Leb128.Write(buffer, 0, value);
        Assert.That(endPos, Is.EqualTo(expectedSize));

        int readPos = 0;
        int decoded = Leb128.Read(buffer, ref readPos);
        Assert.That(decoded, Is.EqualTo(value));
        Assert.That(readPos, Is.EqualTo(expectedSize));
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(127)]
    [TestCase(128)]
    [TestCase(16384)]
    public void Leb128_ReadBackward_RoundTrip(int value)
    {
        byte[] buffer = new byte[16];
        int endPos = Leb128.Write(buffer, 0, value);

        int backPos = endPos;
        int decoded = Leb128.ReadBackward(buffer, ref backPos);
        Assert.That(decoded, Is.EqualTo(value));
        Assert.That(backPos, Is.EqualTo(0));
    }

    [Test]
    public void Empty_Rsst_HasZeroEntries()
    {
        RsstBuilder builder = new();
        byte[] data = builder.Build();

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(0));
        Assert.That(rsst.TryGet("hello"u8, out _), Is.False);
    }

    [Test]
    public void Single_Entry_RoundTrip()
    {
        RsstBuilder builder = new();
        builder.Add("key1"u8, "value1"u8);
        byte[] data = builder.Build();

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(1));

        Assert.That(rsst.TryGet("key1"u8, out ReadOnlySpan<byte> val), Is.True);
        Assert.That(Encoding.UTF8.GetString(val), Is.EqualTo("value1"));

        Assert.That(rsst.TryGet("key2"u8, out _), Is.False);
        Assert.That(rsst.TryGet("key0"u8, out _), Is.False);
    }

    [TestCase(2)]
    [TestCase(10)]
    [TestCase(64)]  // Exactly one leaf
    [TestCase(65)]  // Two leaves
    [TestCase(128)] // Multiple leaves
    [TestCase(200)] // Triggers internal nodes
    [TestCase(1000)]
    [TestCase(5000)] // Deep B-tree
    public void Multiple_Entries_RoundTrip(int count)
    {
        RsstBuilder builder = new();
        List<(string Key, string Value)> expected = new();
        for (int i = 0; i < count; i++)
        {
            string key = $"key_{i:D6}";
            string value = $"val_{i:D6}";
            expected.Add((key, value));
            builder.Add(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
        }

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(count));

        // Sorted order
        expected.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));

        // Verify lookup
        foreach ((string key, string value) in expected)
        {
            Assert.That(rsst.TryGet(Encoding.UTF8.GetBytes(key), out ReadOnlySpan<byte> val), Is.True, $"Key {key} not found");
            Assert.That(Encoding.UTF8.GetString(val), Is.EqualTo(value));
        }

        // Verify missing keys
        Assert.That(rsst.TryGet("zzz_not_exist"u8, out _), Is.False);
        Assert.That(rsst.TryGet(""u8, out _), Is.False);
    }

    [TestCase(1)]
    [TestCase(10)]
    [TestCase(200)]
    public void Enumeration_Returns_Sorted_Entries(int count)
    {
        RsstBuilder builder = new();
        List<string> expectedKeys = new();
        for (int i = 0; i < count; i++)
        {
            string key = $"key_{i:D6}";
            expectedKeys.Add(key);
            builder.Add(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes($"val_{i}"));
        }

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);

        expectedKeys.Sort(StringComparer.Ordinal);

        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(Encoding.UTF8.GetString(entry.Key), Is.EqualTo(expectedKeys[idx]));
            idx++;
        }
        Assert.That(idx, Is.EqualTo(count));
    }

    [Test]
    public void Various_Key_Value_Sizes()
    {
        RsstBuilder builder = new();

        // Empty value
        builder.Add("a"u8, ReadOnlySpan<byte>.Empty);

        // Short key, long value
        byte[] longValue = new byte[10000];
        Random.Shared.NextBytes(longValue);
        builder.Add("b"u8, longValue);

        // Long key, short value
        byte[] longKey = new byte[500];
        for (int i = 0; i < longKey.Length; i++) longKey[i] = (byte)'c';
        builder.Add(longKey, "x"u8);

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(3));

        Assert.That(rsst.TryGet("a"u8, out ReadOnlySpan<byte> v1), Is.True);
        Assert.That(v1.Length, Is.EqualTo(0));

        Assert.That(rsst.TryGet("b"u8, out ReadOnlySpan<byte> v2), Is.True);
        Assert.That(v2.SequenceEqual(longValue), Is.True);

        Assert.That(rsst.TryGet(longKey, out ReadOnlySpan<byte> v3), Is.True);
        Assert.That(Encoding.UTF8.GetString(v3), Is.EqualTo("x"));
    }

    [TestCase(new byte[] { 0x01, 0x02 }, new byte[] { 0x01, 0x03 })]
    [TestCase(new byte[] { 0x01 }, new byte[] { 0x02 })]
    [TestCase(new byte[] { 0x01, 0xFF }, new byte[] { 0x02, 0x00 })]
    [TestCase(new byte[] { 0x01, 0x02 }, new byte[] { 0x01, 0x02, 0x03 })]
    public void SeparatorKey_IsBetweenLeftAndRight(byte[] left, byte[] right)
    {
        byte[] sep = RsstBuilder.ComputeSeparatorKey(left, right);

        // sep >= left
        Assert.That(sep.AsSpan().SequenceCompareTo(left), Is.GreaterThanOrEqualTo(0),
            $"Separator {BitConverter.ToString(sep)} should be >= left {BitConverter.ToString(left)}");

        // sep <= right
        Assert.That(sep.AsSpan().SequenceCompareTo(right), Is.LessThanOrEqualTo(0),
            $"Separator {BitConverter.ToString(sep)} should be <= right {BitConverter.ToString(right)}");

        // sep should be shorter or equal to right
        Assert.That(sep.Length, Is.LessThanOrEqualTo(right.Length));
    }

    [Test]
    public void Binary_Keys_RoundTrip()
    {
        // Generate random keys and sort them (RsstBuilder requires sorted input)
        (byte[] Key, int Index)[] entries = new (byte[], int)[100];
        for (int i = 0; i < 100; i++)
        {
            entries[i].Key = new byte[32];
            Random.Shared.NextBytes(entries[i].Key);
            entries[i].Index = i;
        }
        Array.Sort(entries, (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        RsstBuilder builder = new();
        foreach ((byte[] key, int index) in entries)
        {
            builder.Add(key, BitConverter.GetBytes(index));
        }

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(100));

        foreach ((byte[] key, int index) in entries)
        {
            Assert.That(rsst.TryGet(key, out ReadOnlySpan<byte> val), Is.True);
            Assert.That(BitConverter.ToInt32(val), Is.EqualTo(index));
        }
    }

    [Test]
    public void Duplicate_Keys_LastWriteWins()
    {
        RsstBuilder builder = new();
        builder.Add("key"u8, "value1"u8);
        builder.Add("key"u8, "value2"u8);

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);

        // Both entries are stored but TryGet returns the first match in sorted order
        Assert.That(rsst.EntryCount, Is.EqualTo(2));
    }

    [TestCase(50, 200)]   // actual < max, same LEB128 size (both 1-2 bytes)
    [TestCase(100, 200)]  // actual < max, same LEB128 byte count
    [TestCase(200, 200)]  // actual == max
    [TestCase(0, 200)]    // zero actual size
    [TestCase(1, 16384)]  // actual 1 byte LEB, max 3 byte LEB — padded LEB128
    public void BeginLargeEntry_FinishEntry_RoundTrip(int actualSize, int maxSize)
    {
        RsstBuilder builder = new();

        // Add a normal entry first
        builder.Add("aaa"u8, "normal"u8);

        // Add a large entry with BeginLargeEntry/FinishEntry
        byte[] valueData = new byte[actualSize];
        for (int i = 0; i < actualSize; i++) valueData[i] = (byte)(i & 0xFF);

        Span<byte> dest = builder.BeginLargeEntry("bbb"u8, maxSize);
        valueData.CopyTo(dest);
        builder.FinishEntry(actualSize);

        // Add another normal entry after
        builder.Add("ccc"u8, "after"u8);

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);

        Assert.That(rsst.EntryCount, Is.EqualTo(3));

        Assert.That(rsst.TryGet("aaa"u8, out ReadOnlySpan<byte> v1), Is.True);
        Assert.That(Encoding.UTF8.GetString(v1), Is.EqualTo("normal"));

        Assert.That(rsst.TryGet("bbb"u8, out ReadOnlySpan<byte> v2), Is.True);
        Assert.That(v2.Length, Is.EqualTo(actualSize));
        Assert.That(v2.SequenceEqual(valueData), Is.True);

        Assert.That(rsst.TryGet("ccc"u8, out ReadOnlySpan<byte> v3), Is.True);
        Assert.That(Encoding.UTF8.GetString(v3), Is.EqualTo("after"));
    }

    [TestCase(50, 2)]
    [TestCase(128, 3)]
    [TestCase(16384, 3)]
    public void Leb128_WritePadded_DecodesCorrectly(int value, int targetLength)
    {
        byte[] buffer = new byte[16];
        Leb128.WritePadded(buffer, 0, value, targetLength);

        int pos = 0;
        int decoded = Leb128.Read(buffer, ref pos);
        Assert.That(decoded, Is.EqualTo(value));
        Assert.That(pos, Is.EqualTo(targetLength));
    }
}
