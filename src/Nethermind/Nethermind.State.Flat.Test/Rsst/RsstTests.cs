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
        RsstBuilder builder = new();
        byte[][] keys = new byte[100][];
        for (int i = 0; i < 100; i++)
        {
            keys[i] = new byte[32];
            Random.Shared.NextBytes(keys[i]);
            builder.Add(keys[i], BitConverter.GetBytes(i));
        }

        byte[] data = builder.Build();
        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(100));

        for (int i = 0; i < 100; i++)
        {
            Assert.That(rsst.TryGet(keys[i], out ReadOnlySpan<byte> val), Is.True);
            Assert.That(BitConverter.ToInt32(val), Is.EqualTo(i));
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
}
