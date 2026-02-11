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

    [Test]
    public void NestedRsst_RoundTrip()
    {
        // Build inner RSST
        RsstBuilder innerBuilder = new();
        innerBuilder.Add([0x01, 0x02], [0xAA, 0xBB]);
        byte[] innerData = innerBuilder.Build();

        // Store as value in outer RSST
        RsstBuilder outerBuilder = new();
        outerBuilder.Add([0x00], innerData);
        byte[] outerData = outerBuilder.Build();

        // Read outer RSST
        Rsst.Rsst outer = new(outerData);
        Assert.That(outer.EntryCount, Is.EqualTo(1));
        Assert.That(outer.TryGet([0x00], out ReadOnlySpan<byte> columnData), Is.True);

        // columnData should be innerData
        Assert.That(columnData.ToArray(), Is.EqualTo(innerData), "Inner RSST bytes mismatch");

        // Read inner RSST from columnData
        Rsst.Rsst inner = new(columnData);
        Assert.That(inner.EntryCount, Is.EqualTo(1));
        Assert.That(inner.TryGet([0x01, 0x02], out ReadOnlySpan<byte> value), Is.True);
        Assert.That(value.ToArray(), Is.EqualTo(new byte[] { 0xAA, 0xBB }));
    }

    [Test]
    public void NestedRsst_MultipleColumns_RoundTrip()
    {
        // Simulate the columnar design: 5 columns with 1-byte tags
        byte[] addr = new byte[20];
        addr[0] = 0xAB;
        addr[19] = 0xCD;
        byte[] accountRlp = new byte[50];
        accountRlp[0] = 0xC0;
        for (int i = 1; i < 50; i++) accountRlp[i] = (byte)(i & 0xFF);

        // Build inner account RSST
        RsstBuilder accountBuilder = new();
        accountBuilder.Add(addr, accountRlp);
        byte[] accountsInner = accountBuilder.Build();

        // Build empty inner RSSTs for other columns
        byte[] emptyInner = new RsstBuilder().Build();

        // Build outer RSST with 5 columns
        RsstBuilder outerBuilder = new();
        outerBuilder.Add([0x00], accountsInner);
        outerBuilder.Add([0x01], emptyInner);
        outerBuilder.Add([0x02], emptyInner);
        outerBuilder.Add([0x03], emptyInner);
        outerBuilder.Add([0x04], emptyInner);
        byte[] outerData = outerBuilder.Build();

        // Read outer RSST
        Rsst.Rsst outer = new(outerData);
        Assert.That(outer.EntryCount, Is.EqualTo(5));

        // Look up accounts column
        Assert.That(outer.TryGet([0x00], out ReadOnlySpan<byte> columnData), Is.True);
        Assert.That(columnData.Length, Is.EqualTo(accountsInner.Length),
            $"Column data length {columnData.Length} != accounts inner {accountsInner.Length}");
        Assert.That(columnData.ToArray(), Is.EqualTo(accountsInner), "Column data mismatch");

        // Parse inner RSST
        Rsst.Rsst inner = new(columnData);
        Assert.That(inner.EntryCount, Is.EqualTo(1));
        Assert.That(inner.TryGet(addr, out ReadOnlySpan<byte> value), Is.True);
        Assert.That(value.ToArray(), Is.EqualTo(accountRlp));
    }

}
