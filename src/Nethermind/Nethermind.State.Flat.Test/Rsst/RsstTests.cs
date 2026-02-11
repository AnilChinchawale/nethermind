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
        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) => { });

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(0));
        Assert.That(rsst.TryGet("hello"u8, out _), Is.False);
    }

    [Test]
    public void Single_Entry_RoundTrip()
    {
        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add("key1"u8, "value1"u8);
        });

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
        List<(string Key, string Value)> expected = new();
        for (int i = 0; i < count; i++)
        {
            string key = $"key_{i:D6}";
            string value = $"val_{i:D6}";
            expected.Add((key, value));
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((string key, string value) in expected)
            {
                builder.Add(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            }
        });

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
        List<(string Key, string Value)> entries = new();
        for (int i = 0; i < count; i++)
        {
            string key = $"key_{i:D6}";
            string value = $"val_{i}";
            entries.Add((key, value));
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((string key, string value) in entries)
            {
                builder.Add(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            }
        });

        List<string> expectedKeys = new();
        foreach ((string key, _) in entries)
        {
            expectedKeys.Add(key);
        }

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
        byte[] longValue = new byte[10000];
        Random.Shared.NextBytes(longValue);
        byte[] longKey = new byte[500];
        for (int i = 0; i < longKey.Length; i++) longKey[i] = (byte)'c';

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            // Empty value
            builder.Add("a"u8, ReadOnlySpan<byte>.Empty);

            // Short key, long value
            builder.Add("b"u8, longValue);

            // Long key, short value
            builder.Add(longKey, "x"u8);
        });

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(3));

        Assert.That(rsst.TryGet("a"u8, out ReadOnlySpan<byte> v1), Is.True);
        Assert.That(v1.Length, Is.EqualTo(0));

        Assert.That(rsst.TryGet("b"u8, out ReadOnlySpan<byte> v2), Is.True);
        Assert.That(v2.SequenceEqual(longValue), Is.True);

        Assert.That(rsst.TryGet(longKey, out ReadOnlySpan<byte> v3), Is.True);
        Assert.That(Encoding.UTF8.GetString(v3), Is.EqualTo("x"));
    }

    [TestCase(100, 42)]
    [TestCase(1000, 123)]
    [TestCase(5000, 999)]
    public void Binary_Keys_RoundTrip(int count, int seed)
    {
        // Generate random 32-byte keys and 32-byte values, then sort (RsstBuilder requires sorted input)
        Random rng = new(seed);
        (byte[] Key, byte[] Value)[] entries = new (byte[], byte[])[count];
        for (int i = 0; i < count; i++)
        {
            entries[i].Key = new byte[32];
            entries[i].Value = new byte[32];
            rng.NextBytes(entries[i].Key);
            rng.NextBytes(entries[i].Value);
        }
        Array.Sort(entries, (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((byte[] key, byte[] value) in entries)
            {
                builder.Add(key, value);
            }
        });

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(count));

        foreach ((byte[] key, byte[] value) in entries)
        {
            Assert.That(rsst.TryGet(key, out ReadOnlySpan<byte> val), Is.True);
            Assert.That(val.SequenceEqual(value), Is.True);
        }

        // Verify enumeration returns all entries in sorted order
        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(entry.Key.SequenceEqual(entries[idx].Key), Is.True);
            Assert.That(entry.Value.SequenceEqual(entries[idx].Value), Is.True);
            idx++;
        }
        Assert.That(idx, Is.EqualTo(count));
    }

    /// <summary>
    /// Regression test for internal node boundary separator bug.
    /// With small leaf size (4), entry 11 (last in Leaf 2) has key 0x7A8B29... and its
    /// separator is just 0x7A (1 byte — enough to distinguish from entry 10's 0x738F...).
    /// Entry 12 (first in Leaf 3) has key 0x7A8B49... with separator 0x7A8B49 (3 bytes).
    /// The internal node boundary between Leaf 2 and 3 was incorrectly computed as 0x7A8B
    /// (2 bytes) from the truncated separators. Since 0x7A8B29... > 0x7A8B, the search
    /// for entry 11's key incorrectly routed to Leaf 3.
    /// </summary>
    [Test]
    public void Binary_Keys_SmallLeaf_RoundTrip()
    {
        // 8 sorted 32-byte keys with maxLeafEntries=4. The bug is at the Leaf 0/1 boundary:
        // Entry 3 (Leaf 0 end): key=0x7A8B29... sep=0x7A (1 byte, distinguishes from entry 2's 0x738F...)
        // Entry 4 (Leaf 1 start): key=0x7A8B49... sep=0x7A8B49 (3 bytes, distinguishes from entry 3)
        // The buggy boundary separator was 0x7A8B (2 bytes) — too short, since 0x7A8B29... > 0x7A8B.
        (string Key, string Value)[] hexEntries =
        [
            ("6C3A850F2A4303CEBEFC75F9B169ACB5A07E12F84F6CC55DFAFC9AE609EED608", "F9FF8903DBBD1C853B1890B3CA2C73D23739913597EB1C007527152EA91CC4D0"), // Leaf 0
            ("7374A05BF4BBD243F66331CF6F11E06DFC3D3E8BCD6D3658B8C0B76651D29E34", "193CACB56E5C0B2B740A2023E46F7C99C75BC73062FC90063D47A233046CF123"),
            ("738F9ED9F043D768AFD784BD11F7C9018A8EFE476FB3B01D804B4E0BDB1652BE", "A49E2265C7C899BDC359B364BDCFD53F77AA2A981978C5BFDF8058A5F5CB8C99"),
            ("7A8B29876DFAC78D26FC5F3831BAB1F4C60DFBEDD136B05BA4A8A56CF9E44C2D", "9DD3F80D7D63230198B8A8FEBCD81AA48CFC616F5628F343DBCEE3C5555B9442"), // ← Leaf 0 end: sep=0x7A
            ("7A8B49E56B67F911A381C08315CD3629A3F325C7C3E0C1706C14D6C9CAF8367D", "15A35D6966D927BAAE1E43B59C2AB552B76FCFE9CE8A3D99CAD97957903047AB"), // ← Leaf 1 start: sep=0x7A8B49
            ("82B8686069E521734064E0BB203C6C6C014F8ECBC90977A28F1B637D0BE0370E", "DAEF0267D21A77A154992BE299ACD41BFB14E494EBC37D7841C5D04E81A3685F"), // Leaf 1
            ("84C61872D56339C1F4418316004B5FB0750E9430EBB9A52BD96286466FF4C7F8", "CC1ADFF7B7636A137068A3D7F4AFBF9321A730E7375CADCB20ED9972DDF35200"),
            ("9A3F37BBBE6820FE83BE2B55F78AC9B64FA4C24637B0A6A0B7203DA68728A5CC", "CB7EDAB045ACA26B99923FF2F17B9A8720E015B5603CD8EA9896049D2B79775A"),
        ];

        byte[] HexToBytes(string hex)
        {
            byte[] bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
                bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            return bytes;
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((string key, string value) in hexEntries)
                builder.Add(HexToBytes(key), HexToBytes(value));
        }, maxLeafEntries: 4);

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(hexEntries.Length));

        foreach ((string key, string value) in hexEntries)
        {
            byte[] keyBytes = HexToBytes(key);
            Assert.That(rsst.TryGet(keyBytes, out ReadOnlySpan<byte> val), Is.True, $"Key {key} not found");
            Assert.That(val.SequenceEqual(HexToBytes(value)), Is.True);
        }

        // Verify enumeration returns all entries in sorted order
        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(entry.Key.SequenceEqual(HexToBytes(hexEntries[idx].Key)), Is.True);
            Assert.That(entry.Value.SequenceEqual(HexToBytes(hexEntries[idx].Value)), Is.True);
            idx++;
        }
        Assert.That(idx, Is.EqualTo(hexEntries.Length));
    }

    // maxLeafEntries=4, 100 entries → 25 leaves → 7 internal → 2 level-2 → 1 root (4 levels)
    // maxLeafEntries=4, 300 entries → 75 leaves → 19 internal → 5 level-2 → 2 level-3 → 1 root (5 levels)
    [TestCase(100, 4, 32, 32, 42)]
    [TestCase(300, 4, 32, 32, 77)]
    // Variable-length keys (1–64 bytes) and values (0–128 bytes) stress separator computation
    [TestCase(200, 4, 64, 128, 55)]
    [TestCase(500, 8, 64, 128, 101)]
    [TestCase(1000, 64, 64, 128, 202)]
    public void Binary_Keys_MultiLevel_And_VariableSize_RoundTrip(int count, int maxLeafEntries, int maxKeyLen, int maxValLen, int seed)
    {
        Random rng = new(seed);
        (byte[] Key, byte[] Value)[] entries = new (byte[], byte[])[count];
        for (int i = 0; i < count; i++)
        {
            int keyLen = rng.Next(1, maxKeyLen + 1);
            int valLen = rng.Next(0, maxValLen + 1);
            entries[i].Key = new byte[keyLen];
            entries[i].Value = new byte[valLen];
            rng.NextBytes(entries[i].Key);
            rng.NextBytes(entries[i].Value);
        }
        Array.Sort(entries, (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        // Deduplicate — keep last value for duplicate keys (sorted, so adjacent)
        List<(byte[] Key, byte[] Value)> deduped = new(count);
        for (int i = 0; i < entries.Length; i++)
        {
            if (i + 1 < entries.Length && entries[i].Key.AsSpan().SequenceEqual(entries[i + 1].Key))
                continue;
            deduped.Add(entries[i]);
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((byte[] key, byte[] value) in deduped)
                builder.Add(key, value);
        }, maxLeafEntries);

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(deduped.Count));

        foreach ((byte[] key, byte[] value) in deduped)
        {
            Assert.That(rsst.TryGet(key, out ReadOnlySpan<byte> val), Is.True,
                $"Key {BitConverter.ToString(key)} not found");
            Assert.That(val.SequenceEqual(value), Is.True);
        }

        // Verify enumeration returns all entries in sorted order
        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(entry.Key.SequenceEqual(deduped[idx].Key), Is.True);
            Assert.That(entry.Value.SequenceEqual(deduped[idx].Value), Is.True);
            idx++;
        }
        Assert.That(idx, Is.EqualTo(deduped.Count));
    }

    [TestCase(100, 32, 32, 42, 0)]
    [TestCase(100, 32, 32, 42, 2)]
    [TestCase(100, 32, 32, 42, 30)]
    [TestCase(200, 20, 64, 55, 18)]
    [TestCase(500, 52, 32, 101, 50)]
    public void Binary_Keys_WithExtraSeparatorLength_RoundTrip(int count, int keyLen, int maxValLen, int seed, int extraSepLen)
    {
        Random rng = new(seed);
        (byte[] Key, byte[] Value)[] entries = new (byte[], byte[])[count];
        for (int i = 0; i < count; i++)
        {
            entries[i].Key = new byte[keyLen];
            entries[i].Value = new byte[rng.Next(0, maxValLen + 1)];
            rng.NextBytes(entries[i].Key);
            rng.NextBytes(entries[i].Value);
        }
        Array.Sort(entries, (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        // Deduplicate — keep last value for duplicate keys
        List<(byte[] Key, byte[] Value)> deduped = new(count);
        for (int i = 0; i < entries.Length; i++)
        {
            if (i + 1 < entries.Length && entries[i].Key.AsSpan().SequenceEqual(entries[i + 1].Key))
                continue;
            deduped.Add(entries[i]);
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((byte[] key, byte[] value) in deduped)
                builder.Add(key, value);
        }, extraSeparatorLength: extraSepLen);

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(deduped.Count));

        // Positive lookups
        foreach ((byte[] key, byte[] value) in deduped)
        {
            Assert.That(rsst.TryGet(key, out ReadOnlySpan<byte> val), Is.True,
                $"Key {BitConverter.ToString(key)} not found");
            Assert.That(val.SequenceEqual(value), Is.True);
        }

        // Negative lookups — 50 random non-existent keys
        HashSet<byte[]> existingKeys = new(deduped.ConvertAll(e => e.Key), new ByteArrayComparer());
        Random negRng = new(seed + 9999);
        int negChecked = 0;
        while (negChecked < 50)
        {
            byte[] randomKey = new byte[keyLen];
            negRng.NextBytes(randomKey);
            if (existingKeys.Contains(randomKey)) continue;
            Assert.That(rsst.TryGet(randomKey, out _), Is.False,
                $"Non-existent key {BitConverter.ToString(randomKey)} falsely found");
            negChecked++;
        }

        // Enumeration order
        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(entry.Key.SequenceEqual(deduped[idx].Key), Is.True);
            Assert.That(entry.Value.SequenceEqual(deduped[idx].Value), Is.True);
            idx++;
        }
        Assert.That(idx, Is.EqualTo(deduped.Count));
    }

    [TestCase(100, 4, 32, 32, 42, 30)]
    [TestCase(300, 4, 32, 32, 77, 30)]
    public void Binary_Keys_MultiLevel_WithExtraSeparatorLength_RoundTrip(int count, int maxLeaf, int keyLen, int maxValLen, int seed, int extraSepLen)
    {
        Random rng = new(seed);
        (byte[] Key, byte[] Value)[] entries = new (byte[], byte[])[count];
        for (int i = 0; i < count; i++)
        {
            entries[i].Key = new byte[keyLen];
            entries[i].Value = new byte[rng.Next(0, maxValLen + 1)];
            rng.NextBytes(entries[i].Key);
            rng.NextBytes(entries[i].Value);
        }
        Array.Sort(entries, (a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));

        // Deduplicate
        List<(byte[] Key, byte[] Value)> deduped = new(count);
        for (int i = 0; i < entries.Length; i++)
        {
            if (i + 1 < entries.Length && entries[i].Key.AsSpan().SequenceEqual(entries[i + 1].Key))
                continue;
            deduped.Add(entries[i]);
        }

        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            foreach ((byte[] key, byte[] value) in deduped)
                builder.Add(key, value);
        }, maxLeafEntries: maxLeaf, extraSeparatorLength: extraSepLen);

        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(deduped.Count));

        // Positive lookups
        foreach ((byte[] key, byte[] value) in deduped)
        {
            Assert.That(rsst.TryGet(key, out ReadOnlySpan<byte> val), Is.True,
                $"Key {BitConverter.ToString(key)} not found");
            Assert.That(val.SequenceEqual(value), Is.True);
        }

        // Negative lookups
        HashSet<byte[]> existingKeys = new(deduped.ConvertAll(e => e.Key), new ByteArrayComparer());
        Random negRng = new(seed + 9999);
        int negChecked = 0;
        while (negChecked < 50)
        {
            byte[] randomKey = new byte[keyLen];
            negRng.NextBytes(randomKey);
            if (existingKeys.Contains(randomKey)) continue;
            Assert.That(rsst.TryGet(randomKey, out _), Is.False,
                $"Non-existent key {BitConverter.ToString(randomKey)} falsely found");
            negChecked++;
        }

        // Enumeration order
        int idx = 0;
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            Assert.That(entry.Key.SequenceEqual(deduped[idx].Key), Is.True);
            Assert.That(entry.Value.SequenceEqual(deduped[idx].Value), Is.True);
            idx++;
        }
        Assert.That(idx, Is.EqualTo(deduped.Count));
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? x, byte[]? y) =>
            x is not null && y is not null && x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            HashCode hash = new();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }

    [Test]
    public void Duplicate_Keys_LastWriteWins()
    {
        byte[] data = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add("key"u8, "value1"u8);
            builder.Add("key"u8, "value2"u8);
        });

        Rsst.Rsst rsst = new(data);

        // Both entries are stored but TryGet returns the first match in sorted order
        Assert.That(rsst.EntryCount, Is.EqualTo(2));
    }

    [Test]
    public void NestedRsst_RoundTrip()
    {
        // Build inner RSST
        byte[] innerData = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add([0x01, 0x02], [0xAA, 0xBB]);
        });

        // Store as value in outer RSST
        byte[] outerData = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add([0x00], innerData);
        });

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
        byte[] accountsInner = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add(addr, accountRlp);
        });

        // Build empty inner RSSTs for other columns
        byte[] emptyInner = RsstTestUtil.BuildToArray((ref RsstBuilder builder) => { });

        // Build outer RSST with 5 columns
        byte[] outerData = RsstTestUtil.BuildToArray((ref RsstBuilder builder) =>
        {
            builder.Add([0x00], accountsInner);
            builder.Add([0x01], emptyInner);
            builder.Add([0x02], emptyInner);
            builder.Add([0x03], emptyInner);
            builder.Add([0x04], emptyInner);
        });

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
