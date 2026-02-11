// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.State.Flat.PersistedSnapshots;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;
using NUnit.Framework;

namespace Nethermind.State.Flat.Test;

[TestFixture]
public class SnapshotBloomFilterTests
{
    private ResourcePool _pool = null!;

    [SetUp]
    public void SetUp() => _pool = new ResourcePool(new FlatDbConfig());

    [Test]
    public void AddAndQuery_InsertedKeysAreFound()
    {
        SnapshotBloomFilter bloom = new(100);

        byte[][] keys =
        [
            [0x00, 0x01, 0x02],
            [0x03, 0x04, 0x05],
            [0xFF, 0xFE, 0xFD],
        ];

        foreach (byte[] key in keys)
            bloom.Add(key);

        foreach (byte[] key in keys)
            Assert.That(bloom.MightContain(key), Is.True, $"Key {BitConverter.ToString(key)} should be found");
    }

    [Test]
    public void MightContain_EmptyBloom_ReturnsFalse()
    {
        SnapshotBloomFilter bloom = new(100);
        Assert.That(bloom.MightContain([0x01, 0x02]), Is.False);
    }

    [TestCase(100, 10.0)]
    [TestCase(1000, 10.0)]
    [TestCase(500, 8.0)]
    public void FalsePositiveRate_WithinExpectedBounds(int entryCount, double bitsPerKey)
    {
        SnapshotBloomFilter bloom = new(entryCount, bitsPerKey);

        // Insert entryCount keys
        for (int i = 0; i < entryCount; i++)
        {
            byte[] key = BitConverter.GetBytes(i);
            bloom.Add(key);
        }

        // Test with keys that were NOT inserted
        int falsePositives = 0;
        int testCount = 10000;
        for (int i = entryCount; i < entryCount + testCount; i++)
        {
            byte[] key = BitConverter.GetBytes(i);
            if (bloom.MightContain(key)) falsePositives++;
        }

        double fpRate = (double)falsePositives / testCount;
        // With 10 bits per key, theoretical FPR is ~0.8%. Allow generous margin.
        Assert.That(fpRate, Is.LessThan(0.05), $"False positive rate {fpRate:P2} is too high");
    }

    [Test]
    public void BuildFromRsst_ContainsAllKeys()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        content.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(100).TestObject;
        content.Accounts[TestItem.AddressB] = Build.An.Account.WithBalance(200).TestObject;
        TreePath path = new(Keccak.Compute("path"), 4);
        content.StateNodes[path] = new TrieNode(NodeType.Leaf, [0xC0, 0x80]);

        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] rsstData = PersistedSnapshotBuilder.Build(snap);

        SnapshotBloomFilter bloom = SnapshotBloomFilter.BuildFromRsst(rsstData);

        // Verify all inner RSST keys (with column prefix) are in the bloom
        Rsst.Rsst outerRsst = new(rsstData);
        foreach (Rsst.Rsst.KeyValueEntry column in outerRsst)
        {
            Rsst.Rsst innerRsst = new(column.Value);
            foreach (Rsst.Rsst.KeyValueEntry entry in innerRsst)
            {
                byte[] bloomKey = new byte[column.Key.Length + entry.Key.Length];
                column.Key.CopyTo(bloomKey);
                entry.Key.CopyTo(bloomKey.AsSpan(column.Key.Length));
                Assert.That(bloom.MightContain(bloomKey), Is.True,
                    $"Bloom filter missing key for column 0x{column.Key[0]:X2}");
            }
        }
    }

    [Test]
    public void PersistedSnapshot_WithBloom_StillFindsExistingKeys()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        content.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(100).TestObject;
        TreePath path = new(Keccak.Compute("path"), 4);
        content.StateNodes[path] = new TrieNode(NodeType.Leaf, [0xC0, 0x80, 0x80]);

        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snap);

        PersistedSnapshot persisted = new(1, s0, s1, PersistedSnapshotType.Base, data);
        persisted.BuildBloom();

        // Existing keys should still be found
        Assert.That(persisted.TryGetAccount(TestItem.AddressA), Is.Not.Null);
        Assert.That(persisted.TryLoadStateNodeRlp(path), Is.Not.Null);

        // Non-existing keys should return null (bloom rejects without RSST lookup)
        Assert.That(persisted.TryGetAccount(TestItem.AddressB), Is.Null);
        TreePath otherPath = new(Keccak.Compute("other"), 3);
        Assert.That(persisted.TryLoadStateNodeRlp(otherPath), Is.Null);
    }

    [Test]
    public void PersistedSnapshot_WithBloom_SelfDestruct()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        content.SelfDestructedStorageAddresses[TestItem.AddressA] = false;
        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snap);

        PersistedSnapshot persisted = new(1, s0, s1, PersistedSnapshotType.Base, data);
        persisted.BuildBloom();

        Assert.That(persisted.IsSelfDestructed(TestItem.AddressA), Is.True);
        Assert.That(persisted.IsSelfDestructed(TestItem.AddressB), Is.False);
    }

    [Test]
    public void PersistedSnapshot_WithBloom_StorageSlots()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        byte[] value = new byte[32];
        value[31] = 0x42;

        SnapshotContent content = new();
        content.Storages[(TestItem.AddressA, (UInt256)7)] = new SlotValue(value);
        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snap);

        PersistedSnapshot persisted = new(1, s0, s1, PersistedSnapshotType.Base, data);
        persisted.BuildBloom();

        Assert.That(persisted.TryGetSlot(TestItem.AddressA, (UInt256)7), Is.Not.Null);
        Assert.That(persisted.TryGetSlot(TestItem.AddressA, (UInt256)999), Is.Null);
        Assert.That(persisted.TryGetSlot(TestItem.AddressB, (UInt256)7), Is.Null);
    }

    [Test]
    public void PersistedSnapshot_WithBloom_StorageNodes()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        Hash256 address = Keccak.Compute("address");
        TreePath path = new(Keccak.Compute("path"), 6);
        byte[] nodeRlp = [0xC1, 0x80];

        SnapshotContent content = new();
        content.StorageNodes[(address, path)] = new TrieNode(NodeType.Branch, nodeRlp);
        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snap);

        PersistedSnapshot persisted = new(1, s0, s1, PersistedSnapshotType.Base, data);
        persisted.BuildBloom();

        Assert.That(persisted.TryLoadStorageNodeRlp(address, path), Is.EqualTo(nodeRlp));

        Hash256 otherAddr = Keccak.Compute("other_address");
        Assert.That(persisted.TryLoadStorageNodeRlp(otherAddr, path), Is.Null);
    }

    [Test]
    public void ConstructorFromData_ReconstructsFilter()
    {
        SnapshotBloomFilter original = new(100, 10.0);
        byte[] key = [0x01, 0x02, 0x03];
        original.Add(key);

        // Reconstruct from data
        SnapshotBloomFilter restored = new(original.Data.ToArray(), original.NumHashFunctions, original.NumBits);
        Assert.That(restored.MightContain(key), Is.True);
    }
}
