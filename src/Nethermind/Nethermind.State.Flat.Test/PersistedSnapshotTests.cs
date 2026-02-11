// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Flat.PersistedSnapshots;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;
using NUnit.Framework;

namespace Nethermind.State.Flat.Test;

[TestFixture]
public class PersistedSnapshotTests
{
    private ResourcePool _resourcePool = null!;

    [SetUp]
    public void SetUp()
    {
        _resourcePool = new ResourcePool(new FlatDbConfig());
    }

    private Snapshot CreateTestSnapshot(StateId from, StateId to, Address[]? accounts = null, (Address, UInt256, byte[])[]? storages = null)
    {
        SnapshotContent content = new();

        if (accounts is not null)
        {
            foreach (Address addr in accounts)
            {
                content.Accounts[addr] = Build.An.Account.WithBalance(1000).TestObject;
            }
        }

        if (storages is not null)
        {
            foreach ((Address addr, UInt256 slot, byte[] val) in storages)
            {
                content.Storages[(addr, slot)] = new SlotValue(val);
            }
        }

        return new Snapshot(from, to, content, _resourcePool, ResourcePool.Usage.MainBlockProcessing);
    }

    [Test]
    public void Account_RoundTrip()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));
        Address addr = TestItem.AddressA;

        Snapshot snapshot = CreateTestSnapshot(from, to, accounts: [addr]);
        byte[] data = PersistedSnapshotBuilder.Build(snapshot);

        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        byte[]? accountRlp = persisted.TryGetAccount(addr);
        Assert.That(accountRlp, Is.Not.Null);

        // Decode and verify
        Rlp.ValueDecoderContext ctx = new(accountRlp);
        Account decoded = AccountDecoder.Slim.Decode(ref ctx)!;
        Assert.That(decoded.Balance, Is.EqualTo((UInt256)1000));

        // Missing address
        Assert.That(persisted.TryGetAccount(TestItem.AddressB), Is.Null);
    }

    [Test]
    public void Storage_RoundTrip()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));
        Address addr = TestItem.AddressA;
        UInt256 slot = 42;
        byte[] value = new byte[32];
        value[31] = 0xFF;

        Snapshot snapshot = CreateTestSnapshot(from, to, storages: [(addr, slot, value)]);
        byte[] data = PersistedSnapshotBuilder.Build(snapshot);

        // Verify outer RSST has 5 columns
        Rsst.Rsst outerRsst = new(data);
        Assert.That(outerRsst.EntryCount, Is.EqualTo(5), "Outer RSST should have 5 column entries");

        // Verify nested structure: storage column → address-level RSST → inner RSST
        Assert.That(outerRsst.TryGet([PersistedSnapshot.StorageTag], out ReadOnlySpan<byte> storageColumn), Is.True);
        Rsst.Rsst addressRsst = new(storageColumn);
        Assert.That(addressRsst.EntryCount, Is.EqualTo(1), "Address-level RSST should have 1 address entry");

        // Verify address key and inner slot entry
        Assert.That(addressRsst.TryGet(addr.Bytes, out ReadOnlySpan<byte> innerData), Is.True);
        Rsst.Rsst innerRsst = new(innerData);
        Assert.That(innerRsst.EntryCount, Is.EqualTo(1), "Inner RSST should have 1 slot entry");

        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        byte[]? slotBytes = persisted.TryGetSlot(addr, slot);
        Assert.That(slotBytes, Is.Not.Null);
        Assert.That(slotBytes!.Length, Is.GreaterThan(0));

        // Missing slot
        Assert.That(persisted.TryGetSlot(addr, (UInt256)999), Is.Null);
        Assert.That(persisted.TryGetSlot(TestItem.AddressB, slot), Is.Null);
    }

    [Test]
    public void SelfDestruct_RoundTrip()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        content.SelfDestructedStorageAddresses[TestItem.AddressA] = false;
        Snapshot snapshot = new(from, to, content, _resourcePool, ResourcePool.Usage.MainBlockProcessing);

        byte[] data = PersistedSnapshotBuilder.Build(snapshot);
        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);

        Assert.That(persisted.IsSelfDestructed(TestItem.AddressA), Is.True);
        Assert.That(persisted.IsSelfDestructed(TestItem.AddressB), Is.False);
    }

    [Test]
    public void StateNode_RoundTrip()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        TreePath path = new(Keccak.Compute("path"), 4);
        byte[] nodeRlp = [0xC0, 0x80, 0x80]; // minimal RLP
        TrieNode node = new(NodeType.Leaf, nodeRlp);
        content.StateNodes[path] = node;

        Snapshot snapshot = new(from, to, content, _resourcePool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snapshot);

        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        byte[]? loadedRlp = persisted.TryLoadStateNodeRlp(path);
        Assert.That(loadedRlp, Is.Not.Null);
        Assert.That(loadedRlp, Is.EqualTo(nodeRlp));

        // Missing path
        TreePath otherPath = new(Keccak.Compute("other"), 3);
        Assert.That(persisted.TryLoadStateNodeRlp(otherPath), Is.Null);
    }

    [Test]
    public void StorageNode_RoundTrip()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));

        SnapshotContent content = new();
        Hash256 address = Keccak.Compute("address");
        TreePath path = new(Keccak.Compute("path"), 6);
        byte[] nodeRlp = [0xC1, 0x80];
        TrieNode node = new(NodeType.Branch, nodeRlp);
        content.StorageNodes[(address, path)] = node;

        Snapshot snapshot = new(from, to, content, _resourcePool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data = PersistedSnapshotBuilder.Build(snapshot);

        // Verify nested structure
        Rsst.Rsst outerRsst = new(data);
        Assert.That(outerRsst.TryGet([PersistedSnapshot.StorageNodeTag], out ReadOnlySpan<byte> snColumn), Is.True);
        Rsst.Rsst hashRsst = new(snColumn);
        Assert.That(hashRsst.EntryCount, Is.EqualTo(1), "Hash-level RSST should have 1 entry");
        Assert.That(hashRsst.TryGet(address.Bytes, out ReadOnlySpan<byte> innerData), Is.True);
        Rsst.Rsst innerRsst = new(innerData);
        Assert.That(innerRsst.EntryCount, Is.EqualTo(1), "Inner RSST should have 1 path entry");

        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        byte[]? loadedRlp = persisted.TryLoadStorageNodeRlp(address, path);
        Assert.That(loadedRlp, Is.Not.Null);
        Assert.That(loadedRlp, Is.EqualTo(nodeRlp));
    }

    [Test]
    public void Storage_MultipleAddresses_GroupedCorrectly()
    {
        StateId from = new(0, Keccak.EmptyTreeHash);
        StateId to = new(1, Keccak.Compute("1"));

        Address addrA = TestItem.AddressA;
        Address addrB = TestItem.AddressB;
        byte[] val1 = new byte[32]; val1[31] = 0x01;
        byte[] val2 = new byte[32]; val2[31] = 0x02;
        byte[] val3 = new byte[32]; val3[31] = 0x03;

        Snapshot snapshot = CreateTestSnapshot(from, to, storages:
        [
            (addrA, (UInt256)1, val1),
            (addrA, (UInt256)2, val2),
            (addrB, (UInt256)5, val3)
        ]);
        byte[] data = PersistedSnapshotBuilder.Build(snapshot);

        // Verify grouping: address-level RSST should have 2 entries
        Rsst.Rsst outerRsst = new(data);
        Assert.That(outerRsst.TryGet([PersistedSnapshot.StorageTag], out ReadOnlySpan<byte> storageColumn), Is.True);
        Rsst.Rsst addressRsst = new(storageColumn);
        Assert.That(addressRsst.EntryCount, Is.EqualTo(2), "Address-level RSST should have 2 address entries");

        // Verify inner slot counts
        Assert.That(addressRsst.TryGet(addrA.Bytes, out ReadOnlySpan<byte> innerA), Is.True);
        Assert.That(new Rsst.Rsst(innerA).EntryCount, Is.EqualTo(2), "AddressA inner RSST should have 2 slots");

        Assert.That(addressRsst.TryGet(addrB.Bytes, out ReadOnlySpan<byte> innerB), Is.True);
        Assert.That(new Rsst.Rsst(innerB).EntryCount, Is.EqualTo(1), "AddressB inner RSST should have 1 slot");

        // Verify round-trip reads
        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        Assert.That(persisted.TryGetSlot(addrA, (UInt256)1), Is.Not.Null);
        Assert.That(persisted.TryGetSlot(addrA, (UInt256)2), Is.Not.Null);
        Assert.That(persisted.TryGetSlot(addrB, (UInt256)5), Is.Not.Null);
        Assert.That(persisted.TryGetSlot(addrA, (UInt256)5), Is.Null);
        Assert.That(persisted.TryGetSlot(addrB, (UInt256)1), Is.Null);
    }

    [Test]
    public void Storage_NestedMerge_OverlappingAddresses()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        Address addrA = TestItem.AddressA;
        Address addrB = TestItem.AddressB;
        byte[] val1 = new byte[32]; val1[31] = 0x01;
        byte[] val2 = new byte[32]; val2[31] = 0x02;
        byte[] val3 = new byte[32]; val3[31] = 0x03;

        // Older: addrA slot 1 = val1, addrB slot 5 = val2
        Snapshot snap1 = CreateTestSnapshot(s0, s1, storages:
        [
            (addrA, (UInt256)1, val1),
            (addrB, (UInt256)5, val2)
        ]);
        byte[] data1 = PersistedSnapshotBuilder.Build(snap1);

        // Newer: addrA slot 1 = val3 (override), addrA slot 2 = val2 (new)
        Snapshot snap2 = CreateTestSnapshot(s1, s2, storages:
        [
            (addrA, (UInt256)1, val3),
            (addrA, (UInt256)2, val2)
        ]);
        byte[] data2 = PersistedSnapshotBuilder.Build(snap2);

        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, data1),
            new(1, s1, s2, PersistedSnapshotType.Base, data2)
        ]));
        PersistedSnapshot persisted = new(1, s0, s2, PersistedSnapshotType.Base, merged);

        // addrA slot 1 should be overridden to val3
        byte[]? slot1 = persisted.TryGetSlot(addrA, (UInt256)1);
        Assert.That(slot1, Is.Not.Null);
        Assert.That(slot1![0], Is.EqualTo(0x03));

        // addrA slot 2 should be val2 (from newer)
        byte[]? slot2 = persisted.TryGetSlot(addrA, (UInt256)2);
        Assert.That(slot2, Is.Not.Null);
        Assert.That(slot2![0], Is.EqualTo(0x02));

        // addrB slot 5 should be val2 (from older, carried through)
        byte[]? slot5 = persisted.TryGetSlot(addrB, (UInt256)5);
        Assert.That(slot5, Is.Not.Null);
        Assert.That(slot5![0], Is.EqualTo(0x02));
    }

    [Test]
    public void NodeRef_ReadWrite_RoundTrip()
    {
        NodeRef original = new(42, 12345);
        byte[] buffer = new byte[NodeRef.Size];
        NodeRef.Write(buffer, original);
        NodeRef decoded = NodeRef.Read(buffer);

        Assert.That(decoded.SnapshotId, Is.EqualTo(42));
        Assert.That(decoded.ValueLengthOffset, Is.EqualTo(12345));
    }

    [Test]
    public void PersistedSnapshotList_Queries_NewestFirst()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        TreePath path = new(Keccak.Compute("path"), 4);
        byte[] rlp1 = [0xC0];
        byte[] rlp2 = [0xC1, 0x80];

        SnapshotContent content1 = new();
        content1.StateNodes[path] = new TrieNode(NodeType.Leaf, rlp1);
        Snapshot snap1 = new(s0, s1, content1, _resourcePool, ResourcePool.Usage.MainBlockProcessing);

        SnapshotContent content2 = new();
        content2.StateNodes[path] = new TrieNode(NodeType.Leaf, rlp2);
        Snapshot snap2 = new(s1, s2, content2, _resourcePool, ResourcePool.Usage.MainBlockProcessing);

        byte[] data1 = PersistedSnapshotBuilder.Build(snap1);
        byte[] data2 = PersistedSnapshotBuilder.Build(snap2);

        PersistedSnapshot p1 = new(1, s0, s1, PersistedSnapshotType.Base, data1);
        PersistedSnapshot p2 = new(2, s1, s2, PersistedSnapshotType.Base, data2);

        // Ordered oldest-first; query newest-first via indexer
        PersistedSnapshotList list = new([p1, p2]);
        byte[]? result = null;
        for (int i = list.Count - 1; i >= 0; i--)
        {
            result = list[i].TryLoadStateNodeRlp(path);
            if (result is not null) break;
        }

        // Should return the newest (p2) value
        Assert.That(result, Is.EqualTo(rlp2));
    }
}
