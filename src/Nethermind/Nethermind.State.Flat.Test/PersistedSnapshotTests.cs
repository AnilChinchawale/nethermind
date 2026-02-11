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

        // Verify RSST content directly
        Rsst.Rsst rsst = new(data);
        Assert.That(rsst.EntryCount, Is.EqualTo(1), "RSST should have 1 entry");

        // Build lookup key the same way PersistedSnapshot does
        byte[] lookupKey = new byte[1 + Address.Size + 32];
        lookupKey[0] = PersistedSnapshot.StorageTag;
        addr.Bytes.CopyTo(lookupKey.AsSpan(1));
        slot.ToBigEndian(lookupKey.AsSpan(1 + Address.Size));

        // Enumerate to see what key is actually stored
        foreach (Rsst.Rsst.KeyValueEntry entry in rsst)
        {
            byte[] storedKey = entry.Key.ToArray();
            Assert.That(storedKey.Length, Is.EqualTo(lookupKey.Length),
                $"Key lengths differ. Stored: {storedKey.Length}, Lookup: {lookupKey.Length}");
            Assert.That(storedKey, Is.EqualTo(lookupKey),
                $"Keys differ.\nStored: {BitConverter.ToString(storedKey)}\nLookup: {BitConverter.ToString(lookupKey)}");
        }

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

        PersistedSnapshot persisted = new(1, from, to, PersistedSnapshotType.Base, data);
        byte[]? loadedRlp = persisted.TryLoadStorageNodeRlp(address, path);
        Assert.That(loadedRlp, Is.Not.Null);
        Assert.That(loadedRlp, Is.EqualTo(nodeRlp));
    }

    [Test]
    public void NodeRef_ReadWrite_RoundTrip()
    {
        NodeRef original = new(42, 12345);
        byte[] buffer = new byte[NodeRef.Size];
        NodeRef.Write(buffer, original);
        NodeRef decoded = NodeRef.Read(buffer);

        Assert.That(decoded.SnapshotId, Is.EqualTo(42));
        Assert.That(decoded.EntryOffset, Is.EqualTo(12345));
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
