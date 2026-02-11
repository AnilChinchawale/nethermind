// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.State.Flat.PersistedSnapshots;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;
using NSubstitute;
using NUnit.Framework;

namespace Nethermind.State.Flat.Test;

[TestFixture]
public class PersistenceManagerPersistedTests
{
    private string _testDir = null!;
    private ResourcePool _pool = null!;

    [SetUp]
    public void SetUp()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"nethermind_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _pool = new ResourcePool(new FlatDbConfig());
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testDir))
            Directory.Delete(_testDir, recursive: true);
    }

    [Test]
    public void MergeSnapshotData_NewerOverridesOlder()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        TreePath path = new(Keccak.Compute("path"), 4);
        byte[] rlpOlder = [0xC0];
        byte[] rlpNewer = [0xC1, 0x80];

        // Build older snapshot
        SnapshotContent content1 = new();
        content1.StateNodes[path] = new TrieNode(NodeType.Leaf, rlpOlder);
        content1.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(100).TestObject;
        Snapshot snap1 = new(s0, s1, content1, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] olderData = PersistedSnapshotBuilder.Build(snap1);

        // Build newer snapshot with same path but different value, and a new account
        SnapshotContent content2 = new();
        content2.StateNodes[path] = new TrieNode(NodeType.Leaf, rlpNewer);
        content2.Accounts[TestItem.AddressB] = Build.An.Account.WithBalance(200).TestObject;
        Snapshot snap2 = new(s1, s2, content2, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] newerData = PersistedSnapshotBuilder.Build(snap2);

        // Merge
        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, olderData),
            new(1, s1, s2, PersistedSnapshotType.Base, newerData)
        ]));

        // Create PersistedSnapshot from merged data
        PersistedSnapshot mergedSnapshot = new(2, s0, s2, PersistedSnapshotType.Compacted, merged);

        // State node should have newer value
        byte[]? nodeRlp = mergedSnapshot.TryLoadStateNodeRlp(path);
        Assert.That(nodeRlp, Is.EqualTo(rlpNewer));

        // Both accounts should be present
        Assert.That(mergedSnapshot.TryGetAccount(TestItem.AddressA), Is.Not.Null);
        Assert.That(mergedSnapshot.TryGetAccount(TestItem.AddressB), Is.Not.Null);
    }

    [Test]
    public void MergeSnapshotData_PreservesNonOverlappingEntries()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        TreePath path1 = new(Keccak.Compute("path1"), 4);
        TreePath path2 = new(Keccak.Compute("path2"), 4);
        byte[] rlp1 = [0xC0];
        byte[] rlp2 = [0xC1, 0x80];

        SnapshotContent content1 = new();
        content1.StateNodes[path1] = new TrieNode(NodeType.Leaf, rlp1);
        Snapshot snap1 = new(s0, s1, content1, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data1 = PersistedSnapshotBuilder.Build(snap1);

        SnapshotContent content2 = new();
        content2.StateNodes[path2] = new TrieNode(NodeType.Leaf, rlp2);
        Snapshot snap2 = new(s1, s2, content2, _pool, ResourcePool.Usage.MainBlockProcessing);
        byte[] data2 = PersistedSnapshotBuilder.Build(snap2);

        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, data1),
            new(1, s1, s2, PersistedSnapshotType.Base, data2)
        ]));
        PersistedSnapshot mergedSnapshot = new(2, s0, s2, PersistedSnapshotType.Compacted, merged);

        // Both paths should be present
        Assert.That(mergedSnapshot.TryLoadStateNodeRlp(path1), Is.EqualTo(rlp1));
        Assert.That(mergedSnapshot.TryLoadStateNodeRlp(path2), Is.EqualTo(rlp2));
    }

    [Test]
    public void ConvertToPersistedSnapshot_PersistsViaManager()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096);
        repo.LoadFromCatalog();

        IFlatDbConfig config = new FlatDbConfig();
        PersistedSnapshotManager manager = new(repo, config, LimboLogs.Instance);

        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        SnapshotContent content = new();
        content.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(500).TestObject;
        Snapshot snap = new(s0, s1, content, _pool, ResourcePool.Usage.MainBlockProcessing);

        manager.ConvertToPersistedSnapshot(snap);

        Assert.That(repo.SnapshotCount, Is.EqualTo(1));

        using PersistedSnapshotList list = repo.AssembleSnapshots(s1, s0);
        Assert.That(list.Count, Is.EqualTo(1));
    }

    [Test]
    public void TryCompactPersistedSnapshots_MergesMultipleBaseSnapshots()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 64 * 1024);
        repo.LoadFromCatalog();

        // CompactSize=4, MinCompactSize=2 so compaction triggers at block 4
        IFlatDbConfig config = new FlatDbConfig { CompactSize = 4, MinCompactSize = 2 };
        PersistedSnapshotManager manager = new(repo, config, LimboLogs.Instance);

        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));
        StateId s3 = new(3, Keccak.Compute("3"));
        StateId s4 = new(4, Keccak.Compute("4"));

        // Create 4 consecutive base snapshots with different accounts
        SnapshotContent c1 = new();
        c1.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(100).TestObject;
        manager.ConvertToPersistedSnapshot(new Snapshot(s0, s1, c1, _pool, ResourcePool.Usage.MainBlockProcessing));

        SnapshotContent c2 = new();
        c2.Accounts[TestItem.AddressB] = Build.An.Account.WithBalance(200).TestObject;
        manager.ConvertToPersistedSnapshot(new Snapshot(s1, s2, c2, _pool, ResourcePool.Usage.MainBlockProcessing));

        SnapshotContent c3 = new();
        c3.Accounts[TestItem.AddressC] = Build.An.Account.WithBalance(300).TestObject;
        manager.ConvertToPersistedSnapshot(new Snapshot(s2, s3, c3, _pool, ResourcePool.Usage.MainBlockProcessing));

        SnapshotContent c4 = new();
        c4.Accounts[TestItem.AddressD] = Build.An.Account.WithBalance(400).TestObject;
        manager.ConvertToPersistedSnapshot(new Snapshot(s3, s4, c4, _pool, ResourcePool.Usage.MainBlockProcessing));

        // Compaction should have been triggered at block 4 (4 & -4 == 4 >= MinCompactSize=2)
        // Verify compacted snapshot exists and contains all data
        using PersistedSnapshotList list = repo.AssembleSnapshots(s4, s0);
        Assert.That(list.Count, Is.GreaterThanOrEqualTo(1));

        // The compacted snapshot should have all 4 accounts accessible
        PersistedSnapshot compacted = list[0];
        Assert.That(compacted.From, Is.EqualTo(s0));
        Assert.That(compacted.TryGetAccount(TestItem.AddressA), Is.Not.Null);
        Assert.That(compacted.TryGetAccount(TestItem.AddressB), Is.Not.Null);
        Assert.That(compacted.TryGetAccount(TestItem.AddressC), Is.Not.Null);
        Assert.That(compacted.TryGetAccount(TestItem.AddressD), Is.Not.Null);
    }

    [Test]
    public void SelfDestructMerge_DestructedAddressClearsOlderStorage()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        // Older: storage for addrA slot 1
        SnapshotContent older = new();
        older.Storages[(TestItem.AddressA, 1)] = new SlotValue(new byte[] { 0x42 });
        byte[] olderData = PersistedSnapshotBuilder.Build(new Snapshot(s0, s1, older, _pool, ResourcePool.Usage.MainBlockProcessing));

        // Newer: self-destruct=false for addrA (destructed), new storage for addrA slot 2
        SnapshotContent newer = new();
        newer.SelfDestructedStorageAddresses[TestItem.AddressA] = false; // destructed
        newer.Storages[(TestItem.AddressA, 2)] = new SlotValue(new byte[] { 0x99 });
        byte[] newerData = PersistedSnapshotBuilder.Build(new Snapshot(s1, s2, newer, _pool, ResourcePool.Usage.MainBlockProcessing));

        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, olderData),
            new(1, s1, s2, PersistedSnapshotType.Base, newerData)
        ]));
        PersistedSnapshot result = new(2, s0, s2, PersistedSnapshotType.Compacted, merged);

        // Slot 1 from older should be gone (address was destructed)
        Assert.That(result.TryGetSlot(TestItem.AddressA, 1), Is.Null);
        // Slot 2 from newer should be present (added after self-destruct)
        Assert.That(result.TryGetSlot(TestItem.AddressA, 2), Is.Not.Null);
        // Self-destruct flag should be false (destructed)
        Assert.That(result.TryGetSelfDestructFlag(TestItem.AddressA), Is.EqualTo(false));
    }

    [Test]
    public void SelfDestructMerge_NewAccountDoesNotOverwriteDestructFlag()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        // Older: self-destruct=false for addrA (destructed)
        SnapshotContent older = new();
        older.SelfDestructedStorageAddresses[TestItem.AddressA] = false; // destructed
        byte[] olderData = PersistedSnapshotBuilder.Build(new Snapshot(s0, s1, older, _pool, ResourcePool.Usage.MainBlockProcessing));

        // Newer: self-destruct=true for addrA (new account, TryAdd should not overwrite)
        SnapshotContent newer = new();
        newer.SelfDestructedStorageAddresses[TestItem.AddressA] = true; // new account
        byte[] newerData = PersistedSnapshotBuilder.Build(new Snapshot(s1, s2, newer, _pool, ResourcePool.Usage.MainBlockProcessing));

        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, olderData),
            new(1, s1, s2, PersistedSnapshotType.Base, newerData)
        ]));
        PersistedSnapshot result = new(2, s0, s2, PersistedSnapshotType.Compacted, merged);

        // TryAdd semantics: older (false/destructed) should be preserved
        Assert.That(result.TryGetSelfDestructFlag(TestItem.AddressA), Is.EqualTo(false));
    }

    [Test]
    public void SelfDestructMerge_StorageNodesNotAffected()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        Hash256 addrHash = Keccak.Compute(TestItem.AddressA.Bytes);
        TreePath storagePath = new(Keccak.Compute("storage_path"), 4);
        byte[] nodeRlp = [0xC1, 0x80];

        // Older: storage trie node for addrA
        SnapshotContent older = new();
        older.StorageNodes[(addrHash, storagePath)] = new TrieNode(NodeType.Leaf, nodeRlp);
        byte[] olderData = PersistedSnapshotBuilder.Build(new Snapshot(s0, s1, older, _pool, ResourcePool.Usage.MainBlockProcessing));

        // Newer: self-destruct=false for addrA (destructed), but no storage nodes changes
        SnapshotContent newer = new();
        newer.SelfDestructedStorageAddresses[TestItem.AddressA] = false; // destructed
        byte[] newerData = PersistedSnapshotBuilder.Build(new Snapshot(s1, s2, newer, _pool, ResourcePool.Usage.MainBlockProcessing));

        byte[] merged = PersistedSnapshotManager.MergeSnapshots(new PersistedSnapshotList([
            new(0, s0, s1, PersistedSnapshotType.Base, olderData),
            new(1, s1, s2, PersistedSnapshotType.Base, newerData)
        ]));
        PersistedSnapshot result = new(2, s0, s2, PersistedSnapshotType.Compacted, merged);

        // Storage trie nodes should still be present (not affected by self-destruct)
        Assert.That(result.TryLoadStorageNodeRlp(addrHash, storagePath), Is.EqualTo(nodeRlp));
    }

    [Test]
    public void PrunePersistedSnapshots_RemovesOldSnapshots()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096);
        repo.LoadFromCatalog();

        IFlatDbConfig config = new FlatDbConfig();
        PersistedSnapshotManager manager = new(repo, config, LimboLogs.Instance);

        // Persist snapshots at various block heights
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s3 = new(3, Keccak.Compute("3"));
        StateId s6 = new(6, Keccak.Compute("6"));

        SnapshotContent c1 = new();
        c1.Accounts[TestItem.AddressA] = Build.An.Account.WithBalance(1).TestObject;
        repo.PersistSnapshot(new Snapshot(s0, s1, c1, _pool, ResourcePool.Usage.MainBlockProcessing));

        SnapshotContent c2 = new();
        c2.Accounts[TestItem.AddressB] = Build.An.Account.WithBalance(2).TestObject;
        repo.PersistSnapshot(new Snapshot(s1, s3, c2, _pool, ResourcePool.Usage.MainBlockProcessing));

        SnapshotContent c3 = new();
        c3.Accounts[TestItem.AddressC] = Build.An.Account.WithBalance(3).TestObject;
        repo.PersistSnapshot(new Snapshot(s3, s6, c3, _pool, ResourcePool.Usage.MainBlockProcessing));

        Assert.That(repo.SnapshotCount, Is.EqualTo(3));

        // Prune before block 5 (removes snapshots with To < 5, i.e., s1 and s3)
        manager.PrunePersistedSnapshots(new StateId(5, Keccak.Compute("5")));

        Assert.That(repo.SnapshotCount, Is.EqualTo(1)); // Only s6 remains
    }
}
