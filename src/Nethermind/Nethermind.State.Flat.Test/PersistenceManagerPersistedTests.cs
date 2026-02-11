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
using Nethermind.State.Flat.Persistence;
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
        byte[] merged = PersistenceManager.MergeSnapshotData(olderData, newerData);

        // Create PersistedSnapshot from merged data
        PersistedSnapshot mergedSnapshot = new(1, s0, s2, PersistedSnapshotType.Compacted, merged);

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

        byte[] merged = PersistenceManager.MergeSnapshotData(data1, data2);
        PersistedSnapshot mergedSnapshot = new(1, s0, s2, PersistedSnapshotType.Compacted, merged);

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

        using PersistedSnapshotList list = repo.CompileSnapshotList();
        Assert.That(list.Count, Is.EqualTo(1));
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
