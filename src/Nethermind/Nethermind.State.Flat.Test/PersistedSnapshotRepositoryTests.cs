// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.IO;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Flat.PersistedSnapshots;
using Nethermind.Trie;
using NUnit.Framework;

namespace Nethermind.State.Flat.Test;

[TestFixture]
public class PersistedSnapshotRepositoryTests
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

    private Snapshot CreateTestSnapshot(StateId from, StateId to, Address? account = null, UInt256 balance = default)
    {
        SnapshotContent content = new();
        if (account is not null)
            content.Accounts[account] = Build.An.Account.WithBalance(balance == 0 ? 1000 : balance).TestObject;
        return new Snapshot(from, to, content, _pool, ResourcePool.Usage.MainBlockProcessing);
    }

    [Test]
    public void PersistSnapshot_And_Query()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096);
        repo.LoadFromCatalog();

        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        Snapshot snap = CreateTestSnapshot(s0, s1, TestItem.AddressA);

        PersistedSnapshot persisted = repo.AddBaseSnapshot(snap);
        Assert.That(persisted.Id, Is.GreaterThanOrEqualTo(0));
        Assert.That(persisted.From, Is.EqualTo(s0));
        Assert.That(persisted.To, Is.EqualTo(s1));
        Assert.That(repo.SnapshotCount, Is.EqualTo(1));

        // Query through the snapshot
        Assert.That(persisted.TryGetAccount(TestItem.AddressA, out ReadOnlySpan<byte> accountRlp), Is.True);

        Rlp.ValueDecoderContext ctx = new(accountRlp);
        Account decoded = AccountDecoder.Slim.Decode(ref ctx)!;
        Assert.That(decoded.Balance, Is.EqualTo((UInt256)1000));
    }

    [Test]
    public void AssembleSnapshots_OrderedOldestFirst()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096);
        repo.LoadFromCatalog();

        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));

        // Persist two snapshots with different state trie nodes
        TreePath path = new(Keccak.Compute("path"), 4);
        byte[] rlp1 = [0xC0];
        byte[] rlp2 = [0xC1, 0x80];

        SnapshotContent content1 = new();
        content1.StateNodes[path] = new TrieNode(NodeType.Leaf, rlp1);
        Snapshot snap1 = new(s0, s1, content1, _pool, ResourcePool.Usage.MainBlockProcessing);

        SnapshotContent content2 = new();
        content2.StateNodes[path] = new TrieNode(NodeType.Leaf, rlp2);
        Snapshot snap2 = new(s1, s2, content2, _pool, ResourcePool.Usage.MainBlockProcessing);

        repo.AddBaseSnapshot(snap1);
        repo.AddBaseSnapshot(snap2);

        using PersistedSnapshotList list = repo.AssembleSnapshots(s2, s0);

        // Should return newest value (rlp2) when queried newest-first
        ReadOnlySpan<byte> result = default;
        bool found = false;
        for (int i = list.Count - 1; i >= 0; i--)
        {
            if (list[i].TryLoadStateNodeRlp(path, out result))
            {
                found = true;
                break;
            }
        }
        Assert.That(found, Is.True);
        Assert.That(result.ToArray(), Is.EqualTo(rlp2));
    }

    [Test]
    public void LoadFromCatalog_RestoresSnapshots()
    {
        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));

        // Session 1: persist a snapshot
        using (PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096))
        {
            repo.LoadFromCatalog();
            Snapshot snap = CreateTestSnapshot(s0, s1, TestItem.AddressA);
            repo.AddBaseSnapshot(snap);
        }

        // Session 2: reload from disk
        using (PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096))
        {
            repo.LoadFromCatalog();
            Assert.That(repo.SnapshotCount, Is.EqualTo(1));

            using PersistedSnapshotList list = repo.AssembleSnapshots(s1, s0);
            Assert.That(list.Count, Is.EqualTo(1));
        }
    }

    [Test]
    public void PruneBefore_RemovesOldSnapshots()
    {
        using PersistedSnapshotRepository repo = new(_testDir, maxArenaSize: 4096);
        repo.LoadFromCatalog();

        StateId s0 = new(0, Keccak.EmptyTreeHash);
        StateId s1 = new(1, Keccak.Compute("1"));
        StateId s2 = new(2, Keccak.Compute("2"));
        StateId s3 = new(3, Keccak.Compute("3"));

        Snapshot snap1 = CreateTestSnapshot(s0, s1, TestItem.AddressA);
        Snapshot snap2 = CreateTestSnapshot(s1, s2, TestItem.AddressB);
        Snapshot snap3 = CreateTestSnapshot(s2, s3, TestItem.AddressC);

        repo.AddBaseSnapshot(snap1);
        repo.AddBaseSnapshot(snap2);
        repo.AddBaseSnapshot(snap3);
        Assert.That(repo.SnapshotCount, Is.EqualTo(3));

        // Prune before block 2 (removes snap1 with To=1)
        int pruned = repo.PruneBefore(new StateId(2, Keccak.Compute("prune")));
        Assert.That(pruned, Is.EqualTo(1));
        Assert.That(repo.SnapshotCount, Is.EqualTo(2));
    }
}
