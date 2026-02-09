using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FluentAssertions;
using Nethermind.Core;
using Nethermind.Core.Collections;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Core.Test;
using Nethermind.Core.Test.Builders;
using Nethermind.Db;
using Nethermind.Int256;
using Nethermind.Logging;
using Nethermind.State;
using Nethermind.State.Snap;
using Nethermind.State.SnapServer;
using Nethermind.Synchronization.FastSync;
using Nethermind.Synchronization.SnapSync;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;
using NSubstitute;
using NUnit.Framework;

namespace Nethermind.Synchronization.Test.SnapSync;

public class SnapServerTest
{
    internal interface ISnapServerContext
    {
        ISnapServer Server { get; }
        SnapProvider SnapProvider { get; }
        StateTree Tree { get; }
        MemDb ClientStateDb { get; }

        Hash256 FillAccountWithDefaultStorage();
        Hash256 FillAccountWithStorage(int slotCount);
        void SetSlot(Hash256 storagePath, ValueHash256 slotKey, byte[] value, bool rlpEncode = true);
        Hash256 CommitStorage(Hash256 storagePath);
    }

    private class TrieSnapServerContext : ISnapServerContext
    {
        private readonly TestRawTrieStore _store;
        private readonly Dictionary<Hash256, StorageTree> _storageTrees = new();

        public ISnapServer Server { get; }
        public SnapProvider SnapProvider { get; }
        public StateTree Tree { get; }
        public MemDb ClientStateDb { get; }

        internal TrieSnapServerContext(ILastNStateRootTracker? lastNStateRootTracker = null)
        {
            MemDb stateDbServer = new();
            MemDb codeDbServer = new();
            _store = new TestRawTrieStore(stateDbServer);
            Tree = new StateTree(_store, LimboLogs.Instance);
            Server = new SnapServer(_store.AsReadOnly(), codeDbServer, LimboLogs.Instance, lastNStateRootTracker);

            ClientStateDb = new MemDb();
            using ProgressTracker progressTracker = new(ClientStateDb, new TestSyncConfig(), new StateSyncPivot(null!, new TestSyncConfig(), LimboLogs.Instance), LimboLogs.Instance);
            INodeStorage nodeStorage = new NodeStorage(ClientStateDb);
            SnapProvider = new SnapProvider(progressTracker, new MemDb(), new PatriciaSnapTrieFactory(nodeStorage, LimboLogs.Instance), LimboLogs.Instance);
        }

        public Hash256 FillAccountWithDefaultStorage()
        {
            for (int i = 0; i < 6; i++)
                SetSlot(TestItem.Tree.AccountAddress0, TestItem.Tree.SlotsWithPaths[i].Path, TestItem.Tree.SlotsWithPaths[i].SlotRlpValue, rlpEncode: false);

            Hash256 storageRoot = CommitStorage(TestItem.Tree.AccountAddress0);
            var account = Build.An.Account.WithBalance(1).WithStorageRoot(storageRoot).TestObject;
            Tree.Set(TestItem.Tree.AccountAddress0, account);
            Tree.Commit();
            return storageRoot;
        }

        public Hash256 FillAccountWithStorage(int slotCount)
        {
            for (int i = 0; i < slotCount; i++)
            {
                var key = Keccak.Compute(i.ToBigEndianByteArray());
                SetSlot(TestItem.Tree.AccountAddress0, key, key.BytesToArray(), rlpEncode: false);
            }

            Hash256 storageRoot = CommitStorage(TestItem.Tree.AccountAddress0);
            var account = Build.An.Account.WithBalance(1).WithStorageRoot(storageRoot).TestObject;
            Tree.Set(TestItem.Tree.AccountAddress0, account);
            Tree.Commit();
            return storageRoot;
        }

        public void SetSlot(Hash256 storagePath, ValueHash256 slotKey, byte[] value, bool rlpEncode = true)
        {
            if (!_storageTrees.TryGetValue(storagePath, out StorageTree? st))
            {
                st = new StorageTree(_store.GetTrieStore(storagePath), LimboLogs.Instance);
                _storageTrees[storagePath] = st;
            }
            st.Set(slotKey, value, rlpEncode);
        }

        public Hash256 CommitStorage(Hash256 storagePath)
        {
            StorageTree st = _storageTrees[storagePath];
            st.Commit();
            return st.RootHash;
        }
    }

    private static ISnapServerContext CreateContext(ILastNStateRootTracker? lastNStateRootTracker = null) =>
        new TrieSnapServerContext(lastNStateRootTracker);

    [Test]
    public void TestGetAccountRange()
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeWithTestAccounts(context.Tree);

        (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> proofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 4000, CancellationToken.None);

        AddRangeResult result = context.SnapProvider.AddAccountRange(1, context.Tree.RootHash, Keccak.Zero,
            accounts.ToArray(), proofs.ToArray());

        result.Should().Be(AddRangeResult.OK);
        context.ClientStateDb.Keys.Count.Should().Be(10);
        accounts.Dispose();
        proofs.Dispose();
    }

    [Test]
    public void TestGetAccountRange_InvalidRange()
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeWithTestAccounts(context.Tree);

        (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> proofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.MaxValue, Keccak.Zero, 4000, CancellationToken.None);

        accounts.Count.Should().Be(0);
        accounts.Dispose();
        proofs.Dispose();
    }

    [Test]
    public void TestGetTrieNode_Root()
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeWithTestAccounts(context.Tree);

        using IOwnedReadOnlyList<byte[]> result = context.Server.GetTrieNodes([
            new PathGroup()
            {
                Group = [[]]
            }
        ], context.Tree.RootHash, default)!;

        result.Count.Should().Be(1);
    }

    [Test]
    public void TestGetTrieNode_Storage_Root()
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeWithTestAccounts(context.Tree);

        using IOwnedReadOnlyList<byte[]> result = context.Server.GetTrieNodes([
            new PathGroup()
            {
                Group = [TestItem.Tree.AccountsWithPaths[0].Path.Bytes.ToArray(), []]
            }
        ], context.Tree.RootHash, default)!;

        result.Count.Should().Be(1);
    }

    [TestCase(true)]
    [TestCase(false)]
    public void TestNoState(bool withLastNStateTracker)
    {
        ILastNStateRootTracker? lastNStateTracker = null;
        if (withLastNStateTracker)
        {
            lastNStateTracker = Substitute.For<ILastNStateRootTracker>();
            lastNStateTracker.HasStateRoot(Arg.Any<Hash256>()).Returns(false);
        }

        var context = CreateContext(lastNStateRootTracker: lastNStateTracker);

        (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> accountProofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 4000, CancellationToken.None);

        accounts.Count.Should().Be(0);

        (IOwnedReadOnlyList<IOwnedReadOnlyList<PathWithStorageSlot>> storageSlots, IOwnedReadOnlyList<byte[]>? proofs) =
            context.Server.GetStorageRanges(context.Tree.RootHash, [TestItem.Tree.AccountsWithPaths[0]],
                ValueKeccak.Zero, ValueKeccak.MaxValue, 10, CancellationToken.None);

        storageSlots.Count.Should().Be(0);

        accounts.Dispose();
        accountProofs.Dispose();
        proofs?.Dispose();
        storageSlots.DisposeRecursive();
    }

    [Test]
    public void TestGetAccountRangeMultiple()
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeWithTestAccounts(context.Tree);

        Hash256 startRange = Keccak.Zero;
        while (true)
        {
            (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> proofs) =
                context.Server.GetAccountRanges(context.Tree.RootHash, startRange, Keccak.MaxValue, 100, CancellationToken.None);

            try
            {
                AddRangeResult result = context.SnapProvider.AddAccountRange(1, context.Tree.RootHash, startRange,
                    accounts, proofs);

                result.Should().Be(AddRangeResult.OK);
                startRange = accounts[^1].Path.ToCommitment();
                if (startRange.Bytes.SequenceEqual(TestItem.Tree.AccountsWithPaths[^1].Path.Bytes))
                {
                    break;
                }
            }
            finally
            {
                accounts.Dispose();
                proofs.Dispose();
            }
        }
        context.ClientStateDb.Keys.Count.Should().Be(10);
    }

    [TestCase(10, 10)]
    [TestCase(10000, 1000)]
    [TestCase(10000, 10000000)]
    [TestCase(10000, 10000)]
    public void TestGetAccountRangeMultipleLarger(int stateSize, int byteLimit)
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeMultipleAccount(context.Tree, stateSize);

        Hash256 startRange = Keccak.Zero;
        while (true)
        {
            (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> proofs) =
                context.Server.GetAccountRanges(context.Tree.RootHash, startRange, Keccak.MaxValue, byteLimit, CancellationToken.None);

            try
            {
                AddRangeResult result = context.SnapProvider.AddAccountRange(1, context.Tree.RootHash, startRange,
                    accounts, proofs);

                result.Should().Be(AddRangeResult.OK);
                if (startRange == accounts[^1].Path.ToCommitment())
                {
                    break;
                }

                startRange = accounts[^1].Path.ToCommitment();
            }
            finally
            {
                accounts.Dispose();
                proofs.Dispose();
            }
        }
    }

    [TestCase(10, 10)]
    [TestCase(10000, 10)]
    [TestCase(100, 100)]
    [TestCase(10000, 10000000)]
    public void TestGetAccountRangeArtificialLimit(int stateSize, int byteLimit)
    {
        var context = CreateContext();
        TestItem.Tree.FillStateTreeMultipleAccount(context.Tree, stateSize);
        Hash256 startRange = Keccak.Zero;

        ValueHash256 limit = new ValueHash256("0x8000000000000000000000000000000000000000000000000000000000000000");
        while (true)
        {
            (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> proofs) = context.Server
                .GetAccountRanges(context.Tree.RootHash, startRange, limit, byteLimit, CancellationToken.None);

            try
            {
                AddRangeResult result = context.SnapProvider.AddAccountRange(1, context.Tree.RootHash, startRange,
                    accounts, proofs);

                result.Should().Be(AddRangeResult.OK);
                if (startRange == accounts[^1].Path.ToCommitment())
                {
                    break;
                }

                startRange = accounts[^1].Path.ToCommitment();
            }
            finally
            {
                accounts.Dispose();
                proofs.Dispose();
            }
        }
    }

    [Test]
    public void TestGetStorageRange()
    {
        var context = CreateContext();
        Hash256 storageRoot = context.FillAccountWithDefaultStorage();

        (IOwnedReadOnlyList<IOwnedReadOnlyList<PathWithStorageSlot>> storageSlots, IOwnedReadOnlyList<byte[]>? proofs) =
            context.Server.GetStorageRanges(context.Tree.RootHash, [TestItem.Tree.AccountsWithPaths[0]],
                ValueKeccak.Zero, ValueKeccak.MaxValue, 10, CancellationToken.None);

        try
        {
            var storageRangeRequest = new StorageRange()
            {
                StartingHash = Keccak.Zero,
                Accounts = new ArrayPoolList<PathWithAccount>(1) { new(TestItem.Tree.AccountsWithPaths[0].Path, new Account(UInt256.Zero).WithChangedStorageRoot(storageRoot)) }
            };
            AddRangeResult result = context.SnapProvider.AddStorageRangeForAccount(storageRangeRequest, 0, storageSlots[0], proofs);

            result.Should().Be(AddRangeResult.OK);
        }
        finally
        {
            storageSlots.DisposeRecursive();
            proofs?.Dispose();
        }
    }

    [Test]
    public void TestGetStorageRange_NoSlotsForAccount()
    {
        var context = CreateContext();
        context.FillAccountWithDefaultStorage();

        ValueHash256 lastStorageHash = TestItem.Tree.SlotsWithPaths[^1].Path;
        var asInt = lastStorageHash.ToUInt256();
        ValueHash256 beyondLast = new ValueHash256((++asInt).ToBigEndian());

        (IOwnedReadOnlyList<IOwnedReadOnlyList<PathWithStorageSlot>> storageSlots, IOwnedReadOnlyList<byte[]>? proofs) =
            context.Server.GetStorageRanges(context.Tree.RootHash, [TestItem.Tree.AccountsWithPaths[0]],
                beyondLast, ValueKeccak.MaxValue, 10, CancellationToken.None);

        storageSlots.Count.Should().Be(0);
        proofs?.Count.Should().BeGreaterThan(0); //in worst case should get at least root node

        storageSlots.DisposeRecursive();
        proofs?.Dispose();
    }

    [Test]
    public void TestGetStorageRangeMulti()
    {
        var context = CreateContext();
        Hash256 storageRoot = context.FillAccountWithStorage(10000);

        Hash256 startRange = Keccak.Zero;
        while (true)
        {
            (IOwnedReadOnlyList<IOwnedReadOnlyList<PathWithStorageSlot>> storageSlots, IOwnedReadOnlyList<byte[]>? proofs) =
                context.Server.GetStorageRanges(context.Tree.RootHash, [TestItem.Tree.AccountsWithPaths[0]],
                    startRange, ValueKeccak.MaxValue, 10000, CancellationToken.None);

            try
            {
                var storageRangeRequest = new StorageRange()
                {
                    StartingHash = startRange,
                    Accounts = new ArrayPoolList<PathWithAccount>(1) { new(TestItem.Tree.AccountsWithPaths[0].Path, new Account(UInt256.Zero).WithChangedStorageRoot(storageRoot)) }
                };
                AddRangeResult result = context.SnapProvider.AddStorageRangeForAccount(storageRangeRequest, 0, storageSlots[0], proofs);

                result.Should().Be(AddRangeResult.OK);
                if (startRange == storageSlots[0][^1].Path.ToCommitment())
                {
                    break;
                }

                startRange = storageSlots[0][^1].Path.ToCommitment();
            }
            finally
            {
                storageSlots.DisposeRecursive();
                proofs?.Dispose();
            }
        }
    }

    [Test]
    public void TestWithHugeTree()
    {
        var context = CreateContext();

        // generate Remote Tree
        for (int accountIndex = 0; accountIndex < 10000; accountIndex++)
        {
            context.Tree.Set(TestItem.GetRandomAddress(), TestItem.GenerateRandomAccount());
        }
        context.Tree.Commit();

        List<PathWithAccount> accountWithStorage = new();
        for (int i = 1000; i < 10000; i += 1000)
        {
            Address address = TestItem.GetRandomAddress();
            Hash256 storagePath = address.ToAccountPath.ToCommitment();
            for (int j = 0; j < i; j += 1)
                context.SetSlot(storagePath, TestItem.GetRandomKeccak(), TestItem.GetRandomKeccak().Bytes.ToArray());
            Hash256 storageRoot = context.CommitStorage(storagePath);
            var account = TestItem.GenerateRandomAccount().WithChangedStorageRoot(storageRoot);
            context.Tree.Set(address, account);
            accountWithStorage.Add(new PathWithAccount() { Path = Keccak.Compute(address.Bytes), Account = account });
        }
        context.Tree.Commit();

        // size of one PathWithAccount ranges from 39 -> 72
        (IOwnedReadOnlyList<PathWithAccount> accounts, IOwnedReadOnlyList<byte[]> accountProofs)
            = context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 10, CancellationToken.None);
        accounts.Count.Should().Be(1);
        accounts.Dispose();
        accountProofs.Dispose();

        (accounts, accountProofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 100, CancellationToken.None);
        accounts.Count.Should().BeGreaterThan(2);
        accounts.Dispose();
        accountProofs.Dispose();

        (accounts, accountProofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 10000, CancellationToken.None);
        accounts.Count.Should().BeGreaterThan(138);
        accounts.Dispose();
        accountProofs.Dispose();

        // TODO: Double check the threshold
        (accounts, accountProofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 720000, CancellationToken.None);
        accounts.Count.Should().Be(10009);

        accounts.Dispose();
        accountProofs.Dispose();

        (accounts, accountProofs) =
            context.Server.GetAccountRanges(context.Tree.RootHash, Keccak.Zero, Keccak.MaxValue, 10000000, CancellationToken.None);
        accounts.Count.Should().Be(10009);
        accounts.Dispose();
        accountProofs.Dispose();

        var accountWithStorageArray = accountWithStorage.ToArray();

        (IOwnedReadOnlyList<IOwnedReadOnlyList<PathWithStorageSlot>> slots, IOwnedReadOnlyList<byte[]>? proofs) = context.Server.GetStorageRanges(context.Tree.RootHash, accountWithStorageArray[..1], ValueKeccak.Zero, ValueKeccak.MaxValue, 10, CancellationToken.None);
        slots.Count.Should().Be(1);
        slots[0].Count.Should().Be(1);
        proofs.Should().NotBeNull();

        slots.DisposeRecursive();
        proofs?.Dispose();

        (slots, proofs) = context.Server.GetStorageRanges(context.Tree.RootHash, accountWithStorageArray[..1], ValueKeccak.Zero, ValueKeccak.MaxValue, 1000000, CancellationToken.None);
        slots.Count.Should().Be(1);
        slots[0].Count.Should().Be(1000);
        proofs.Should().BeEmpty();

        slots.DisposeRecursive();
        proofs?.Dispose();

        (slots, proofs) = context.Server.GetStorageRanges(context.Tree.RootHash, accountWithStorageArray[..2], ValueKeccak.Zero, ValueKeccak.MaxValue, 10, CancellationToken.None);
        slots.Count.Should().Be(1);
        slots[0].Count.Should().Be(1);
        proofs.Should().NotBeNull();
        slots.DisposeRecursive();
        proofs?.Dispose();

        (slots, proofs) = context.Server.GetStorageRanges(context.Tree.RootHash, accountWithStorageArray[..2], ValueKeccak.Zero, ValueKeccak.MaxValue, 100000, CancellationToken.None);
        slots.Count.Should().Be(2);
        slots[0].Count.Should().Be(1000);
        slots[1].Count.Should().Be(539);
        proofs.Should().NotBeNull();
        slots.DisposeRecursive();
        proofs?.Dispose();


        // incomplete tree will be returned as the hard limit is 2000000
        (slots, proofs) = context.Server.GetStorageRanges(context.Tree.RootHash, accountWithStorageArray, ValueKeccak.Zero, ValueKeccak.MaxValue, 3000000, CancellationToken.None);
        slots.Count.Should().Be(8);
        slots[^1].Count.Should().BeLessThan(8000);
        proofs.Should().NotBeEmpty();

        slots.DisposeRecursive();
        proofs?.Dispose();
    }
}
