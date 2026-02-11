// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;

namespace Nethermind.State.Flat.PersistedSnapshots;

/// <summary>
/// Builds columnar RSST byte data from an in-memory <see cref="Snapshot"/>.
/// The outer RSST has 5 column entries (tags 0x00-0x04), each containing an inner RSST.
/// Inner RSST keys are the entity keys without the tag prefix.
/// </summary>
public static class PersistedSnapshotBuilder
{
    public static byte[] Build(Snapshot snapshot)
    {
        // Build 5 inner RSSTs, one per column
        byte[] accountsRsst = BuildAccountsColumn(snapshot);
        byte[] storageRsst = BuildStorageColumn(snapshot);
        byte[] selfDestructRsst = BuildSelfDestructColumn(snapshot);
        byte[] stateNodesRsst = BuildStateNodesColumn(snapshot);
        byte[] storageNodesRsst = BuildStorageNodesColumn(snapshot);

        // Build outer RSST with single-byte column keys
        RsstBuilder outer = new();
        outer.Add([PersistedSnapshot.AccountTag], accountsRsst);
        outer.Add([PersistedSnapshot.StorageTag], storageRsst);
        outer.Add([PersistedSnapshot.SelfDestructTag], selfDestructRsst);
        outer.Add([PersistedSnapshot.StateNodeTag], stateNodesRsst);
        outer.Add([PersistedSnapshot.StorageNodeTag], storageNodesRsst);

        return outer.Build();
    }

    private static byte[] BuildAccountsColumn(Snapshot snapshot)
    {
        List<(AddressAsKey Key, Account? Value)> accounts = new();
        foreach (KeyValuePair<AddressAsKey, Account?> kv in snapshot.Accounts)
        {
            accounts.Add((kv.Key, kv.Value));
        }
        accounts.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        RsstBuilder builder = new();
        byte[] rlpBuffer = new byte[256];
        RlpStream rlpStream = new(rlpBuffer);

        foreach ((AddressAsKey key, Account? value) in accounts)
        {
            if (value is null)
            {
                builder.Add(key.Value.Bytes, ReadOnlySpan<byte>.Empty);
            }
            else
            {
                int len = AccountDecoder.Slim.GetLength(value);
                rlpStream.Reset();
                AccountDecoder.Slim.Encode(rlpStream, value);
                builder.Add(key.Value.Bytes, rlpBuffer.AsSpan(0, len));
            }
        }

        return builder.Build();
    }

    private static byte[] BuildStorageColumn(Snapshot snapshot)
    {
        List<((AddressAsKey Addr, UInt256 Slot) Key, SlotValue? Value)> storages = new();
        foreach (KeyValuePair<(AddressAsKey, UInt256), SlotValue?> kv in snapshot.Storages)
        {
            storages.Add((kv.Key, kv.Value));
        }
        storages.Sort((a, b) =>
        {
            int cmp = a.Key.Addr.Value.Bytes.SequenceCompareTo(b.Key.Addr.Value.Bytes);
            if (cmp != 0) return cmp;
            return a.Key.Slot.CompareTo(b.Key.Slot);
        });

        RsstBuilder builder = new();
        byte[] keyBuffer = new byte[Address.Size + 32];

        foreach (((AddressAsKey addr, UInt256 slotIdx) key, SlotValue? value) in storages)
        {
            key.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan());
            key.slotIdx.ToBigEndian(keyBuffer.AsSpan(Address.Size, 32));

            if (value.HasValue)
            {
                ReadOnlySpan<byte> withoutLeadingZeros = value.Value.AsReadOnlySpan.WithoutLeadingZeros();
                builder.Add(keyBuffer.AsSpan(0, Address.Size + 32), withoutLeadingZeros);
            }
            else
            {
                builder.Add(keyBuffer.AsSpan(0, Address.Size + 32), ReadOnlySpan<byte>.Empty);
            }
        }

        return builder.Build();
    }

    private static byte[] BuildSelfDestructColumn(Snapshot snapshot)
    {
        List<(AddressAsKey Key, bool Value)> selfDestructs = new();
        foreach (KeyValuePair<AddressAsKey, bool> kv in snapshot.SelfDestructedStorageAddresses)
        {
            selfDestructs.Add((kv.Key, kv.Value));
        }
        selfDestructs.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        RsstBuilder builder = new();
        foreach ((AddressAsKey key, bool value) in selfDestructs)
        {
            builder.Add(key.Value.Bytes, value ? [0x01] : ReadOnlySpan<byte>.Empty);
        }

        return builder.Build();
    }

    private static byte[] BuildStateNodesColumn(Snapshot snapshot)
    {
        List<(TreePath Path, TrieNode Node)> stateNodes = new();
        foreach (KeyValuePair<TreePath, TrieNode> kv in snapshot.StateNodes)
        {
            if (kv.Value.FullRlp.Length == 0 && kv.Value.NodeType == NodeType.Unknown) continue;
            stateNodes.Add((kv.Key, kv.Value));
        }
        stateNodes.Sort((a, b) =>
        {
            int cmp = a.Path.Path.Bytes.SequenceCompareTo(b.Path.Path.Bytes);
            if (cmp != 0) return cmp;
            return a.Path.Length.CompareTo(b.Path.Length);
        });

        RsstBuilder builder = new();
        byte[] keyBuffer = new byte[32 + 1];
        foreach ((TreePath path, TrieNode node) in stateNodes)
        {
            path.Path.Bytes.CopyTo(keyBuffer.AsSpan());
            keyBuffer[32] = (byte)path.Length;
            builder.Add(keyBuffer.AsSpan(0, 33), node.FullRlp.Span);
        }

        return builder.Build();
    }

    private static byte[] BuildStorageNodesColumn(Snapshot snapshot)
    {
        List<((Hash256AsKey Addr, TreePath Path) Key, TrieNode Node)> storageNodes = new();
        foreach (KeyValuePair<(Hash256AsKey, TreePath), TrieNode> kv in snapshot.StorageNodes)
        {
            if (kv.Value.FullRlp.Length == 0 && kv.Value.NodeType == NodeType.Unknown) continue;
            storageNodes.Add((kv.Key, kv.Value));
        }
        storageNodes.Sort((a, b) =>
        {
            int cmp = a.Key.Addr.Value.Bytes.SequenceCompareTo(b.Key.Addr.Value.Bytes);
            if (cmp != 0) return cmp;
            cmp = a.Key.Path.Path.Bytes.SequenceCompareTo(b.Key.Path.Path.Bytes);
            if (cmp != 0) return cmp;
            return a.Key.Path.Length.CompareTo(b.Key.Path.Length);
        });

        RsstBuilder builder = new();
        byte[] keyBuffer = new byte[32 + 32 + 1];
        foreach (((Hash256AsKey addr, TreePath path) snKey, TrieNode node) in storageNodes)
        {
            snKey.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan());
            snKey.path.Path.Bytes.CopyTo(keyBuffer.AsSpan(32));
            keyBuffer[64] = (byte)snKey.path.Length;
            builder.Add(keyBuffer.AsSpan(0, 65), node.FullRlp.Span);
        }

        return builder.Build();
    }
}
