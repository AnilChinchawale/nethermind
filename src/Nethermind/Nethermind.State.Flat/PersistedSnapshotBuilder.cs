// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Extensions;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
using Nethermind.State.Flat.Rsst;
using Nethermind.Trie;

namespace Nethermind.State.Flat;

/// <summary>
/// Builds RSST byte data from an in-memory <see cref="Snapshot"/>.
/// Entries are added in sorted key order (by tag prefix, then by key bytes within each group).
/// </summary>
public static class PersistedSnapshotBuilder
{
    public static byte[] Build(Snapshot snapshot)
    {
        RsstBuilder builder = new();
        byte[] keyBuffer = new byte[1 + 32 + 32 + 1]; // Max key size: tag(1) + hash(32) + path(32) + len(1)

        // Entries must be added in sorted order by their full key (tag prefix ensures group ordering).
        // Tag 0x00: Accounts, Tag 0x01: Storage, Tag 0x02: SelfDestruct, Tag 0x03: StateNodes, Tag 0x04: StorageNodes

        // Accounts: tag 0x00 + address bytes (20) - sort by address
        AddSortedAccounts(builder, snapshot, keyBuffer);

        // Storage: tag 0x01 + address bytes (20) + slot (32 big-endian) - sort by (address, slot)
        AddSortedStorage(builder, snapshot, keyBuffer);

        // Self-destructs: tag 0x02 + address bytes (20) - sort by address
        AddSortedSelfDestructs(builder, snapshot, keyBuffer);

        // State trie nodes: tag 0x03 + path (32) + length (1) - sort by (path, length)
        AddSortedStateNodes(builder, snapshot, keyBuffer);

        // Storage trie nodes: tag 0x04 + address hash (32) + path (32) + length (1) - sort by (address hash, path, length)
        AddSortedStorageNodes(builder, snapshot, keyBuffer);

        return builder.Build();
    }

    private static void AddSortedAccounts(RsstBuilder builder, Snapshot snapshot, byte[] keyBuffer)
    {
        // Collect and sort account keys
        List<(AddressAsKey Key, Account? Value)> accounts = new();
        foreach (KeyValuePair<AddressAsKey, Account?> kv in snapshot.Accounts)
        {
            accounts.Add((kv.Key, kv.Value));
        }
        accounts.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        foreach ((AddressAsKey key, Account? value) in accounts)
        {
            int keyLen = 1 + Address.Size;
            keyBuffer[0] = PersistedSnapshot.AccountTag;
            key.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));

            if (value is null)
            {
                builder.Add(keyBuffer.AsSpan(0, keyLen), ReadOnlySpan<byte>.Empty);
            }
            else
            {
                using NettyRlpStream stream = AccountDecoder.Slim.EncodeToNewNettyStream(value);
                builder.Add(keyBuffer.AsSpan(0, keyLen), stream.AsSpan());
            }
        }
    }

    private static void AddSortedStorage(RsstBuilder builder, Snapshot snapshot, byte[] keyBuffer)
    {
        // Collect and sort storage keys by their full RSST key bytes
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

        foreach (((AddressAsKey addr, UInt256 slotIdx) key, SlotValue? value) in storages)
        {
            int keyLen = 1 + Address.Size + 32;
            keyBuffer[0] = PersistedSnapshot.StorageTag;
            key.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            key.slotIdx.ToBigEndian(keyBuffer.AsSpan(1 + Address.Size, 32));

            if (value.HasValue)
            {
                ReadOnlySpan<byte> withoutLeadingZeros = value.Value.AsReadOnlySpan.WithoutLeadingZeros();
                builder.Add(keyBuffer.AsSpan(0, keyLen), withoutLeadingZeros);
            }
            else
            {
                builder.Add(keyBuffer.AsSpan(0, keyLen), ReadOnlySpan<byte>.Empty);
            }
        }
    }

    private static void AddSortedSelfDestructs(RsstBuilder builder, Snapshot snapshot, byte[] keyBuffer)
    {
        List<(AddressAsKey Key, bool Value)> selfDestructs = new();
        foreach (KeyValuePair<AddressAsKey, bool> kv in snapshot.SelfDestructedStorageAddresses)
        {
            selfDestructs.Add((kv.Key, kv.Value));
        }
        selfDestructs.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        foreach ((AddressAsKey key, bool value) in selfDestructs)
        {
            int keyLen = 1 + Address.Size;
            keyBuffer[0] = PersistedSnapshot.SelfDestructTag;
            key.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            builder.Add(keyBuffer.AsSpan(0, keyLen), value ? [0x01] : ReadOnlySpan<byte>.Empty);
        }
    }

    private static void AddSortedStateNodes(RsstBuilder builder, Snapshot snapshot, byte[] keyBuffer)
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

        foreach ((TreePath path, TrieNode node) in stateNodes)
        {
            int keyLen = 1 + 32 + 1;
            keyBuffer[0] = PersistedSnapshot.StateNodeTag;
            path.Path.Bytes.CopyTo(keyBuffer.AsSpan(1));
            keyBuffer[33] = (byte)path.Length;
            builder.Add(keyBuffer.AsSpan(0, keyLen), node.FullRlp.Span);
        }
    }

    private static void AddSortedStorageNodes(RsstBuilder builder, Snapshot snapshot, byte[] keyBuffer)
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

        foreach (((Hash256AsKey addr, TreePath path) snKey, TrieNode node) in storageNodes)
        {
            int keyLen = 1 + 32 + 32 + 1;
            keyBuffer[0] = PersistedSnapshot.StorageNodeTag;
            snKey.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            snKey.path.Path.Bytes.CopyTo(keyBuffer.AsSpan(33));
            keyBuffer[65] = (byte)snKey.path.Length;
            builder.Add(keyBuffer.AsSpan(0, keyLen), node.FullRlp.Span);
        }
    }
}
