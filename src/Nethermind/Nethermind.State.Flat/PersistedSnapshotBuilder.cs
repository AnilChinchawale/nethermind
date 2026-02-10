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
/// </summary>
public static class PersistedSnapshotBuilder
{
    /// <summary>
    /// Build RSST bytes from an in-memory snapshot, encoding all accounts, storage,
    /// self-destructs, and trie nodes with tagged keys.
    /// </summary>
    public static byte[] Build(Snapshot snapshot)
    {
        RsstBuilder builder = new();
        byte[] keyBuffer = new byte[1 + 32 + 32 + 1]; // Max key size: tag(1) + hash(32) + path(32) + len(1)

        // Accounts: tag 0x00 + address bytes (20)
        foreach (KeyValuePair<AddressAsKey, Account?> kv in snapshot.Accounts)
        {
            int keyLen = 1 + Address.Size;
            keyBuffer[0] = PersistedSnapshot.AccountTag;
            kv.Key.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));

            if (kv.Value is null)
            {
                builder.Add(keyBuffer.AsSpan(0, keyLen), ReadOnlySpan<byte>.Empty);
            }
            else
            {
                using NettyRlpStream stream = AccountDecoder.Slim.EncodeToNewNettyStream(kv.Value);
                builder.Add(keyBuffer.AsSpan(0, keyLen), stream.AsSpan());
            }
        }

        // Storage: tag 0x01 + address bytes (20) + slot (32 big-endian)
        foreach (KeyValuePair<(AddressAsKey, UInt256), SlotValue?> kv in snapshot.Storages)
        {
            ((AddressAsKey addr, UInt256 slotIdx), _) = kv;
            int keyLen = 1 + Address.Size + 32;
            keyBuffer[0] = PersistedSnapshot.StorageTag;
            addr.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            slotIdx.ToBigEndian(keyBuffer.AsSpan(1 + Address.Size, 32));

            if (kv.Value.HasValue)
            {
                ReadOnlySpan<byte> withoutLeadingZeros = kv.Value.Value.AsReadOnlySpan.WithoutLeadingZeros();
                builder.Add(keyBuffer.AsSpan(0, keyLen), withoutLeadingZeros);
            }
            else
            {
                builder.Add(keyBuffer.AsSpan(0, keyLen), ReadOnlySpan<byte>.Empty);
            }
        }

        // Self-destructs: tag 0x02 + address bytes (20)
        foreach (KeyValuePair<AddressAsKey, bool> kv in snapshot.SelfDestructedStorageAddresses)
        {
            int keyLen = 1 + Address.Size;
            keyBuffer[0] = PersistedSnapshot.SelfDestructTag;
            kv.Key.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            builder.Add(keyBuffer.AsSpan(0, keyLen), ReadOnlySpan<byte>.Empty);
        }

        // State trie nodes: tag 0x03 + path (32) + length (1)
        foreach (KeyValuePair<TreePath, TrieNode> kv in snapshot.StateNodes)
        {
            if (kv.Value.FullRlp.Length == 0 && kv.Value.NodeType == NodeType.Unknown) continue;

            int keyLen = 1 + 32 + 1;
            keyBuffer[0] = PersistedSnapshot.StateNodeTag;
            kv.Key.Path.Bytes.CopyTo(keyBuffer.AsSpan(1));
            keyBuffer[33] = (byte)kv.Key.Length;
            builder.Add(keyBuffer.AsSpan(0, keyLen), kv.Value.FullRlp.Span);
        }

        // Storage trie nodes: tag 0x04 + address hash (32) + path (32) + length (1)
        foreach (KeyValuePair<(Hash256AsKey, TreePath), TrieNode> kv in snapshot.StorageNodes)
        {
            if (kv.Value.FullRlp.Length == 0 && kv.Value.NodeType == NodeType.Unknown) continue;

            int keyLen = 1 + 32 + 32 + 1;
            keyBuffer[0] = PersistedSnapshot.StorageNodeTag;
            kv.Key.Item1.Value.Bytes.CopyTo(keyBuffer.AsSpan(1));
            kv.Key.Item2.Path.Bytes.CopyTo(keyBuffer.AsSpan(33));
            keyBuffer[65] = (byte)kv.Key.Item2.Length;
            builder.Add(keyBuffer.AsSpan(0, keyLen), kv.Value.FullRlp.Span);
        }

        return builder.Build();
    }
}
