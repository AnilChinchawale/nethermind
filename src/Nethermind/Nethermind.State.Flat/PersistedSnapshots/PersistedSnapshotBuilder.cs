// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers;
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
    public static int Build(Snapshot snapshot, Span<byte> output)
    {
        RsstBuilder outer = new(output);
        try
        {
            // Column 0: Accounts
            WriteAccountsColumn(ref outer, snapshot, output);

            // Column 1: Storage
            WriteStorageColumn(ref outer, snapshot, output);

            // Column 2: Self-destruct
            WriteSelfDestructColumn(ref outer, snapshot, output);

            // Column 3: State nodes
            WriteStateNodesColumn(ref outer, snapshot, output);

            // Column 4: Storage nodes
            WriteStorageNodesColumn(ref outer, snapshot, output);

            return outer.Build();
        }
        finally
        {
            outer.Dispose();
        }
    }

    /// <summary>
    /// Convenience method: allocate output buffer and build.
    /// </summary>
    public static byte[] Build(Snapshot snapshot)
    {
        // Estimate size conservatively
        int estimatedSize = (snapshot.Accounts.Count() + snapshot.Storages.Count() + snapshot.StateNodes.Count() + snapshot.StorageNodes.Count()) * 128 + 1024 * 1024;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        try
        {
            int written = Build(snapshot, buffer);
            return buffer.AsSpan(0, written).ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void WriteAccountsColumn(ref RsstBuilder outer, Snapshot snapshot, Span<byte> fullOutput)
    {
        // Sort accounts
        List<(AddressAsKey Key, Account? Value)> accounts = new();
        foreach (KeyValuePair<AddressAsKey, Account?> kv in snapshot.Accounts)
        {
            accounts.Add((kv.Key, kv.Value));
        }
        accounts.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        // Begin outer value write for accounts column
        Span<byte> valueSpan = outer.BeginValueWrite(fullOutput.Length);
        using (RsstBuilder inner = new(valueSpan))
        {
            byte[] rlpBuffer = new byte[256];
            RlpStream rlpStream = new(rlpBuffer);

            foreach ((AddressAsKey key, Account? value) in accounts)
            {
                if (value is null)
                {
                    inner.Add(key.Value.Bytes, ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    int len = AccountDecoder.Slim.GetLength(value);
                    rlpStream.Reset();
                    AccountDecoder.Slim.Encode(rlpStream, value);
                    inner.Add(key.Value.Bytes, rlpBuffer.AsSpan(0, len));
                }
            }

            int innerLen = inner.Build();
            ReadOnlySpan<byte> accountTag = new byte[] { PersistedSnapshot.AccountTag };
            outer.FinishValueWrite(innerLen, accountTag);
        }
    }

    private static void WriteStorageColumn(ref RsstBuilder outer, Snapshot snapshot, Span<byte> fullOutput)
    {
        // Sort storage
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

        Span<byte> valueSpan = outer.BeginValueWrite(fullOutput.Length);
        using (RsstBuilder inner = new(valueSpan))
        {
            byte[] keyBuffer = new byte[Address.Size + 32];

            foreach (((AddressAsKey addr, UInt256 slotIdx) key, SlotValue? value) in storages)
            {
                key.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan());
                key.slotIdx.ToBigEndian(keyBuffer.AsSpan(Address.Size, 32));

                if (value.HasValue)
                {
                    ReadOnlySpan<byte> withoutLeadingZeros = value.Value.AsReadOnlySpan.WithoutLeadingZeros();
                    inner.Add(keyBuffer.AsSpan(0, Address.Size + 32), withoutLeadingZeros);
                }
                else
                {
                    inner.Add(keyBuffer.AsSpan(0, Address.Size + 32), ReadOnlySpan<byte>.Empty);
                }
            }

            int innerLen = inner.Build();
            ReadOnlySpan<byte> storageTag = new byte[] { PersistedSnapshot.StorageTag };
            outer.FinishValueWrite(innerLen, storageTag);
        }
    }

    private static void WriteSelfDestructColumn(ref RsstBuilder outer, Snapshot snapshot, Span<byte> fullOutput)
    {
        // Sort self-destructs
        List<(AddressAsKey Key, bool Value)> selfDestructs = new();
        foreach (KeyValuePair<AddressAsKey, bool> kv in snapshot.SelfDestructedStorageAddresses)
        {
            selfDestructs.Add((kv.Key, kv.Value));
        }
        selfDestructs.Sort((a, b) => a.Key.Value.Bytes.SequenceCompareTo(b.Key.Value.Bytes));

        Span<byte> valueSpan = outer.BeginValueWrite(fullOutput.Length);
        using (RsstBuilder inner = new(valueSpan))
        {
            ReadOnlySpan<byte> trueValue = new byte[] { 0x01 };
            foreach ((AddressAsKey key, bool value) in selfDestructs)
            {
                inner.Add(key.Value.Bytes, value ? trueValue : ReadOnlySpan<byte>.Empty);
            }

            int innerLen = inner.Build();
            ReadOnlySpan<byte> selfDestructTag = new byte[] { PersistedSnapshot.SelfDestructTag };
            outer.FinishValueWrite(innerLen, selfDestructTag);
        }
    }

    private static void WriteStateNodesColumn(ref RsstBuilder outer, Snapshot snapshot, Span<byte> fullOutput)
    {
        // Sort state nodes
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

        Span<byte> valueSpan = outer.BeginValueWrite(fullOutput.Length);
        using (RsstBuilder inner = new(valueSpan))
        {
            byte[] keyBuffer = new byte[32 + 1];
            foreach ((TreePath path, TrieNode node) in stateNodes)
            {
                path.Path.Bytes.CopyTo(keyBuffer.AsSpan());
                keyBuffer[32] = (byte)path.Length;
                inner.Add(keyBuffer.AsSpan(0, 33), node.FullRlp.Span);
            }

            int innerLen = inner.Build();
            ReadOnlySpan<byte> stateNodeTag = new byte[] { PersistedSnapshot.StateNodeTag };
            outer.FinishValueWrite(innerLen, stateNodeTag);
        }
    }

    private static void WriteStorageNodesColumn(ref RsstBuilder outer, Snapshot snapshot, Span<byte> fullOutput)
    {
        // Sort storage nodes
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

        Span<byte> valueSpan = outer.BeginValueWrite(fullOutput.Length);
        using (RsstBuilder inner = new(valueSpan))
        {
            byte[] keyBuffer = new byte[32 + 32 + 1];
            foreach (((Hash256AsKey addr, TreePath path) snKey, TrieNode node) in storageNodes)
            {
                snKey.addr.Value.Bytes.CopyTo(keyBuffer.AsSpan());
                snKey.path.Path.Bytes.CopyTo(keyBuffer.AsSpan(32));
                keyBuffer[64] = (byte)snKey.path.Length;
                inner.Add(keyBuffer.AsSpan(0, 65), node.FullRlp.Span);
            }

            int innerLen = inner.Build();
            ReadOnlySpan<byte> storageNodeTag = new byte[] { PersistedSnapshot.StorageNodeTag };
            outer.FinishValueWrite(innerLen, storageNodeTag);
        }
    }
}
