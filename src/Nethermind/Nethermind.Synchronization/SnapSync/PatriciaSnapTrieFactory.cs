// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core.Crypto;
using Nethermind.Logging;
using Nethermind.State;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;

namespace Nethermind.Synchronization.SnapSync;

public class PatriciaSnapTrieFactory(INodeStorage nodeStorage, ILogManager logManager) : ISnapTrieFactory
{
    private readonly RawScopedTrieStore _stateTrieStore = new(nodeStorage, null);

    public ISnapTree CreateStateTree()
    {
        var adapter = new SnapUpperBoundAdapter(_stateTrieStore);
        return new PatriciaSnapStateTree(new StateTree(adapter, logManager), adapter);
    }

    public ISnapTree CreateStorageTree(in ValueHash256 accountPath)
    {
        var adapter = new SnapUpperBoundAdapter(new RawScopedTrieStore(nodeStorage, accountPath.ToCommitment()));
        return new PatriciaSnapStorageTree(new StorageTree(adapter, logManager), adapter);
    }

    public Hash256? ResolveStorageRoot(byte[] nodeData)
    {
        try
        {
            TreePath emptyTreePath = TreePath.Empty;
            TrieNode node = new(NodeType.Unknown, nodeData, isDirty: true);
            node.ResolveNode(_stateTrieStore, emptyTreePath);
            node.ResolveKey(_stateTrieStore, ref emptyTreePath);
            return node.Keccak;
        }
        catch
        {
            return null;
        }
    }
}
