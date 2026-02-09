// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Collections;
using Nethermind.Core.Crypto;
using Nethermind.State;
using Nethermind.Trie;

namespace Nethermind.Synchronization.SnapSync;

public class PatriciaSnapStateTree(StateTree tree, SnapUpperBoundAdapter adapter) : ISnapTree
{
    public Hash256 RootHash => tree.RootHash;

    public void SetRootFromProof(TrieNode root) => tree.RootRef = root;

    public bool IsPersisted(in TreePath path, in ValueHash256 keccak) =>
        adapter.IsPersisted(path, keccak);

    public void BulkSetAndUpdateRootHash(in ArrayPoolListRef<PatriciaTree.BulkSetEntry> entries, PatriciaTree.Flags flags)
    {
        tree.BulkSet(entries, flags);
        tree.UpdateRootHash();
    }

    public void Commit(WriteFlags writeFlags, ValueHash256 upperBound)
    {
        adapter.UpperBound = upperBound;
        tree.Commit(skipRoot: true, writeFlags);
    }

    public void Dispose() { } // No-op - Patricia doesn't own resources
}
