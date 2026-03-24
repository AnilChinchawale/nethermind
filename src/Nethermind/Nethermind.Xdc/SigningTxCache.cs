// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using Nethermind.Blockchain;
using Nethermind.Core;
using Nethermind.Core.Caching;
using Nethermind.Core.Crypto;
using Nethermind.Logging;
using Nethermind.Xdc.Spec;

namespace Nethermind.Xdc;

/// <summary>
/// Caches signing transactions (txs targeting BlockSigners contract 0x89) per block hash.
/// Used by XdcRewardCalculator to tally masternode signatures for reward distribution.
/// Mirrors geth-xdc's signingTxsCache behavior.
/// </summary>
public class SigningTxCache : ISigningTxCache
{
    private readonly IBlockTree _blockTree;
    private readonly ILogger _logger;

    // LRU cache: blockHash → signing transactions for that block
    private readonly LruCache<Hash256, Transaction[]> _cache = new(4096, "SigningTxCache");

    public SigningTxCache(IBlockTree blockTree, ILogManager logManager)
    {
        _blockTree = blockTree ?? throw new ArgumentNullException(nameof(blockTree));
        _logger = logManager.GetClassLogger<SigningTxCache>();
    }

    public Transaction[] GetSigningTransactions(Hash256 blockHash, long blockNumber, IXdcReleaseSpec spec)
    {
        if (_cache.TryGet(blockHash, out Transaction[] cached))
            return cached;

        // Load from block tree
        Block? block = _blockTree.FindBlock(blockHash, BlockTreeLookupOptions.None);
        if (block is null)
        {
            if (_logger.IsDebug)
                _logger.Debug($"SigningTxCache: block {blockNumber} ({blockHash}) not found");
            return Array.Empty<Transaction>();
        }

        var signingTxs = new List<Transaction>();
        foreach (Transaction tx in block.Transactions)
        {
            if (tx.To is not null && tx.To == XdcConstants.BlockSignersAddress)
            {
                signingTxs.Add(tx);
            }
        }

        Transaction[] result = signingTxs.ToArray();
        _cache.Set(blockHash, result);
        return result;
    }
}
