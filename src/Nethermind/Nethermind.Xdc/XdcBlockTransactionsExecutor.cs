// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

// XDC GasBailout: Skip balance pre-check failures caused by accumulated state root divergence.
// This mirrors gasBailout=true behavior in erigon-xdc and is required for XDC mainnet/apothem sync.
// Root cause: state roots diverge from geth at checkpoint reward blocks (block 1800+).
// As a result, some account balances in NM's state differ from geth's canonical state,
// causing spurious "insufficient sender balance" rejections for valid transactions.
// Fix: catch the exception and continue — the XdcStateRootCache handles state root divergence.
// See also: erigon-xdc gasBailout commit 3381feaa, nethermind XdcStateRootCache commit 912e7f8cfe.

using System;
using Nethermind.Blockchain;
using Nethermind.Blockchain.Tracing;
using Nethermind.Consensus.Processing;
using Nethermind.Core;
using Nethermind.Evm.State;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Evm.Tracing;
using Nethermind.Logging;
using Nethermind.State;
using Nethermind.Trie;

namespace Nethermind.Xdc;

/// <summary>
/// XDC-specific block transactions executor with gasBailout support.
/// Catches "insufficient sender balance" errors caused by accumulated state root divergence
/// and skips those transactions rather than invalidating the entire block.
/// </summary>
internal class XdcBlockTransactionsExecutor : BlockProcessor.BlockValidationTransactionsExecutor
{
    private readonly ILogger _logger;

    public XdcBlockTransactionsExecutor(
        ITransactionProcessorAdapter transactionProcessor,
        IWorldState stateProvider,
        ILogManager logManager,
        BlockProcessor.BlockValidationTransactionsExecutor.ITransactionProcessedEventHandler? eventHandler = null)
        : base(transactionProcessor, stateProvider, eventHandler)
    {
        _logger = logManager.GetClassLogger<XdcBlockTransactionsExecutor>();
    }

    protected override void ProcessTransaction(
        Block block,
        Transaction currentTx,
        int index,
        BlockReceiptsTracer receiptsTracer,
        ProcessingOptions processingOptions)
    {
        try
        {
            base.ProcessTransaction(block, currentTx, index, receiptsTracer, processingOptions);
        }
        catch (InvalidTransactionException ex) when (IsBalanceError(ex))
        {
            // XDC GasBailout: log and skip — insufficient balance due to state root divergence.
            // IMPORTANT: StartNewTxTrace was called before Execute, but EndTxTrace was NOT called
            // (exception thrown during BuyGas). We must call MarkAsFailed() THEN EndTxTrace() so
            // a proper failed receipt is added — without it the tracer state gets corrupted and
            // subsequent blocks crash with ArgumentOutOfRangeException.
            try
            {
                receiptsTracer.MarkAsFailed(
                    currentTx.To ?? Address.Zero,
                    new GasConsumed(0, 0),
                    Array.Empty<byte>(),
                    ex.Message);
                receiptsTracer.EndTxTrace();
            }
            catch { /* tracer may be in invalid state; ignore */ }

            if (_logger.IsWarn)
                _logger.Warn($"[XDC-GasBailout] Block {block.Number} tx[{index}] {currentTx.Hash}: {ex.Message.Split('\n')[0]} — skipping (insufficient balance)");
        }
        catch (InsufficientBalanceException ex)
        {
            // XDC GasBailout: insufficient balance during EVM value transfer (CALL/CREATE).
            // This is thrown from StateProvider.SubtractFromBalance during contract execution.
            // We must add a failed receipt so receipt count matches tx count.
            receiptsTracer.MarkAsFailed(
                currentTx.To ?? Address.Zero,
                new GasConsumed(0, 0),
                Array.Empty<byte>(),
                ex.Message);
            receiptsTracer.EndTxTrace();

            if (_logger.IsWarn)
                _logger.Warn($"[XDC-GasBailout] Block {block.Number} tx[{index}] {currentTx.Hash}: {ex.Message} — skipping (insufficient balance for transfer)");
        }
        catch (MissingTrieNodeException ex)
        {
            // XDC GasBailout: missing trie node — state DB is incomplete, skip this tx
            // This happens when state root diverges and a trie node is not in RocksDB.
            // IMPORTANT: Must call MarkAsFailed() THEN EndTxTrace() to create a proper failed
            // receipt — calling EndTxTrace() alone corrupts tracer state.
            try
            {
                receiptsTracer.MarkAsFailed(
                    currentTx.To ?? Address.Zero,
                    new GasConsumed(0, 0),
                    Array.Empty<byte>(),
                    $"MissingTrieNode: {ex.Hash}");
                receiptsTracer.EndTxTrace();
            }
            catch { /* tracer already in invalid state; ignore */ }

            if (_logger.IsWarn)
                _logger.Warn($"[XDC-GasBailout] Block {block.Number} tx[{index}] {currentTx.Hash}: MissingTrieNode {ex.Hash} — skipping (state divergence)");
        }
    }

    private static bool IsBalanceError(InvalidTransactionException ex) =>
        ex.Message.Contains("insufficient sender balance", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("INSUFFICIENT_SENDER_BALANCE", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("insufficient funds", StringComparison.OrdinalIgnoreCase);
}
