// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Evm.TransactionProcessing;

namespace Nethermind.Xdc;

/// <summary>
/// XDC-specific transaction result codes mapped to upstream ErrorType values.
/// </summary>
internal static class XdcTransactionResult
{
    // Blacklisted addresses are treated as malformed transactions
    public static readonly TransactionResult ContainsBlacklistedAddress = TransactionResult.MalformedTransaction;
    // Nonce errors reuse the upstream wrong-nonce error
    public static readonly TransactionResult NonceTooHigh = TransactionResult.WrongTransactionNonce;
    public static readonly TransactionResult NonceTooLow = TransactionResult.WrongTransactionNonce;
}
