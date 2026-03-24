// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Xdc.Spec;

namespace Nethermind.Xdc;

/// <summary>
/// Extension methods for identifying XDC special transactions.
/// In XDPoS, certain system contract calls (BlockSigners, Validator, Randomize)
/// bypass normal gas/balance validation and EVM execution.
/// </summary>
internal static class XdcTransactionExtensions
{
    /// <summary>
    /// Returns true if the transaction targets one of the XDC system contracts
    /// (BlockSigners 0x89, Validator 0x88, or Randomize 0x90).
    /// These transactions get zero gas price and skip balance checks.
    /// </summary>
    public static bool IsSpecialTransaction(this Transaction tx, IXdcReleaseSpec spec)
    {
        if (tx.To is null) return false;
        return tx.To == XdcConstants.BlockSignersAddress
            || tx.To == XdcConstants.ValidatorAddress
            || tx.To == XdcConstants.RandomizeAddress;
    }

    /// <summary>
    /// Returns true if the transaction is a signing transaction (targets BlockSigners 0x89).
    /// Sign transactions record masternode block signatures for reward calculation.
    /// </summary>
    public static bool IsSignTransaction(this Transaction tx, IXdcReleaseSpec spec)
    {
        return tx.To is not null && tx.To == XdcConstants.BlockSignersAddress;
    }

    /// <summary>
    /// Returns true if the transaction requires special handling (same as IsSpecialTransaction).
    /// Used in gas calculation and nonce validation overrides.
    /// </summary>
    public static bool RequiresSpecialHandling(this Transaction tx, IXdcReleaseSpec spec)
    {
        return IsSpecialTransaction(tx, spec);
    }
}
