// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Xdc.Spec;

namespace Nethermind.Xdc;

/// <summary>
/// Cache for signing transactions used in reward calculation.
/// Signing transactions in XDC are special transactions sent by masternodes
/// to confirm they've signed a particular block.
/// </summary>
public interface ISigningTxCache
{
    /// <summary>
    /// Get signing transactions for a specific block.
    /// </summary>
    Transaction[] GetSigningTransactions(Hash256 blockHash, long blockNumber, IXdcReleaseSpec spec);
}
