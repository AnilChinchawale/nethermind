// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;

namespace Nethermind.Xdc.Contracts;

/// <summary>
/// Abstraction for the XDC Masternode Voting contract (0x88).
/// Used by <see cref="XdcRewardCalculator"/> to resolve the owner address
/// of a masternode candidate when distributing block rewards.
/// </summary>
public interface IMasternodeVotingContract
{
    /// <summary>
    /// Returns the owner address for the given masternode candidate as of the
    /// provided block header. Falls back to <paramref name="candidate"/> if
    /// the contract returns Address.Zero or the call fails.
    /// </summary>
    Address GetCandidateOwner(BlockHeader header, Address candidate);
}
