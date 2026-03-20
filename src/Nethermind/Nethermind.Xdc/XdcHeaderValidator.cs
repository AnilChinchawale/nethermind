// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain;
using Nethermind.Consensus;
using Nethermind.Consensus.Validators;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Logging;

namespace Nethermind.Xdc;

/// <summary>
/// XDC-specific header validator that relaxes gas limit validation.
/// XDC uses XDPoS consensus which manages gas limits differently from Ethereum's
/// EIP-1559 based adjustment (validators can set gas limit directly).
/// </summary>
internal class XdcHeaderValidator : HeaderValidator
{
    private readonly IQuorumCertificateManager _qcManager;

    public XdcHeaderValidator(
        IBlockTree blockTree,
        IQuorumCertificateManager qcManager,
        ISealValidator sealValidator,
        ISpecProvider specProvider,
        ILogManager logManager)
        : base(blockTree, sealValidator, specProvider, logManager)
    {
        _qcManager = qcManager;
    }

    /// <summary>
    /// XDC validators control gas limit directly - skip Ethereum gas limit range validation
    /// </summary>
    protected override bool ValidateGasLimitRange(BlockHeader header, BlockHeader parent, IReleaseSpec spec, ref string? error)
    {
        // XDPoS consensus allows validators to set gas limit freely
        return true;
    }

    /// <summary>
    /// Allow variable difficulty for V1 blocks (clique-style consensus).
    /// V1 blocks use difficulty values like 1 (no-turn) or 2 (in-turn).
    /// V2 blocks always have difficulty 1.
    /// </summary>
    protected override bool ValidateTotalDifficulty(BlockHeader header, BlockHeader parent, ref string? error)
    {
        // V1 blocks use clique-style difficulty (variable)
        // V2 blocks always have difficulty 1
        // Skip strict difficulty check for V1 blocks
        if (header is XdcBlockHeader xdcH && xdcH.IsV1Block)
        {
            // V1 blocks can have variable difficulty - don't validate strictly
            return true;
        }
        
        // For V2 blocks, validate normally (difficulty should be 1)
        return base.ValidateTotalDifficulty(header, parent, ref error);
    }

    /// <summary>
    /// XDC blocks can have extra data up to 2048 bytes (vs Ethereum's 32 or 1024).
    /// This is needed because XDPoS encodes validator signatures and other consensus
    /// data in the extra data field. Blocks around 5.5M have 1037 bytes of extra data.
    /// </summary>
    protected override bool ValidateExtraData(BlockHeader header, IReleaseSpec spec, bool isUncle, ref string? error)
    {
        // XDC allows larger extra data for consensus metadata (validators, signatures, penalties)
        // Maximum observed: 1037 bytes at ~5.5M blocks
        // Set limit to 2048 (0x800) to be safe
        const int XdcMaxExtraDataSize = 2048;
        
        if (header.ExtraData.Length > XdcMaxExtraDataSize)
        {
            error = $"XDC extra data too large: {header.ExtraData.Length} > {XdcMaxExtraDataSize}";
            return false;
        }
        
        return true;
    }
}
