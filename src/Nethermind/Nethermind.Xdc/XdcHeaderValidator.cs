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
    /// XDC blocks can have extra data larger than Ethereum's default limit.
    /// XDPoS encodes validator signatures and consensus metadata in extra data.
    /// Blocks around 5.5M have 1037 bytes. Set limit to 2048.
    /// </summary>
    protected override bool ValidateExtraData(BlockHeader header, IReleaseSpec spec, bool isUncle, ref string? error)
    {
        const int XdcMaxExtraDataSize = 2048;
        
        if (header.ExtraData.Length > XdcMaxExtraDataSize)
        {
            error = $"XDC extra data too large: {header.ExtraData.Length} > {XdcMaxExtraDataSize}";
            return false;
        }
        
        return true;
    }
}
