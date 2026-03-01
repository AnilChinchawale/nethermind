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
}
