// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain;
using Nethermind.Consensus;
using Nethermind.Consensus.Validators;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Logging;
using Nethermind.Xdc.Types;
using System;

namespace Nethermind.Xdc;

public class XdcHeaderValidator(IBlockTree blockTree, IQuorumCertificateManager quorumCertificateManager, ISealValidator sealValidator, ISpecProvider specProvider, ILogManager? logManager = null) : HeaderValidator(blockTree, sealValidator, specProvider, logManager)
{
    // XDC mainnet V2 switch block
    private const long SwitchBlock = 80_370_000;

    private bool IsV1Block(BlockHeader header) => header.Number < SwitchBlock;

    protected override bool Validate<TOrphaned>(BlockHeader header, BlockHeader parent, bool isUncle, out string? error)
    {
        if (parent is null)
            throw new ArgumentNullException(nameof(parent));

        // V1 blocks: skip V2-specific XDC validation, use base Ethereum validation only
        if (IsV1Block(header))
        {
            // For V1, just validate basic header fields
            if (!base.Validate<TOrphaned>(header, parent, isUncle, out error))
                return false;
            error = null;
            return true;
        }

        // V2 blocks: full XDC validation
        if (header is not XdcBlockHeader xdcHeader)
            throw new ArgumentException($"Only type of {nameof(XdcBlockHeader)} is allowed, but got type {header.GetType().Name}.", nameof(header));
        if (parent is not XdcBlockHeader parentXdcHeader)
            throw new ArgumentException($"Only type of {nameof(XdcBlockHeader)} is allowed, but got type {parent.GetType().Name}.", nameof(parent));

        if (xdcHeader.Validator is null || xdcHeader.Validator.Length == 0)
        {
            error = "Validator field is required in XDC header.";
            return false;
        }

        ExtraFieldsV2? extraFields = xdcHeader.ExtraConsensusData;
        if (extraFields is null)
        {
            error = "Header ExtraData doesn't contain required consensus data.";
            return false;
        }

        if (!quorumCertificateManager.VerifyCertificate(extraFields.QuorumCert, parentXdcHeader, out error))
        {
            return false;
        }

        if (xdcHeader.Nonce != XdcConstants.NonceDropVoteValue && xdcHeader.Nonce != XdcConstants.NonceAuthVoteValue)
        {
            error = $"Invalid nonce value ({xdcHeader.Nonce}) in XDC header.";
            return false;
        }

        if (xdcHeader.MixHash != Hash256.Zero)
        {
            error = "Non-zero mix hash.";
            return false;
        }

        if (xdcHeader.UnclesHash != Keccak.OfAnEmptySequenceRlp)
        {
            error = "Cannot contain uncles.";
            return false;
        }

        if (!base.Validate<TOrphaned>(header, parent, isUncle, out error))
        {
            return false;
        }

        error = null;
        return true;
    }

    protected override bool ValidateGasLimitRange(BlockHeader header, BlockHeader parent, IReleaseSpec spec, ref string? error)
    {
        //We ignore gas limit validation for genesis block
        if (parent.Number == 0)
            return true;
        return base.ValidateGasLimitRange(header, parent, spec, ref error);
    }

    protected override bool ValidateSeal(BlockHeader header, BlockHeader parent, bool isUncle, ref string? error)
    {
        // Dispatching seal validator handles V1/V2 internally
        if (!_sealValidator.ValidateSeal(header, false))
        {
            error = "Invalid validator signature.";
            return false;
        }

        // V1 blocks: skip V2-specific ValidateParams (QC, masternodes etc.)
        if (IsV1Block(header))
            return true;

        if (_sealValidator is XdcSealValidator xdcSealValidator ?
            !xdcSealValidator.ValidateParams(parent, header, out error) :
            !_sealValidator.ValidateParams(parent, header, isUncle))
        {
            error = "Invalid consensus data in header.";
            return false;
        }

        return true;
    }

    // Extra consensus data is validated in SealValidator
    protected override bool ValidateExtraData(BlockHeader header, IReleaseSpec spec, bool isUncle, ref string? error) => true;

    protected override bool ValidateTotalDifficulty(BlockHeader header, BlockHeader parent, ref string? error)
    {
        // V1: difficulty varies (Clique-style, any positive value)
        // V2: difficulty is always 1
        if (IsV1Block(header))
        {
            if (header.Difficulty == 0)
            {
                error = $"V1 difficulty must be positive, got 0.";
                return false;
            }
        }
        else if (header.Difficulty != 1)
        {
            error = "Difficulty must be 1.";
            return false;
        }
        return base.ValidateTotalDifficulty(header, parent, ref error);
    }

    protected override bool ValidateTimestamp(BlockHeader header, BlockHeader parent, ref string? error)
    {
        // V1: use standard Ethereum timestamp validation
        if (IsV1Block(header))
            return base.ValidateTimestamp(header, parent, ref error);

        // V2: check min period
        var xdcSpec = _specProvider.GetXdcSpec((XdcBlockHeader)header);
        if (parent.Timestamp + (ulong)xdcSpec.MinePeriod > header.Timestamp)
        {
            error = "Timestamp in header cannot be lower than ancestor plus slot time.";
            return false;
        }

        return true;
    }
}
