// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain;
using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Crypto;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.Spec;
using Nethermind.Xdc.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Nethermind.Xdc;
internal class XdcSealValidator : ISealValidator
{
    private readonly ISnapshotManager _snapshotManager;
    private readonly IEpochSwitchManager _epochSwitchManager;
    private readonly ISpecProvider _specProvider;
    private readonly IBlockTree _blockTree;
    private readonly ILogger _logger;
    private readonly EthereumEcdsa _ethereumEcdsa = new(0); // Ignore chainId since we don't sign transactions here
    private readonly XdcHeaderDecoder _headerDecoder = new();

    /// <summary>
    /// TIPRandomize activation block (mainnet = 3464000).
    /// After this block, M2 assignment shifts based on block position in epoch.
    /// </summary>
    private const long TipRandomizeBlock = 3_464_000;

    /// <summary>
    /// M2 validator index byte length in checkpoint header's Validators field.
    /// Each M2 index is stored as a 4-byte zero-padded ASCII integer string.
    /// </summary>
    private const int M2ByteLength = 4;

    public XdcSealValidator(
        ISnapshotManager snapshotManager,
        IEpochSwitchManager epochSwitchManager,
        ISpecProvider specProvider,
        IBlockTree blockTree,
        ILogManager logManager)
    {
        _snapshotManager = snapshotManager;
        _epochSwitchManager = epochSwitchManager;
        _specProvider = specProvider;
        _blockTree = blockTree;
        _logger = logManager.GetClassLogger();
    }

    public bool ValidateParams(BlockHeader parent, BlockHeader header, bool isUncle = false)
    {
        return ValidateParams(parent, header, out _);
    }

    public bool ValidateParams(BlockHeader parent, BlockHeader header, out string error)
    {
        if (header is not XdcBlockHeader xdcHeader)
            throw new ArgumentException($"Only type of {nameof(XdcBlockHeader)} is allowed, but got type {header.GetType().Name}.", nameof(header));

        IXdcReleaseSpec xdcSpec = _specProvider.GetXdcSpec(xdcHeader);

        // Determine V1 vs V2 based on whether this block is before the switch block
        if (xdcHeader.Number <= xdcSpec.SwitchBlock)
        {
            return ValidateV1Params(xdcHeader, xdcSpec, out error);
        }

        return ValidateV2Params(xdcHeader, xdcSpec, out error);
    }

    /// <summary>
    /// V1 parameter validation: verify M2 double validation for non-genesis blocks.
    /// In V1, every non-checkpoint block after epoch 1 must have a valid M2 signature
    /// in header.Validator, and the recovered M2 address must match the assigned M2
    /// for this block's M1 signer.
    /// </summary>
    private bool ValidateV1Params(XdcBlockHeader xdcHeader, IXdcReleaseSpec xdcSpec, out string error)
    {
        long number = xdcHeader.Number;
        long epoch = xdcSpec.EpochLength;

        // Skip validation for genesis and first epoch (block <= epoch)
        if (number <= epoch)
        {
            error = null;
            return true;
        }

        // M2 double validation: header.Validator must contain M2's ECDSA signature (65 bytes)
        if (xdcHeader.Validator is null || xdcHeader.Validator.Length == 0)
        {
            // geth-xdc: "header must contain validator info following double validation design"
            error = $"V1 block {number}: missing M2 validator signature (double validation required after epoch 1).";
            return false;
        }

        // Recover M2 address from header.Validator signature
        Address m2Recovered = RecoverV1Validator(xdcHeader);
        if (m2Recovered is null)
        {
            error = $"V1 block {number}: failed to recover M2 validator address from signature.";
            return false;
        }

        // The M1 signer (block creator) should already be recovered as header.Author by ValidateSeal
        Address m1 = xdcHeader.Author;
        if (m1 is null)
        {
            // Try recovering from ExtraData seal
            m1 = RecoverV1Signer(xdcHeader);
            if (m1 is null)
            {
                error = $"V1 block {number}: cannot determine M1 signer for double validation.";
                return false;
            }
        }

        // Get assigned M2 for this M1 from the checkpoint header
        Address assignedM2 = GetAssignedValidator(m1, xdcHeader, xdcSpec);
        if (assignedM2 is null)
        {
            // For checkpoint blocks themselves (number % epoch == 0), the M2 may be self-assigned
            if (number % epoch == 0)
            {
                error = null;
                return true;
            }
            error = $"V1 block {number}: could not determine assigned M2 validator for M1={m1}.";
            return false;
        }

        if (m2Recovered != assignedM2)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 double validation failed at block {number}: M1={m1}, " +
                              $"recovered M2={m2Recovered}, assigned M2={assignedM2}");
            error = $"V1 block {number}: M2 double validation failed. " +
                    $"Recovered={m2Recovered}, expected={assignedM2} for M1={m1}.";
            return false;
        }

        error = null;
        return true;
    }

    /// <summary>
    /// V2 parameter validation (existing logic).
    /// </summary>
    private bool ValidateV2Params(XdcBlockHeader xdcHeader, IXdcReleaseSpec xdcSpec, out string error)
    {
        if (xdcHeader.ExtraConsensusData is null)
        {
            error = "ExtraData doesn't contain required consensus data.";
            return false;
        }

        ExtraFieldsV2 extraFieldsV2 = xdcHeader.ExtraConsensusData!;

        if (extraFieldsV2.BlockRound <= extraFieldsV2.QuorumCert.ProposedBlockInfo.Round)
        {
            error = "Round number is not greater than the round in the QC.";
            return false;
        }

        Address[] masternodes;

        if (_epochSwitchManager.IsEpochSwitchAtBlock(xdcHeader))
        {
            if (xdcHeader.Nonce != XdcConstants.NonceDropVoteValue)
            {
                error = "Vote nonce in checkpoint block non-zero.";
                return false;
            }
            if (xdcHeader.Validators is null || xdcHeader.Validators.Length == 0)
            {
                error = "Empty validators list on epoch switch block.";
                return false;
            }
            if (xdcHeader.Validators.Length % Address.Size != 0)
            {
                error = "Invalid signer list on checkpoint block.";
                return false;
            }

            (masternodes, var penaltiesAddresses) = _snapshotManager.CalculateNextEpochMasternodes(xdcHeader.Number, xdcHeader.ParentHash, xdcSpec);
            if (!xdcHeader.ValidatorsAddress.SequenceEqual(masternodes))
            {
                error = "Validators does not match what's stored in snapshot minus its penalty.";
                return false;
            }

            if (!xdcHeader.PenaltiesAddress.SequenceEqual(penaltiesAddresses))
            {
                error = "Penalties does not match.";
                return false;
            }
        }
        else
        {
            if (xdcHeader.Validators is not null &&
                xdcHeader.Validators.Length != 0)
            {
                error = "Validators are not empty in non-epoch switch header.";
                return false;
            }
            if (xdcHeader.Penalties is not null &&
                xdcHeader.Penalties?.Length != 0)
            {
                error = "Penalties are not empty in non-epoch switch header.";
                return false;
            }
            EpochSwitchInfo epochSwitchInfo = _epochSwitchManager.GetEpochSwitchInfo(xdcHeader);
            masternodes = epochSwitchInfo.Masternodes;
            if (masternodes is null || masternodes.Length == 0)
                throw new InvalidOperationException($"Snap shot returned no master nodes for header \n{xdcHeader.ToString()}");
        }

        ulong currentLeaderIndex = (xdcHeader.ExtraConsensusData.BlockRound % (ulong)xdcSpec.EpochLength % (ulong)masternodes.Length);
        if (masternodes[(int)currentLeaderIndex] != xdcHeader.Author)
        {
            error = $"Block proposer {xdcHeader.Author} is not the current leader.";
            return false;
        }

        error = null;
        return true;
    }

    public bool ValidateSeal(BlockHeader header, bool force) => ValidateSeal(header);
    public bool ValidateSeal(BlockHeader header)
    {
        if (header is not XdcBlockHeader xdcHeader)
            throw new ArgumentException($"Only type of {nameof(XdcBlockHeader)} is allowed, but got type {header.GetType().Name}.", nameof(header));

        IXdcReleaseSpec xdcSpec = _specProvider.GetXdcSpec(xdcHeader);

        // V1 seal validation: recover signer from ExtraData seal (last 65 bytes)
        if (xdcHeader.Number <= xdcSpec.SwitchBlock)
        {
            return ValidateV1Seal(xdcHeader);
        }

        // V2 seal validation (existing logic)
        return ValidateV2Seal(xdcHeader);
    }

    /// <summary>
    /// V1 seal validation: recover M1 signer from ExtraData (last 65 bytes).
    /// In V1, the seal is the ECDSA signature appended to ExtraData.
    /// The signer address must equal header.Beneficiary (Coinbase).
    /// Note: In V1, Beneficiary is 0x0 for non-checkpoint blocks (voting system),
    /// so we just set Author and return true — M1 turn validation is done separately.
    /// </summary>
    private bool ValidateV1Seal(XdcBlockHeader xdcHeader)
    {
        if (xdcHeader.Author is not null)
            return true; // Already recovered

        byte[] extraData = xdcHeader.ExtraData;
        if (extraData is null || extraData.Length < XdcConstants.ExtraVanity + XdcConstants.ExtraSeal)
            return false;

        // Extract signature (last 65 bytes of ExtraData)
        byte[] signature = new byte[XdcConstants.ExtraSeal];
        Buffer.BlockCopy(extraData, extraData.Length - XdcConstants.ExtraSeal, signature, 0, XdcConstants.ExtraSeal);

        byte v = signature[64];
        if (v >= 4) return false; // Invalid recovery ID

        // Compute V1 sigHash (standard 15 Ethereum header fields with ExtraData truncated)
        ValueHash256 hash = ComputeV1SigHash(xdcHeader);

        // Recover address
        Address signer = _ethereumEcdsa.RecoverAddress(
            new Signature(signature.AsSpan(0, 64), signature[64]),
            hash);

        xdcHeader.Author = signer;
        return true; // V1 doesn't require Beneficiary == Author (Beneficiary is used for voting)
    }

    /// <summary>
    /// V2 seal validation (existing logic).
    /// </summary>
    private bool ValidateV2Seal(XdcBlockHeader xdcHeader)
    {
        if (xdcHeader.Author is null)
        {
            if (xdcHeader.Validator is null
                || xdcHeader.Validator.Length != 65
                || xdcHeader.Validator[64] >= 4)
                return false;

            Address signer = _ethereumEcdsa.RecoverAddress(
                new Signature(xdcHeader.Validator.AsSpan(0, 64), xdcHeader.Validator[64]),
                Keccak.Compute(_headerDecoder.Encode(xdcHeader, RlpBehaviors.ForSealing).Bytes));

            xdcHeader.Author = signer;
        }
        return xdcHeader.Beneficiary == xdcHeader.Author;
    }

    // ─── V1 Double Validation Helpers ───────────────────────────────────────

    /// <summary>
    /// Recover the M2 validator address from header.Validator (65-byte ECDSA signature).
    /// The signature is over the V1 sigHash of the header.
    /// </summary>
    private Address RecoverV1Validator(XdcBlockHeader header)
    {
        if (header.Validator is null || header.Validator.Length != XdcConstants.ExtraSeal)
            return null;

        byte v = header.Validator[64];
        if (v >= 4) return null;

        ValueHash256 hash = ComputeV1SigHash(header);
        try
        {
            return _ethereumEcdsa.RecoverAddress(
                new Signature(header.Validator.AsSpan(0, 64), header.Validator[64]),
                hash);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Recover M1 signer from ExtraData seal (last 65 bytes).
    /// </summary>
    private Address RecoverV1Signer(XdcBlockHeader header)
    {
        byte[] extraData = header.ExtraData;
        if (extraData is null || extraData.Length < XdcConstants.ExtraVanity + XdcConstants.ExtraSeal)
            return null;

        byte[] signature = new byte[XdcConstants.ExtraSeal];
        Buffer.BlockCopy(extraData, extraData.Length - XdcConstants.ExtraSeal, signature, 0, XdcConstants.ExtraSeal);

        byte v = signature[64];
        if (v >= 4) return null;

        ValueHash256 hash = ComputeV1SigHash(header);
        try
        {
            return _ethereumEcdsa.RecoverAddress(
                new Signature(signature.AsSpan(0, 64), signature[64]),
                hash);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Compute the V1 sigHash: RLP([15 standard Ethereum fields with ExtraData truncated by 65 bytes]).
    /// This matches geth-xdc consensus/XDPoS/engines/engine_v1/utils.go sigHash().
    /// </summary>
    private ValueHash256 ComputeV1SigHash(XdcBlockHeader header)
    {
        byte[] extraData = header.ExtraData;
        byte[] extraDataWithoutSeal = new byte[extraData.Length - XdcConstants.ExtraSeal];
        Buffer.BlockCopy(extraData, 0, extraDataWithoutSeal, 0, extraDataWithoutSeal.Length);

        int contentLength = GetV1SigHashContentLength(header, extraDataWithoutSeal);
        var stream = new RlpStream(Rlp.LengthOfSequence(contentLength));
        stream.StartSequence(contentLength);

        stream.Encode(header.ParentHash);
        stream.Encode(header.UnclesHash);
        stream.Encode(header.Beneficiary);
        stream.Encode(header.StateRoot);
        stream.Encode(header.TxRoot);
        stream.Encode(header.ReceiptsRoot);
        stream.Encode(header.Bloom);
        stream.Encode(header.Difficulty);
        stream.Encode(header.Number);
        stream.Encode(header.GasLimit);
        stream.Encode(header.GasUsed);
        stream.Encode(header.Timestamp);
        stream.Encode(extraDataWithoutSeal);
        stream.Encode(header.MixHash);
        stream.Encode(header.Nonce, 8);

        if (!header.BaseFeePerGas.IsZero)
            stream.Encode(header.BaseFeePerGas);

        return ValueKeccak.Compute(stream.Data);
    }

    private int GetV1SigHashContentLength(XdcBlockHeader header, byte[] extraDataWithoutSeal)
    {
        int length = 0;
        length += Rlp.LengthOf(header.ParentHash);
        length += Rlp.LengthOf(header.UnclesHash);
        length += Rlp.LengthOf(header.Beneficiary);
        length += Rlp.LengthOf(header.StateRoot);
        length += Rlp.LengthOf(header.TxRoot);
        length += Rlp.LengthOf(header.ReceiptsRoot);
        length += Rlp.LengthOf(header.Bloom);
        length += Rlp.LengthOf(header.Difficulty);
        length += Rlp.LengthOf(header.Number);
        length += Rlp.LengthOf(header.GasLimit);
        length += Rlp.LengthOf(header.GasUsed);
        length += Rlp.LengthOf(header.Timestamp);
        length += Rlp.LengthOf(extraDataWithoutSeal);
        length += Rlp.LengthOf(header.MixHash);
        length += Rlp.LengthOfNonce(header.Nonce);

        if (!header.BaseFeePerGas.IsZero)
            length += Rlp.LengthOf(header.BaseFeePerGas);

        return length;
    }

    /// <summary>
    /// Get the assigned M2 validator for a given M1 creator at a specific block.
    /// Ports geth-xdc GetValidator() / getM1M2FromCheckpointHeader().
    /// </summary>
    private Address GetAssignedValidator(Address creator, XdcBlockHeader header, IXdcReleaseSpec xdcSpec)
    {
        long epoch = xdcSpec.EpochLength;
        long number = header.Number;

        // Compute checkpoint block number
        long cpNo = number;
        if (number % epoch != 0)
        {
            cpNo = number - (number % epoch);
        }
        if (cpNo == 0)
        {
            return null; // No double validation for first epoch
        }

        // Get checkpoint header
        BlockHeader cpHeaderRaw;
        if (number % epoch == 0)
        {
            // For checkpoint blocks, use the header itself
            cpHeaderRaw = header;
        }
        else
        {
            cpHeaderRaw = _blockTree.FindHeader(cpNo);
        }

        if (cpHeaderRaw is null)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 double validation: checkpoint header at {cpNo} not found for block {number}");
            return null;
        }

        XdcBlockHeader cpHeader = cpHeaderRaw as XdcBlockHeader;
        if (cpHeader is null)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 double validation: checkpoint header at {cpNo} is not XdcBlockHeader");
            return null;
        }

        // Build M1→M2 mapping from checkpoint header
        var m1m2 = GetM1M2FromCheckpointHeader(cpHeader, header, xdcSpec);
        if (m1m2 is null)
            return null;

        m1m2.TryGetValue(creator, out Address assignedM2);
        return assignedM2;
    }

    /// <summary>
    /// Port of geth-xdc getM1M2FromCheckpointHeader().
    /// Parses masternodes from checkpoint ExtraData and validator indices from checkpoint Validators field,
    /// then builds the M1→M2 mapping.
    /// </summary>
    private Dictionary<Address, Address> GetM1M2FromCheckpointHeader(
        XdcBlockHeader checkpointHeader, XdcBlockHeader currentHeader, IXdcReleaseSpec xdcSpec)
    {
        long epoch = xdcSpec.EpochLength;

        // Verify this is a checkpoint block
        if (checkpointHeader.Number % epoch != 0)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 M1M2: block {checkpointHeader.Number} is not a checkpoint (epoch={epoch})");
            return null;
        }

        // Parse masternodes from checkpoint's ExtraData: [32 vanity][N*20 addresses][65 seal]
        Address[] masternodes;
        try
        {
            masternodes = checkpointHeader.ExtraData.ParseV1Masternodes();
        }
        catch (Exception ex)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 M1M2: failed to parse masternodes from checkpoint {checkpointHeader.Number}: {ex.Message}");
            return null;
        }

        // Parse M2 validator indices from checkpoint's Validators field
        // Format: each index is a 4-byte zero-padded ASCII string (e.g., "0\x00\x00\x00" = 0)
        long[] validators = ExtractV1ValidatorIndices(checkpointHeader.Validators);
        if (validators is null || validators.Length == 0)
        {
            if (_logger.IsDebug)
                _logger.Debug($"V1 M1M2: no validator indices in checkpoint {checkpointHeader.Number}");
            return null;
        }

        // Build M1→M2 mapping
        return BuildM1M2Map(masternodes, validators, currentHeader, xdcSpec);
    }

    /// <summary>
    /// Port of geth-xdc ExtractValidatorsFromBytes().
    /// Parses 4-byte zero-padded ASCII integer strings into int64 validator indices.
    /// </summary>
    private static long[] ExtractV1ValidatorIndices(byte[] validatorsBytes)
    {
        if (validatorsBytes is null || validatorsBytes.Length == 0)
            return null;

        int count = validatorsBytes.Length / M2ByteLength;
        if (count == 0) return null;

        long[] validators = new long[count];
        for (int i = 0; i < count; i++)
        {
            // Extract 4-byte chunk and trim null bytes
            ReadOnlySpan<byte> chunk = validatorsBytes.AsSpan(i * M2ByteLength, M2ByteLength);

            // Trim trailing \x00 bytes
            int len = M2ByteLength;
            while (len > 0 && chunk[len - 1] == 0) len--;

            if (len == 0)
            {
                validators[i] = 0;
                continue;
            }

            string numStr = Encoding.ASCII.GetString(chunk.Slice(0, len));
            if (!long.TryParse(numStr, out long val))
                return null; // Parse error

            validators[i] = val;
        }

        return validators;
    }

    /// <summary>
    /// Port of geth-xdc getM1M2().
    /// Maps each masternode (M1) to its assigned second validator (M2) using the
    /// randomized indices from the checkpoint header.
    /// </summary>
    private static Dictionary<Address, Address> BuildM1M2Map(
        Address[] masternodes, long[] validators, XdcBlockHeader currentHeader, IXdcReleaseSpec xdcSpec)
    {
        var m1m2 = new Dictionary<Address, Address>();
        int maxMNs = masternodes.Length;

        if (validators.Length < maxMNs)
        {
            // "len(m2) is less than len(m1)" — this shouldn't happen in valid blocks
            return null;
        }

        if (maxMNs == 0) return m1m2;

        long epoch = xdcSpec.EpochLength;
        bool isForked = currentHeader.Number >= TipRandomizeBlock;

        ulong moveM2 = 0;
        if (isForked)
        {
            // After TIPRandomize: shift M2 assignment based on position in epoch
            moveM2 = ((ulong)currentHeader.Number % (ulong)epoch / (ulong)maxMNs) % (ulong)maxMNs;
        }

        for (int i = 0; i < maxMNs; i++)
        {
            Address m1 = masternodes[i];
            ulong m2Index = (ulong)(validators[i] % maxMNs);
            m2Index = (m2Index + moveM2) % (ulong)maxMNs;
            m1m2[m1] = masternodes[(int)m2Index];
        }

        return m1m2;
    }
}
