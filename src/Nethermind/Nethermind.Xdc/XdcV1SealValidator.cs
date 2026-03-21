// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Consensus;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Crypto;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.Spec;
using Nethermind.Xdc.Types;
using System;
using System.Linq;

namespace Nethermind.Xdc;

/// <summary>
/// Validates XDPoS V1 block seals (blocks before SwitchBlock).
///
/// V1 extra data layout:
///   [32 bytes vanity][65 bytes ECDSA seal][optional N×20 bytes validator addresses]
///
/// Signing hash: keccak256(RLP(header with ExtraData = vanity + validators, Validator field empty))
/// The 65-byte seal at ExtraData[32..97] is recovered via secp256k1 to obtain the block author.
/// </summary>
internal class XdcV1SealValidator : ISealValidator
{
    private const int VanityLength = 32;
    private const int SealLength = 65;
    private const int MinExtraDataLength = VanityLength + SealLength; // 97

    private readonly ISnapshotManager _snapshotManager;
    private readonly ISpecProvider _specProvider;
    private readonly EthereumEcdsa _ethereumEcdsa = new(0); // chainId=0: we sign headers, not transactions
    private readonly XdcHeaderDecoder _headerDecoder = new();

    public XdcV1SealValidator(ISnapshotManager snapshotManager, ISpecProvider specProvider)
    {
        _snapshotManager = snapshotManager;
        _specProvider = specProvider;
    }

    public bool ValidateParams(BlockHeader parent, BlockHeader header, bool isUncle = false)
        => ValidateParams(parent, header, out _);

    public bool ValidateParams(BlockHeader parent, BlockHeader header, out string? error)
    {
        // ── 1. Extra data minimum length ───────────────────────────────────────
        if (header.ExtraData is null || header.ExtraData.Length < MinExtraDataLength)
        {
            error = $"V1 extra data too short: {header.ExtraData?.Length ?? 0} bytes (minimum {MinExtraDataLength}).";
            return false;
        }

        // ── 2. Difficulty must be 1 (out-of-turn) or 2 (in-turn) ──────────────
        if (header.Difficulty != 1 && header.Difficulty != 2)
        {
            error = $"Invalid V1 difficulty {header.Difficulty}: must be 1 (out-of-turn) or 2 (in-turn).";
            return false;
        }

        // ── 3. Get XDC spec ────────────────────────────────────────────────────
        // GetXdcSpec requires an XdcBlockHeader; for V1 blocks that arrive as plain
        // BlockHeader we promote first, then retrieve the spec.
        XdcBlockHeader xdcHeader = header is XdcBlockHeader xdcH
            ? xdcH
            : XdcBlockHeader.FromBlockHeader(header);

        IXdcReleaseSpec spec = _specProvider.GetXdcSpec(xdcHeader);
        if (spec is null)
        {
            error = "Could not obtain IXdcReleaseSpec for V1 block.";
            return false;
        }

        int epoch = spec.EpochLength;
        int gap = spec.Gap;

        bool isCheckpoint = epoch > 0 && header.Number % epoch == 0;
        bool isGap = epoch > 0 && gap > 0 && !isCheckpoint &&
                     (header.Number % epoch >= epoch - gap);

        int extraValidatorBytes = header.ExtraData.Length - MinExtraDataLength;

        // ── 4. Epoch (checkpoint) block rules ──────────────────────────────────
        if (isCheckpoint)
        {
            // Nonce must be 0 or NonceDropVoteValue
            if (header.Nonce != 0 && header.Nonce != XdcConstants.NonceDropVoteValue)
            {
                error = $"V1 checkpoint block nonce must be 0 or max, got {header.Nonce}.";
                return false;
            }

            // Validators list must be an integer multiple of 20 bytes
            if (extraValidatorBytes < 0 || extraValidatorBytes % Address.Size != 0)
            {
                error = $"V1 checkpoint block extra data has invalid validators section ({extraValidatorBytes} bytes).";
                return false;
            }
        }
        // ── 5. Gap block rules ─────────────────────────────────────────────────
        else if (isGap)
        {
            // Gap blocks announce next-epoch validators; allow N*20 extra bytes
            if (extraValidatorBytes < 0 || extraValidatorBytes % Address.Size != 0)
            {
                error = $"V1 gap block extra data has invalid validators section ({extraValidatorBytes} bytes).";
                return false;
            }
        }
        // ── 6. Normal block rules ──────────────────────────────────────────────
        else
        {
            // No validators in extra data for normal blocks
            if (extraValidatorBytes != 0)
            {
                error = $"V1 normal block extra data must be exactly {MinExtraDataLength} bytes, got {header.ExtraData.Length}.";
                return false;
            }
        }

        // ── 7. In-turn difficulty check (requires recovering signer first) ─────
        // We must know the signer to verify in-turn vs out-of-turn.
        // If the author is already cached, use it directly; otherwise recover here.
        Address? author = header.Author;
        if (author is null)
        {
            if (!TryRecoverSigner(header, out author))
            {
                error = "V1 seal: failed to recover block signer.";
                return false;
            }
            header.Author = author;
        }

        // Get masternodes snapshot to check signer membership and turn order
        Snapshot? snapshot = _snapshotManager.GetSnapshotByBlockNumber(header.Number, spec);
        if (snapshot is null)
        {
            // No snapshot available: skip in-turn check (common during initial sync)
            error = null;
            return true;
        }

        // V1: masternodes are NextEpochCandidates of the most recent gap-block snapshot
        Address[] masternodes = snapshot.NextEpochCandidates;
        if (masternodes.Length == 0)
        {
            error = "V1 ValidateParams: empty masternode list in snapshot.";
            return false;
        }

        // Signer must be in the masternode list
        int signerIndex = Array.IndexOf(masternodes, author);
        if (signerIndex < 0)
        {
            error = $"V1 block signer {author} is not in the masternode list.";
            return false;
        }

        // In-turn: (blockNumber % masternodeCount) == signerIndex → difficulty 2, else 1
        long expectedDifficulty = (header.Number % masternodes.Length == signerIndex) ? 2 : 1;
        if ((long)header.Difficulty != expectedDifficulty)
        {
            error = $"V1 difficulty mismatch: expected {expectedDifficulty} for signer at index {signerIndex}, got {header.Difficulty}.";
            return false;
        }

        error = null;
        return true;
    }

    public bool ValidateSeal(BlockHeader header, bool force) => ValidateSeal(header);

    public bool ValidateSeal(BlockHeader header)
    {
        // Genesis block always passes seal validation
        if (header.Number == 0) return true;

        if (header.ExtraData is null || header.ExtraData.Length < MinExtraDataLength)
            return false;

        if (header.Author is not null)
            return true; // Already verified (cached during ValidateParams)

        return TryRecoverSigner(header, out Address? signer) && signer is not null;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Recovers the block signer from the 65-byte seal.
    /// For normal blocks: seal is at ExtraData[32..97]
    /// For checkpoint/gap blocks: seal is at ExtraData[length-65..length]
    /// Caches the result in header.Author.
    /// </summary>
    internal bool TryRecoverSigner(BlockHeader header, out Address? signer)
    {
        signer = null;

        if (header.ExtraData is null || header.ExtraData.Length < MinExtraDataLength)
            return false;

        // Seal is ALWAYS the last 65 bytes (geth compatibility)
        byte[] seal = header.ExtraData[^SealLength..];

        // y-parity must be 0, 1, 2, or 3; values ≥ 4 crash the syscall
        if (seal[64] >= 4)
            return false;

        // Build the "for-sealing" ExtraData: everything except the last 65 bytes (seal)
        byte[] sealingExtraData = header.ExtraData[..^SealLength];

        // Encode the header for sealing (strips Validator RLP field, replaces ExtraData)
        Hash256 sealingHash = ComputeV1SealingHash(header, sealingExtraData);

        signer = _ethereumEcdsa.RecoverAddress(
            new Signature(seal.AsSpan(0, 64), seal[64]),
            sealingHash);

        if (header.Author is null)
            header.Author = signer;

        return signer is not null;
    }

    /// <summary>
    /// Encodes the header for V1 signing: uses XdcHeaderDecoder with ForSealing behavior
    /// but substitutes <paramref name="sealingExtraData"/> (vanity + optional validators,
    /// sans the 65-byte ECDSA seal) for the original ExtraData.
    ///
    /// NOTE: This produces an 18-field XDC RLP (with empty Validators/Penalties fields).
    /// If geth-xdc V1 used pure 15-field Ethereum RLP for signing, this will need to
    /// be switched to a standard HeaderDecoder. Adjust when testing against real V1 blocks.
    /// </summary>
    private Hash256 ComputeV1SealingHash(BlockHeader header, byte[] sealingExtraData)
    {
        // We need an XdcBlockHeader to use XdcHeaderDecoder.Encode.
        XdcBlockHeader temp = header is XdcBlockHeader xdcHdr
            ? CloneWithExtraData(xdcHdr, sealingExtraData)
            : WrapAsXdcHeader(header, sealingExtraData);

        Rlp encoded = _headerDecoder.Encode(temp, RlpBehaviors.ForSealing);
        return Keccak.Compute(encoded.Bytes);
    }

    private static XdcBlockHeader CloneWithExtraData(XdcBlockHeader src, byte[] extraData)
    {
        var clone = new XdcBlockHeader(
            src.ParentHash,
            src.UnclesHash,
            src.Beneficiary,
            src.Difficulty,
            src.Number,
            src.GasLimit,
            src.Timestamp,
            extraData)
        {
            StateRoot = src.StateRoot,
            TxRoot = src.TxRoot,
            ReceiptsRoot = src.ReceiptsRoot,
            Bloom = src.Bloom,
            GasUsed = src.GasUsed,
            MixHash = src.MixHash,
            Nonce = src.Nonce,
            BaseFeePerGas = src.BaseFeePerGas,
            // V1 blocks have empty Validators/Validator/Penalties
            Validators = Array.Empty<byte>(),
            Validator = Array.Empty<byte>(),
            Penalties = Array.Empty<byte>(),
        };
        return clone;
    }

    private static XdcBlockHeader WrapAsXdcHeader(BlockHeader src, byte[] extraData)
    {
        return XdcBlockHeader.FromBlockHeader(src) is XdcBlockHeader xdc
            ? CloneWithExtraData(xdc, extraData)
            : throw new InvalidOperationException("FromBlockHeader must return XdcBlockHeader.");
    }

    // Static helper ─ used by tests / dispatcher
    internal static bool IsCheckpointBlock(long blockNum, int epoch) =>
        epoch > 0 && blockNum % epoch == 0;

    internal static bool IsGapBlock(long blockNum, int epoch, int gap) =>
        epoch > 0 && gap > 0 && blockNum % epoch != 0 &&
        blockNum % epoch >= epoch - gap;
}
