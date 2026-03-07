// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
namespace Nethermind.Xdc;
public sealed class XdcHeaderDecoder : IHeaderDecoder
{
    private const int NonceLength = 8;

    public BlockHeader? Decode(ref Rlp.ValueDecoderContext decoderContext,
        RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (decoderContext.IsNextItemNull())
        {
            return null;
        }
        ReadOnlySpan<byte> headerRlp = decoderContext.PeekNextItem();
        int headerSequenceLength = decoderContext.ReadSequenceLength();
        int headerCheck = decoderContext.Position + headerSequenceLength;
        var x = new BlockDecoder(new XdcHeaderDecoder());
        Hash256? parentHash = decoderContext.DecodeKeccak();
        Hash256? unclesHash = decoderContext.DecodeKeccak();
        Address? beneficiary = decoderContext.DecodeAddress();
        Hash256? stateRoot = decoderContext.DecodeKeccak();
        Hash256? transactionsRoot = decoderContext.DecodeKeccak();
        Hash256? receiptsRoot = decoderContext.DecodeKeccak();
        Bloom? bloom = decoderContext.DecodeBloom();
        UInt256 difficulty = decoderContext.DecodeUInt256();
        long number = decoderContext.DecodeLong();
        long gasLimit = decoderContext.DecodeLong();
        long gasUsed = decoderContext.DecodeLong();
        ulong timestamp = decoderContext.DecodeULong();
        byte[]? extraData = decoderContext.DecodeByteArray();

        XdcBlockHeader blockHeader = new(
            parentHash,
            unclesHash,
            beneficiary,
            difficulty,
            number,
            gasLimit,
            timestamp,
            extraData)
        {
            StateRoot = stateRoot,
            TxRoot = transactionsRoot,
            ReceiptsRoot = receiptsRoot,
            Bloom = bloom,
            GasUsed = gasUsed,
            Hash = Keccak.Compute(headerRlp)
        };

        blockHeader.MixHash = decoderContext.DecodeKeccak();
        blockHeader.Nonce = (ulong)decoderContext.DecodeUInt256(NonceLength);

        // Check if we've reached the end of the header (15-field V1 format)
        if (decoderContext.Position == headerCheck)
        {
            // Standard geth 15-field RLP (mainnet V1 blocks).
            // ExtraData = vanity(32) + sig(65).
            blockHeader.IsV1Block = true;
            blockHeader.Validators = Array.Empty<byte>();
            blockHeader.Penalties = Array.Empty<byte>();
        }
        else
        {
            // 18-field RLP: Validators, Validator, Penalties (+ optional BaseFee)
            // Note: Apothem uses 18-field format for ALL blocks (V1 has empty fields)
            blockHeader.Has18FieldRlp = true;
            blockHeader.Validators = decoderContext.DecodeByteArray();
            if ((rlpBehaviors & RlpBehaviors.ForSealing) != RlpBehaviors.ForSealing)
            {
                blockHeader.Validator = decoderContext.DecodeByteArray();
            }
            blockHeader.Penalties = decoderContext.DecodeByteArray();

            // Detect V1 blocks in 18-field format: Validators and Validator are empty
            // V2 blocks MUST have non-empty Validator (signer's signature)
            // V1 blocks have signature in ExtraData, not Validator field
            if ((blockHeader.Validators is null || blockHeader.Validators.Length == 0) &&
                (blockHeader.Validator is null || blockHeader.Validator.Length == 0))
            {
                blockHeader.IsV1Block = true;
            }

            // Optional tail: BaseFeePerGas exists if there are remaining bytes
            if (decoderContext.Position != headerCheck)
            {
                blockHeader.BaseFeePerGas = decoderContext.DecodeUInt256();
            }
        }

        if ((rlpBehaviors & RlpBehaviors.AllowExtraBytes) != RlpBehaviors.AllowExtraBytes)
        {
            decoderContext.Check(headerCheck);
        }

        return blockHeader;
    }

    public BlockHeader? Decode(RlpStream rlpStream, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (rlpStream.IsNextItemNull())
        {
            rlpStream.ReadByte();
            return null;
        }

        Span<byte> headerRlp = rlpStream.PeekNextItem();
        int headerSequenceLength = rlpStream.ReadSequenceLength();
        int headerCheck = rlpStream.Position + headerSequenceLength;

        Hash256? parentHash = rlpStream.DecodeKeccak();
        Hash256? unclesHash = rlpStream.DecodeKeccak();
        Address? beneficiary = rlpStream.DecodeAddress();
        Hash256? stateRoot = rlpStream.DecodeKeccak();
        Hash256? transactionsRoot = rlpStream.DecodeKeccak();
        Hash256? receiptsRoot = rlpStream.DecodeKeccak();
        Bloom? bloom = rlpStream.DecodeBloom();
        UInt256 difficulty = rlpStream.DecodeUInt256();
        long number = rlpStream.DecodeLong();
        long gasLimit = rlpStream.DecodeLong();
        long gasUsed = rlpStream.DecodeLong();
        ulong timestamp = rlpStream.DecodeULong();
        byte[]? extraData = rlpStream.DecodeByteArray();

        XdcBlockHeader blockHeader = new(
            parentHash,
            unclesHash,
            beneficiary,
            difficulty,
            number,
            gasLimit,
            timestamp,
            extraData)
        {
            StateRoot = stateRoot,
            TxRoot = transactionsRoot,
            ReceiptsRoot = receiptsRoot,
            Bloom = bloom,
            GasUsed = gasUsed,
            Hash = Keccak.Compute(headerRlp)
        };

        blockHeader.MixHash = rlpStream.DecodeKeccak();
        blockHeader.Nonce = (ulong)rlpStream.DecodeUInt256(NonceLength);

        // Check if we've reached the end of the header (15-field V1 format)
        if (rlpStream.Position == headerCheck)
        {
            // Standard geth 15-field RLP (mainnet V1 blocks).
            blockHeader.IsV1Block = true;
            blockHeader.Validators = Array.Empty<byte>();
            blockHeader.Penalties = Array.Empty<byte>();
        }
        else
        {
            // 18-field RLP
            blockHeader.Has18FieldRlp = true;
            blockHeader.Validators = rlpStream.DecodeByteArray();
            if ((rlpBehaviors & RlpBehaviors.ForSealing) != RlpBehaviors.ForSealing)
            {
                blockHeader.Validator = rlpStream.DecodeByteArray();
            }
            blockHeader.Penalties = rlpStream.DecodeByteArray();

            // Detect V1 blocks in 18-field format
            if ((blockHeader.Validators is null || blockHeader.Validators.Length == 0) &&
                (blockHeader.Validator is null || blockHeader.Validator.Length == 0))
            {
                blockHeader.IsV1Block = true;
            }

            if (rlpStream.Position != headerCheck)
            {
                blockHeader.BaseFeePerGas = rlpStream.DecodeUInt256();
            }
        }

        if ((rlpBehaviors & RlpBehaviors.AllowExtraBytes) != RlpBehaviors.AllowExtraBytes)
        {
            rlpStream.Check(headerCheck);
        }

        return blockHeader;
    }

    /// <summary>
    /// Wraps a plain <see cref="BlockHeader"/> as an <see cref="XdcBlockHeader"/> with empty
    /// XDC-specific fields.  Used for the genesis block (created from chain-spec JSON, not RLP)
    /// and any other non-XDC header that must be sent on the wire in 18-field XDC format.
    /// </summary>
    private static XdcBlockHeader AsXdcHeader(BlockHeader src) =>
        new(src.ParentHash, src.UnclesHash, src.Beneficiary,
            src.Difficulty, src.Number, src.GasLimit,
            src.Timestamp, src.ExtraData)
        {
            StateRoot = src.StateRoot,
            TxRoot = src.TxRoot,
            ReceiptsRoot = src.ReceiptsRoot,
            Bloom = src.Bloom,
            GasUsed = src.GasUsed,
            MixHash = src.MixHash,
            Nonce = src.Nonce,
            BaseFeePerGas = src.BaseFeePerGas,
            Hash = src.Hash,
            IsV1Block = src is XdcBlockHeader xh ? xh.IsV1Block : false,
            Has18FieldRlp = src is XdcBlockHeader xh2 ? xh2.Has18FieldRlp : false,
        };

    public void Encode(RlpStream rlpStream, BlockHeader? header, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (header is null)
        {
            rlpStream.EncodeNullObject();
            return;
        }

        // Promote a plain BlockHeader (e.g. genesis from chain-spec) to XdcBlockHeader with
        // empty validator fields so we always write the 18-field XDC format on the wire.
        if (header is not XdcBlockHeader h)
            h = AsXdcHeader(header);

        // For true V1 blocks (15-field format), encode without XDC-specific fields
        if (h.IsV1Block && !h.Has18FieldRlp)
        {
            EncodeV1(rlpStream, h, rlpBehaviors);
            return;
        }

        bool notForSealing = (rlpBehaviors & RlpBehaviors.ForSealing) != RlpBehaviors.ForSealing;
        rlpStream.StartSequence(GetContentLength(h, rlpBehaviors));
        rlpStream.Encode(h.ParentHash);
        rlpStream.Encode(h.UnclesHash);
        rlpStream.Encode(h.Beneficiary);
        rlpStream.Encode(h.StateRoot);
        rlpStream.Encode(h.TxRoot);
        rlpStream.Encode(h.ReceiptsRoot);
        rlpStream.Encode(h.Bloom);
        rlpStream.Encode(h.Difficulty);
        rlpStream.Encode(h.Number);
        rlpStream.Encode(h.GasLimit);
        rlpStream.Encode(h.GasUsed);
        rlpStream.Encode(h.Timestamp);
        rlpStream.Encode(h.ExtraData);
        rlpStream.Encode(h.MixHash);
        rlpStream.Encode(h.Nonce, NonceLength);
        rlpStream.Encode(h.Validators ?? Array.Empty<byte>());
        if (notForSealing)
        {
            rlpStream.Encode(h.Validator ?? Array.Empty<byte>());
        }
        rlpStream.Encode(h.Penalties ?? Array.Empty<byte>());

        if (!h.BaseFeePerGas.IsZero) rlpStream.Encode(h.BaseFeePerGas);
    }

    private void EncodeV1(RlpStream rlpStream, XdcBlockHeader h, RlpBehaviors rlpBehaviors)
    {
        // V1 format: 15 fields only (standard Ethereum header fields)
        rlpStream.StartSequence(GetV1ContentLength(h, rlpBehaviors));
        rlpStream.Encode(h.ParentHash);
        rlpStream.Encode(h.UnclesHash);
        rlpStream.Encode(h.Beneficiary);
        rlpStream.Encode(h.StateRoot);
        rlpStream.Encode(h.TxRoot);
        rlpStream.Encode(h.ReceiptsRoot);
        rlpStream.Encode(h.Bloom);
        rlpStream.Encode(h.Difficulty);
        rlpStream.Encode(h.Number);
        rlpStream.Encode(h.GasLimit);
        rlpStream.Encode(h.GasUsed);
        rlpStream.Encode(h.Timestamp);
        rlpStream.Encode(h.ExtraData);
        rlpStream.Encode(h.MixHash);
        rlpStream.Encode(h.Nonce, NonceLength);

        if (!h.BaseFeePerGas.IsZero) rlpStream.Encode(h.BaseFeePerGas);
    }

    public Rlp Encode(BlockHeader? item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (item is null)
        {
            return Rlp.OfEmptySequence;
        }

        XdcBlockHeader header = item as XdcBlockHeader ?? AsXdcHeader(item);

        RlpStream rlpStream = new(GetLength(header, rlpBehaviors));
        Encode(rlpStream, header, rlpBehaviors);

        return new Rlp(rlpStream.Data.ToArray());
    }

    private static int GetContentLength(XdcBlockHeader? item, RlpBehaviors rlpBehaviors)
    {
        if (item is null)
        {
            return 0;
        }

        // For true V1 blocks (15-field format)
        if (item.IsV1Block && !item.Has18FieldRlp)
        {
            return GetV1ContentLength(item, rlpBehaviors);
        }

        bool notForSealing = (rlpBehaviors & RlpBehaviors.ForSealing) != RlpBehaviors.ForSealing;
        int contentLength = 0
                            + Rlp.LengthOf(item.ParentHash)
                            + Rlp.LengthOf(item.UnclesHash)
                            + Rlp.LengthOf(item.Beneficiary)
                            + Rlp.LengthOf(item.StateRoot)
                            + Rlp.LengthOf(item.TxRoot)
                            + Rlp.LengthOf(item.ReceiptsRoot)
                            + Rlp.LengthOf(item.Bloom)
                            + Rlp.LengthOf(item.Difficulty)
                            + Rlp.LengthOf(item.Number)
                            + Rlp.LengthOf(item.GasLimit)
                            + Rlp.LengthOf(item.GasUsed)
                            + Rlp.LengthOf(item.Timestamp)
                            + Rlp.LengthOf(item.ExtraData)
                            + Rlp.LengthOf(item.MixHash)
                            + Rlp.LengthOfNonce(item.Nonce);

        contentLength += Rlp.LengthOf(item.Validators ?? Array.Empty<byte>());
        if (notForSealing)
        {
            contentLength += Rlp.LengthOf(item.Validator ?? Array.Empty<byte>());
        }
        contentLength += Rlp.LengthOf(item.Penalties ?? Array.Empty<byte>());

        if (!item.BaseFeePerGas.IsZero) contentLength += Rlp.LengthOf(item.BaseFeePerGas);
        return contentLength;
    }

    private static int GetV1ContentLength(XdcBlockHeader? item, RlpBehaviors rlpBehaviors)
    {
        if (item is null)
        {
            return 0;
        }

        int contentLength = 0
                            + Rlp.LengthOf(item.ParentHash)
                            + Rlp.LengthOf(item.UnclesHash)
                            + Rlp.LengthOf(item.Beneficiary)
                            + Rlp.LengthOf(item.StateRoot)
                            + Rlp.LengthOf(item.TxRoot)
                            + Rlp.LengthOf(item.ReceiptsRoot)
                            + Rlp.LengthOf(item.Bloom)
                            + Rlp.LengthOf(item.Difficulty)
                            + Rlp.LengthOf(item.Number)
                            + Rlp.LengthOf(item.GasLimit)
                            + Rlp.LengthOf(item.GasUsed)
                            + Rlp.LengthOf(item.Timestamp)
                            + Rlp.LengthOf(item.ExtraData)
                            + Rlp.LengthOf(item.MixHash)
                            + Rlp.LengthOfNonce(item.Nonce);

        if (!item.BaseFeePerGas.IsZero) contentLength += Rlp.LengthOf(item.BaseFeePerGas);
        return contentLength;
    }

    public int GetLength(BlockHeader? item, RlpBehaviors rlpBehaviors)
    {
        if (item is null) return Rlp.LengthOfSequence(0);
        // Always compute length as an XDC header (18 fields) so GetLength and Encode agree.
        // A plain BlockHeader (e.g. genesis) is wrapped with empty validator fields.
        XdcBlockHeader header = item as XdcBlockHeader ?? AsXdcHeader(item);
        return Rlp.LengthOfSequence(GetContentLength(header, rlpBehaviors));
    }
}
