// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;

namespace Nethermind.Xdc;

public sealed class XdcHeaderDecoder : BaseXdcHeaderDecoder<XdcBlockHeader>
{
    protected override XdcBlockHeader CreateHeader(
        Hash256? parentHash,
        Hash256? unclesHash,
        Address? beneficiary,
        UInt256 difficulty,
        long number,
        long gasLimit,
        ulong timestamp,
        byte[]? extraData)
        => new(parentHash, unclesHash, beneficiary, difficulty, number, gasLimit, timestamp, extraData);

    protected override void DecodeHeaderSpecificFields(ref Rlp.ValueDecoderContext decoderContext, XdcBlockHeader header, RlpBehaviors rlpBehaviors, int headerCheck)
    {
        header.Validators = decoderContext.DecodeByteArray();
        if (!IsForSealing(rlpBehaviors))
        {
            header.Validator = decoderContext.DecodeByteArray();
        }
        header.Penalties = decoderContext.DecodeByteArray();

        // Optional tail: BaseFeePerGas exists if there are remaining bytes
        if (decoderContext.Position != headerCheck)
        {
            header.BaseFeePerGas = decoderContext.DecodeUInt256();
        }
    }

    protected override void EncodeHeaderSpecificFields(RlpStream rlpStream, XdcBlockHeader header, RlpBehaviors rlpBehaviors)
    {
        rlpStream.Encode(header.Validators);
        if (!IsForSealing(rlpBehaviors))
        {
            rlpStream.Encode(header.Validator);
        }
        rlpStream.Encode(header.Penalties);

        if (!header.BaseFeePerGas.IsZero)
        {
            rlpStream.Encode(header.BaseFeePerGas);
        }
    }

<<<<<<< HEAD
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
        };

    public void Encode(RlpStream rlpStream, BlockHeader? header, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
=======
    protected override int GetHeaderSpecificContentLength(XdcBlockHeader header, RlpBehaviors rlpBehaviors)
>>>>>>> upstream/master
    {
        int len = 0
            + Rlp.LengthOf(header.Validators)
            + Rlp.LengthOf(header.Penalties);

        if (!IsForSealing(rlpBehaviors))
        {
            len += Rlp.LengthOf(header.Validator);
        }

<<<<<<< HEAD
        // Promote a plain BlockHeader (e.g. genesis from chain-spec) to XdcBlockHeader with
        // empty validator fields so we always write the 18-field XDC format on the wire.
        if (header is not XdcBlockHeader h)
            h = AsXdcHeader(header);

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

    public int GetLength(BlockHeader? item, RlpBehaviors rlpBehaviors)
    {
        if (item is null) return Rlp.LengthOfSequence(0);
        // Always compute length as an XDC header (18 fields) so GetLength and Encode agree.
        // A plain BlockHeader (e.g. genesis) is wrapped with empty validator fields.
        XdcBlockHeader header = item as XdcBlockHeader ?? AsXdcHeader(item);
        return Rlp.LengthOfSequence(GetContentLength(header, rlpBehaviors));
=======
        if (!header.BaseFeePerGas.IsZero)
        {
            len += Rlp.LengthOf(header.BaseFeePerGas);
        }

        return len;
>>>>>>> upstream/master
    }
}
