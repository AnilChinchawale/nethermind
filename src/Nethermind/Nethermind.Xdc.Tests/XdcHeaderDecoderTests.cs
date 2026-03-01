// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
using NUnit.Framework;

namespace Nethermind.Xdc.Tests;

/// <summary>
/// RLP round-trip tests for XdcBlockHeader / XdcHeaderDecoder.
///
/// Verifies that encoding an XdcBlockHeader and then decoding it produces a
/// header with identical field values — the most fundamental correctness
/// guarantee for block propagation over the wire.
/// </summary>
[TestFixture]
public class XdcHeaderDecoderTests
{
    private XdcHeaderDecoder _decoder = null!;

    [SetUp]
    public void SetUp()
    {
        _decoder = new XdcHeaderDecoder();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static XdcBlockHeader BuildSampleHeader(
        long number = 1800,
        byte[]? validators = null,
        byte[]? validator = null,
        byte[]? penalties = null,
        UInt256 baseFee = default)
    {
        var header = new XdcBlockHeader(
            parentHash: Keccak.Compute("parent"u8.ToArray()),
            unclesHash: Keccak.OfAnEmptySequenceRlp,
            beneficiary: Address.Zero,
            difficulty: UInt256.One,
            number: number,
            gasLimit: XdcConstants.TargetGasLimit,
            timestamp: (ulong)DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            extraData: new byte[] { XdcConstants.ConsensusVersion, 0x00 })
        {
            StateRoot = Keccak.EmptyTreeHash,
            TxRoot = Keccak.EmptyTreeHash,
            ReceiptsRoot = Keccak.EmptyTreeHash,
            Bloom = Bloom.Empty,
            GasUsed = 0,
            MixHash = Keccak.Zero,
            Nonce = 0,
            Validators = validators ?? Array.Empty<byte>(),
            Validator = validator ?? Array.Empty<byte>(),
            Penalties = penalties ?? Array.Empty<byte>(),
            BaseFeePerGas = baseFee,
        };
        return header;
    }

    private XdcBlockHeader RoundTrip(XdcBlockHeader original, RlpBehaviors behaviors = RlpBehaviors.None)
    {
        Rlp encoded = _decoder.Encode(original, behaviors);
        var rlpStream = new RlpStream(encoded.Bytes);
        var decoded = _decoder.Decode(rlpStream, behaviors);
        Assert.That(decoded, Is.Not.Null, "Decoded header must not be null.");
        return (XdcBlockHeader)decoded!;
    }

    // ── Round-trip tests ─────────────────────────────────────────────────────

    [Test]
    public void Encode_Decode_RoundTrip_BasicFields()
    {
        XdcBlockHeader original = BuildSampleHeader(number: 1800);
        XdcBlockHeader decoded = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Number, Is.EqualTo(original.Number));
            Assert.That(decoded.GasLimit, Is.EqualTo(original.GasLimit));
            Assert.That(decoded.Difficulty, Is.EqualTo(original.Difficulty));
            Assert.That(decoded.Timestamp, Is.EqualTo(original.Timestamp));
            Assert.That(decoded.Beneficiary, Is.EqualTo(original.Beneficiary));
            Assert.That(decoded.ParentHash, Is.EqualTo(original.ParentHash));
            Assert.That(decoded.UnclesHash, Is.EqualTo(original.UnclesHash));
            Assert.That(decoded.StateRoot, Is.EqualTo(original.StateRoot));
            Assert.That(decoded.TxRoot, Is.EqualTo(original.TxRoot));
            Assert.That(decoded.ReceiptsRoot, Is.EqualTo(original.ReceiptsRoot));
            Assert.That(decoded.MixHash, Is.EqualTo(original.MixHash));
            Assert.That(decoded.Nonce, Is.EqualTo(original.Nonce));
        });
    }

    [Test]
    public void Encode_Decode_RoundTrip_ExtraData_IsPreserved()
    {
        XdcBlockHeader original = BuildSampleHeader();
        XdcBlockHeader decoded = RoundTrip(original);

        Assert.That(decoded.ExtraData, Is.EqualTo(original.ExtraData));
    }

    [Test]
    public void Encode_Decode_RoundTrip_XdcValidatorsField_IsPreserved()
    {
        // Pack two 20-byte addresses into Validators field (as geth-xdc does in checkpoint headers).
        byte[] twoAddresses = new byte[40];
        new Address("0x0000000000000000000000000000000000000088").Bytes
            .CopyTo(twoAddresses.AsSpan(0, 20));
        new Address("0x0000000000000000000000000000000000000089").Bytes
            .CopyTo(twoAddresses.AsSpan(20, 20));

        XdcBlockHeader original = BuildSampleHeader(validators: twoAddresses);
        XdcBlockHeader decoded = RoundTrip(original);

        Assert.That(decoded.Validators, Is.EqualTo(original.Validators));
    }

    [Test]
    public void Encode_Decode_RoundTrip_PenaltiesField_IsPreserved()
    {
        byte[] penaltyAddr = new byte[20];
        new Address("0x0000000000000000000000000000000000000090").Bytes
            .CopyTo(penaltyAddr.AsSpan(0, 20));

        XdcBlockHeader original = BuildSampleHeader(penalties: penaltyAddr);
        XdcBlockHeader decoded = RoundTrip(original);

        Assert.That(decoded.Penalties, Is.EqualTo(original.Penalties));
    }

    [Test]
    public void Encode_Decode_RoundTrip_BaseFeePerGas_IsPreserved()
    {
        UInt256 baseFee = (UInt256)1_000_000_000; // 1 Gwei
        XdcBlockHeader original = BuildSampleHeader(baseFee: baseFee);
        XdcBlockHeader decoded = RoundTrip(original);

        Assert.That(decoded.BaseFeePerGas, Is.EqualTo(original.BaseFeePerGas));
    }

    [Test]
    public void Encode_Decode_ForSealing_OmitsValidatorField()
    {
        byte[] validatorSig = new byte[65]; // 65-byte ECDSA signature
        Array.Fill(validatorSig, (byte)0xAB);

        XdcBlockHeader original = BuildSampleHeader(validator: validatorSig);

        // ForSealing omits the Validator (seal) field so the pre-seal hash is reproducible.
        Rlp sealingRlp = _decoder.Encode(original, RlpBehaviors.ForSealing);
        var rlpStream = new RlpStream(sealingRlp.Bytes);
        var decoded = (XdcBlockHeader)_decoder.Decode(rlpStream, RlpBehaviors.ForSealing)!;

        // When decoded with ForSealing, Validator is not present → null or empty.
        bool validatorAbsent =
            decoded.Validator is null ||
            decoded.Validator.Length == 0;

        Assert.That(validatorAbsent, Is.True,
            "ForSealing encoding must not include the Validator (seal) field.");
    }

    [Test]
    public void GetLength_MatchesActualEncodedLength()
    {
        XdcBlockHeader header = BuildSampleHeader();
        int declared = _decoder.GetLength(header, RlpBehaviors.None);
        Rlp encoded = _decoder.Encode(header, RlpBehaviors.None);

        Assert.That(encoded.Bytes.Length, Is.EqualTo(declared),
            "GetLength must return the exact byte count produced by Encode.");
    }

    [Test]
    public void Encode_NullHeader_ProducesEmptySequence()
    {
        Rlp encoded = _decoder.Encode(null, RlpBehaviors.None);
        Assert.That(encoded, Is.EqualTo(Rlp.OfEmptySequence));
    }

    [Test]
    public void Decode_NullInput_ReturnsNull()
    {
        // Build a RLP stream containing a null/empty item.
        RlpStream stream = new RlpStream(1);
        stream.EncodeNullObject();
        stream.Reset();

        BlockHeader? decoded = _decoder.Decode(stream, RlpBehaviors.None);
        Assert.That(decoded, Is.Null, "Decoding a null RLP item must return null.");
    }

    [Test]
    public void Encode_NonXdcBlockHeader_Throws()
    {
        // XdcHeaderDecoder must reject plain BlockHeaders — they lack XDC fields.
        var plain = new BlockHeader(
            Keccak.Zero, Keccak.Zero, Address.Zero,
            UInt256.One, 1, 1_000_000, 0, Array.Empty<byte>());

        Assert.Throws<ArgumentException>(() =>
            _decoder.Encode(plain, RlpBehaviors.None));
    }
}
