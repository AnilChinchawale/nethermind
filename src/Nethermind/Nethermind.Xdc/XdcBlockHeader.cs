// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Crypto;
using Nethermind.Int256;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.RLP;
using Nethermind.Xdc.Types;
using System;
using System.Collections.Immutable;

namespace Nethermind.Xdc;

public class XdcBlockHeader : BlockHeader, IHashResolver
{
    private static readonly XdcHeaderDecoder _headerDecoder = new();
    private static readonly ExtraConsensusDataDecoder _extraConsensusDataDecoder = new();
    public XdcBlockHeader(
        Hash256 parentHash,
        Hash256 unclesHash,
        Address beneficiary,
        in UInt256 difficulty,
        long number,
        long gasLimit,
        ulong timestamp,
        byte[] extraData)
        : base(parentHash, unclesHash, beneficiary, difficulty, number, gasLimit, timestamp, extraData)
    {
    }

    /// <summary>
    /// True if this header was decoded from a 15-field V1 RLP (pre-XDPoS-V2 format).
    /// In this case, the signer signature is in ExtraData[^65..] and seal hash uses ExtraData[..^65].
    /// </summary>
    public bool IsV1Block { get; set; }
    /// <summary>
    /// True when the block was decoded from 18-field RLP (Apothem format).
    /// Even V1 blocks on Apothem use 18 fields with empty Validators/Validator/Penalties.
    /// This flag ensures the encoder produces the same RLP for correct hash computation.
    /// </summary>
    public bool Has18FieldRlp { get; set; }

    public byte[]? Validators { get; set; }

    private ImmutableArray<Address>? _validatorsAddress;
    public ImmutableArray<Address>? ValidatorsAddress
    {
        get
        {
            if (_validatorsAddress is not null)
                return _validatorsAddress;
            _validatorsAddress = XdcExtensions.ExtractAddresses(Validators)?.ToImmutableArray();
            return _validatorsAddress;
        }
        set { _validatorsAddress = value; }
    }
    public byte[]? Validator { get; set; }
    public byte[]? Penalties { get; set; }

    private ImmutableArray<Address>? _penaltiesAddress;
    public ImmutableArray<Address>? PenaltiesAddress
    {
        get
        {
            if (_penaltiesAddress is not null)
                return _penaltiesAddress;
            _penaltiesAddress = XdcExtensions.ExtractAddresses(Penalties)?.ToImmutableArray();
            return _penaltiesAddress;
        }
        set { _penaltiesAddress = value; }
    }

    private ExtraFieldsV2 _extraFieldsV2;
    /// <summary>
    /// Consensus data that must be included in a V2 block, which contains the quorum certificate and round information.
    /// </summary>
    public ExtraFieldsV2? ExtraConsensusData
    {
        get
        {
            if (_extraFieldsV2 is not null)
            {
                return _extraFieldsV2;
            }

            if (ExtraData is null || ExtraData.Length == 0)
                return null;

            //Check V2 consensus version in ExtraData field.
            if (ExtraData.Length < 3 || ExtraData[0] != XdcConstants.ConsensusVersion)
                return null;
            Rlp.ValueDecoderContext valueDecoderContext = new Rlp.ValueDecoderContext(ExtraData.AsSpan(1));
            _extraFieldsV2 = _extraConsensusDataDecoder.Decode(ref valueDecoderContext);
            return _extraFieldsV2;
        }
        set { _extraFieldsV2 = value; }
    }

    public virtual ValueHash256 CalculateHash()
    {
        KeccakRlpStream rlpStream = new KeccakRlpStream();
        _headerDecoder.Encode(rlpStream, this);
        return rlpStream.GetHash();
    }

    public static XdcBlockHeader FromBlockHeader(BlockHeader src)
    {
        var x = new XdcBlockHeader(
            src.ParentHash,
            src.UnclesHash,
            src.Beneficiary,
            src.Difficulty,
            src.Number,
            src.GasLimit,
            src.Timestamp,
            src.ExtraData)
        {
            Bloom = src.Bloom ?? Bloom.Empty,
            Hash = src.Hash,
            MixHash = src.MixHash,
            Nonce = src.Nonce,
            TxRoot = src.TxRoot,
            TotalDifficulty = src.TotalDifficulty,
            AuRaStep = src.AuRaStep,
            AuRaSignature = src.AuRaSignature,
            ReceiptsRoot = src.ReceiptsRoot,
            BaseFeePerGas = src.BaseFeePerGas,
            WithdrawalsRoot = src.WithdrawalsRoot,
            RequestsHash = src.RequestsHash,
            IsPostMerge = src.IsPostMerge,
            ParentBeaconBlockRoot = src.ParentBeaconBlockRoot,
            ExcessBlobGas = src.ExcessBlobGas,
            BlobGasUsed = src.BlobGasUsed,
        };

        return x;
    }
}
