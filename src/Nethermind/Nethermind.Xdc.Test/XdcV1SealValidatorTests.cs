// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Core.Test.Builders;
using Nethermind.Crypto;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.Spec;
using Nethermind.Xdc.Types;
using NSubstitute;
using NUnit.Framework;
using System;
using System.Linq;

namespace Nethermind.Xdc.Test;

[TestFixture]
internal class XdcV1SealValidatorTests
{
    private const int VanityLength = 32;
    private const int SealLength = 65;
    private const int MinExtraDataLength = VanityLength + SealLength; // 97

    private ISnapshotManager _snapshotManager = null!;
    private ISpecProvider _specProvider = null!;
    private IXdcReleaseSpec _releaseSpec = null!;
    private EthereumEcdsa _ecdsa = null!;
    private XdcHeaderDecoder _headerDecoder = null!;

    [SetUp]
    public void SetUp()
    {
        _snapshotManager = Substitute.For<ISnapshotManager>();
        _specProvider = Substitute.For<ISpecProvider>();
        _releaseSpec = Substitute.For<IXdcReleaseSpec>();
        _releaseSpec.EpochLength.Returns(900);
        _releaseSpec.Gap.Returns(450);
        _specProvider.GetSpec(Arg.Any<ForkActivation>()).Returns(_releaseSpec);

        _ecdsa = new EthereumEcdsa(0);
        _headerDecoder = new XdcHeaderDecoder();
    }

    #region ValidateParams Tests

    [Test]
    public void ValidateParams_ExtraDataTooShort_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = CreateV1Header(extraDataLength: 96); // too short (needs 97)

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("too short"));
    }

    [Test]
    public void ValidateParams_NullExtraData_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = Build.A.XdcBlockHeader()
            .WithNumber(100)
            .WithDifficulty(1)
            .TestObject;
        header.ExtraData = null!;

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("too short"));
    }

    [Test]
    public void ValidateParams_InvalidDifficulty_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = CreateV1Header(difficulty: 3); // must be 1 or 2

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("difficulty"));
    }

    [Test]
    public void ValidateParams_ValidDifficulty1_PassesDifficultyCheck()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, _) = CreateSignedV1Header(difficulty: 1);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null); // no snapshot = skip turn check

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void ValidateParams_ValidDifficulty2_PassesDifficultyCheck()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, _) = CreateSignedV1Header(difficulty: 2);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.True);
    }

    [Test]
    public void ValidateParams_CheckpointBlock_InvalidNonce_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        // Block 900 is a checkpoint (900 % 900 == 0)
        var (header, _) = CreateSignedV1Header(blockNumber: 900, difficulty: 1, nonce: 42);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("nonce"));
    }

    [Test]
    public void ValidateParams_CheckpointBlock_ZeroNonce_Passes()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, _) = CreateSignedV1Header(blockNumber: 900, difficulty: 1, nonce: 0);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.True);
    }

    [Test]
    public void ValidateParams_CheckpointBlock_MaxNonce_Passes()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, _) = CreateSignedV1Header(blockNumber: 900, difficulty: 1, nonce: XdcConstants.NonceDropVoteValue);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.True);
    }

    [Test]
    public void ValidateParams_NormalBlock_ExtraValidatorsInExtraData_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        // Block 100 is a normal block (not checkpoint, not gap)
        var (header, _) = CreateSignedV1Header(blockNumber: 100, difficulty: 1, extraValidatorCount: 3);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns((Snapshot?)null);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("must be exactly"));
    }

    [Test]
    public void ValidateParams_SignerNotInMasternodes_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, signer) = CreateSignedV1Header(blockNumber: 100, difficulty: 1);

        // Create snapshot with different masternodes (not including our signer)
        var otherSigners = Enumerable.Range(0, 3)
            .Select(_ => new PrivateKeyGenerator().Generate().Address)
            .ToArray();
        var snapshot = new Snapshot(99, TestItem.KeccakA, otherSigners);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns(snapshot);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("not in the masternode list"));
    }

    [Test]
    public void ValidateParams_InTurnDifficultyMismatch_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var signerKey = new PrivateKeyGenerator().Generate();
        var (header, _) = CreateSignedV1Header(blockNumber: 100, difficulty: 1, signerKey: signerKey);

        // Signer at index 0, block 100: 100 % 3 == 1 → signer should be at index 1 for in-turn
        // But our signer is at index 0, so expected difficulty is 1 (out-of-turn) ✓
        // Let's make signer at index 1 so expected is 2 but we set 1
        var masternodes = new[]
        {
            TestItem.AddressA,
            signerKey.Address, // index 1: in-turn for block 100 (100 % 3 == 1) → expect diff=2
            TestItem.AddressC,
        };
        var snapshot = new Snapshot(99, TestItem.KeccakA, masternodes);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns(snapshot);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.False);
        Assert.That(error, Does.Contain("difficulty mismatch"));
    }

    [Test]
    public void ValidateParams_CorrectInTurnDifficulty_Passes()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var signerKey = new PrivateKeyGenerator().Generate();
        // Block 99: 99 % 3 == 0, so signer at index 0 is in-turn → expect difficulty 2
        var (header, _) = CreateSignedV1Header(blockNumber: 99, difficulty: 2, signerKey: signerKey);

        var masternodes = new[]
        {
            signerKey.Address, // index 0: in-turn for block 99 (99 % 3 == 0) → expect diff=2
            TestItem.AddressB,
            TestItem.AddressC,
        };
        var snapshot = new Snapshot(98, TestItem.KeccakA, masternodes);
        _snapshotManager.GetSnapshotByBlockNumber(Arg.Any<long>(), Arg.Any<IXdcReleaseSpec>())
            .Returns(snapshot);

        bool result = validator.ValidateParams(Build.A.BlockHeader.TestObject, header, out string? error);

        Assert.That(result, Is.True);
        Assert.That(error, Is.Null);
    }

    #endregion

    #region ValidateSeal Tests

    [Test]
    public void ValidateSeal_ExtraDataTooShort_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = CreateV1Header(extraDataLength: 50);

        bool result = validator.ValidateSeal(header);

        Assert.That(result, Is.False);
    }

    [Test]
    public void ValidateSeal_InvalidYParity_ReturnsFalse()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = CreateV1Header();
        // Set invalid y-parity (>= 4)
        header.ExtraData![VanityLength + SealLength - 1] = 5;

        bool result = validator.ValidateSeal(header);

        Assert.That(result, Is.False);
    }

    [Test]
    public void ValidateSeal_ValidSignature_ReturnsTrue()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var (header, _) = CreateSignedV1Header();

        bool result = validator.ValidateSeal(header);

        Assert.That(result, Is.True);
        Assert.That(header.Author, Is.Not.Null);
    }

    [Test]
    public void ValidateSeal_AlreadyHasAuthor_ReturnsTrue()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var header = CreateV1Header();
        header.Author = TestItem.AddressA; // Pre-set author

        bool result = validator.ValidateSeal(header);

        Assert.That(result, Is.True);
    }

    #endregion

    #region TryRecoverSigner Tests

    [Test]
    public void TryRecoverSigner_ValidSignature_RecoversSigner()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var signerKey = new PrivateKeyGenerator().Generate();
        var (header, _) = CreateSignedV1Header(signerKey: signerKey);

        bool success = validator.TryRecoverSigner(header, out Address? signer);

        Assert.That(success, Is.True);
        Assert.That(signer, Is.EqualTo(signerKey.Address));
    }

    [Test]
    public void TryRecoverSigner_CachesAuthor()
    {
        var validator = new XdcV1SealValidator(_snapshotManager, _specProvider);
        var signerKey = new PrivateKeyGenerator().Generate();
        var (header, _) = CreateSignedV1Header(signerKey: signerKey);
        header.Author = null; // Ensure not pre-set

        validator.TryRecoverSigner(header, out _);

        Assert.That(header.Author, Is.EqualTo(signerKey.Address));
    }

    #endregion

    #region Static Helper Tests

    [Test]
    [TestCase(0, 900, true)]
    [TestCase(900, 900, true)]
    [TestCase(1800, 900, true)]
    [TestCase(1, 900, false)]
    [TestCase(899, 900, false)]
    [TestCase(901, 900, false)]
    public void IsCheckpointBlock_ReturnsExpected(long blockNum, int epoch, bool expected)
    {
        bool result = XdcV1SealValidator.IsCheckpointBlock(blockNum, epoch);
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    [TestCase(450, 900, 450, true)]  // 450 % 900 = 450 >= 900 - 450 = 450 ✓
    [TestCase(451, 900, 450, true)]  // 451 % 900 = 451 >= 450 ✓
    [TestCase(899, 900, 450, true)]  // 899 % 900 = 899 >= 450 ✓
    [TestCase(449, 900, 450, false)] // 449 % 900 = 449 < 450 ✗
    [TestCase(900, 900, 450, false)] // checkpoint, not gap
    [TestCase(100, 900, 450, false)] // 100 < 450
    public void IsGapBlock_ReturnsExpected(long blockNum, int epoch, int gap, bool expected)
    {
        bool result = XdcV1SealValidator.IsGapBlock(blockNum, epoch, gap);
        Assert.That(result, Is.EqualTo(expected));
    }

    #endregion

    #region Helpers

    private XdcBlockHeader CreateV1Header(
        long blockNumber = 100,
        ulong difficulty = 1,
        int extraDataLength = MinExtraDataLength,
        ulong nonce = 0)
    {
        XdcBlockHeader header = (XdcBlockHeader)Build.A.XdcBlockHeader()
            .WithNumber(blockNumber)
            .WithDifficulty(difficulty)
            .WithNonce(nonce)
            .TestObject;
        header.ExtraData = new byte[extraDataLength];
        header.Validators = Array.Empty<byte>();
        header.Validator = Array.Empty<byte>();
        header.Penalties = Array.Empty<byte>();
        return header;
    }

    /// <summary>
    /// Creates a V1 header with a valid ECDSA seal embedded in ExtraData[32..97].
    /// </summary>
    private (XdcBlockHeader header, PrivateKey signer) CreateSignedV1Header(
        long blockNumber = 100,
        ulong difficulty = 1,
        PrivateKey? signerKey = null,
        ulong nonce = 0,
        int extraValidatorCount = 0)
    {
        signerKey ??= new PrivateKeyGenerator().Generate();

        // Build header with ExtraData containing just vanity (+ optional validators) initially
        int extraLen = VanityLength + SealLength + (extraValidatorCount * Address.Size);
        var vanity = new byte[VanityLength];
        var validators = new byte[extraValidatorCount * Address.Size];
        for (int i = 0; i < extraValidatorCount; i++)
        {
            TestItem.AddressA.Bytes.CopyTo(validators.AsSpan(i * Address.Size));
        }

        XdcBlockHeader header = (XdcBlockHeader)Build.A.XdcBlockHeader()
            .WithNumber(blockNumber)
            .WithDifficulty(difficulty)
            .WithNonce(nonce)
            .WithBeneficiary(signerKey.Address)
            .TestObject;

        // Set empty XDC-specific fields (V1 doesn't use them)
        header.Validators = Array.Empty<byte>();
        header.Validator = Array.Empty<byte>();
        header.Penalties = Array.Empty<byte>();

        // Compute sealing hash with ExtraData = vanity + validators (no seal)
        byte[] sealingExtraData = vanity.Concat(validators).ToArray();
        header.ExtraData = sealingExtraData;

        // Create a temporary header for sealing hash computation
        var tempHeader = CloneForSealing(header, sealingExtraData);
        Rlp encoded = _headerDecoder.Encode(tempHeader, RlpBehaviors.ForSealing);
        Hash256 sealingHash = Keccak.Compute(encoded.Bytes);

        // Sign the sealing hash
        Signature sig = _ecdsa.Sign(signerKey, sealingHash);

        // Build final ExtraData: vanity + seal + validators
        byte[] finalExtraData = new byte[extraLen];
        vanity.CopyTo(finalExtraData, 0);
        sig.Bytes.CopyTo(finalExtraData.AsSpan(VanityLength, 64));
        finalExtraData[VanityLength + 64] = (byte)sig.RecoveryId;
        validators.CopyTo(finalExtraData.AsSpan(VanityLength + SealLength));

        header.ExtraData = finalExtraData;

        return (header, signerKey);
    }

    private static XdcBlockHeader CloneForSealing(XdcBlockHeader src, byte[] extraData)
    {
        return new XdcBlockHeader(
            src.ParentHash!,
            src.UnclesHash!,
            src.Beneficiary!,
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
            Validators = Array.Empty<byte>(),
            Validator = Array.Empty<byte>(),
            Penalties = Array.Empty<byte>(),
        };
    }

    #endregion
}
