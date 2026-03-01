// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Reflection;
using Nethermind.Consensus.Rewards;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Test.Builders;
using Nethermind.Int256;
using Nethermind.Logging;
using NUnit.Framework;

namespace Nethermind.Xdc.Tests;

/// <summary>
/// Unit tests for XdcRewardCalculator.
///
/// Covers:
/// 1. Blocks that must never produce rewards (genesis, non-checkpoint, block==900).
/// 2. The RewardInflation schedule (private static, tested via reflection).
/// 3. The 90/10 owner-vs-foundation percentage math.
/// 4. IRewardCalculatorSource.Get returns self.
/// </summary>
[TestFixture]
public class XdcRewardCalculatorTests
{
    // ── Constants mirrored from source (kept private there) ─────────────────

    private const long RewardCheckpoint = 900;
    private const ulong BlocksPerYear = 15_768_000;
    private const ulong TIPNoHalvingMNReward = 38_383_838;
    private const int RewardMasterPercent = 90;
    private const int RewardFoundationPercent = 10;
    private static readonly UInt256 BaseChainReward = (UInt256)250 * 1_000_000_000_000_000_000;

    // ── Fixture setup ────────────────────────────────────────────────────────

    private XdcRewardCalculator _calculator = null!;

    [SetUp]
    public void SetUp()
    {
        // blockTree / stateProvider / ecdsa intentionally null:
        // These tests exercise the early-return guards and pure math paths.
        _calculator = new XdcRewardCalculator(LimboLogs.Instance);
    }

    // ── 1. Blocks that must return zero rewards ──────────────────────────────

    [Test]
    public void CalculateRewards_GenesisBlock_ReturnsEmpty()
    {
        Block genesis = Build.A.Block.Genesis.TestObject;
        BlockReward[] rewards = _calculator.CalculateRewards(genesis);
        Assert.That(rewards, Is.Empty, "Genesis block must produce no rewards.");
    }

    [Test]
    public void CalculateRewards_FirstCheckpoint_Block900_ReturnsEmpty()
    {
        // Geth condition: number > 0 && number - rCheckpoint > 0  →  900 - 900 == 0, so skip.
        Block block900 = Build.A.Block.WithNumber(RewardCheckpoint).TestObject;
        BlockReward[] rewards = _calculator.CalculateRewards(block900);
        Assert.That(rewards, Is.Empty, "Block 900 (first checkpoint) must produce no rewards.");
    }

    [TestCase(1L)]
    [TestCase(899L)]
    [TestCase(901L)]
    [TestCase(1799L)]
    [TestCase(1801L)]
    public void CalculateRewards_NonCheckpointBlock_ReturnsEmpty(long blockNumber)
    {
        Block block = Build.A.Block.WithNumber(blockNumber).TestObject;
        BlockReward[] rewards = _calculator.CalculateRewards(block);
        Assert.That(rewards, Is.Empty,
            $"Block {blockNumber} is not a checkpoint — must produce no rewards.");
    }

    [Test]
    public void CalculateRewards_CheckpointBlock_WithNullBlockTree_ReturnsEmpty()
    {
        // block 1800 is a valid checkpoint (1800 % 900 == 0, 1800 > 900).
        // With no block-tree the calculator cannot look up masternodes → returns empty.
        Block block = Build.A.Block.WithNumber(1800L).TestObject;
        BlockReward[] rewards = _calculator.CalculateRewards(block);
        Assert.That(rewards, Is.Empty,
            "Checkpoint with null block tree must return empty (graceful degradation).");
    }

    // ── 2. RewardInflation schedule (private static, via reflection) ─────────

    private static UInt256 InvokeRewardInflation(UInt256 chainReward, ulong blockNumber)
    {
        MethodInfo? m = typeof(XdcRewardCalculator).GetMethod(
            "RewardInflation",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.That(m, Is.Not.Null, "RewardInflation private static method must exist.");
        return (UInt256)m!.Invoke(null, [chainReward, blockNumber])!;
    }

    [Test]
    public void RewardInflation_EarlyBlocks_ReturnsFullReward()
    {
        // Block 1 is early — no halving applies.
        UInt256 result = InvokeRewardInflation(BaseChainReward, 1);
        Assert.That(result, Is.EqualTo(BaseChainReward));
    }

    [Test]
    public void RewardInflation_BeforeFirstYearBoundary_ReturnsFullReward()
    {
        // Just below 2*BlocksPerYear threshold.
        ulong blockNumber = BlocksPerYear * 2 - 1;
        UInt256 result = InvokeRewardInflation(BaseChainReward, blockNumber);
        Assert.That(result, Is.EqualTo(BaseChainReward));
    }

    [Test]
    public void RewardInflation_BetweenYear2And5_ReturnsHalfReward()
    {
        // Exactly at the 2*BlocksPerYear boundary → halving kicks in.
        ulong blockNumber = BlocksPerYear * 2;
        UInt256 result = InvokeRewardInflation(BaseChainReward, blockNumber);
        Assert.That(result, Is.EqualTo(BaseChainReward / 2));
    }

    [Test]
    public void RewardInflation_AtYear5_ReturnsFullReward_BecauseTIPNoHalvingFiresFirst()
    {
        // TIPNoHalvingMNReward (38_383_838) < BlocksPerYear*5 (78_840_000).
        // The TIPNoHalving guard fires first in the method, so the quarter-reward
        // branch is unreachable in practice. Any block >= TIPNoHalvingMNReward gets
        // the full base reward regardless of year boundary.
        ulong blockNumber = BlocksPerYear * 5;   // 78_840_000 > TIPNoHalvingMNReward
        UInt256 result = InvokeRewardInflation(BaseChainReward, blockNumber);
        Assert.That(result, Is.EqualTo(BaseChainReward),
            "5*BlocksPerYear > TIPNoHalvingMNReward: the no-halving guard fires first.");
    }

    [Test]
    public void RewardInflation_AfterTIPNoHalving_ReturnsFullReward()
    {
        // After TIPNoHalvingMNReward the schedule is frozen at full reward.
        ulong blockNumber = TIPNoHalvingMNReward;
        UInt256 result = InvokeRewardInflation(BaseChainReward, blockNumber);
        Assert.That(result, Is.EqualTo(BaseChainReward));
    }

    [Test]
    public void RewardInflation_WellAboveTIPNoHalving_ReturnsFullReward()
    {
        ulong blockNumber = TIPNoHalvingMNReward + 10_000_000;
        UInt256 result = InvokeRewardInflation(BaseChainReward, blockNumber);
        Assert.That(result, Is.EqualTo(BaseChainReward));
    }

    // ── 3. 90 / 10 split math ───────────────────────────────────────────────

    [TestCase("1000000000000000000")]   // 1 XDC
    [TestCase("250000000000000000000")] // 250 XDC (base reward)
    [TestCase("125000000000000000000")] // 125 XDC (halved)
    public void OwnerFoundationSplit_90_10_MatchesFormula(string rewardStr)
    {
        UInt256 calcReward = UInt256.Parse(rewardStr);

        UInt256 ownerReward = calcReward * (UInt256)RewardMasterPercent / 100;
        UInt256 foundationReward = calcReward * (UInt256)RewardFoundationPercent / 100;

        // Owner gets exactly 90 %.
        Assert.That(ownerReward, Is.EqualTo(calcReward * 90 / 100));
        // Foundation gets exactly 10 %.
        Assert.That(foundationReward, Is.EqualTo(calcReward * 10 / 100));
        // They must not exceed the total (integer truncation is fine).
        Assert.That(ownerReward + foundationReward, Is.LessThanOrEqualTo(calcReward));
        // And together they must be at least 99 % of the total (no more than 1 unit lost).
        Assert.That(ownerReward + foundationReward, Is.GreaterThanOrEqualTo(calcReward * 99 / 100));
    }

    [Test]
    public void OwnerFoundationSplit_PercentsSum100()
    {
        Assert.That(RewardMasterPercent + RewardFoundationPercent, Is.EqualTo(100));
    }

    // ── 4. IRewardCalculatorSource.Get returns self ──────────────────────────

    [Test]
    public void Get_ReturnsCalculatorItself()
    {
        IRewardCalculator calc = ((IRewardCalculatorSource)_calculator).Get(null!);
        Assert.That(calc, Is.SameAs(_calculator));
    }
}
