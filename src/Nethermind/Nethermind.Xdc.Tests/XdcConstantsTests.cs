// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using NUnit.Framework;

namespace Nethermind.Xdc.Tests;

/// <summary>
/// Verifies XdcConstants contract addresses and key protocol constants.
/// These are consensus-critical: any drift from the deployed addresses breaks mainnet compatibility.
/// </summary>
[TestFixture]
public class XdcConstantsTests
{
    // ── Contract Addresses ──────────────────────────────────────────────────

    [Test]
    public void ValidatorAddress_Is0x88()
    {
        Address expected = new("0x0000000000000000000000000000000000000088");
        Assert.That(XdcConstants.ValidatorAddress, Is.EqualTo(expected));
    }

    [Test]
    public void BlockSignersAddress_Is0x89()
    {
        Address expected = new("0x0000000000000000000000000000000000000089");
        Assert.That(XdcConstants.BlockSignersAddress, Is.EqualTo(expected));
    }

    [Test]
    public void RandomizeAddress_Is0x90()
    {
        Address expected = new("0x0000000000000000000000000000000000000090");
        Assert.That(XdcConstants.RandomizeAddress, Is.EqualTo(expected));
    }

    [Test]
    public void FoundationWalletAddress_MatchesKnownValue()
    {
        // Intentional typo in the original geth-xdc codebase — must be preserved.
        Address expected = new("0x92a289fe95a85c53b8d0d113cbaef0c1ec98ac65");
        Assert.That(XdcConstants.FoundationWalletAddress, Is.EqualTo(expected));
    }

    [Test]
    public void ThreeSpecialAddresses_AreDistinct()
    {
        // Sanity: 0x88, 0x89, 0x90 must not collide.
        Assert.That(XdcConstants.ValidatorAddress,    Is.Not.EqualTo(XdcConstants.BlockSignersAddress));
        Assert.That(XdcConstants.ValidatorAddress,    Is.Not.EqualTo(XdcConstants.RandomizeAddress));
        Assert.That(XdcConstants.BlockSignersAddress, Is.Not.EqualTo(XdcConstants.RandomizeAddress));
    }

    // ── Protocol Constants ──────────────────────────────────────────────────

    [Test]
    public void ConsensusVersion_Is2()
    {
        Assert.That(XdcConstants.ConsensusVersion, Is.EqualTo((byte)2));
    }

    [Test]
    public void DifficultyDefault_Is1()
    {
        Assert.That(XdcConstants.DifficultyDefault, Is.EqualTo(1UL));
    }

    [Test]
    public void NonceDropVoteValue_IsULongMaxValue()
    {
        Assert.That(XdcConstants.NonceDropVoteValue, Is.EqualTo(ulong.MaxValue));
    }

    [Test]
    public void TargetGasLimit_Is420Million()
    {
        Assert.That(XdcConstants.TargetGasLimit, Is.EqualTo(420_000_000L));
    }

    [Test]
    public void InMemoryEpochs_Is21()
    {
        Assert.That(XdcConstants.InMemoryEpochs, Is.EqualTo(21));
    }

    [Test]
    public void SnapshotDbName_IsNonEmpty()
    {
        Assert.That(XdcConstants.SnapshotDbName, Is.Not.Null.And.Not.Empty);
    }
}
