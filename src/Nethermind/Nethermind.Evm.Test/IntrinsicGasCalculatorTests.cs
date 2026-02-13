// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using MathNet.Numerics.Random;
using Nethermind.Core;
using Nethermind.Core.Eip2930;
using Nethermind.Core.Extensions;
using Nethermind.Core.Specs;
using Nethermind.Core.Test.Builders;
using Nethermind.Int256;
using Nethermind.Specs.Forks;
using NUnit.Framework;

namespace Nethermind.Evm.Test
{

    [Flags]
    public enum GasOptions
    {
        None = 0,
        AfterRepricing = 1,
        FloorCostEnabled = 2,
    }

    [TestFixture]
    public class IntrinsicGasCalculatorTests
    {
        public static IEnumerable<(Transaction Tx, long cost, string Description)> TestCaseSource()
        {
            yield return (Build.A.Transaction.SignedAndResolved().TestObject, 21000, "empty");
        }

        public static IEnumerable<(List<object> orderQueue, long Cost)> AccessTestCaseSource()
        {
            yield return (new List<object> { }, 0);
            yield return (new List<object> { Address.Zero }, 2400);
            yield return (new List<object> { Address.Zero, (UInt256)1 }, 4300);
            yield return (new List<object> { Address.Zero, (UInt256)1, TestItem.AddressA, (UInt256)1 }, 8600);
            yield return (new List<object> { Address.Zero, (UInt256)1, Address.Zero, (UInt256)1 }, 8600);
        }

        public static IEnumerable<(byte[] Data, int OldCost, int NewCost, int FloorCost)> DataTestCaseSource()
        {
            yield return ([0], 4, 4, 21010);
            yield return ([1], 68, 16, 21040);
            yield return ([0, 0, 1], 76, 24, 21060);
            yield return ([1, 1, 0], 140, 36, 21090);
            yield return ([0, 0, 1, 1], 144, 40, 21100);
        }
        [TestCaseSource(nameof(TestCaseSource))]
        public void Intrinsic_cost_is_calculated_properly((Transaction Tx, long Cost, string Description) testCase)
        {
            EthereumIntrinsicGas gas = IntrinsicGasCalculator.Calculate(testCase.Tx, Berlin.Instance);
            gas.Should().Be(new EthereumIntrinsicGas(Standard: testCase.Cost, FloorGas: 0));
        }

        [TestCaseSource(nameof(AccessTestCaseSource))]
        public void Intrinsic_cost_is_calculated_properly((List<object> orderQueue, long Cost) testCase)
        {
            AccessList.Builder accessListBuilder = new();
            foreach (object o in testCase.orderQueue)
            {
                if (o is Address address)
                {
                    accessListBuilder.AddAddress(address);
                }
                else if (o is UInt256 index)
                {
                    accessListBuilder.AddStorage(index);
                }
            }

            AccessList accessList = accessListBuilder.Build();
            Transaction tx = Build.A.Transaction.SignedAndResolved().WithAccessList(accessList).TestObject;

            void Test(IReleaseSpec spec, bool supportsAccessLists)
            {
                if (!supportsAccessLists)
                {
                    Assert.Throws<InvalidDataException>(() => IntrinsicGasCalculator.Calculate(tx, spec));
                }
                else
                {
                    EthereumIntrinsicGas gas = IntrinsicGasCalculator.Calculate(tx, spec);
                    gas.Should().Be(new EthereumIntrinsicGas(Standard: 21000 + testCase.Cost, FloorGas: 0), spec.Name);
                }
            }

            Test(Homestead.Instance, false);
            Test(Frontier.Instance, false);
            Test(SpuriousDragon.Instance, false);
            Test(TangerineWhistle.Instance, false);
            Test(Byzantium.Instance, false);
            Test(Constantinople.Instance, false);
            Test(ConstantinopleFix.Instance, false);
            Test(Istanbul.Instance, false);
            Test(MuirGlacier.Instance, false);
            Test(Berlin.Instance, true);
        }

        [TestCaseSource(nameof(DataTestCaseSource))]
        public void Intrinsic_cost_of_data_is_calculated_properly((byte[] Data, int OldCost, int NewCost, int FloorCost) testCase)
        {
            Transaction tx = Build.A.Transaction.SignedAndResolved().WithData(testCase.Data).TestObject;


            void Test(IReleaseSpec spec, GasOptions options)
            {
                EthereumIntrinsicGas gas = IntrinsicGasCalculator.Calculate(tx, spec);

                bool isAfterRepricing = options.HasFlag(GasOptions.AfterRepricing);
                bool floorCostEnabled = options.HasFlag(GasOptions.FloorCostEnabled);

                gas.Standard.Should()
                    .Be(21000 + (isAfterRepricing ? testCase.NewCost : testCase.OldCost), spec.Name,
                        testCase.Data.ToHexString());
                gas.FloorGas.Should().Be(floorCostEnabled ? testCase.FloorCost : 0);

                gas.Should().Be(new EthereumIntrinsicGas(
                        Standard: 21000 + (isAfterRepricing ? testCase.NewCost : testCase.OldCost),
                        FloorGas: floorCostEnabled ? testCase.FloorCost : 0),
                    spec.Name, testCase.Data.ToHexString());
            }

            Test(Homestead.Instance, GasOptions.None);
            Test(Frontier.Instance, GasOptions.None);
            Test(SpuriousDragon.Instance, GasOptions.None);
            Test(TangerineWhistle.Instance, GasOptions.None);
            Test(Byzantium.Instance, GasOptions.None);
            Test(Constantinople.Instance, GasOptions.None);
            Test(ConstantinopleFix.Instance, GasOptions.None);
            Test(Istanbul.Instance, GasOptions.AfterRepricing);
            Test(MuirGlacier.Instance, GasOptions.AfterRepricing);
            Test(Berlin.Instance, GasOptions.AfterRepricing);
            Test(GrayGlacier.Instance, GasOptions.AfterRepricing);
            Test(Shanghai.Instance, GasOptions.AfterRepricing);
            Test(Cancun.Instance, GasOptions.AfterRepricing);
            Test(Prague.Instance, GasOptions.AfterRepricing | GasOptions.FloorCostEnabled);
        }

        public static IEnumerable<(AuthorizationTuple[] contractCode, long expectedCost)> AuthorizationListTestCaseSource()
        {
            yield return (
                [], 0);
            yield return (
                [new AuthorizationTuple(
                    TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)))
                ],
                GasCostOf.NewAccount);
            yield return (
               [new AuthorizationTuple(
                   TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10))),
                   new AuthorizationTuple(
                    TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)))
               ],
               GasCostOf.NewAccount * 2);
            yield return (
               [new AuthorizationTuple(
                    TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10))),
                   new AuthorizationTuple(
                    TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10))),
                   new AuthorizationTuple(
                    TestContext.CurrentContext.Random.NextULong(),
                    new Address(TestContext.CurrentContext.Random.NextBytes(20)),
                    TestContext.CurrentContext.Random.NextULong(),
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)))
               ],
               GasCostOf.NewAccount * 3);
        }
        [TestCaseSource(nameof(AuthorizationListTestCaseSource))]
        public void Calculate_TxHasAuthorizationList_ReturnsExpectedCostOfTx((AuthorizationTuple[] AuthorizationList, long ExpectedCost) testCase)
        {
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithAuthorizationCode(testCase.AuthorizationList)
                .TestObject;

            EthereumIntrinsicGas gas = IntrinsicGasCalculator.Calculate(tx, Prague.Instance);
            gas.Standard.Should().Be(GasCostOf.Transaction + (testCase.ExpectedCost));
        }

        [Test]
        public void Calculate_TxHasAuthorizationListBeforePrague_ThrowsInvalidDataException()
        {
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithAuthorizationCode(
                new AuthorizationTuple(
                    0,
                    TestItem.AddressF,
                    0,
                    TestContext.CurrentContext.Random.NextByte(),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)),
                    new UInt256(TestContext.CurrentContext.Random.NextBytes(10)))
                )
                .TestObject;

            Assert.That(() => IntrinsicGasCalculator.Calculate(tx, Cancun.Instance), Throws.InstanceOf<InvalidDataException>());
        }

        [Test]
        public void Cache_ReturnsSameValueOnSecondCallWithSameSpec()
        {
            // Arrange
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithData([1, 2, 3, 4, 5])
                .TestObject;

            // Act - Calculate intrinsic gas twice with the same spec
            EthereumIntrinsicGas firstResult = IntrinsicGasCalculator.Calculate(tx, Istanbul.Instance);
            EthereumIntrinsicGas secondResult = IntrinsicGasCalculator.Calculate(tx, Istanbul.Instance);

            // Assert - Results should be identical
            secondResult.Should().Be(firstResult);
            secondResult.Standard.Should().Be(firstResult.Standard);
            secondResult.FloorGas.Should().Be(firstResult.FloorGas);
            
            // Verify cache was actually used (both fields cached)
            tx._cachedIntrinsicGasStandard.Should().NotBeNull();
            tx._cachedIntrinsicGasFloor.Should().NotBeNull();
            tx._cachedIntrinsicGasSpec.Should().BeSameAs(Istanbul.Instance);
        }

        [Test]
        public void Cache_InvalidatesWhenSpecChanges()
        {
            // Arrange
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithData([1, 2, 3, 4, 5])
                .TestObject;

            // Act - Calculate with Istanbul (old gas costs)
            EthereumIntrinsicGas istanbulResult = IntrinsicGasCalculator.Calculate(tx, Istanbul.Instance);
            
            // Calculate again with Homestead (different gas costs)
            EthereumIntrinsicGas homesteadResult = IntrinsicGasCalculator.Calculate(tx, Homestead.Instance);

            // Assert - Results should be different due to EIP-2028 (Istanbul repricing)
            homesteadResult.Standard.Should().NotBe(istanbulResult.Standard, 
                "Homestead and Istanbul have different gas costs for data");
            
            // Verify cache now holds Homestead spec
            tx._cachedIntrinsicGasSpec.Should().BeSameAs(Homestead.Instance);
        }

        [Test]
        public void Cache_WorksCorrectlyForPragueFloorCost()
        {
            // Arrange
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithData([1, 1, 1, 1, 1])
                .TestObject;

            // Act - Calculate twice with Prague (has floor cost)
            EthereumIntrinsicGas firstResult = IntrinsicGasCalculator.Calculate(tx, Prague.Instance);
            EthereumIntrinsicGas secondResult = IntrinsicGasCalculator.Calculate(tx, Prague.Instance);

            // Assert
            secondResult.Should().Be(firstResult);
            firstResult.FloorGas.Should().BeGreaterThan(0, "Prague should have floor cost");
            tx._cachedIntrinsicGasSpec.Should().BeSameAs(Prague.Instance);
        }

        [Test]
        public void Cache_InvalidatesWhenSwitchingBetweenPragueAndCancun()
        {
            // Arrange - Prague has floor cost, Cancun doesn't
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithData([1, 1, 1, 1, 1])
                .TestObject;

            // Act
            EthereumIntrinsicGas pragueResult = IntrinsicGasCalculator.Calculate(tx, Prague.Instance);
            EthereumIntrinsicGas cancunResult = IntrinsicGasCalculator.Calculate(tx, Cancun.Instance);

            // Assert - Floor costs should differ
            pragueResult.FloorGas.Should().BeGreaterThan(0, "Prague has floor cost");
            cancunResult.FloorGas.Should().Be(0, "Cancun doesn't have floor cost");
            
            // Verify cache correctly updated
            tx._cachedIntrinsicGasSpec.Should().BeSameAs(Cancun.Instance);
        }

        [Test]
        public void Cache_WorksForAccessListTransactions()
        {
            // Arrange
            AccessList.Builder builder = new();
            builder.AddAddress(Address.Zero);
            builder.AddStorage((UInt256)1);
            AccessList accessList = builder.Build();
            
            Transaction tx = Build.A.Transaction.SignedAndResolved()
                .WithAccessList(accessList)
                .TestObject;

            // Act
            EthereumIntrinsicGas firstResult = IntrinsicGasCalculator.Calculate(tx, Berlin.Instance);
            EthereumIntrinsicGas secondResult = IntrinsicGasCalculator.Calculate(tx, Berlin.Instance);

            // Assert
            secondResult.Should().Be(firstResult);
            firstResult.Standard.Should().BeGreaterThan(21000, "Access list should add cost");
            tx._cachedIntrinsicGasSpec.Should().BeSameAs(Berlin.Instance);
        }
    }
}
