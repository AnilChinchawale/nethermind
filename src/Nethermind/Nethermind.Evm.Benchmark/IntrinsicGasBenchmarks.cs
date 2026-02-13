// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using Nethermind.Core;
using Nethermind.Core.Eip2930;
using Nethermind.Core.Extensions;
using Nethermind.Core.Specs;
using Nethermind.Core.Test.Builders;
using Nethermind.Specs;

namespace Nethermind.Evm.Benchmark
{
    /// <summary>
    /// Benchmarks to measure the overhead of double intrinsic gas calculation (Issue #9260).
    /// Intrinsic gas is calculated twice per transaction:
    /// 1. During validation in TxValidator.cs (IntrinsicGasTxValidator.IsWellFormed)
    /// 2. During execution in TransactionProcessor.cs (Execute method)
    /// 
    /// This benchmark quantifies the cost to justify optimization.
    /// </summary>
    [MemoryDiagnoser]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
    public class IntrinsicGasBenchmarks
    {
        private IReleaseSpec _londonSpec = null!;
        private IReleaseSpec _pragueSpec = null!;
        
        // Different transaction types for testing
        private Transaction _legacyTxSmall = null!;
        private Transaction _legacyTxLarge = null!;
        private Transaction _eip1559TxSmall = null!;
        private Transaction _eip1559TxLarge = null!;
        private Transaction _eip2930TxSmall = null!;
        private Transaction _eip4844TxSmall = null!;
        private Transaction _createTxSmall = null!;
        private Transaction _createTxLarge = null!;

        [GlobalSetup]
        public void GlobalSetup()
        {
            MainnetSpecProvider specProvider = MainnetSpecProvider.Instance;
            
            // London = EIP-1559 support (block number 12965000)
            _londonSpec = specProvider.GetSpec((ForkActivation)MainnetSpecProvider.LondonBlockNumber);
            // Prague = Latest spec with all EIPs (use timestamp activation)
            _pragueSpec = specProvider.GetSpec(MainnetSpecProvider.PragueActivation);

            // Small calldata (10 bytes - mix of zero and non-zero)
            byte[] smallData = new byte[10];
            smallData[0] = 0x12;
            smallData[5] = 0xAB;
            smallData[9] = 0xFF;

            // Large calldata (10KB - realistic contract interaction)
            byte[] largeData = new byte[10240];
            Random rng = new Random(42); // Deterministic
            rng.NextBytes(largeData);

            // Legacy transaction (small)
            _legacyTxSmall = Build.A.Transaction
                .WithType(TxType.Legacy)
                .WithData(smallData)
                .WithGasLimit(100000)
                .WithGasPrice(20)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // Legacy transaction (large)
            _legacyTxLarge = Build.A.Transaction
                .WithType(TxType.Legacy)
                .WithData(largeData)
                .WithGasLimit(1000000)
                .WithGasPrice(20)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // EIP-1559 transaction (small)
            _eip1559TxSmall = Build.A.Transaction
                .WithType(TxType.EIP1559)
                .WithData(smallData)
                .WithGasLimit(100000)
                .WithMaxFeePerGas(100)
                .WithMaxPriorityFeePerGas(1)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // EIP-1559 transaction (large)
            _eip1559TxLarge = Build.A.Transaction
                .WithType(TxType.EIP1559)
                .WithData(largeData)
                .WithGasLimit(1000000)
                .WithMaxFeePerGas(100)
                .WithMaxPriorityFeePerGas(1)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // EIP-2930 transaction with access list (small)
            var accessList = new AccessList.Builder()
                .AddAddress(TestItem.AddressB)
                .AddStorage(0)
                .AddStorage(1)
                .AddStorage(2)
                .Build();
            
            _eip2930TxSmall = Build.A.Transaction
                .WithType(TxType.AccessList)
                .WithData(smallData)
                .WithGasLimit(100000)
                .WithGasPrice(20)
                .WithAccessList(accessList)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // EIP-4844 transaction (blob transaction - small)
            _eip4844TxSmall = Build.A.Transaction
                .WithType(TxType.Blob)
                .WithData(smallData)
                .WithGasLimit(100000)
                .WithMaxFeePerGas(100)
                .WithMaxPriorityFeePerGas(1)
                .WithMaxFeePerBlobGas(10)
                .WithBlobVersionedHashes(1)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            // Contract creation (small)
            _createTxSmall = Build.A.Transaction
                .WithType(TxType.Legacy)
                .WithData(smallData)
                .WithCode(smallData)
                .WithGasLimit(100000)
                .WithGasPrice(20)
                .SignedAndResolved()
                .TestObject;

            // Contract creation (large)
            _createTxLarge = Build.A.Transaction
                .WithType(TxType.Legacy)
                .WithData(largeData)
                .WithCode(largeData)
                .WithGasLimit(1000000)
                .WithGasPrice(20)
                .SignedAndResolved()
                .TestObject;
        }

        // ============================================================================
        // Single intrinsic gas calculations - measure cost per transaction type
        // ============================================================================

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas LegacyTx_SmallData()
        {
            return IntrinsicGasCalculator.Calculate(_legacyTxSmall, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas LegacyTx_LargeData()
        {
            return IntrinsicGasCalculator.Calculate(_legacyTxLarge, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas EIP1559Tx_SmallData()
        {
            return IntrinsicGasCalculator.Calculate(_eip1559TxSmall, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas EIP1559Tx_LargeData()
        {
            return IntrinsicGasCalculator.Calculate(_eip1559TxLarge, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas EIP2930Tx_WithAccessList()
        {
            return IntrinsicGasCalculator.Calculate(_eip2930TxSmall, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas EIP4844Tx_BlobTx()
        {
            return IntrinsicGasCalculator.Calculate(_eip4844TxSmall, _pragueSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas CreateTx_SmallData()
        {
            return IntrinsicGasCalculator.Calculate(_createTxSmall, _londonSpec);
        }

        [Benchmark]
        [BenchmarkCategory("SingleCall")]
        public EthereumIntrinsicGas CreateTx_LargeData()
        {
            return IntrinsicGasCalculator.Calculate(_createTxLarge, _londonSpec);
        }

        // ============================================================================
        // Double calculation overhead - simulates current behavior (Issue #9260)
        // ============================================================================

        [Benchmark]
        [BenchmarkCategory("DoubleCall")]
        public (EthereumIntrinsicGas, EthereumIntrinsicGas) DoubleLegacyTx_SmallData()
        {
            // Simulates: 1st call in validation, 2nd call in execution
            var validation = IntrinsicGasCalculator.Calculate(_legacyTxSmall, _londonSpec);
            var execution = IntrinsicGasCalculator.Calculate(_legacyTxSmall, _londonSpec);
            return (validation, execution);
        }

        [Benchmark]
        [BenchmarkCategory("DoubleCall")]
        public (EthereumIntrinsicGas, EthereumIntrinsicGas) DoubleLegacyTx_LargeData()
        {
            var validation = IntrinsicGasCalculator.Calculate(_legacyTxLarge, _londonSpec);
            var execution = IntrinsicGasCalculator.Calculate(_legacyTxLarge, _londonSpec);
            return (validation, execution);
        }

        [Benchmark]
        [BenchmarkCategory("DoubleCall")]
        public (EthereumIntrinsicGas, EthereumIntrinsicGas) DoubleEIP1559Tx_LargeData()
        {
            var validation = IntrinsicGasCalculator.Calculate(_eip1559TxLarge, _londonSpec);
            var execution = IntrinsicGasCalculator.Calculate(_eip1559TxLarge, _londonSpec);
            return (validation, execution);
        }

        // ============================================================================
        // Calldata size scaling - measure how cost grows with data size
        // ============================================================================

        public IEnumerable<int> CalldataSizes => new[] { 0, 10, 100, 1000, 10000, 100000 };

        [Benchmark]
        [ArgumentsSource(nameof(CalldataSizes))]
        [BenchmarkCategory("Scaling")]
        public EthereumIntrinsicGas IntrinsicGas_VaryingDataSize(int dataSize)
        {
            byte[] data = new byte[dataSize];
            // Fill with mix of zeros and non-zeros (50/50)
            for (int i = 0; i < dataSize; i++)
            {
                data[i] = (byte)(i % 2 == 0 ? 0 : i % 256);
            }

            Transaction tx = Build.A.Transaction
                .WithType(TxType.EIP1559)
                .WithData(data)
                .WithGasLimit(dataSize * 100 + 21000)
                .WithMaxFeePerGas(100)
                .WithMaxPriorityFeePerGas(1)
                .WithTo(TestItem.AddressA)
                .SignedAndResolved()
                .TestObject;

            return IntrinsicGasCalculator.Calculate(tx, _londonSpec);
        }

        // ============================================================================
        // Baseline comparisons - what else can we do with same time?
        // ============================================================================

        private byte[] _testData = null!;

        [IterationSetup(Target = nameof(Baseline_CountZeros))]
        public void SetupCountZeros()
        {
            _testData = new byte[10240];
            new Random(42).NextBytes(_testData);
        }

        [Benchmark]
        [BenchmarkCategory("Baseline")]
        public int Baseline_CountZeros()
        {
            // This is part of what intrinsic gas calculation does
            return _testData.AsSpan().CountZeros();
        }

        [Benchmark]
        [BenchmarkCategory("Baseline")]
        public long Baseline_SimpleMath()
        {
            // Simple arithmetic operations for comparison
            long result = 21000; // Base transaction gas
            result += 10 * 4; // 10 non-zero bytes
            result += 5 * 16; // 5 zero bytes
            return result;
        }
    }
}
