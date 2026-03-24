// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain;
using Nethermind.Consensus.Rewards;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Core.Specs;
using Nethermind.Int256;
using Nethermind.Xdc.Spec;
using System;
using System.Collections.Generic;
using Nethermind.Crypto;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Logging;
using Nethermind.Xdc.Contracts;

namespace Nethermind.Xdc
{
    /// <summary>
    /// IRewardCalculatorSource that creates a fully-wired XdcRewardCalculatorInstance
    /// when Get(ITransactionProcessor) is called by the BlockProcessingModule.
    ///
    /// Reward model (current mainnet):
    /// - Rewards are paid only at epoch checkpoints (number % EpochLength == 0).
    /// - Current split: 90% to masternode owner, 10% to foundation.
    ///   (RewardVoterPercent = 0 on XDC mainnet, so voters receive nothing.)
    /// </summary>
    public class XdcRewardCalculatorSource : IRewardCalculatorSource
    {
        private readonly IEpochSwitchManager _epochSwitchManager;
        private readonly ISpecProvider _specProvider;
        private readonly IBlockTree _blockTree;
        private readonly IMasternodeVotingContract _masternodeVotingContract;
        private readonly ISigningTxCache _signingTxCache;
        private readonly ILogManager _logManager;

        public XdcRewardCalculatorSource(
            IEpochSwitchManager epochSwitchManager,
            ISpecProvider specProvider,
            IBlockTree blockTree,
            IMasternodeVotingContract masternodeVotingContract,
            ISigningTxCache signingTxCache,
            ILogManager logManager)
        {
            _epochSwitchManager = epochSwitchManager ?? throw new ArgumentNullException(nameof(epochSwitchManager));
            _specProvider = specProvider ?? throw new ArgumentNullException(nameof(specProvider));
            _blockTree = blockTree ?? throw new ArgumentNullException(nameof(blockTree));
            _masternodeVotingContract = masternodeVotingContract ?? throw new ArgumentNullException(nameof(masternodeVotingContract));
            _signingTxCache = signingTxCache ?? throw new ArgumentNullException(nameof(signingTxCache));
            _logManager = logManager ?? throw new ArgumentNullException(nameof(logManager));
        }

        public IRewardCalculator Get(ITransactionProcessor processor)
        {
            return new XdcRewardCalculator(
                _epochSwitchManager,
                _specProvider,
                _blockTree,
                _masternodeVotingContract,
                _signingTxCache,
                processor,
                _logManager);
        }
    }

    /// <summary>
    /// Calculates block rewards according to XDPoS consensus rules.
    /// Rewards are only distributed at epoch checkpoints.
    /// Split: 90% masternode owner, 10% foundation wallet.
    /// </summary>
    public class XdcRewardCalculator : IRewardCalculator
    {
        private readonly EthereumEcdsa _ethereumEcdsa;
        private readonly IEpochSwitchManager _epochSwitchManager;
        private readonly ISpecProvider _specProvider;
        private readonly IBlockTree _blockTree;
        private readonly IMasternodeVotingContract _masternodeVotingContract;
        private readonly ISigningTxCache _signingTxCache;
        private readonly ITransactionProcessor _transactionProcessor;
        private readonly ILogger _logger;

        public XdcRewardCalculator(
            IEpochSwitchManager epochSwitchManager,
            ISpecProvider specProvider,
            IBlockTree blockTree,
            IMasternodeVotingContract masternodeVotingContract,
            ISigningTxCache signingTxCache,
            ITransactionProcessor transactionProcessor,
            ILogManager logManager)
        {
            _ethereumEcdsa = new EthereumEcdsa(specProvider.ChainId);
            _epochSwitchManager = epochSwitchManager ?? throw new ArgumentNullException(nameof(epochSwitchManager));
            _specProvider = specProvider ?? throw new ArgumentNullException(nameof(specProvider));
            _blockTree = blockTree ?? throw new ArgumentNullException(nameof(blockTree));
            _masternodeVotingContract = masternodeVotingContract ?? throw new ArgumentNullException(nameof(masternodeVotingContract));
            _signingTxCache = signingTxCache ?? throw new ArgumentNullException(nameof(signingTxCache));
            _transactionProcessor = transactionProcessor ?? throw new ArgumentNullException(nameof(transactionProcessor));
            _logger = logManager.GetClassLogger<XdcRewardCalculator>();
        }

        public BlockReward[] CalculateRewards(Block block)
        {
            if (block is null)
                throw new ArgumentNullException(nameof(block));
            if (block.Header is not XdcBlockHeader xdcHeader)
                throw new InvalidOperationException("Only supports XDC headers");
            if (xdcHeader.Number == 0)
                return Array.Empty<BlockReward>();

            // Rewards in XDC are calculated only if it's an epoch switch block
            if (!_epochSwitchManager.IsEpochSwitchAtBlock(xdcHeader)) return Array.Empty<BlockReward>();

            var number = xdcHeader.Number;
            IXdcReleaseSpec spec = _specProvider.GetXdcSpec(xdcHeader, xdcHeader.ExtraConsensusData?.BlockRound ?? 0);
            if (number == spec.SwitchBlock + 1) return Array.Empty<BlockReward>();

            Address foundationWalletAddr = spec.FoundationWallet;
            if (foundationWalletAddr == default || foundationWalletAddr == Address.Zero)
                throw new InvalidOperationException("Foundation wallet address cannot be empty");

            var (signers, count) = GetSigningTxCount(xdcHeader, spec);

            if (count == 0)
            {
                if (_logger.IsDebug)
                    _logger.Debug($"XdcRewardCalculator: block {number} is checkpoint but no signers found — no rewards");
                return Array.Empty<BlockReward>();
            }

            UInt256 chainReward = (UInt256)spec.Reward * Unit.Ether;
            Dictionary<Address, UInt256> rewardSigners = CalculateRewardForSigners(chainReward, signers, count);

            UInt256 totalFoundationWalletReward = UInt256.Zero;
            var rewards = new List<BlockReward>();
            foreach (var (signer, reward) in rewardSigners)
            {
                (BlockReward holderReward, UInt256 foundationWalletReward) = DistributeRewards(signer, reward, xdcHeader);
                totalFoundationWalletReward += foundationWalletReward;
                rewards.Add(holderReward);
            }
            if (totalFoundationWalletReward > UInt256.Zero)
                rewards.Add(new BlockReward(foundationWalletAddr, totalFoundationWalletReward));

            if (_logger.IsInfo)
                _logger.Info($"XdcRewardCalculator: block {number} checkpoint — {signers.Count} signers, {count} total signs, {rewards.Count} reward entries");

            return rewards.ToArray();
        }

        private (Dictionary<Address, long> Signers, long Count) GetSigningTxCount(XdcBlockHeader epochHeader, IXdcReleaseSpec spec)
        {
            var signers = new Dictionary<Address, long>();
            long number = epochHeader.Number;
            if (number == 0) return (signers, 0);

            long signEpochCount = 1, rewardEpochCount = 2, epochCount = 0, endBlockNumber = 0, startBlockNumber = 0, signingCount = 0;

            var blockNumberToHash = new Dictionary<long, Hash256>();
            var hashToSigningAddress = new Dictionary<Hash256, HashSet<Address>>();
            var masternodes = new HashSet<Address>();
            var mergeSignRange = spec.MergeSignRange;

            XdcBlockHeader h = epochHeader;
            for (long i = number - 1; i >= 0; i--)
            {
                Hash256 parentHash = h.ParentHash;
                h = _blockTree.FindHeader(parentHash!, i) as XdcBlockHeader;
                if (h == null) throw new InvalidOperationException($"Header with hash {parentHash} not found");
                if (_epochSwitchManager.IsEpochSwitchAtBlock(h) && h.Number != spec.SwitchBlock + 1)
                {
                    epochCount++;
                    if (epochCount == signEpochCount) endBlockNumber = i;
                    if (epochCount == rewardEpochCount)
                    {
                        startBlockNumber = i + 1;
                        if (h.Number <= spec.SwitchBlock)
                            masternodes = new HashSet<Address>(h.ExtraData.ParseV1Masternodes());
                        else
                            masternodes = new HashSet<Address>(h.ValidatorsAddress!);
                        break;
                    }
                }

                blockNumberToHash[i] = h.Hash;
                Transaction[] signingTxs = _signingTxCache.GetSigningTransactions(h.Hash, i, spec);

                foreach (Transaction tx in signingTxs)
                {
                    Hash256 blockHash = ExtractBlockHashFromSigningTxData(tx.Data);
                    tx.SenderAddress ??= _ethereumEcdsa.RecoverAddress(tx);
                    if (!hashToSigningAddress.ContainsKey(blockHash))
                        hashToSigningAddress[blockHash] = new HashSet<Address>();
                    hashToSigningAddress[blockHash].Add(tx.SenderAddress);
                }
            }

            // Only blocks at heights that are multiples of MergeSignRange are considered.
            long start = ((startBlockNumber + mergeSignRange - 1) / mergeSignRange) * mergeSignRange;
            for (long i = start; i < endBlockNumber; i += mergeSignRange)
            {
                if (!blockNumberToHash.TryGetValue(i, out var blockHash)) continue;
                if (!hashToSigningAddress.TryGetValue(blockHash, out var addresses)) continue;
                foreach (Address addr in addresses)
                {
                    if (!masternodes.Contains(addr)) continue;
                    if (!signers.ContainsKey(addr)) signers[addr] = 0;
                    signers[addr] += 1;
                    signingCount++;
                }
            }
            return (signers, signingCount);
        }

        private Hash256 ExtractBlockHashFromSigningTxData(ReadOnlyMemory<byte> data)
        {
            ReadOnlySpan<byte> span = data.Span;
            if (span.Length != XdcConstants.SignTransactionDataLength)
                throw new ArgumentException($"Signing tx calldata must be exactly {XdcConstants.SignTransactionDataLength} bytes.", nameof(data));

            // 36..67: bytes32 blockHash
            ReadOnlySpan<byte> hashBytes = span.Slice(36, 32);
            return new Hash256(hashBytes);
        }

        private Dictionary<Address, UInt256> CalculateRewardForSigners(UInt256 totalReward,
            Dictionary<Address, long> signers, long totalSigningCount)
        {
            var rewardSigners = new Dictionary<Address, UInt256>();
            foreach (var (signer, count) in signers)
            {
                UInt256 reward = CalculateProportionalReward(count, totalSigningCount, totalReward);
                rewardSigners.Add(signer, reward);
            }
            return rewardSigners;
        }

        /// <summary>
        /// Formula: (totalReward / totalSignatures) * signatureCount
        /// Matches geth-xdc: calcReward.Div(chainReward, totalSigner).Mul(calcReward, rLog.Sign)
        /// </summary>
        internal UInt256 CalculateProportionalReward(
            long signatureCount,
            long totalSignatures,
            UInt256 totalReward)
        {
            if (signatureCount <= 0 || totalSignatures <= 0)
                return UInt256.Zero;

            var signatures = (UInt256)signatureCount;
            var total = (UInt256)totalSignatures;

            UInt256 portion = totalReward / total;
            UInt256 reward = portion * signatures;
            return reward;
        }

        internal (BlockReward HolderReward, UInt256 FoundationWalletReward) DistributeRewards(
            Address masternodeAddress, UInt256 reward, XdcBlockHeader header)
        {
            Address owner = _masternodeVotingContract.GetCandidateOwner(_transactionProcessor, header, masternodeAddress);

            // 90% of the reward goes to the masternode owner (RewardMasterPercent = 90)
            UInt256 masterReward = reward * 90 / 100;

            // 10% of the reward goes to the foundation wallet (RewardFoundationPercent = 10)
            UInt256 foundationReward = reward / 10;

            return (new BlockReward(owner, masterReward), foundationReward);
        }
    }
}
