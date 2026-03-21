// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac;
using Autofac.Features.AttributeFilters;
using Nethermind.Abi;
using Nethermind.Blockchain;
using Nethermind.Blockchain.Blocks;
using Nethermind.Blockchain.Headers;
using Nethermind.Consensus;
using Nethermind.Consensus.Processing;
using Nethermind.Consensus.Rewards;
using Nethermind.Consensus.Producers;
using Nethermind.Consensus.Validators;
using Nethermind.Core;
using Nethermind.Core.Specs;
using Nethermind.Db;
using Nethermind.Init.Modules;
using Nethermind.Specs.ChainSpecStyle;
using Nethermind.Xdc.Contracts;
using Nethermind.Xdc.Spec;
using Nethermind.TxPool;
using Nethermind.Logging;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Xdc.TxPool;
using Nethermind.Api.Steps;
using Nethermind.Synchronization;
using Nethermind.Synchronization.FastSync;
using Nethermind.Synchronization.ParallelSync;

namespace Nethermind.Xdc;

public class XdcModule : Module
{
    private const string SnapshotDbName = "XdcSnapshots";

    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        builder
            .AddStep(typeof(InitializeBlockchainXdc))
            .Intercept<ChainSpec>(XdcChainSpecLoader.ProcessChainSpec)
            .AddSingleton<ISpecProvider, XdcChainSpecBasedSpecProvider>()
            .Map<XdcChainSpecEngineParameters, ChainSpec>(chainSpec =>
                chainSpec.EngineChainSpecParametersProvider.GetChainSpecParameters<XdcChainSpecEngineParameters>())

            .AddDecorator<IGenesisBuilder, XdcGenesisBuilder>()
            .AddScoped<IBlockProcessor, XdcBlockProcessor>()

            // stores
            .AddSingleton<IHeaderStore, XdcHeaderStore>()
            .AddSingleton<IXdcHeaderStore, XdcHeaderStore>()
            .AddSingleton<IBlockStore, XdcBlockStore>()
            .AddSingleton<IBlockTree, XdcBlockTree>()

            // Sys contracts
            //TODO this might not be wired correctly
            .AddSingleton<
                IMasternodeVotingContract,
                IAbiEncoder,
                ISpecProvider,
                IReadOnlyTxProcessingEnvFactory>(CreateVotingContract)

            // sealer
            .AddSingleton<ISealer, XdcSealer>()

            // penalty handler

            // reward handler
            .AddScoped<IRewardCalculator, XdcRewardCalculator>()

            // forensics handler

            // Validators
            .AddSingleton<IHeaderValidator, XdcHeaderValidator>()
            .AddSingleton<XdcSealValidator>()
            .AddSingleton<XdcV1SealValidator>()
            .AddSingleton<ISealValidator, XdcV1SealValidator, XdcSealValidator, XdcChainSpecEngineParameters>(CreateDispatchingSealValidator)
            .AddSingleton<IUnclesValidator, MustBeEmptyUnclesValidator>()
            .AddSingleton<IMasternodesCalculator, MasternodesCalculator>()
            .AddSingleton<ISigningTxCache, SigningTxCache>()
            .AddSingleton<IForensicsProcessor, ForensicsProcessor>()

            // managers
            .AddSingleton<IVotesManager, VotesManager>()
            .AddSingleton<IQuorumCertificateManager, QuorumCertificateManager>()
            .AddSingleton<ITimeoutCertificateManager, TimeoutCertificateManager>()
            .AddSingleton<IEpochSwitchManager, EpochSwitchManager>()
            .AddSingleton<IXdcConsensusContext, XdcConsensusContext>()
            .AddDatabase(SnapshotDbName)
            .AddSingleton<ISnapshotManager, IDb, IBlockTree, IMasternodeVotingContract, ISpecProvider>(CreateSnapshotManager)
            .AddSingleton<ISignTransactionManager, ISigner, ITxPool, ILogManager>(CreateSignTransactionManager)
            .AddSingleton<IPenaltyHandler, PenaltyHandler>()
            .AddSingleton<ITimeoutTimer, TimeoutTimer>()
            .AddSingleton<ISyncInfoManager, SyncInfoManager>()

            // sync
            .AddSingleton<IBeaconSyncStrategy, XdcBeaconSyncStrategy>()
            .AddSingleton<XdcStateSyncSnapshotManager>()
            .AddSingleton<IStateSyncPivot, XdcStateSyncPivot>()
            .AddSingleton<IPeerAllocationStrategyFactory<StateSyncBatch>, XdcStateSyncAllocationStrategyFactory>()

            .AddSingleton<IBlockProducerTxSourceFactory, XdcTxPoolTxSourceFactory>()

            // block processing
            .AddScoped<ITransactionProcessor, XdcTransactionProcessor>()
            ;
    }

    private static ISealValidator CreateDispatchingSealValidator(
        XdcV1SealValidator v1,
        XdcSealValidator v2,
        XdcChainSpecEngineParameters xdcParams)
    {
        return new XdcDispatchingSealValidator(v1, v2, xdcParams.SwitchBlock, xdcParams.SkipV1Validation);
    }

    private ISnapshotManager CreateSnapshotManager([KeyFilter(XdcRocksDbConfigFactory.XdcSnapshotDbName)] IDb db, IBlockTree blockTree, IMasternodeVotingContract votingContract, ISpecProvider specProvider)
    {
        return new SnapshotManager(db, blockTree, votingContract, specProvider);
    }
    private ISignTransactionManager CreateSignTransactionManager(ISigner signer, ITxPool txPool, ILogManager logManager)
    {
        return new SignTransactionManager(signer, txPool, logManager.GetClassLogger<SignTransactionManager>());
    }

    private IMasternodeVotingContract CreateVotingContract(
        IAbiEncoder abiEncoder,
        ISpecProvider specProvider,
        IReadOnlyTxProcessingEnvFactory readOnlyTxProcessingEnv)
    {
        IXdcReleaseSpec spec = (XdcReleaseSpec)specProvider.GetFinalSpec();
        return new MasternodeVotingContract(abiEncoder, spec.MasternodeVotingContract, readOnlyTxProcessingEnv);
    }
}
