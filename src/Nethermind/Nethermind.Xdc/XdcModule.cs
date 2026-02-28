// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Blockchain.Headers;
using Autofac;
using Nethermind.Blockchain;
using Nethermind.Core;
using Nethermind.Core.Container;
using Nethermind.Consensus;
using Nethermind.Consensus.Processing;
using Nethermind.Crypto;
using Nethermind.Consensus.Rewards;
using Nethermind.Consensus.Scheduler;
using Nethermind.Db;
using Nethermind.Evm.State;
using Nethermind.Evm.TransactionProcessing;
using Nethermind.Logging;
using Nethermind.Network;
using Nethermind.Network.P2P.Subprotocols.Eth;
using Nethermind.Specs.ChainSpecStyle;
using Nethermind.Stats;
using Nethermind.Synchronization;
using Nethermind.TxPool;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.P2P;
using Nethermind.Xdc.Spec;

namespace Nethermind.Xdc;

/// <summary>
/// Autofac module for XDC Network support
/// Registers XDPoS v2 consensus components and eth/100 protocol
/// </summary>
public class XdcModule : Module
{
    protected override void Load(ContainerBuilder builder)
    {
        base.Load(builder);

        // Register XDC header decoder so HeaderStore uses it for DB read/write
        // Without this, headers are stored/loaded with standard 15-field encoding
        // instead of XDC 18-field encoding (Validators, Validator, Penalties)
        builder.RegisterType<XdcHeaderDecoder>()
            .As<IHeaderDecoder>()
            .SingleInstance();

        // CRITICAL: Register XdcHeaderStore as IHeaderStore
        // Standard HeaderStore has optional IHeaderDecoder with default null, which falls back
        // to standard 15-field decoder. We resolve the keyed IDb services manually and construct
        // XdcHeaderStore which passes XdcHeaderDecoder to HeaderStore base constructor.
        builder.Register(ctx =>
        {
            var headerDb = ctx.ResolveKeyed<IDb>(DbNames.Headers);
            var blockNumberDb = ctx.ResolveKeyed<IDb>(DbNames.BlockNumbers);
            return new XdcHeaderStore(headerDb, blockNumberDb);
        }).As<Nethermind.Blockchain.Headers.IHeaderStore>()
          .SingleInstance();

        // Register penalty handler for masternode penalties
        builder.RegisterType<PenaltyHandler>()
            .As<IPenaltyHandler>()
            .SingleInstance();

        // Register snapshot manager for XDPoS consensus state
        builder.Register(ctx =>
        {
            var blockTree = ctx.Resolve<IBlockTree>();
            var penaltyHandler = ctx.Resolve<IPenaltyHandler>();
            
            // Use in-memory DB for snapshots (TODO: register proper DB column in DbProvider)
            var snapshotDb = new MemDb("xdc_snapshot");
            
            return new SnapshotManager(snapshotDb, blockTree, penaltyHandler);
        }).As<ISnapshotManager>()
          .SingleInstance();

        // Register XDC-specific genesis builder that uses XdcBlockHeader
        builder.RegisterType<XdcGenesisBuilder>()
            .As<IGenesisBuilder>()
            .InstancePerLifetimeScope();

        // Register XDC block validation module so XdcBlockTransactionsExecutor is used
        // in the MainProcessingContext's inner scope (where StandardBlockValidationModule also registers).
        // This follows the Taiko plugin pattern: AddSingleton<IBlockValidationModule> so the module
        // is applied AFTER StandardBlockValidationModule and thus overrides it.
        builder.AddSingleton<IBlockValidationModule, XdcBlockValidationModule>();

        // Register XDC block processor that preserves XdcBlockHeader during processing
        // IHeaderStore is auto-resolved by Autofac from BlockTreeModule registration
        builder.RegisterType<XdcBlockProcessor>()
            .As<IBlockProcessor>()
            .InstancePerLifetimeScope();

        // Register XDC header validator (relaxes gas limit validation for XDPoS)
        builder.RegisterType<XdcHeaderValidator>()
            .As<Nethermind.Consensus.Validators.IHeaderValidator>()
            .SingleInstance();

        // Register XDC reward calculator for checkpoint block rewards
        // NOTE: We only register as IRewardCalculatorSource, NOT as IRewardCalculator directly.
        // The BlockProcessingModule creates IRewardCalculator from IRewardCalculatorSource.
        // XdcBlockProcessor will create its own instance to ensure it uses the correct calculator.
        builder.Register(ctx =>
        {
            var logManager = ctx.Resolve<ILogManager>();
            var blockTree = ctx.Resolve<IBlockTree>();
            var worldState = ctx.Resolve<IWorldState>();
            var ecdsa = ctx.Resolve<IEthereumEcdsa>();
            var chainSpec = ctx.Resolve<ChainSpec>();
            
            // Get foundation wallet address from chainspec engine parameters
            Address? foundationWallet = null;
            if (chainSpec.EngineChainSpecParametersProvider is not null)
            {
                try
                {
                    var xdcParams = chainSpec.EngineChainSpecParametersProvider.GetChainSpecParameters<XdcChainSpecEngineParameters>();
                    foundationWallet = xdcParams.FoundationWalletAddr;
                }
                catch
                {
                    // Fallback to default constant if not found in chainspec
                }
            }
            
            return new XdcRewardCalculator(logManager, blockTree, worldState, ecdsa, foundationWallet);
        }).As<IRewardCalculatorSource>()
          .InstancePerLifetimeScope();

        // Register XDC transaction processor for BlockSigners special handling
        builder.RegisterType<XdcTransactionProcessor>()
            .As<ITransactionProcessor>()
            .InstancePerLifetimeScope();

        // Register XDC consensus message processor
        builder.RegisterType<XdcConsensusMessageProcessor>()
            .As<IXdcConsensusMessageProcessor>()
            .SingleInstance();

        // Register custom eth protocol factory for XDC eth/100 using lambda to handle optional dependencies
        builder.Register(ctx =>
        {
            var serializer = ctx.Resolve<IMessageSerializationService>();
            var nodeStatsManager = ctx.Resolve<INodeStatsManager>();
            var syncServer = ctx.Resolve<ISyncServer>();
            var backgroundTaskScheduler = ctx.Resolve<IBackgroundTaskScheduler>();
            var txPool = ctx.Resolve<ITxPool>();
            var gossipPolicy = ctx.Resolve<IGossipPolicy>();
            var logManager = ctx.Resolve<ILogManager>();
            var txGossipPolicy = ctx.ResolveOptional<ITxGossipPolicy>();
            var consensusProcessor = ctx.ResolveOptional<IXdcConsensusMessageProcessor>();

            return new Eth100ProtocolFactory(
                serializer,
                nodeStatsManager,
                syncServer,
                backgroundTaskScheduler,
                txPool,
                gossipPolicy,
                logManager,
                txGossipPolicy,
                consensusProcessor);
        }).As<ICustomEthProtocolFactory>()
          .SingleInstance();
    }
}

/// <summary>
/// IBlockValidationModule for XDC that registers XdcBlockTransactionsExecutor
/// in the MainProcessingContext's inner lifetime scope, overriding the standard
/// BlockValidationTransactionsExecutor. Follows the same pattern as TaikoBlockValidationModule.
/// </summary>
public class XdcBlockValidationModule : Module, IBlockValidationModule
{
    protected override void Load(ContainerBuilder builder)
    {
        // Register XDC-specific block transactions executor with gasBailout support.
        // This overrides the standard BlockValidationTransactionsExecutor to catch
        // MissingTrieNodeException and InvalidTransactionException (insufficient balance)
        // caused by accumulated state root divergence in XDC mainnet/apothem.
        builder.AddScoped<IBlockProcessor.IBlockTransactionsExecutor, XdcBlockTransactionsExecutor>();
    }
}
