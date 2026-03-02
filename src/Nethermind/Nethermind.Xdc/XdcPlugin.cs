// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Autofac.Core;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Consensus;
using Nethermind.Serialization.Rlp;
using Nethermind.Specs.ChainSpecStyle;
using System;

namespace Nethermind.Xdc;

public class XdcPlugin(ChainSpec chainSpec) : IConsensusPlugin
{
    public const string Xdc = "Xdc";
    public string Author => "Nethermind";
    public string Name => Xdc;
    public string Description => "Xdc support for Nethermind";
    public bool Enabled => chainSpec.SealEngineType == SealEngineType;
    public string SealEngineType => Core.SealEngineType.XDPoS;
    public IModule Module => new XdcModule();

    public void InitTxTypesAndRlpDecoders(INethermindApi api)
    {
        // Register XdcHeaderDecoder as the active encoder for outbound BlockHeaders messages.
        // Without this, Nethermind would encode headers as 15-field Ethereum format and XDC geth
        // peers would disconnect with BreachOfProtocol (DiscProtocolError).  See #39 / #45.
        RlpStream.SetHeaderDecoder(new XdcHeaderDecoder());
    }

    // IConsensusPlugin
    public IBlockProducerRunner InitBlockProducerRunner(IBlockProducer _)
    {
        throw new NotSupportedException();
    }
    public IBlockProducer InitBlockProducer()
    {
        throw new NotSupportedException();
    }
}
