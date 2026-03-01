// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Core.Crypto;
using Nethermind.Int256;
using Nethermind.Xdc;
using Nethermind.Xdc.Spec;
using Nethermind.Xdc.Test.Helpers;

namespace Nethermind.Core.Test.Builders;

public static class BuildExtentions
{
    public static XdcBlockHeaderBuilder XdcBlockHeader(this Build build)
    {
        return new XdcBlockHeaderBuilder();
    }

    public static QuorumCertificateBuilder QuorumCertificate(this Build build)
    {
        return new QuorumCertificateBuilder();
    }

    /// <summary>
    /// Set transaction data for XDC block signing.
    /// Encodes: [32 bytes blockNumber (big-endian)] [32 bytes blockHash]
    /// </summary>
    public static TransactionBuilder<T> WithXdcSigningData<T>(
        this TransactionBuilder<T> builder,
        long blockNumber,
        Hash256 blockHash) where T : Transaction, new()
    {
        byte[] data = new byte[64];
        // First 32 bytes: block number as big-endian UInt256
        new UInt256((ulong)blockNumber).ToBigEndian(new Span<byte>(data, 0, 32));
        // Last 32 bytes: block hash
        blockHash.Bytes.CopyTo(new Span<byte>(data, 32, 32));
        return builder.WithData(data);
    }

    /// <summary>
    /// Set transaction recipient to the BlockSignerContract from the spec.
    /// </summary>
    public static TransactionBuilder<T> ToBlockSignerContract<T>(
        this TransactionBuilder<T> builder,
        IXdcReleaseSpec spec) where T : Transaction, new()
    {
        return builder.WithTo(spec.BlockSignerContract);
    }
}
