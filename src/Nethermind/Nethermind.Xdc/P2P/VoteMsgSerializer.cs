// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using DotNetty.Buffers;
using Nethermind.Core;
using Nethermind.Network;
using Nethermind.Serialization.Rlp;
using System;

namespace Nethermind.Xdc.P2P;

internal class VoteMsgSerializer : IZeroInnerMessageSerializer<VoteMsg>
{
    private static readonly VoteDecoder _voteDecoder = new VoteDecoder();

    public void Serialize(IByteBuffer byteBuffer, VoteMsg message)
    {
        Rlp rlp = _voteDecoder.Encode(message.Vote);
        byteBuffer.EnsureWritable(rlp.Length);
        byteBuffer.WriteBytes(rlp.Bytes);
    }

    public VoteMsg Deserialize(IByteBuffer byteBuffer)
    {
        Memory<byte> memory = byteBuffer.AsMemory();
        Rlp.ValueDecoderContext ctx = new(memory, true);
        Types.Vote vote = _voteDecoder.Decode(ref ctx, RlpBehaviors.None);
        byteBuffer.SkipBytes(memory.Length);
        return new() { Vote = vote };
    }

    public int GetLength(VoteMsg message, out int contentLength)
    {
        Rlp rlp = _voteDecoder.Encode(message.Vote);
        contentLength = rlp.Length;
        return rlp.Length;
    }
}
