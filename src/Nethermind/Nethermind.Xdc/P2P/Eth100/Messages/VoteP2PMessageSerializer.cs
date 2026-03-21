// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using DotNetty.Buffers;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.RLP;
using Nethermind.Network;

namespace Nethermind.Xdc.P2P.Eth100.Messages
{
    public class VoteP2PMessageSerializer : IZeroInnerMessageSerializer<VoteP2PMessage>
    {
        private readonly VoteDecoder _voteDecoder = new();

        public void Serialize(IByteBuffer byteBuffer, VoteP2PMessage message)
        {
            Rlp rlp = _voteDecoder.Encode(message.Vote);
            byteBuffer.EnsureWritable(rlp.Length);
            byteBuffer.WriteBytes(rlp.Bytes);
        }

        public VoteP2PMessage Deserialize(IByteBuffer byteBuffer)
        {
            Rlp.ValueDecoderContext ctx = new(byteBuffer.ReadAllBytesAsSpan());
            var vote = _voteDecoder.Decode(ref ctx);
            return new VoteP2PMessage(vote);
        }

        public int GetLength(VoteP2PMessage message, out int contentLength)
        {
            Rlp rlp = _voteDecoder.Encode(message.Vote);
            contentLength = rlp.Length;
            return contentLength;
        }
    }
}
