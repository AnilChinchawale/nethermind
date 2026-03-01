// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using DotNetty.Buffers;
using Nethermind.Core;
using Nethermind.Serialization.Rlp;
using Nethermind.Stats.SyncLimits;

namespace Nethermind.Network.P2P.Subprotocols.Eth.V62.Messages
{
    public class BlockHeadersMessageSerializer : IZeroInnerMessageSerializer<BlockHeadersMessage>
    {
        private static readonly RlpLimit RlpLimit = RlpLimit.For<BlockHeadersMessage>(NethermindSyncLimits.MaxHeaderFetch, nameof(BlockHeadersMessage.BlockHeaders));

        // Use the same decoder as RlpStream.Encode(BlockHeader) so that GetLength and Serialize
        // agree on the encoded size.  For XDC this will be XdcHeaderDecoder (set via
        // RlpStream.SetHeaderDecoder()); for all other chains it falls back to HeaderDecoder.
        private static IHeaderDecoder GetHeaderDecoder() => RlpStream.ActiveHeaderDecoder;

        public void Serialize(IByteBuffer byteBuffer, BlockHeadersMessage message)
        {
            int length = GetLength(message, out int contentLength);
            byteBuffer.EnsureWritable(length);
            RlpStream rlpStream = new NettyRlpStream(byteBuffer);

            rlpStream.StartSequence(contentLength);
            for (int i = 0; i < message.BlockHeaders.Count; i++)
            {
                rlpStream.Encode(message.BlockHeaders[i]);
            }
        }

        public BlockHeadersMessage Deserialize(IByteBuffer byteBuffer)
        {
            RlpStream rlpStream = new NettyRlpStream(byteBuffer);
            return Deserialize(rlpStream);
        }

        public int GetLength(BlockHeadersMessage message, out int contentLength)
        {
            contentLength = 0;
            IHeaderDecoder decoder = GetHeaderDecoder();
            for (int i = 0; i < message.BlockHeaders.Count; i++)
            {
                contentLength += decoder.GetLength(message.BlockHeaders[i], RlpBehaviors.None);
            }

            return Rlp.LengthOfSequence(contentLength);
        }

        public static BlockHeadersMessage Deserialize(RlpStream rlpStream)
        {
            BlockHeadersMessage message = new();
            message.BlockHeaders = Rlp.DecodeArrayPool<BlockHeader>(rlpStream, limit: RlpLimit);
            return message;
        }
    }
}
