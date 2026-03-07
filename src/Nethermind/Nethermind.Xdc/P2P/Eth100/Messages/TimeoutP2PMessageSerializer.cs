// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using DotNetty.Buffers;
using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.RLP;
using Nethermind.Network;
using System;

namespace Nethermind.Xdc.P2P.Eth100.Messages
{
    public class TimeoutP2PMessageSerializer : IZeroInnerMessageSerializer<TimeoutP2PMessage>
    {
        private readonly TimeoutDecoder _timeoutDecoder = new();

        public void Serialize(IByteBuffer byteBuffer, TimeoutP2PMessage message)
        {
            Rlp rlp = _timeoutDecoder.Encode(message.Timeout);
            byteBuffer.EnsureWritable(rlp.Length);
            byteBuffer.WriteBytes(rlp.Bytes);
        }

        public TimeoutP2PMessage Deserialize(IByteBuffer byteBuffer)
        {
            Memory<byte> memory = byteBuffer.AsMemory();
            Rlp.ValueDecoderContext ctx = new(memory, true);
            var timeout = _timeoutDecoder.Decode(ref ctx);
            byteBuffer.SkipBytes(memory.Length);
            return new TimeoutP2PMessage(timeout);
        }

        public int GetLength(TimeoutP2PMessage message, out int contentLength)
        {
            Rlp rlp = _timeoutDecoder.Encode(message.Timeout);
            contentLength = rlp.Length;
            return contentLength;
        }
    }
}
