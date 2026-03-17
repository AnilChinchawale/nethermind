<<<<<<< HEAD
// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.Types;

namespace Nethermind.Xdc.RLP
{
    public class SyncInfoDecoder : IRlpStreamDecoder<SyncInfo>, IRlpValueDecoder<SyncInfo>
    {
        private readonly QuorumCertificateDecoder _qcDecoder = new();
        private readonly TimeoutCertificateDecoder _tcDecoder = new();

        public SyncInfo Decode(RlpStream rlpStream, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
        {
            rlpStream.ReadSequenceLength();
            QuorumCertificate highestQC = _qcDecoder.Decode(rlpStream, rlpBehaviors);
            TimeoutCertificate highestTC = _tcDecoder.Decode(rlpStream, rlpBehaviors);
            return new SyncInfo(highestQC, highestTC);
        }

        public SyncInfo Decode(ref Rlp.ValueDecoderContext decoderContext, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
        {
            decoderContext.ReadSequenceLength();
            QuorumCertificate highestQC = _qcDecoder.Decode(ref decoderContext, rlpBehaviors);
            TimeoutCertificate highestTC = _tcDecoder.Decode(ref decoderContext, rlpBehaviors);
            return new SyncInfo(highestQC, highestTC);
        }

        public void Encode(RlpStream stream, SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
        {
            int contentLength = GetLength(item, rlpBehaviors);
            stream.StartSequence(contentLength);
            _qcDecoder.Encode(stream, item.HighestQuorumCert, rlpBehaviors);
            _tcDecoder.Encode(stream, item.HighestTimeoutCert, rlpBehaviors);
        }

        public Rlp Encode(SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
        {
            RlpStream rlpStream = new(GetLength(item, rlpBehaviors));
            Encode(rlpStream, item, rlpBehaviors);
            return new Rlp(rlpStream.Data.ToArray());
        }

        public int GetLength(SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
        {
            int contentLength = 0;
            contentLength += _qcDecoder.GetLength(item.HighestQuorumCert, rlpBehaviors);
            contentLength += _tcDecoder.GetLength(item.HighestTimeoutCert, rlpBehaviors);
            return Rlp.LengthOfSequence(contentLength);
        }
=======
// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Serialization.Rlp;
using Nethermind.Xdc.RLP;
using Nethermind.Xdc.Types;

namespace Nethermind.Xdc;

internal class SyncInfoDecoder : RlpValueDecoder<SyncInfo>
{
    private readonly QuorumCertificateDecoder _quorumCertificateDecoder = new();
    private readonly TimeoutCertificateDecoder _timeoutCertificateDecoder = new();

    protected override SyncInfo DecodeInternal(ref Rlp.ValueDecoderContext decoderContext, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (decoderContext.IsNextItemEmptyList())
        {
            decoderContext.ReadByte();
            return null;
        }

        int sequenceLength = decoderContext.ReadSequenceLength();
        int endPosition = decoderContext.Position + sequenceLength;

        QuorumCertificate highestQuorumCert = _quorumCertificateDecoder.Decode(ref decoderContext, rlpBehaviors);
        TimeoutCertificate highestTimeoutCert = _timeoutCertificateDecoder.Decode(ref decoderContext, rlpBehaviors);

        if ((rlpBehaviors & RlpBehaviors.AllowExtraBytes) != RlpBehaviors.AllowExtraBytes)
        {
            decoderContext.Check(endPosition);
        }

        return new SyncInfo(highestQuorumCert, highestTimeoutCert);
    }

    public Rlp Encode(SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (item is null)
            return Rlp.OfEmptyList;

        RlpStream rlpStream = new(GetLength(item, rlpBehaviors));
        Encode(rlpStream, item, rlpBehaviors);

        return new Rlp(rlpStream.Data.ToArray());
    }

    public override void Encode(RlpStream stream, SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (item is null)
        {
            stream.EncodeNullObject();
            return;
        }

        stream.StartSequence(GetContentLength(item, rlpBehaviors));
        _quorumCertificateDecoder.Encode(stream, item.HighestQuorumCert, rlpBehaviors);
        _timeoutCertificateDecoder.Encode(stream, item.HighestTimeoutCert, rlpBehaviors);
    }

    public override int GetLength(SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        return Rlp.LengthOfSequence(GetContentLength(item, rlpBehaviors));
    }

    public int GetContentLength(SyncInfo item, RlpBehaviors rlpBehaviors = RlpBehaviors.None)
    {
        if (item is null)
            return 0;

        return _quorumCertificateDecoder.GetLength(item.HighestQuorumCert, rlpBehaviors)
               + _timeoutCertificateDecoder.GetLength(item.HighestTimeoutCert, rlpBehaviors);
>>>>>>> upstream/master
    }
}
