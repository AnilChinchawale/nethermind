// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Logging;
using Nethermind.Xdc.Types;

namespace Nethermind.Xdc
{
    /// <summary>
    /// Default implementation of XDPoS v2 consensus message processor
    /// Routes P2P messages to appropriate consensus components
    /// </summary>
    public class XdcConsensusMessageProcessor : IXdcConsensusMessageProcessor
    {
        private readonly ILogger _logger;
        private readonly IVotesManager? _votesManager;
        private readonly ITimeoutCertificateManager? _timeoutManager;
        private readonly ISyncInfoManager? _syncInfoManager;
        private readonly IQuorumCertificateManager? _qcManager;

        public XdcConsensusMessageProcessor(
            ILogManager logManager,
            IVotesManager? votesManager = null,
            ITimeoutCertificateManager? timeoutManager = null,
            ISyncInfoManager? syncInfoManager = null,
            IQuorumCertificateManager? qcManager = null)
        {
            _logger = logManager?.GetClassLogger() ?? throw new ArgumentNullException(nameof(logManager));
            _votesManager = votesManager;
            _timeoutManager = timeoutManager;
            _syncInfoManager = syncInfoManager;
            _qcManager = qcManager;
        }

        public void ProcessVote(Vote vote)
        {
            if (_logger.IsTrace)
                _logger.Trace($"Processing Vote: {vote}");

            if (_votesManager == null)
            {
                if (_logger.IsDebug)
                    _logger.Debug("VotesManager not configured, vote dropped");
                return;
            }

            try
            {
                // Delegate to VotesManager.OnReceiveVote which:
                // 1. Validates vote distance from current block
                // 2. Filters by round and verifies signer is a masternode
                // 3. Adds to vote pool and checks QC threshold
                _votesManager.OnReceiveVote(vote);
            }
            catch (Exception ex)
            {
                if (_logger.IsDebug)
                    _logger.Debug($"Failed to process vote: {ex.Message}");
            }
        }

        public void ProcessTimeout(Timeout timeout)
        {
            if (_logger.IsTrace)
                _logger.Trace($"Processing Timeout: {timeout}");

            if (_timeoutManager == null)
            {
                if (_logger.IsDebug)
                    _logger.Debug("TimeoutManager not configured, timeout dropped");
                return;
            }

            try
            {
                // Delegate to TimeoutCertificateManager.OnReceiveTimeout which:
                // 1. Validates timeout distance from current epoch
                // 2. Filters by round and verifies signer is a masternode
                // 3. Adds to timeout pool and checks TC threshold
                _timeoutManager.OnReceiveTimeout(timeout);
            }
            catch (Exception ex)
            {
                if (_logger.IsDebug)
                    _logger.Debug($"Failed to process timeout: {ex.Message}");
            }
        }

        public void ProcessSyncInfo(SyncInfo syncInfo)
        {
            if (_logger.IsTrace)
                _logger.Trace($"Processing SyncInfo");

            if (syncInfo == null)
            {
                if (_logger.IsDebug)
                    _logger.Debug("Received null SyncInfo, ignoring");
                return;
            }

            // Process embedded QC if present and we have a QC manager
            if (syncInfo.HighestQuorumCert != null && _qcManager != null)
            {
                try
                {
                    _qcManager.CommitCertificate(syncInfo.HighestQuorumCert);
                }
                catch (Exception ex)
                {
                    if (_logger.IsDebug)
                        _logger.Debug($"Failed to commit QC from SyncInfo: {ex.Message}");
                }
            }

            // Process embedded TC if present and we have a timeout manager
            if (syncInfo.HighestTimeoutCert != null && _timeoutManager != null)
            {
                try
                {
                    _timeoutManager.ProcessTimeoutCertificate(syncInfo.HighestTimeoutCert);
                }
                catch (Exception ex)
                {
                    if (_logger.IsDebug)
                        _logger.Debug($"Failed to process TC from SyncInfo: {ex.Message}");
                }
            }

            // Delegate to SyncInfoManager for any additional sync logic
            if (_syncInfoManager != null)
            {
                try
                {
                    _syncInfoManager.ProcessSyncInfo(syncInfo);
                }
                catch (Exception ex)
                {
                    if (_logger.IsDebug)
                        _logger.Debug($"Failed to process SyncInfo: {ex.Message}");
                }
            }

            if (_logger.IsDebug)
                _logger.Debug($"SyncInfo processed: QC at {syncInfo.HighestQuorumCert?.ProposedBlockInfo}, TC at {syncInfo.HighestTimeoutCert?.Round}");
        }

        public void ProcessQuorumCertificate(QuorumCertificate qc)
        {
            if (_logger.IsTrace)
                _logger.Trace($"Processing QuorumCertificate: {qc?.ProposedBlockInfo}");

            if (_qcManager == null)
            {
                if (_logger.IsDebug)
                    _logger.Debug("QCManager not configured, QC dropped");
                return;
            }

            if (qc == null)
            {
                if (_logger.IsDebug)
                    _logger.Debug("Received null QC, ignoring");
                return;
            }

            try
            {
                // Delegate to QuorumCertificateManager.CommitCertificate which:
                // 1. Updates highest QC if this one is newer
                // 2. Triggers 3-chain finality updates
                // 3. Updates lock QC and commit block info
                _qcManager.CommitCertificate(qc);
            }
            catch (Exception ex)
            {
                if (_logger.IsDebug)
                    _logger.Debug($"Failed to process QC: {ex.Message}");
            }

            if (_logger.IsDebug)
                _logger.Debug($"QC processed: {qc.ProposedBlockInfo}");
        }
    }
}
