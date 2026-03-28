// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Consensus.Processing;
using Nethermind.Core;
using Nethermind.Core.Collections;
using Nethermind.Core.Crypto;

namespace Nethermind.Xdc;

/// <summary>
/// Intercepts outbound block headers for XDC chains to replace local (NM-computed)
/// state roots with remote (geth) state roots. This prevents peers from disconnecting
/// due to state root mismatches. (Issue #53)
///
/// The XdcStateRootCache stores mappings between NM's computed roots and geth's roots.
/// When NM sends headers to peers (GetBlockHeaders response), we swap our local root
/// back to the geth root so peers see the expected state root.
/// </summary>
public static class XdcSyncServerInterceptor
{
    /// <summary>
    /// Swap state roots in outbound headers for XDC chains.
    /// Call this on headers returned by FindHeaders before sending to peers.
    /// </summary>
    public static void SwapOutboundStateRoots(IOwnedReadOnlyList<BlockHeader> headers)
    {
        if (headers is null || headers.Count == 0) return;

        for (int i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            if (header?.StateRoot is null) continue;

            // Look up the remote (geth) root for this block number
            var remoteRoot = XdcStateRootCache.GetRemoteStateRoot(header.Number);
            if (remoteRoot is not null && remoteRoot != header.StateRoot)
            {
                header.StateRoot = remoteRoot;
            }
        }
    }
}
