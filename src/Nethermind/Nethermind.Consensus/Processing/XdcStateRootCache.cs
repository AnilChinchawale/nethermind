// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Nethermind.Core;
using Nethermind.Core.Collections;
using Nethermind.Core.Crypto;

namespace Nethermind.Consensus.Processing;

/// <summary>
/// Persistent cache mapping block number → our computed state root for XDC chains.
/// XDC state roots diverge from geth at checkpoint blocks (every 900 blocks starting at 1800).
/// Since all subsequent blocks inherit the diverged state, every block from 1800 onwards
/// has a different state root than what geth computes.
///
/// Headers stored in the DB have geth's state root (from download).
/// This cache stores our computed state root so BeginScope can use it
/// instead of the stored header's root.
///
/// The remoteToLocal mapping allows any code that asks "do we have state for root X?"
/// (where X is geth's root from stored headers) to be redirected to our local root Y.
/// This prevents MissingTrieNodeException when loading state for subsequent blocks.
///
/// PERSISTENCE: The latest mapping (last block's local+remote roots) is saved to disk
/// every 100 blocks. On restart, it loads the saved mapping so the cache isn't empty.
///
/// PR54 ENHANCEMENTS:
/// - Inbound sync tracking for better sync mode detection
/// - Checkpoint-aware state root handling for blocks 1800, 62101, 3000000
/// - Batch processing optimization for historical sync
/// - State root mismatch detection and recovery
/// </summary>
public static class XdcStateRootCache
{
    private static readonly ConcurrentDictionary<long, Hash256> _computedStateRoots = new();
    private static readonly ConcurrentDictionary<Hash256, Hash256> _remoteToLocal = new();
    private static readonly ConcurrentDictionary<long, Hash256> _remoteRootsByBlock = new();
    
    // PR54: Track sync state for inbound handling
    private static long _currentSyncBlock;
    private static long _syncTargetBlock;
    private static bool _isInFastSync;
    private static readonly object _syncStateLock = new();

    // PR54: Checkpoint blocks that need special handling
    private static readonly HashSet<long> CheckpointBlocks = new()
    {
        1800,      // First epoch checkpoint
        62101,     // Early network checkpoint  
        3000000    // V1/V2 transition
    };

    private static string? _persistPath;
    private static long _lastPersistedBlock;
    private const int PersistEveryNBlocks = 100;
    private static readonly object _persistLock = new();
    private static bool _loaded;

    /// <summary>
    /// Set the path for persistence file. Call once during startup.
    /// </summary>
    public static void SetPersistPath(string dataDir)
    {
        _persistPath = Path.Combine(dataDir, "xdc-state-root-cache.json");
        LoadFromDisk();
    }

    /// <summary>
    /// PR54: Update sync state for inbound sync handling
    /// Call this when sync progress changes
    /// </summary>
    public static void UpdateSyncState(long currentBlock, long targetBlock, bool isFastSync)
    {
        lock (_syncStateLock)
        {
            _currentSyncBlock = currentBlock;
            _syncTargetBlock = targetBlock;
            _isInFastSync = isFastSync;
        }
    }

    /// <summary>
    /// PR54: Check if we're currently in inbound sync mode
    /// </summary>
    public static bool IsInInboundSync(long blockNumber)
    {
        lock (_syncStateLock)
        {
            // We're in inbound sync if:
            // 1. We're actively syncing (target > current)
            // 2. The block is ahead of our current position
            // 3. We're in fast sync mode
            if (_isInFastSync)
                return true;
            
            if (_syncTargetBlock > _currentSyncBlock)
            {
                // During sync, blocks at or ahead of current are "inbound"
                return blockNumber >= _currentSyncBlock - 10; // Small buffer for reorgs
            }
            
            return false;
        }
    }

    /// <summary>
    /// PR54: Check if block is a checkpoint that needs special handling
    /// </summary>
    public static bool IsCheckpointBlock(long blockNumber) => CheckpointBlocks.Contains(blockNumber);

    /// <summary>
    /// Store the computed state root for a given block number, with the remote (geth) root for reverse lookup.
    /// </summary>
    public static void SetComputedStateRoot(long blockNumber, Hash256 localStateRoot, Hash256? remoteStateRoot = null)
    {
        _computedStateRoots[blockNumber] = localStateRoot;

        if (remoteStateRoot is not null && remoteStateRoot != localStateRoot)
        {
            _remoteToLocal[remoteStateRoot] = localStateRoot;
            _remoteRootsByBlock[blockNumber] = remoteStateRoot;
        }

        // Evict old entries to prevent unbounded memory growth (keep last 10M blocks for XDC mainnet)
        // XDC mainnet: EVERY block from 1800+ diverges, so we need massive cache (10M+ entries)
        const int MaxCachedBlocks = 10_000_000;
        if (blockNumber > MaxCachedBlocks)
        {
            long evictBlock = blockNumber - MaxCachedBlocks;
            _computedStateRoots.TryRemove(evictBlock, out _);
            if (_remoteRootsByBlock.TryRemove(evictBlock, out var oldRemote))
            {
                _remoteToLocal.TryRemove(oldRemote, out _);
            }
        }

        // Persist to disk periodically
        if (blockNumber - _lastPersistedBlock >= PersistEveryNBlocks)
        {
            PersistToDisk(blockNumber, localStateRoot, remoteStateRoot);
        }
    }

    /// <summary>
    /// PR54: Batch insert for sync mode - more efficient during historical sync
    /// </summary>
    public static void SetComputedStateRootsBatch(IEnumerable<(long blockNumber, Hash256 localRoot, Hash256? remoteRoot)> entries)
    {
        long maxBlock = 0;
        Hash256? lastLocalRoot = null;
        Hash256? lastRemoteRoot = null;

        foreach (var (blockNumber, localRoot, remoteRoot) in entries)
        {
            _computedStateRoots[blockNumber] = localRoot;
            
            if (remoteRoot is not null && remoteRoot != localRoot)
            {
                _remoteToLocal[remoteRoot] = localRoot;
                _remoteRootsByBlock[blockNumber] = remoteRoot;
            }

            if (blockNumber > maxBlock)
            {
                maxBlock = blockNumber;
                lastLocalRoot = localRoot;
                lastRemoteRoot = remoteRoot;
            }
        }

        // Persist once after batch
        if (maxBlock > _lastPersistedBlock + PersistEveryNBlocks / 2)
        {
            PersistToDisk(maxBlock, lastLocalRoot!, lastRemoteRoot);
        }
    }

    /// <summary>
    /// Get the computed state root for a given block number.
    /// Returns null if no override exists.
    /// </summary>
    public static Hash256? GetComputedStateRoot(long blockNumber) =
        _computedStateRoots.TryGetValue(blockNumber, out var root) ? root : null;

    /// <summary>
    /// Given a remote (geth) state root, find the locally-computed state root.
    /// </summary>
    public static Hash256? FindLocalRootForRemote(Hash256 remoteRoot) =
        _remoteToLocal.TryGetValue(remoteRoot, out var localRoot) ? localRoot : null;

    /// <summary>
    /// PR54: Given a local (computed) state root, find the original remote (geth) state root.
    /// Used for outbound messages — we need to advertise geth-compatible roots to peers.
    /// </summary>
    public static Hash256? FindRemoteRootForLocal(Hash256 localRoot)
    {
        foreach (var kvp in _remoteToLocal)
        {
            if (kvp.Value == localRoot)
                return kvp.Key;
        }
        return null;
    }

    /// <summary>
    /// PR54: Get the remote (geth) state root for a specific block number.
    /// </summary>
    public static Hash256? GetRemoteStateRoot(long blockNumber)
    {
        return _remoteRootsByBlock.TryGetValue(blockNumber, out var root) ? root : null;
    }

    /// <summary>
    /// Check if a given root is either a known local root or can be mapped from a remote root.
    /// Returns the usable root (local if mapping exists, original otherwise).
    /// </summary>
    public static Hash256 ResolveRoot(Hash256 root)
    {
        if (_remoteToLocal.TryGetValue(root, out var localRoot))
            return localRoot;
        return root;
    }

    /// <summary>
    /// PR54: Check if we have a mapping for a given root
    /// Used to detect state root mismatches
    /// </summary>
    public static bool HasRootMapping(Hash256 remoteRoot)
    {
        return _remoteToLocal.ContainsKey(remoteRoot);
    }

    /// <summary>
    /// Get the latest cached block number and its computed root.
    /// </summary>
    public static (long blockNumber, Hash256 root)? GetLatestCachedRoot()
    {
        if (_computedStateRoots.IsEmpty) return null;
        long maxBlock = _computedStateRoots.Keys.Max();
        return (maxBlock, _computedStateRoots[maxBlock]);
    }

    /// <summary>
    /// PR54: Get the highest consecutive block number we've cached
    /// This helps detect gaps during sync
    /// </summary>
    public static long GetHighestConsecutiveBlock()
    {
        if (_computedStateRoots.IsEmpty) return 0;
        
        var blocks = _computedStateRoots.Keys.OrderBy(b => b).ToList();
        if (blocks.Count == 0) return 0;
        
        long highest = blocks[0];
        for (int i = 1; i < blocks.Count; i++)
        {
            if (blocks[i] == highest + 1)
                highest = blocks[i];
            else
                break; // Gap found
        }
        
        return highest;
    }

    /// <summary>
    /// Number of cached entries.
    /// </summary>
    public static int Count => _computedStateRoots.Count;

    /// <summary>
    /// PR54: Replace local state root with remote (geth) root in a header for outbound P2P messages.
    /// Returns original header if no mapping exists.
    /// </summary>
    public static BlockHeader SwapStateRootForOutbound(BlockHeader header)
    {
        if (header.StateRoot is null) return header;

        // Try block number lookup first (most reliable)
        var remoteRoot = GetRemoteStateRoot(header.Number);
        if (remoteRoot is not null && remoteRoot != header.StateRoot)
        {
            header.StateRoot = remoteRoot;
            return header;
        }

        // Fallback: reverse lookup from local→remote mapping
        var remote = FindRemoteRootForLocal(header.StateRoot);
        if (remote is not null)
        {
            header.StateRoot = remote;
        }

        return header;
    }

    /// <summary>
    /// PR54: Swap state roots in outbound headers for XDC chains.
    /// Call this on headers returned by FindHeaders before sending to peers.
    /// This is the sync server interceptor for Issue #53.
    /// </summary>
    public static void SwapOutboundStateRoots(IOwnedReadOnlyList<BlockHeader> headers)
    {
        if (headers is null || headers.Count == 0) return;

        for (int i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            if (header?.StateRoot is null) continue;

            // Look up the remote (geth) root for this block number
            var remoteRoot = GetRemoteStateRoot(header.Number);
            if (remoteRoot is not null && remoteRoot != header.StateRoot)
            {
                header.StateRoot = remoteRoot;
            }
        }
    }

    /// <summary>
    /// PR54: Validate state root consistency during sync
    /// Returns true if the state root is consistent with our cache
    /// </summary>
    public static bool ValidateStateRootConsistency(long blockNumber, Hash256 claimedRemoteRoot)
    {
        // If we don't have this block cached, we can't validate
        if (!_remoteRootsByBlock.TryGetValue(blockNumber, out var cachedRemoteRoot))
            return true; // No cached value = no contradiction

        // Check for mismatch
        if (cachedRemoteRoot != claimedRemoteRoot)
        {
            // This could indicate a fork or data corruption
            Console.WriteLine($"[XdcStateRootCache] State root mismatch at block {blockNumber}: " +
                              $"cached={cachedRemoteRoot}, claimed={claimedRemoteRoot}");
            return false;
        }

        return true;
    }

    /// <summary>
    /// PR54: Handle state root mismatch by clearing cache and forcing re-computation
    /// Call this when a mismatch is detected
    /// </summary>
    public static void HandleStateRootMismatch(long blockNumber)
    {
        Console.WriteLine($"[XdcStateRootCache] Handling state root mismatch at block {blockNumber}");
        
        // Clear all entries from this block onwards
        var keysToRemove = _computedStateRoots.Keys.Where(k => k >= blockNumber).ToList();
        foreach (var key in keysToRemove)
        {
            _computedStateRoots.TryRemove(key, out _);
            if (_remoteRootsByBlock.TryRemove(key, out var oldRemote))
            {
                _remoteToLocal.TryRemove(oldRemote, out _);
            }
        }
        
        // Force persistence update
        _lastPersistedBlock = Math.Min(_lastPersistedBlock, blockNumber - 1);
    }

    private static void PersistToDisk(long blockNumber, Hash256 localRoot, Hash256? remoteRoot)
    {
        if (_persistPath is null) return;

        lock (_persistLock)
        {
            try
            {
                // Persist the FULL remote→local mapping, not just the latest entry
                var data = new FullCacheEntry
                {
                    LastBlockNumber = blockNumber,
                    RemoteToLocalMappings = _remoteToLocal.ToDictionary(
                        kvp => kvp.Key.ToString(),
                        kvp => kvp.Value.ToString()
                    )
                };
                var json = JsonSerializer.Serialize(data);
                File.WriteAllText(_persistPath, json);
                _lastPersistedBlock = blockNumber;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"XdcStateRootCache: Persist failed: {ex.Message}");
            }
        }
    }

    private static void LoadFromDisk()
    {
        if (_loaded || _persistPath is null || !File.Exists(_persistPath)) return;
        _loaded = true;

        try
        {
            var json = File.ReadAllText(_persistPath);

            // Try new format first
            try
            {
                var data = JsonSerializer.Deserialize<FullCacheEntry>(json);
                if (data?.RemoteToLocalMappings is not null)
                {
                    foreach (var kvp in data.RemoteToLocalMappings)
                    {
                        var remote = new Hash256(kvp.Key);
                        var local = new Hash256(kvp.Value);
                        _remoteToLocal[remote] = local;
                    }
                    _lastPersistedBlock = data.LastBlockNumber;
                    Console.WriteLine($"XdcStateRootCache: Loaded {_remoteToLocal.Count} mappings from disk (up to block {data.LastBlockNumber})");
                    return;
                }
            }
            catch { /* Fall through to old format */ }

            // Fallback to old single-entry format for backwards compatibility
            var oldData = JsonSerializer.Deserialize<CacheEntry>(json);
            if (oldData?.LocalRoot is not null)
            {
                var localRoot = new Hash256(oldData.LocalRoot);
                _computedStateRoots[oldData.BlockNumber] = localRoot;

                if (oldData.RemoteRoot is not null)
                {
                    var remoteRoot = new Hash256(oldData.RemoteRoot);
                    _remoteToLocal[remoteRoot] = localRoot;
                    _remoteRootsByBlock[oldData.BlockNumber] = remoteRoot;
                }

                _lastPersistedBlock = oldData.BlockNumber;
                Console.WriteLine($"XdcStateRootCache: Loaded legacy format — block {oldData.BlockNumber}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"XdcStateRootCache: Failed to load from disk: {ex.Message}");
        }
    }

    private class CacheEntry
    {
        public long BlockNumber { get; set; }
        public string? LocalRoot { get; set; }
        public string? RemoteRoot { get; set; }
    }

    private class FullCacheEntry
    {
        public long LastBlockNumber { get; set; }
        public Dictionary<string, string>? RemoteToLocalMappings { get; set; }
    }
}
