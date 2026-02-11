// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Buffers;
using System.Diagnostics;
using Nethermind.Core.Extensions;
using Nethermind.Db;
using Nethermind.Logging;
using Nethermind.State.Flat.Rsst;

namespace Nethermind.State.Flat.PersistedSnapshots;

/// <summary>
/// Manages conversion of in-memory snapshots to persisted snapshots (RSST files)
/// and compaction of persisted snapshots. Mirrors <see cref="SnapshotCompactor"/>'s
/// logarithmic compaction strategy for the persisted layer.
/// </summary>
public class PersistedSnapshotManager(
    IPersistedSnapshotRepository persistedSnapshotRepository,
    IFlatDbConfig config,
    ILogManager logManager) : IPersistedSnapshotManager
{
    private readonly ILogger _logger = logManager.GetClassLogger<PersistedSnapshotManager>();
    private readonly int _compactSize = config.CompactSize;
    private readonly int _minCompactSize = Math.Max(config.MinCompactSize, 2);

    public void ConvertToPersistedSnapshot(Snapshot snapshot)
    {
        if (_logger.IsDebug) _logger.Debug($"Converting snapshot to persisted: {snapshot.From} -> {snapshot.To}");

        PersistedSnapshot persisted = persistedSnapshotRepository.AddBaseSnapshot(snapshot);
        Metrics.PersistedSnapshotWrites++;
        Metrics.PersistedSnapshotCount = persistedSnapshotRepository.SnapshotCount;

        TryCompactPersistedSnapshots(snapshot.To);
    }

    /// <summary>
    /// Try to compact persisted snapshots using logarithmic compaction.
    /// Mirrors <see cref="SnapshotCompactor.GetSnapshotsToCompact"/> logic.
    /// </summary>
    internal void TryCompactPersistedSnapshots(StateId snapshotTo)
    {
        if (_compactSize <= 1) return;

        long blockNumber = snapshotTo.BlockNumber;
        if (blockNumber == 0) return;

        int compactSize = (int)Math.Min(blockNumber & -blockNumber, _compactSize);
        if (compactSize < _minCompactSize) return;

        bool isFullCompaction = compactSize == _compactSize;
        long startingBlockNumber = ((blockNumber - 1) / compactSize) * compactSize;

        if (!isFullCompaction)
        {
            // Remove previous compacted snapshots at the prior compaction boundary
            persistedSnapshotRepository.RemoveCompactedSnapshotsAtBlock(blockNumber - compactSize);
        }

        // We need at least 2 snapshots to compact
        if (persistedSnapshotRepository.SnapshotCount < 2) return;

        using PersistedSnapshotList snapshots = persistedSnapshotRepository.AssembleSnapshotsForCompaction(snapshotTo, startingBlockNumber);
        if (snapshots.Count < 2) return;

        if (snapshots[0].From.BlockNumber != startingBlockNumber)
        {
            if (_logger.IsDebug) _logger.Debug($"Unable to compile persisted snapshots to compact. {snapshots[0].From.BlockNumber} -> {snapshots[snapshots.Count - 1].To.BlockNumber}. Starting block number should be {startingBlockNumber}");
            return;
        }

        if (_logger.IsDebug) _logger.Debug($"Compacting {snapshots.Count} persisted snapshots at block {blockNumber}, compact size {compactSize}");

        StateId from = snapshots[0].From;
        StateId to = snapshots[snapshots.Count - 1].To;

        int totalSize = 0;
        for (int i = 0; i < snapshots.Count; i++) totalSize += snapshots[i].Data.Length;
        totalSize += 4096;

        byte[] bufA = ArrayPool<byte>.Shared.Rent(totalSize);
        byte[] bufB = ArrayPool<byte>.Shared.Rent(totalSize);
        try
        {
            int len = MergeSnapshots(snapshots, bufA, bufB, out bool resultInA);
            ReadOnlySpan<byte> merged = (resultInA ? bufA : bufB).AsSpan(0, len);
            persistedSnapshotRepository.AddCompactedSnapshot(from, to, merged);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bufA);
            ArrayPool<byte>.Shared.Return(bufB);
        }

        Metrics.PersistedSnapshotCompactions++;
        Metrics.PersistedSnapshotCount = persistedSnapshotRepository.SnapshotCount;
    }

    /// <summary>
    /// Prune persisted snapshots older than the given state.
    /// </summary>
    public void PrunePersistedSnapshots(StateId currentPersistedState)
    {
        if (currentPersistedState.BlockNumber <= 0) return;

        int pruned = persistedSnapshotRepository.PruneBefore(currentPersistedState);
        if (pruned > 0)
        {
            Metrics.PersistedSnapshotPrunes += pruned;
            Metrics.PersistedSnapshotCount = persistedSnapshotRepository.SnapshotCount;
            if (_logger.IsDebug) _logger.Debug($"Pruned {pruned} persisted snapshots before block {currentPersistedState.BlockNumber}");
        }
    }

    /// <summary>
    /// Merge a list of persisted snapshots (oldest-first) into a single compacted byte[].
    /// Uses pairwise self-destruct-aware merge from oldest to newest.
    /// </summary>
    internal static byte[] MergeSnapshots(PersistedSnapshotList snapshots)
    {
        if (snapshots.Count == 0) throw new ArgumentException("Cannot merge empty snapshot list");
        if (snapshots.Count == 1) return snapshots[0].Data.ToArray();

        int totalSize = 0;
        for (int i = 0; i < snapshots.Count; i++) totalSize += snapshots[i].Data.Length;
        totalSize += 4096;

        byte[] bufA = ArrayPool<byte>.Shared.Rent(totalSize);
        byte[] bufB = ArrayPool<byte>.Shared.Rent(totalSize);
        try
        {
            int len = MergeSnapshots(snapshots, bufA, bufB, out bool resultInA);
            return (resultInA ? bufA : bufB).AsSpan(0, len).ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bufA);
            ArrayPool<byte>.Shared.Return(bufB);
        }
    }

    private static int MergeSnapshots(PersistedSnapshotList snapshots, Span<byte> bufferA, Span<byte> bufferB, out bool resultInA)
    {
        snapshots[0].Data.Span.CopyTo(bufferA);
        int currentLen = snapshots[0].Data.Length;
        resultInA = true;

        for (int i = 1; i < snapshots.Count; i++)
        {
            ReadOnlySpan<byte> src = resultInA ? bufferA[..currentLen] : bufferB[..currentLen];
            Span<byte> dst = resultInA ? bufferB : bufferA;
            currentLen = MergeTwoPersisted(src, snapshots[i].Data.Span, dst);
            resultInA = !resultInA;
        }

        return currentLen;
    }

    /// <summary>
    /// Merge two columnar RSST snapshots with self-destruct awareness.
    ///   - SelfDestruct column: TryAdd semantics (newer=empty→empty, newer=0x01→older if exists)
    ///   - Storage column: destructed addresses' older storage is discarded
    ///   - StorageNodes column: standard NestedStreamingMerge (orphaned nodes skipped during trie traversal)
    /// </summary>
    private static int MergeTwoPersisted(ReadOnlySpan<byte> olderData, ReadOnlySpan<byte> newerData, Span<byte> output)
    {
        Rsst.Rsst olderOuter = new(olderData);
        Rsst.Rsst newerOuter = new(newerData);

        // Pre-extract destructed addresses from newer self-destruct column
        byte[] sdTagKey = [PersistedSnapshot.SelfDestructTag];
        bool hasSdTag = newerOuter.TryGet(sdTagKey, out ReadOnlySpan<byte> newerSd);
        Debug.Assert(hasSdTag, $"Missing required tag 0x{PersistedSnapshot.SelfDestructTag:X2} in persisted snapshot");
        HashSet<byte[]> destructedAddresses = new(Bytes.EqualityComparer);
        Rsst.Rsst sdRsst = new(newerSd);
        using Rsst.Rsst.Enumerator sdEnum = sdRsst.GetEnumerator();
        while (sdEnum.MoveNext())
        {
            if (sdEnum.Current.Value.IsEmpty) // destructed
                destructedAddresses.Add(sdEnum.Current.Key.ToArray());
        }

        using RsstBuilder outerBuilder = new(output);
        ReadOnlySpan<byte> tags = [
            PersistedSnapshot.AccountTag,
            PersistedSnapshot.StorageTag,
            PersistedSnapshot.SelfDestructTag,
            PersistedSnapshot.StateNodeTag,
            PersistedSnapshot.StorageNodeTag
        ];

        byte[] tagKey = new byte[1];
        foreach (byte tag in tags)
        {
            tagKey[0] = tag;
            bool hasOlder = olderOuter.TryGet(tagKey, out ReadOnlySpan<byte> olderColumn);
            bool hasNewer = newerOuter.TryGet(tagKey, out ReadOnlySpan<byte> newerColumn);
            Debug.Assert(hasOlder && hasNewer, $"Missing required tag 0x{tag:X2} in persisted snapshot");

            int maxColumnSize = olderColumn.Length + newerColumn.Length + 1024;
            Span<byte> valueSpan = outerBuilder.BeginValueWrite(maxColumnSize);

            int columnLen = tag switch
            {
                PersistedSnapshot.StorageTag => NestedStreamingMergeWithSelfDestruct(olderColumn, newerColumn, valueSpan, destructedAddresses),
                PersistedSnapshot.SelfDestructTag => SelfDestructMerge(olderColumn, newerColumn, valueSpan),
                PersistedSnapshot.StorageNodeTag => NestedStreamingMerge(olderColumn, newerColumn, valueSpan),
                _ => RsstBuilder.StreamingMerge(olderColumn, newerColumn, valueSpan, 0),
            };

            outerBuilder.FinishValueWrite(columnLen, tagKey);
        }

        return outerBuilder.Build();
    }

    /// <summary>
    /// Merge self-destruct columns with TryAdd semantics:
    ///   - newer=empty (destructed) → always empty
    ///   - newer=0x01 (new account) → use older value if exists, else 0x01
    /// </summary>
    internal static int SelfDestructMerge(ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer, Span<byte> output)
    {
        Rsst.Rsst olderRsst = new(older);
        Rsst.Rsst newerRsst = new(newer);

        if (olderRsst.EntryCount == 0 && newerRsst.EntryCount == 0)
        {
            output[0] = 0x00;
            output[1] = 0x01;
            return 2;
        }

        using RsstBuilder builder = new(output);
        using Rsst.Rsst.Enumerator olderEnum = olderRsst.GetEnumerator();
        using Rsst.Rsst.Enumerator newerEnum = newerRsst.GetEnumerator();

        bool hasOlder = olderEnum.MoveNext();
        bool hasNewer = newerEnum.MoveNext();

        while (hasOlder && hasNewer)
        {
            ReadOnlySpan<byte> olderKey = olderEnum.Current.Key;
            ReadOnlySpan<byte> newerKey = newerEnum.Current.Key;

            int cmp = olderKey.SequenceCompareTo(newerKey);
            if (cmp < 0)
            {
                builder.Add(olderKey, olderEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
            }
            else if (cmp > 0)
            {
                builder.Add(newerKey, newerEnum.Current.Value);
                hasNewer = newerEnum.MoveNext();
            }
            else
            {
                // Keys match: newer=empty → empty, newer=0x01 → use older (TryAdd)
                builder.Add(newerKey, newerEnum.Current.Value.IsEmpty
                    ? ReadOnlySpan<byte>.Empty
                    : olderEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
                hasNewer = newerEnum.MoveNext();
            }
        }

        while (hasOlder)
        {
            builder.Add(olderEnum.Current.Key, olderEnum.Current.Value);
            hasOlder = olderEnum.MoveNext();
        }

        while (hasNewer)
        {
            builder.Add(newerEnum.Current.Key, newerEnum.Current.Value);
            hasNewer = newerEnum.MoveNext();
        }

        return builder.Build();
    }

    /// <summary>
    /// Like <see cref="NestedStreamingMerge"/> but skips older storage for destructed addresses.
    /// When address is destructed:
    ///   - Key in both: use newer only (don't merge inner RSSTs)
    ///   - Key only in older: skip entirely
    ///   - Key only in newer: include (new storage after self-destruct)
    /// </summary>
    internal static int NestedStreamingMergeWithSelfDestruct(
        ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer, Span<byte> output,
        HashSet<byte[]> destructedAddresses)
    {
        Rsst.Rsst olderRsst = new(older);
        Rsst.Rsst newerRsst = new(newer);

        if (olderRsst.EntryCount == 0 && newerRsst.EntryCount == 0)
        {
            output[0] = 0x00;
            output[1] = 0x01;
            return 2;
        }

        var lookup = destructedAddresses.GetAlternateLookup<ReadOnlySpan<byte>>();

        using RsstBuilder builder = new(output);
        using Rsst.Rsst.Enumerator olderEnum = olderRsst.GetEnumerator();
        using Rsst.Rsst.Enumerator newerEnum = newerRsst.GetEnumerator();

        bool hasOlder = olderEnum.MoveNext();
        bool hasNewer = newerEnum.MoveNext();

        while (hasOlder && hasNewer)
        {
            ReadOnlySpan<byte> olderKey = olderEnum.Current.Key;
            ReadOnlySpan<byte> newerKey = newerEnum.Current.Key;

            int cmp = olderKey.SequenceCompareTo(newerKey);
            if (cmp < 0)
            {
                // Only in older: skip if destructed
                if (!lookup.Contains(olderKey))
                    builder.Add(olderKey, olderEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
            }
            else if (cmp > 0)
            {
                builder.Add(newerKey, newerEnum.Current.Value);
                hasNewer = newerEnum.MoveNext();
            }
            else
            {
                if (lookup.Contains(newerKey))
                {
                    // Destructed: use newer only, don't merge inner RSSTs
                    builder.Add(newerKey, newerEnum.Current.Value);
                }
                else
                {
                    // Not destructed: merge inner RSSTs directly into output
                    int maxInner = olderEnum.Current.Value.Length + newerEnum.Current.Value.Length + 256;
                    Span<byte> innerSpan = builder.BeginValueWrite(maxInner);
                    int mergedLen = RsstBuilder.StreamingMerge(
                        olderEnum.Current.Value, newerEnum.Current.Value, innerSpan, 0);
                    builder.FinishValueWrite(mergedLen, newerKey);
                }
                hasOlder = olderEnum.MoveNext();
                hasNewer = newerEnum.MoveNext();
            }
        }

        while (hasOlder)
        {
            if (!lookup.Contains(olderEnum.Current.Key))
                builder.Add(olderEnum.Current.Key, olderEnum.Current.Value);
            hasOlder = olderEnum.MoveNext();
        }

        while (hasNewer)
        {
            builder.Add(newerEnum.Current.Key, newerEnum.Current.Value);
            hasNewer = newerEnum.MoveNext();
        }

        return builder.Build();
    }

    /// <summary>
    /// Merge two address-grouped RSSTs where values are inner RSSTs.
    /// For matching address keys, the inner RSSTs are merged via StreamingMerge.
    /// For non-matching keys, inner RSSTs are copied as-is.
    /// </summary>
    internal static int NestedStreamingMerge(ReadOnlySpan<byte> older, ReadOnlySpan<byte> newer, Span<byte> output)
    {
        Rsst.Rsst olderRsst = new(older);
        Rsst.Rsst newerRsst = new(newer);

        if (olderRsst.EntryCount == 0 && newerRsst.EntryCount == 0)
        {
            output[0] = 0x00;
            output[1] = 0x01;
            return 2;
        }

        using RsstBuilder builder = new(output);
        using Rsst.Rsst.Enumerator olderEnum = olderRsst.GetEnumerator();
        using Rsst.Rsst.Enumerator newerEnum = newerRsst.GetEnumerator();

        bool hasOlder = olderEnum.MoveNext();
        bool hasNewer = newerEnum.MoveNext();

        while (hasOlder && hasNewer)
        {
            ReadOnlySpan<byte> olderKey = olderEnum.Current.Key;
            ReadOnlySpan<byte> newerKey = newerEnum.Current.Key;

            int cmp = olderKey.SequenceCompareTo(newerKey);
            if (cmp < 0)
            {
                builder.Add(olderKey, olderEnum.Current.Value);
                hasOlder = olderEnum.MoveNext();
            }
            else if (cmp > 0)
            {
                builder.Add(newerKey, newerEnum.Current.Value);
                hasNewer = newerEnum.MoveNext();
            }
            else
            {
                // Matching address key: merge the inner RSSTs directly into output
                int maxInner = olderEnum.Current.Value.Length + newerEnum.Current.Value.Length + 256;
                Span<byte> innerSpan = builder.BeginValueWrite(maxInner);
                int mergedLen = RsstBuilder.StreamingMerge(
                    olderEnum.Current.Value, newerEnum.Current.Value, innerSpan, 0);
                builder.FinishValueWrite(mergedLen, newerKey);
                hasOlder = olderEnum.MoveNext();
                hasNewer = newerEnum.MoveNext();
            }
        }

        while (hasOlder)
        {
            builder.Add(olderEnum.Current.Key, olderEnum.Current.Value);
            hasOlder = olderEnum.MoveNext();
        }

        while (hasNewer)
        {
            builder.Add(newerEnum.Current.Key, newerEnum.Current.Value);
            hasNewer = newerEnum.MoveNext();
        }

        return builder.Build();
    }
}
