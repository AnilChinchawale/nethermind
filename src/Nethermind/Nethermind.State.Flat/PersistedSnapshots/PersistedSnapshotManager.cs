// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

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

        byte[] mergedData = MergeSnapshots(snapshots);

        StateId from = snapshots[0].From;
        StateId to = snapshots[snapshots.Count - 1].To;
        persistedSnapshotRepository.AddCompactedSnapshot(from, to, mergedData);

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
        byte[] merged = snapshots[0].Data.ToArray();
        for (int i = 1; i < snapshots.Count; i++)
        {
            merged = MergeTwoPersisted(merged, snapshots[i].Data);
        }
        return merged;
    }

    /// <summary>
    /// Merge two columnar RSST snapshots with self-destruct awareness.
    ///   - SelfDestruct column: TryAdd semantics (newer=empty→empty, newer=0x01→older if exists)
    ///   - Storage column: destructed addresses' older storage is discarded
    ///   - StorageNodes column: standard NestedStreamingMerge (orphaned nodes skipped during trie traversal)
    /// </summary>
    private static byte[] MergeTwoPersisted(ReadOnlyMemory<byte> olderData, ReadOnlyMemory<byte> newerData)
    {
        ReadOnlySpan<byte> olderSpan = olderData.Span;
        ReadOnlySpan<byte> newerSpan = newerData.Span;

        Rsst.Rsst olderOuter = new(olderSpan);
        Rsst.Rsst newerOuter = new(newerSpan);

        // Pre-extract destructed addresses from newer self-destruct column
        byte[] sdTagKey = [PersistedSnapshot.SelfDestructTag];
        HashSet<byte[]> destructedAddresses = new(Bytes.EqualityComparer);
        if (newerOuter.TryGet(sdTagKey, out ReadOnlySpan<byte> newerSd))
        {
            Rsst.Rsst sdRsst = new(newerSd);
            using Rsst.Rsst.Enumerator sdEnum = sdRsst.GetEnumerator();
            while (sdEnum.MoveNext())
            {
                if (sdEnum.Current.Value.IsEmpty) // destructed
                    destructedAddresses.Add(sdEnum.Current.Key.ToArray());
            }
        }

        byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(olderData.Length + newerData.Length + 4096);
        try
        {
            using RsstBuilder outerBuilder = new(buffer);
            ReadOnlySpan<byte> tags = [
                PersistedSnapshot.AccountTag,
                PersistedSnapshot.StorageTag,
                PersistedSnapshot.SelfDestructTag,
                PersistedSnapshot.StateNodeTag,
                PersistedSnapshot.StorageNodeTag
            ];

            byte[] columnBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(Math.Max(olderData.Length, newerData.Length));
            byte[] tagKey = new byte[1];
            try
            {
                foreach (byte tag in tags)
                {
                    tagKey[0] = tag;
                    bool hasOlder = olderOuter.TryGet(tagKey, out ReadOnlySpan<byte> olderColumn);
                    bool hasNewer = newerOuter.TryGet(tagKey, out ReadOnlySpan<byte> newerColumn);

                    int maxColumnSize = Math.Max(olderColumn.Length, newerColumn.Length) + 1024;
                    Span<byte> valueSpan = outerBuilder.BeginValueWrite(maxColumnSize);
                    int columnLen;

                    if (hasOlder && hasNewer)
                    {
                        columnLen = tag switch
                        {
                            PersistedSnapshot.StorageTag => NestedStreamingMergeWithSelfDestruct(olderColumn, newerColumn, columnBuffer, destructedAddresses),
                            PersistedSnapshot.SelfDestructTag => SelfDestructMerge(olderColumn, newerColumn, columnBuffer),
                            PersistedSnapshot.StorageNodeTag => NestedStreamingMerge(olderColumn, newerColumn, columnBuffer),
                            _ => RsstBuilder.StreamingMerge(olderColumn, newerColumn, columnBuffer, 0),
                        };
                        columnBuffer.AsSpan(0, columnLen).CopyTo(valueSpan);
                    }
                    else if (hasNewer)
                    {
                        columnLen = newerColumn.Length;
                        newerColumn.CopyTo(valueSpan);
                    }
                    else if (hasOlder)
                    {
                        if (tag == PersistedSnapshot.StorageTag && destructedAddresses.Count > 0)
                        {
                            columnLen = FilterDestructedFromNested(olderColumn, columnBuffer, destructedAddresses);
                            columnBuffer.AsSpan(0, columnLen).CopyTo(valueSpan);
                        }
                        else
                        {
                            columnLen = olderColumn.Length;
                            olderColumn.CopyTo(valueSpan);
                        }
                    }
                    else
                    {
                        valueSpan[0] = 0x00;
                        valueSpan[1] = 0x01;
                        columnLen = 2;
                    }

                    outerBuilder.FinishValueWrite(columnLen, tagKey);
                }
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(columnBuffer);
            }

            int endPos = outerBuilder.Build();
            return buffer.AsSpan(0, endPos).ToArray();
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
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

        byte[] innerMergeBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(older.Length + newer.Length);
        try
        {
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
                        // Not destructed: merge inner RSSTs
                        int mergedLen = RsstBuilder.StreamingMerge(
                            olderEnum.Current.Value, newerEnum.Current.Value, innerMergeBuffer, 0);
                        builder.Add(newerKey, innerMergeBuffer.AsSpan(0, mergedLen));
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
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(innerMergeBuffer);
        }
    }

    /// <summary>
    /// Filter out destructed addresses from a nested RSST (for the older-only storage case).
    /// </summary>
    private static int FilterDestructedFromNested(ReadOnlySpan<byte> data, Span<byte> output, HashSet<byte[]> destructedAddresses)
    {
        Rsst.Rsst rsst = new(data);
        if (rsst.EntryCount == 0)
        {
            output[0] = 0x00;
            output[1] = 0x01;
            return 2;
        }

        var lookup = destructedAddresses.GetAlternateLookup<ReadOnlySpan<byte>>();
        using RsstBuilder builder = new(output);
        using Rsst.Rsst.Enumerator enumerator = rsst.GetEnumerator();
        while (enumerator.MoveNext())
        {
            if (!lookup.Contains(enumerator.Current.Key))
                builder.Add(enumerator.Current.Key, enumerator.Current.Value);
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

        byte[] innerMergeBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(older.Length + newer.Length);
        try
        {
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
                    // Matching address key: merge the inner RSSTs
                    int mergedLen = RsstBuilder.StreamingMerge(
                        olderEnum.Current.Value, newerEnum.Current.Value, innerMergeBuffer, 0);
                    builder.Add(newerKey, innerMergeBuffer.AsSpan(0, mergedLen));
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
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(innerMergeBuffer);
        }
    }
}
