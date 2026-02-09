// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Linq;
using Autofac.Features.AttributeFilters;
using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.State.Flat;

namespace Nethermind.Synchronization.Test.SnapSync;

public interface ISnapTestHelper
{
    int CountTrieNodes();
    bool TrieNodeKeyExists(Hash256 hash);
    long TrieNodeWritesCount { get; }
}

public class PatriciaSnapTestHelper([KeyFilter(DbNames.State)] IDb stateDb) : ISnapTestHelper
{
    public int CountTrieNodes() => stateDb.GetAllKeys().Count();
    public bool TrieNodeKeyExists(Hash256 hash) => stateDb.KeyExists(hash.Bytes);
    public long TrieNodeWritesCount => ((MemDb)stateDb).WritesCount;
}

public class FlatSnapTestHelper(IColumnsDb<FlatDbColumns> columnsDb) : ISnapTestHelper
{
    private static readonly FlatDbColumns[] TrieNodeColumns =
        [FlatDbColumns.StateTopNodes, FlatDbColumns.StateNodes, FlatDbColumns.StorageNodes, FlatDbColumns.FallbackNodes];

    public int CountTrieNodes()
    {
        int total = 0;
        foreach (var col in TrieNodeColumns)
            total += columnsDb.GetColumnDb(col).GetAllKeys().Count();
        return total;
    }

    public bool TrieNodeKeyExists(Hash256 hash) =>
        columnsDb.GetColumnDb(FlatDbColumns.StateTopNodes).KeyExists(new byte[3]);

    public long TrieNodeWritesCount
    {
        get
        {
            long total = 0;
            foreach (var col in TrieNodeColumns)
                total += ((MemDb)columnsDb.GetColumnDb(col)).WritesCount;
            return total;
        }
    }
}
