// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Logging;

namespace Nethermind.Synchronization.Test.FastSync;

public interface IStateSyncTestOperation
{
    Hash256 RootHash { get; set; }
    void UpdateRootHash();
    void Set(Hash256 address, Account? account);
    void Commit();
    void AssertFlushed();
    void CompareTrees(RemoteDbContext remote, ILogger logger, string stage, bool skipLogs = false);
    void DeleteStateRoot();
}
