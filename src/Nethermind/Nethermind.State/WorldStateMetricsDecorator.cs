// SPDX-FileCopyrightText: 2024 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Diagnostics;
using Nethermind.Core.Specs;
using Nethermind.Evm.State;
using Nethermind.Evm.Tracing.State;

namespace Nethermind.State;

public class WorldStateMetricsDecorator(IWorldState innerWorldState) : WrappedWorldState(innerWorldState)
{
<<<<<<< HEAD
    public void Restore(Snapshot snapshot) => innerState.Restore(snapshot);

    public bool TryGetAccount(Address address, out AccountStruct account) => innerState.TryGetAccount(address, out account);

    public byte[] GetOriginal(in StorageCell storageCell) => innerState.GetOriginal(in storageCell);

    public ReadOnlySpan<byte> Get(in StorageCell storageCell) => innerState.Get(in storageCell);

    public void Set(in StorageCell storageCell, byte[] newValue) => innerState.Set(in storageCell, newValue);

    public ReadOnlySpan<byte> GetTransientState(in StorageCell storageCell) => innerState.GetTransientState(in storageCell);

    public void SetTransientState(in StorageCell storageCell, byte[] newValue) => innerState.SetTransientState(in storageCell, newValue);

    public void Reset(bool resetBlockChanges = true)
    {
        StateMerkleizationTime = 0d;
        innerState.Reset(resetBlockChanges);
    }

    public Snapshot TakeSnapshot(bool newTransactionStart = false) => innerState.TakeSnapshot(newTransactionStart);

    public void WarmUp(AccessList? accessList) => innerState.WarmUp(accessList);

    public void WarmUp(Address address) => innerState.WarmUp(address);

    public void ClearStorage(Address address) => innerState.ClearStorage(address);

    public void RecalculateStateRoot()
    {
        long start = Stopwatch.GetTimestamp();
        innerState.RecalculateStateRoot();
        StateMerkleizationTime += Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }

    public Hash256 StateRoot
    {
        get => innerState.StateRoot;
        set
        {
            if (innerState is WorldState ws)
                ws.StateRoot = value;
        }
    }

=======
>>>>>>> upstream/master
    public double StateMerkleizationTime { get; private set; }

    public override void Reset(bool resetBlockChanges = true)
    {
        StateMerkleizationTime = 0d;
        _innerWorldState.Reset(resetBlockChanges);
    }

    public override void RecalculateStateRoot()
    {
        long start = Stopwatch.GetTimestamp();
        _innerWorldState.RecalculateStateRoot();
        StateMerkleizationTime += Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }

    public override void Commit(IReleaseSpec releaseSpec, IWorldStateTracer tracer, bool isGenesis = false, bool commitRoots = true)
    {
        long start = Stopwatch.GetTimestamp();
        _innerWorldState.Commit(releaseSpec, tracer, isGenesis, commitRoots);
        if (commitRoots)
            StateMerkleizationTime += Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }

    public override void CommitTree(long blockNumber)
    {
        long start = Stopwatch.GetTimestamp();
        _innerWorldState.CommitTree(blockNumber);
        StateMerkleizationTime += Stopwatch.GetElapsedTime(start).TotalMilliseconds;
    }
}
