// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Buffers;
using Nethermind.State.Flat.Rsst;

namespace Nethermind.State.Flat.Test;

internal static class RsstTestUtil
{
    public delegate void BuildAction(ref RsstBuilder builder);

    /// <summary>
    /// Helper for tests: Create builder, execute action, dispose and return result.
    /// </summary>
    public static byte[] BuildToArray(BuildAction buildAction)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);  // Larger buffer for tests
        try
        {
            Span<byte> bufferSpan = buffer.AsSpan();
            RsstBuilder builder = new(bufferSpan);
            try
            {
                buildAction(ref builder);
                int len = builder.Build();
                byte[] result = new byte[len];
                bufferSpan.Slice(0, len).CopyTo(result);
                return result;
            }
            finally
            {
                builder.Dispose();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
