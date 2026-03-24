// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;

namespace Nethermind.Xdc;

/// <summary>
/// Sorting utilities for XDC consensus
/// </summary>
internal static class XdcSort
{
    /// <summary>
    /// Sorts a list in-place using a comparison function.
    /// The comparison returns true if x should come before y (i.e., x >= y for descending).
    /// </summary>
    public static void Slice<T>(IList<T> list, Func<T, T, bool> compare)
    {
        // Simple insertion sort (stable, sufficient for small masternode sets)
        for (int i = 1; i < list.Count; i++)
        {
            T key = list[i];
            int j = i - 1;
            while (j >= 0 && !compare(list[j], key))
            {
                list[j + 1] = list[j];
                j--;
            }
            list[j + 1] = key;
        }
    }
}
