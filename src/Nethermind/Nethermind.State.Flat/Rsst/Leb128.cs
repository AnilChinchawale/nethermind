// SPDX-FileCopyrightText: 2025 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System.Runtime.CompilerServices;

namespace Nethermind.State.Flat.Rsst;

/// <summary>
/// LEB128 variable-length integer encoding/decoding with forward and backward reading.
/// </summary>
public static class Leb128
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Read(ReadOnlySpan<byte> data, ref int offset)
    {
        int result = 0;
        int shift = 0;
        byte b;
        do
        {
            b = data[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        }
        while ((b & 0x80) != 0);

        return result;
    }

    /// <summary>
    /// Read LEB128 backwards from the given offset (exclusive end position).
    /// The offset is decremented to point before the encoded value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadBackward(ReadOnlySpan<byte> data, ref int offset)
    {
        // Find the start of the LEB128 sequence by scanning backwards.
        // The last byte of a LEB128 sequence has bit 7 clear.
        // All preceding bytes have bit 7 set.
        int end = offset;
        offset--;

        // The byte at offset is the last byte (bit 7 clear). Scan back for continuation bytes.
        while (offset > 0 && (data[offset - 1] & 0x80) != 0)
        {
            offset--;
        }

        int start = offset;
        int readPos = start;
        return Read(data, ref readPos);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Write(Span<byte> data, int offset, int value)
    {
        uint v = (uint)value;
        while (v >= 0x80)
        {
            data[offset++] = (byte)(v | 0x80);
            v >>= 7;
        }
        data[offset++] = (byte)v;
        return offset;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int EncodedSize(int value)
    {
        uint v = (uint)value;
        int size = 0;
        do
        {
            size++;
            v >>= 7;
        }
        while (v != 0);
        return size;
    }
}
