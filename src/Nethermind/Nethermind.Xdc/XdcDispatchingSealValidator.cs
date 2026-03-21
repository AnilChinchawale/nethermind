// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using Nethermind.Consensus;
using Nethermind.Core;

namespace Nethermind.Xdc;

/// <summary>
/// Routes block-seal validation to the correct XDPoS version:
///   • header.Number &lt;  SwitchBlock  →  XDPoS V1  (<see cref="XdcV1SealValidator"/>)
///   • header.Number &gt;= SwitchBlock  →  XDPoS V2  (<see cref="XdcSealValidator"/>)
///
/// When <c>skipV1Validation</c> is <c>true</c> (typically for fast-sync or when historical
/// V1 blocks have already been verified) the V2 validator handles all blocks.
/// </summary>
internal class XdcDispatchingSealValidator : ISealValidator
{
    private readonly ISealValidator _v1Validator;
    private readonly ISealValidator _v2Validator;
    private readonly long _switchBlock;
    private readonly bool _skipV1Validation;

    public XdcDispatchingSealValidator(
        XdcV1SealValidator v1Validator,
        XdcSealValidator v2Validator,
        long switchBlock,
        bool skipV1Validation)
    {
        _v1Validator = v1Validator;
        _v2Validator = v2Validator;
        _switchBlock = switchBlock;
        _skipV1Validation = skipV1Validation;
    }

    private ISealValidator Select(BlockHeader header)
    {
        if (!_skipV1Validation && header.Number < _switchBlock)
        {
            Console.WriteLine($"[XDC-SEAL] Block {header.Number} → V1 (switchBlock={_switchBlock})");
            return _v1Validator;
        }
        Console.WriteLine($"[XDC-SEAL] Block {header.Number} → V2 (switchBlock={_switchBlock}, skipV1={_skipV1Validation})");
        return _v2Validator;
    }

    public bool ValidateParams(BlockHeader parent, BlockHeader header, bool isUncle = false)
        => Select(header).ValidateParams(parent, header, isUncle);

    public bool ValidateSeal(BlockHeader header, bool force)
        => Select(header).ValidateSeal(header, force);
}
