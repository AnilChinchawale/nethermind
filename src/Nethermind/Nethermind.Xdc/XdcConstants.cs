// SPDX-FileCopyrightText: 2026 Anil Chinchawale
// SPDX-License-Identifier: LGPL-3.0-only

using Nethermind.Core;

namespace Nethermind.Xdc;

/// <summary>
/// XDC Network constants
/// </summary>
public static class XdcConstants
{
    /// <summary>
    /// BlockSigners contract address (0x89) - requires special transaction handling
    /// Transactions to this address bypass normal EVM execution
    /// </summary>
    public static readonly Address BlockSignersAddress =
        new("0x0000000000000000000000000000000000000089");

    /// <summary>
    /// MasternodeVoting/Validator contract address (0x88)
    /// Transactions to this address may bypass normal balance validation
    /// in XDPoS consensus for masternode staking operations.
    /// </summary>
    public static readonly Address ValidatorAddress =
        new("0x0000000000000000000000000000000000000088");

    /// <summary>
    /// Randomize contract address (0x90)
    /// Used for XDPoS randomization - may bypass balance validation.
    /// </summary>
    public static readonly Address RandomizeAddress =
        new("0x0000000000000000000000000000000000000090");

    /// <summary>
    /// Foundation wallet address (with intentional typo matching geth-xdc)
    /// </summary>
    public static readonly Address FoundationWalletAddress =
        new("0x92a289fe95a85c53b8d0d113cbaef0c1ec98ac65");

    /// <summary>
    /// XDPoS consensus version byte (stored in ExtraData[0])
    /// </summary>
    public const byte ConsensusVersion = 2;

    /// <summary>
    /// Default difficulty for XDPoS blocks
    /// </summary>
    public const ulong DifficultyDefault = 1;

    /// <summary>
    /// Cache size for in-memory epochs
    /// </summary>
    public const int InMemoryEpochs = 21;

    /// <summary>
    /// Cache size for in-memory round-to-epoch mappings
    /// </summary>
    public const int InMemoryRound2Epochs = 500;

    /// <summary>
    /// Target gas limit for XDC blocks
    /// </summary>
    public const long TargetGasLimit = 420000000;

    /// <summary>
    /// Number of bytes reserved for validator vanity in block extra data
    /// </summary>
    public const int ExtraVanity = 32;

    /// <summary>
    /// Number of bytes reserved for ECDSA seal signature in block extra data
    /// </summary>
    public const int ExtraSeal = 65;

    /// <summary>
    /// Nonce value indicating an "auth vote" (validator wants to join) = 0xffffffffffffffff
    /// </summary>
    public const ulong NonceAuthVoteValue = ulong.MaxValue;

    /// <summary>
    /// Nonce value indicating a "drop vote" (validator wants to leave) = 0x0000000000000000
    /// </summary>
    public const ulong NonceDropVoteValue = 0;

    /// <summary>
    /// RocksDB key name for XDPoS snapshot persistence.
    /// Registered as a keyed IDb via ContainerBuilderExtensions.AddDatabase.
    /// </summary>
    public const string SnapshotDbName = "xdcSnapshot";

    /// <summary>
    /// XDC Mainnet chain ID
    /// </summary>
    public const int MainnetChainId = 50;

    /// <summary>
    /// XDC Apothem Testnet chain ID
    /// </summary>
    public const int ApothemChainId = 51;
}
