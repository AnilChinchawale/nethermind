// SPDX-FileCopyrightText: 2026 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

// NOTE: InitializeBlockchainXdc requires reference to Nethermind.Init which creates
// a circular dependency. This file is temporarily stubbed out. The TxPool customization
// (XdcTxGossipPolicy, SignTransactionFilter) should be wired via the plugin system
// (XdcModule/XdcPlugin) instead.

// The original code extended InitializeBlockchain from Nethermind.Init.Steps,
// overriding CreateTxPool to add XDC-specific transaction filtering.
// TODO: Wire XDC TxPool customization via XdcModule's Autofac registrations instead.
