<div align="center">
    <pre style="background: none;">
 █████ ██████   █████   █████████  ███████████   ██████████  █████████   █████████ 
░░███ ░░██████ ░░███   ███░░░░░███░░███░░░░░███ ░░███░░░░░█ ███░░░░░███ ███░░░░░███
 ░███  ░███░███ ░███  ███     ░░░  ░███    ░███  ░███  █ ░ ░███    ░░░ ░███    ░░░ 
 ░███  ░███░░███░███ ░███          ░██████████   ░██████   ░░█████████ ░░█████████ 
 ░███  ░███ ░░██████ ░███    █████ ░███░░░░░███  ░███░░█    ░░░░░░░░███ ░░░░░░░░███
 ░███  ░███  ░░█████ ░░███  ░░███  ░███    ░███  ░███ ░   █ ███    ░███ ███    ░███
 █████ █████  ░░█████ ░░█████████  █████   █████ ██████████░░█████████ ░░█████████ 
░░░░░ ░░░░░    ░░░░░   ░░░░░░░░░  ░░░░░   ░░░░░ ░░░░░░░░░░  ░░░░░░░░░   ░░░░░░░░░  
    </pre>
</div>

<div align="center">
    <h3>Ingress - Multi chain light RPC</h3>
</div>

`ingress` is a Go library for JSON-RPC connectivity to EVM-compatible chains and Solana. It provides thin clients, HTTP/WebSocket connection pooling, shared transport primitives, and typed response DTOs while keeping decoding decisions at the application boundary.

## Design

- **Thin RPC clients**: most RPC methods return raw result bytes so callers can choose their own decoding strategy.
- **HTTP and WebSocket transports**: endpoint URLs are split into transport-specific connection pools and exposed through `HTTP()` and `WS()` clients.
- **Shared connection layer**: EVM and Solana clients use the top-level `transport` package for connection managers, request ID sequencing, timeouts, and keep-alive mechanics.
- **Typed DTO packages**: `evm/model` and `solana/model` describe response shapes. Solana models are DTO-only; application code unmarshals into them.
- **Subscription support**: EVM supports `eth_subscribe`; Solana supports the standard WebSocket subscription methods.

## Package Layout

```text
github.com/0x626f/ingress
├── evm/
│   ├── connection.go          # Legacy EVM transport API kept for compatibility
│   ├── sequence_generator.go  # Legacy EVM request ID sequencer
│   ├── model/
│   │   └── core.go            # EVM response structs and small decode helpers
│   └── rpc/
│       ├── core.go            # EVM CoreClient interface and query DTOs
│       ├── raw.go             # RawClient and ClientConfig
│       ├── spec.go            # EVM JSON-RPC request/response spec helpers
│       ├── thin.go            # ThinClient implementation
│       └── utils.go           # Raw parsing and EVM hex quantity helpers
├── solana/
│   ├── model/
│   │   └── core.go            # Solana response DTOs only
│   ├── rpc/
│   │   ├── core.go            # Solana CoreClient interface
│   │   ├── http.go            # HTTP RPC methods
│   │   ├── raw.go             # RawClient and context-aware construction
│   │   ├── thin.go            # Shared Solana ThinClient code
│   │   ├── utils.go           # Raw parsing and base58 helpers
│   │   ├── vars.go            # Commitment constants and RPC method names
│   │   └── ws.go              # WebSocket subscriptions
│   └── types/
│       ├── base.go            # Compatibility/base aliases
│       └── dto.go             # Existing compatibility DTOs
├── jsonrpc/
│   └── spec.go                # Shared JSON-RPC 2.0 helpers
└── transport/
    ├── connection.go          # Shared HTTP/WS transport and connection manager
    └── sequence_generator.go  # Shared JSON-RPC request ID sequencing
```

## Installation

```bash
go get github.com/0x626f/ingress
```

Requires Go 1.24+. Dependencies are intentionally small: `gobwas/ws` for WebSocket transport and `github.com/mr-tron/base58` for Solana base58 utilities.

## EVM Quick Start

```go
package main

import (
	"fmt"
	"time"

	"github.com/0x626f/ingress/evm/model"
	"github.com/0x626f/ingress/evm/rpc"
)

func main() {
	raw, err := rpc.NewRawClient(&rpc.ClientConfig{
		Resources: []string{
			"https://eth-mainnet.example.com",
			"wss://eth-mainnet.example.com/ws",
		},
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	http := raw.HTTP()

	chainID, err := http.ChainId()
	if err != nil {
		panic(err)
	}
	fmt.Println("chain:", model.DecodeHex(chainID))

	blockRaw, err := http.GetBlockByNumber(rpc.BlockQuery{FullTransactions: true})
	if err != nil {
		panic(err)
	}
	block, err := model.DecodeBlock(blockRaw)
	if err != nil {
		panic(err)
	}
	txs, err := block.TxObjects()
	if err != nil {
		panic(err)
	}
	fmt.Println("transactions:", len(txs))
}
```

## Solana Quick Start

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/0x626f/ingress/solana/model"
	"github.com/0x626f/ingress/solana/rpc"
)

func main() {
	raw, err := rpc.NewRawClient(&rpc.ClientConfig{
		Resources: []string{
			"https://api.mainnet-beta.solana.com",
			"wss://api.mainnet-beta.solana.com",
		},
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	http := raw.HTTP()

	epochRaw, err := http.GetEpochInfo(rpc.FinalizedCommitment)
	if err != nil {
		panic(err)
	}

	var epoch model.EpochInfo
	if err := json.Unmarshal(epochRaw, &epoch); err != nil {
		panic(err)
	}
	fmt.Println("epoch:", epoch.Epoch)

	blockhashRaw, err := http.GetLatestBlockhash(rpc.FinalizedCommitment)
	if err != nil {
		panic(err)
	}

	var blockhash model.LatestBlockhash
	if err := json.Unmarshal(blockhashRaw, &blockhash); err != nil {
		panic(err)
	}
	fmt.Println("blockhash:", blockhash.Value.Blockhash)
}
```

## Configuration

Both EVM and Solana RPC packages expose a `ClientConfig` with the same core fields:

```go
rpc.NewRawClient(&rpc.ClientConfig{
	// Endpoint URLs. Mix http/https and ws/wss; each is routed to the right pool.
	Resources: []string{"https://...", "wss://..."},

	// Return an error for invalid or unsupported resource URLs.
	ErrorOnInvalidResource: true,

	// Per-request deadline. Zero means no deadline.
	RequestTimeout: 5 * time.Second,

	// WebSocket keep-alive interval. Zero uses the default behavior.
	KeepAlivePeriod: 30 * time.Second,

	// Buffer depth for subscription event channels.
	SubscriptionStreamSize: 64,
})
```

Solana also provides `NewRawClientWithContext(ctx, config)` so subscriptions can be tied to an application context.

## EVM RPC Surface

`evm/rpc.CoreClient` covers common Ethereum JSON-RPC methods:

| Go method | JSON-RPC method |
| --- | --- |
| `ChainId()` | `eth_chainId` |
| `BlockNumber()` | `eth_blockNumber` |
| `GetBalance(BalanceQuery)` | `eth_getBalance` |
| `GetCode(CodeQuery)` | `eth_getCode` |
| `GetStorageAt(GetStorageQuery)` | `eth_getStorageAt` |
| `Call(CallQuery)` | `eth_call` |
| `EstimateGas(EstimateGasQuery)` | `eth_estimateGas` |
| `SendRawTransaction(TransactionQuery)` | `eth_sendRawTransaction` |
| `GetTransactionByHash(TransactionQuery)` | `eth_getTransactionByHash` |
| `GetTransactionReceipt(TransactionQuery)` | `eth_getTransactionReceipt` |
| `GetTransactionCount(AddressedQuery)` | `eth_getTransactionCount` |
| `GetBlockByNumber(BlockQuery)` | `eth_getBlockByNumber` |
| `GetBlockByHash(BlockQuery)` | `eth_getBlockByHash` |
| `GetLogs(LogsQuery)` | `eth_getLogs` |
| `Subscribe(SubscribeQuery)` | `eth_subscribe` |
| `UnSubscribe(UnSubscribeQuery)` | `eth_unsubscribe` |

Most EVM methods return `([]byte, error)`. Subscription calls use `evm.Subscription` IDs and raw message channels.

## Solana RPC Surface

`solana/rpc.CoreClient` includes account, token, block, epoch, validator, inflation, transaction, and subscription methods. Most methods return `types.RawResult`, which can be unmarshaled into `solana/model` DTOs or application-specific structs.

Examples of covered RPC methods:

```text
getAccountInfo, getBalance, getMultipleAccounts, getProgramAccounts
getTokenAccountBalance, getTokenAccountsByOwner, getTokenSupply
getLatestBlockhash, getSignaturesForAddress, getSignatureStatuses
getTransaction, getTransactionCount, simulateTransaction, sendTransaction
getBlock, getBlocks, getBlockHeight, getBlockProduction, getBlockTime
getEpochInfo, getEpochSchedule, getLeaderSchedule, getSlot, getSlotLeaders
getClusterNodes, getVoteAccounts, getInflationRate, getSupply
accountSubscribe, blockSubscribe, logsSubscribe, programSubscribe
rootSubscribe, signatureSubscribe, slotSubscribe, slotsUpdatesSubscribe, voteSubscribe
```

Some Solana convenience methods return parsed values directly:

- `GetSlot(commitment)` returns `types.Slot`.
- `GetSlotLeaders(from, limit)` returns `types.SlotLeaders`.
- `GetConfirmedSlots(from, to, commitment)` returns `types.ConfirmedSlots`.
- `SimulateTransaction(serialized, commitment)` returns only an error.
- `SubscribeSlot()` returns a channel of slot events.

## Models and Decoding

EVM keeps small model-side helpers because the EVM client strips quoted scalar values from JSON-RPC results:

```go
hex := model.DecodeHex(raw)
block, err := model.DecodeBlock(raw)
tx, err := model.DecodeTransaction(raw)
receipt, err := model.DecodeReceipt(raw)
logs, err := model.DecodeLogs(raw)
```

Solana `model` is DTO-only. Decode in application code:

```go
var balance model.Balance
err := json.Unmarshal(raw, &balance)
```

Use `json.RawMessage` fields when Solana response shape depends on encoding, transaction version, or parsed instruction configuration.

## Utility Helpers

Both RPC packages include small developer utilities for application-side parsing:

```go
value, err := rpc.ParseRawValue[model.Balance](raw)
number, err := rpc.ParseRawNumber[uint64](raw)
text, err := rpc.ParseRawString(raw)
ok, err := rpc.ParseRawBool(raw)
```

EVM also exposes hex quantity helpers for request values:

```go
gas := rpc.ToHexQuantity(21000)
value := rpc.DecimalStringToHexQuantity("1000000000000000000")
```

Solana exposes base58 helpers backed by `github.com/mr-tron/base58`:

```go
encoded := rpc.EncodeBase58(pubkeyBytes)
decoded, err := rpc.DecodeBase58(encoded)
tuple := rpc.NewBase58EncodedValue(pubkeyBytes) // ["...", "base58"]
```

## WebSocket Subscriptions

EVM:

```go
ws := raw.WS()

id, stream, err := ws.Subscribe(rpc.SubscribeQuery{On: rpc.SubscriptionNewHeads})
if err != nil {
	panic(err)
}
defer ws.UnSubscribe(rpc.UnSubscribeQuery{Subscription: id})

for msg := range stream {
	fmt.Println(string(msg))
}
```

Solana:

```go
ws := raw.WS()

sub, err := ws.LogsSubscribe("all")
if err != nil {
	panic(err)
}
defer sub.Unsubscribe()

for event := range sub.Events {
	if event.Error != nil {
		panic(event.Error)
	}
	fmt.Println(string(event.Data))
}
```

## Development

```bash
go build ./...
go fmt ./...
go vet ./...
```

Focused package checks:

```bash
go test ./evm ./evm/rpc ./evm/model ./jsonrpc
go test ./solana/model ./solana/rpc ./solana/types
```

Solana RPC tests use public live endpoints by default:

```bash
SOLANA_HTTP_URL=https://api.mainnet-beta.solana.com \
SOLANA_WS_URL=wss://api.mainnet-beta.solana.com \
SOLANA_RPC_TEST_DELAY=350ms \
go test ./solana/rpc
```

The Solana defaults are the same as the URLs above. Increase `SOLANA_RPC_TEST_DELAY` when a public endpoint rate-limits the test run. Expensive public calls that still return `429` are skipped.

Some EVM integration tests use live public endpoints and may fail when upstream providers return empty data or are unavailable. Prefer focused package checks for routine development, and use provider-specific endpoints for full integration runs.

## Status

- EVM HTTP and WebSocket JSON-RPC client: implemented.
- EVM model DTOs and helper decoders: implemented.
- Solana HTTP JSON-RPC client: implemented.
- Solana WebSocket subscriptions: implemented.
- Solana model DTOs: implemented.
- Shared top-level transport package: implemented.
- Solana base58 utilities: implemented.
- Well-known address registry, ABI helpers, and Multicall support: not currently present in this repository.
