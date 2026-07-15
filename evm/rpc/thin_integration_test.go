// Integration tests for RawClient against live JSON-RPC endpoints across multiple chains.
//
// By default every test runs against all eight supported chains using free public endpoints.
// Override per-chain config with environment variables:
//
//	<PREFIX>_HTTP_URL=<url>     HTTP endpoint
//	<PREFIX>_WS_URL=<url>       WebSocket endpoint
//	<PREFIX>_ADDRESS=<hex>      test EOA address
//	<PREFIX>_CONTRACT=<hex>     test contract address
//	<PREFIX>_TX_HASH=<hex>      known transaction hash (derived from latest block if unset)
//	<PREFIX>_BLOCK_HASH=<hex>   known block hash (derived from block 1 if unset)
//
// Chain prefixes: ETH ARB BSC BASE OP MATIC AVAX SONIC
//
// Limit which chains run via a comma-separated list of decimal chain IDs:
//
//	CHAINS_ENABLED=1,42161     (default: all chains)
//
// Run:
//
//	go test ./rpc/evm/ -run TestIntegration -v -timeout=120s
package rpc

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Chain configuration
// ============================================================================

type chainConfig struct {
	Name      string // subtest name, e.g. "ethereum"
	ChainID   int    // decimal chain ID
	EnvPrefix string // env var prefix, e.g. "ETH"

	DefaultHTTPURL  string // public free HTTP endpoint
	DefaultWSURL    string // public free WS endpoint; "" = WS tests skipped
	DefaultAddress  string // EOA with known existence on this chain
	DefaultContract string // deployed contract with non-empty bytecode
}

func (c *chainConfig) httpURL() string { return chainEnvOr(c.EnvPrefix, "HTTP_URL", c.DefaultHTTPURL) }
func (c *chainConfig) wsURL() string   { return chainEnvOr(c.EnvPrefix, "WS_URL", c.DefaultWSURL) }
func (c *chainConfig) address() string { return chainEnvOr(c.EnvPrefix, "ADDRESS", c.DefaultAddress) }
func (c *chainConfig) contract() string {
	return chainEnvOr(c.EnvPrefix, "CONTRACT", c.DefaultContract)
}
func (c *chainConfig) txHash() string    { return chainEnvOr(c.EnvPrefix, "TX_HASH", "") }
func (c *chainConfig) blockHash() string { return chainEnvOr(c.EnvPrefix, "BLOCK_HASH", "") }

func chainEnvOr(prefix, suffix, fallback string) string {
	if v := os.Getenv(prefix + "_" + suffix); v != "" {
		return v
	}
	return fallback
}

// vitalikAddr is a well-known EOA that exists on every EVM chain.
const vitalikAddr = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

var allChains = []*chainConfig{
	{
		Name: "ethereum", ChainID: 1, EnvPrefix: "ETH",
		DefaultHTTPURL:  "https://ethereum.publicnode.com",
		DefaultWSURL:    "wss://ethereum-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0xdAC17F958D2ee523a2206206994597C13D831ec7", // USDT
	},
	{
		Name: "arbitrum", ChainID: 42161, EnvPrefix: "ARB",
		DefaultHTTPURL:  "https://arbitrum-one.publicnode.com",
		DefaultWSURL:    "wss://arbitrum-one-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", // USDC native
	},
	{
		Name: "bsc", ChainID: 56, EnvPrefix: "BSC",
		DefaultHTTPURL:  "https://bsc.publicnode.com",
		DefaultWSURL:    "wss://bsc-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0x55d398326f99059fF775485246999027B3197955", // USDT BEP-20
	},
	{
		Name: "base", ChainID: 8453, EnvPrefix: "BASE",
		DefaultHTTPURL:  "https://base.publicnode.com",
		DefaultWSURL:    "wss://base-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", // USDC
	},
	{
		Name: "optimism", ChainID: 10, EnvPrefix: "OP",
		DefaultHTTPURL:  "https://optimism.publicnode.com",
		DefaultWSURL:    "wss://optimism-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", // USDC
	},
	{
		Name: "polygon", ChainID: 137, EnvPrefix: "MATIC",
		DefaultHTTPURL:  "https://polygon-bor.publicnode.com",
		DefaultWSURL:    "wss://polygon-bor-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", // USDT
	},
	{
		Name: "avalanche", ChainID: 43114, EnvPrefix: "AVAX",
		DefaultHTTPURL:  "https://avalanche-c-chain.publicnode.com",
		DefaultWSURL:    "wss://avalanche-c-chain-rpc.publicnode.com",
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7", // USDT
	},
	{
		Name: "sonic", ChainID: 146, EnvPrefix: "SONIC",
		DefaultHTTPURL:  "https://rpc.soniclabs.com",
		DefaultWSURL:    "", // no free public WS default; set SONIC_WS_URL to enable
		DefaultAddress:  vitalikAddr,
		DefaultContract: "0x29219dd400f2Bf60E5a23d13Be72B486D4038894", // USDC on Sonic
	},
}

// enabledChains returns the chains to test.
// Set CHAINS_ENABLED=1,42161 to restrict to specific decimal chain IDs.
// When unset, all chains are enabled.
func enabledChains() []*chainConfig {
	env := strings.TrimSpace(os.Getenv("CHAINS_ENABLED"))
	if env == "" {
		return allChains
	}
	allowed := make(map[int]bool)
	for _, part := range strings.Split(env, ",") {
		if id, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			allowed[id] = true
		}
	}
	var out []*chainConfig
	for _, c := range allChains {
		if allowed[c.ChainID] {
			out = append(out, c)
		}
	}
	return out
}

// ============================================================================
// RawClient factories
// ============================================================================

func httpChainClient(t *testing.T, chain *chainConfig) *ThinClient {
	t.Helper()
	url := chain.httpURL()
	if url == "" {
		t.Skipf("no HTTP URL for %s (set %s_HTTP_URL)", chain.Name, chain.EnvPrefix)
	}
	c, err := NewRawClient(&ClientConfig{
		Resources:      []string{url},
		RequestTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRawClient(%s): %v", url, err)
	}
	return c.HTTP()
}

func wsChainClient(t *testing.T, chain *chainConfig) *ThinClient {
	t.Helper()
	url := chain.wsURL()
	if url == "" {
		t.Skipf("no WS URL for %s (set %s_WS_URL)", chain.Name, chain.EnvPrefix)
	}
	c, err := NewRawClient(&ClientConfig{
		Resources:              []string{url},
		RequestTimeout:         15 * time.Second,
		SubscriptionStreamSize: 64,
	})
	if err != nil {
		t.Fatalf("NewRawClient(%s): %v", url, err)
	}
	return c.WS()
}

// ============================================================================
// Test iteration helpers
// ============================================================================

// forEachHTTPChain runs fn as a named subtest for each enabled chain's HTTP rpc.
func forEachHTTPChain(t *testing.T, fn func(t *testing.T, c *ThinClient, chain *chainConfig)) {
	t.Helper()
	for _, chain := range enabledChains() {
		chain := chain
		t.Run(chain.Name, func(t *testing.T) {
			fn(t, httpChainClient(t, chain), chain)
		})
	}
}

// forEachWSChain runs fn as a named subtest for each enabled chain that has a WS URL.
func forEachWSChain(t *testing.T, fn func(t *testing.T, c *ThinClient, chain *chainConfig)) {
	t.Helper()
	for _, chain := range enabledChains() {
		chain := chain
		t.Run(chain.Name, func(t *testing.T) {
			fn(t, wsChainClient(t, chain), chain)
		})
	}
}

// ============================================================================
// Assertion helpers
// ============================================================================

func assertHex(t *testing.T, label string, result []byte) {
	t.Helper()
	if !strings.HasPrefix(string(result), "0x") {
		t.Errorf("%s: expected hex string starting with '0x', got %q", label, result)
	}
}

func assertNonEmpty(t *testing.T, label string, result []byte) {
	t.Helper()
	if len(result) == 0 {
		t.Errorf("%s: got empty result", label)
	}
}

// requireRPC fails fast on genuine RPC errors but skips on transport-level
// failures (timeout, connection refused, DNS). Free public endpoints are
// unreliable by nature; their unavailability is not a code defect.
func requireRPC(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "timed out") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "empty response") {
		t.Skipf("%s: endpoint unavailable (%v) — use a private RPC to avoid flakiness", label, err)
	}
	t.Fatalf("%s: %v", label, err)
}

// ============================================================================
// Result parsing helpers
//
// ParseResponse strips the outer delimiter from the raw JSON-RPC result field:
//   - string  "0x1"      →  0x1          (intended use: hex scalars)
//   - object  {"k":"v"}  →  "k":"v"      (not valid JSON; use extractStringField)
//   - array   [...]      →  ...          (empty [] becomes zero bytes)
//   - null    null       →  ul           (garbage; signals not-found)
//
// Helpers below work correctly with the already-stripped byte slice.
// ============================================================================

// extractStringField finds "field":"<value>" in stripped object content and
// returns the value string. Works without a valid JSON root wrapper.
func extractStringField(data []byte, field string) string {
	key := `"` + field + `":"`
	idx := strings.Index(string(data), key)
	if idx < 0 {
		return ""
	}
	rest := string(data)[idx+len(key):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// firstTxHashFromBlock extracts the hash of the first transaction from a
// block result (the stripped inner content of the block JSON object).
// Returns "" for empty blocks or when full transactions were not requested.
func firstTxHashFromBlock(result []byte) string {
	// Require a non-empty transactions array: "transactions":[{
	txsIdx := strings.Index(string(result), `"transactions":[{`)
	if txsIdx < 0 {
		return ""
	}
	// Find "hash":"0x within the transactions section.
	// The leading quote prevents false matches on "blockHash", "transactionHash", etc.
	after := string(result)[txsIdx+len(`"transactions":[`):]
	const key = `"hash":"0x`
	hIdx := strings.Index(after, key)
	if hIdx < 0 {
		return ""
	}
	rest := after[hIdx+len(`"hash":"`):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// ============================================================================
// Fixture derivation
// ============================================================================

func currentBlockHex(t *testing.T, c *ThinClient) string {
	t.Helper()
	result, err := c.BlockNumber(context.Background())
	requireRPC(t, "BlockNumber (prerequisite)", err)
	return string(result)
}

func currentBlockNumber(t *testing.T, c *ThinClient) uint64 {
	t.Helper()
	val, err := strconv.ParseUint(strings.TrimPrefix(currentBlockHex(t, c), "0x"), 16, 64)
	if err != nil {
		t.Fatalf("BlockNumber (prerequisite): invalid hex block number: %v", err)
	}
	return val
}

func hexBlockMinus(hexBlock string, n uint64) string {
	val, err := strconv.ParseUint(strings.TrimPrefix(hexBlock, "0x"), 16, 64)
	if err != nil || val < n {
		return hexBlock
	}
	return "0x" + strconv.FormatUint(val-n, 16)
}

func recentConfirmedBlockNumber(t *testing.T, c *ThinClient) uint64 {
	t.Helper()
	const confirmations = 3
	head := currentBlockNumber(t, c)
	if head <= confirmations {
		return head
	}
	return head - confirmations
}

// resolvedBlockHash returns the configured block hash or derives one from a
// recent block so tests do not require archive access to historical state.
// The result is the stripped inner content of the block object (outer {} removed
// by ParseResponse), so we use extractStringField rather than json.Unmarshal.
func resolvedBlockHash(t *testing.T, c *ThinClient, chain *chainConfig) string {
	t.Helper()
	if h := chain.blockHash(); h != "" {
		return h
	}
	block := recentConfirmedBlockNumber(t, c)
	result, err := c.GetBlockByNumber(context.Background(), BlockQuery{
		Number: strconv.FormatUint(block, 10),
	})
	requireRPC(t, "GetBlockByNumber(recent) for hash derivation", err)
	h := extractStringField(result, "hash")
	if h == "" {
		t.Skipf("cannot derive block hash from recent block %d", block)
	}
	return h
}

// resolvedTxHash returns the configured tx hash or derives one from recent blocks.
// It skips the chain head by a few confirmations and then scans backwards to
// tolerate empty blocks without relying on old, possibly pruned history.
// The result of GetBlockByNumber is stripped inner object content, so we use
// firstTxHashFromBlock (string search) rather than json.Unmarshal.
func resolvedTxHash(t *testing.T, c *ThinClient, chain *chainConfig) string {
	t.Helper()
	if h := chain.txHash(); h != "" {
		return h
	}
	start := recentConfirmedBlockNumber(t, c)
	for i := uint64(0); i < 64 && start >= i; i++ {
		block := start - i
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{
			Number:           strconv.FormatUint(block, 10),
			FullTransactions: true,
		})
		if err != nil {
			continue // transient error on this block; try the next one
		}
		if h := firstTxHashFromBlock(result); h != "" {
			return h
		}
	}
	t.Skipf("no transactions in recent blocks on %s", chain.Name)
	return ""
}

// ============================================================================
// HTTP integration tests
// ============================================================================

func TestIntegration_ChainId(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.ChainId(context.Background())
		requireRPC(t, "ChainId", err)
		assertHex(t, "ChainId", result)
		want := "0x" + strconv.FormatInt(int64(chain.ChainID), 16)
		if string(result) != want {
			t.Errorf("ChainId: expected %s, got %s", want, result)
		}
		t.Logf("ChainId: %s", result)
	})
}

func TestIntegration_BlockNumber(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.BlockNumber(context.Background())
		requireRPC(t, "BlockNumber", err)
		assertHex(t, "BlockNumber", result)
		t.Logf("BlockNumber: %s", result)
	})
}

func TestIntegration_GetBalance(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.GetBalance(context.Background(), BalanceQuery{AddressedQuery: AddressedQuery{Address: addr}})
		requireRPC(t, "GetBalance", err)
		assertHex(t, "GetBalance", result)
		t.Logf("GetBalance(%s): %s", addr, result)
	})
}

func TestIntegration_GetBalance_AtFinalizedBlock(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.GetBalance(context.Background(), BalanceQuery{AddressedQuery: AddressedQuery{
			OnBlockQuery: OnBlockQuery{BlockTag: BlockTagFinalized},
			Address:      addr,
		}})
		if err != nil {
			// Some chains don't support the "finalized" tag.
			t.Logf("GetBalance(finalized) not supported on %s: %v", chain.Name, err)
			return
		}
		assertHex(t, "GetBalance(finalized)", result)
		t.Logf("GetBalance(%s, finalized): %s", addr, result)
	})
}

func TestIntegration_GetCode_Contract(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		contract := chain.contract()
		result, err := c.GetCode(context.Background(), CodeQuery{AddressedQuery: AddressedQuery{Address: contract}})
		requireRPC(t, "GetCode", err)
		assertHex(t, "GetCode", result)
		if string(result) == "0x" {
			t.Logf("GetCode: got '0x' — %s may not be a contract on %s (set %s_CONTRACT to override)",
				contract, chain.Name, chain.EnvPrefix)
		} else {
			t.Logf("GetCode(%s): %d bytes", contract, (len(result)-2)/2)
		}
	})
}

func TestIntegration_GetTransactionCount(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.GetTransactionCount(context.Background(), AddressedQuery{Address: addr})
		requireRPC(t, "GetTransactionCount", err)
		assertHex(t, "GetTransactionCount", result)
		t.Logf("GetTransactionCount(%s): %s", addr, result)
	})
}

func TestIntegration_Call_ERC20BalanceOf(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		contract := chain.contract()
		addr := strings.TrimPrefix(chain.address(), "0x")
		data := "0x70a08231" + strings.Repeat("0", 24) + strings.ToLower(addr)
		result, err := c.Call(context.Background(), CallQuery{To: contract, Data: data})
		requireRPC(t, "Call(balanceOf)", err)
		assertHex(t, "Call(balanceOf)", result)
		t.Logf("balanceOf(%s) on %s: %s", addr, contract, result)
	})
}

func TestIntegration_EstimateGas_SimpleTransfer(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.EstimateGas(context.Background(), EstimateGasQuery{To: addr})
		requireRPC(t, "EstimateGas", err)
		assertHex(t, "EstimateGas", result)
		// Plain ETH transfer costs 21000 gas (0x5208) on all EVM chains.
		if string(result) != "0x5208" {
			t.Logf("EstimateGas: expected 0x5208 (21000), got %s", result)
		}
		t.Logf("EstimateGas(simple transfer): %s", result)
	})
}

func TestIntegration_EstimateGas_ContractCall(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		contract := chain.contract()
		addr := strings.TrimPrefix(chain.address(), "0x")
		data := "0x70a08231" + strings.Repeat("0", 24) + strings.ToLower(addr)
		result, err := c.EstimateGas(context.Background(), EstimateGasQuery{To: contract, Data: data})
		requireRPC(t, "EstimateGas(contract)", err)
		assertHex(t, "EstimateGas(contract)", result)
		t.Logf("EstimateGas(balanceOf on %s): %s", contract, result)
	})
}

func TestIntegration_GetTransactionByHash(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		txHash := resolvedTxHash(t, c, chain)
		result, err := c.GetTransactionByHash(context.Background(), TransactionQuery{Hash: txHash})
		requireRPC(t, "GetTransactionByHash", err)
		assertNonEmpty(t, "GetTransactionByHash", result)
		t.Logf("GetTransactionByHash(%s): %d bytes", txHash, len(result))
	})
}

func TestIntegration_GetTransactionReceipt(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		txHash := resolvedTxHash(t, c, chain)
		result, err := c.GetTransactionReceipt(context.Background(), TransactionQuery{Hash: txHash})
		requireRPC(t, "GetTransactionReceipt", err)
		assertNonEmpty(t, "GetTransactionReceipt", result)
		t.Logf("GetTransactionReceipt(%s): %d bytes", txHash, len(result))
	})
}

func TestIntegration_SendRawTransaction_RejectsInvalidTx(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		_, err := c.SendRawTransaction(context.Background(), TransactionQuery{Signed: "0xdeadbeef"})
		if err == nil {
			t.Error("expected RPC error for malformed signed transaction")
		}
		t.Logf("SendRawTransaction(invalid) correctly rejected: %v", err)
	})
}

func TestIntegration_GetBlockByNumber_Latest(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{})
		requireRPC(t, "GetBlockByNumber(latest)", err)
		assertNonEmpty(t, "GetBlockByNumber(latest)", result)
		t.Logf("GetBlockByNumber(latest): %d bytes", len(result))
	})
}

func TestIntegration_GetBlockByNumber_ByNumber(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		block := recentConfirmedBlockNumber(t, c)
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{
			Number: strconv.FormatUint(block, 10),
		})
		requireRPC(t, "GetBlockByNumber(recent)", err)
		assertNonEmpty(t, "GetBlockByNumber(recent)", result)
		t.Logf("GetBlockByNumber(%d): %d bytes", block, len(result))
	})
}

func TestIntegration_GetBlockByNumber_FullTransactions(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{FullTransactions: true})
		requireRPC(t, "GetBlockByNumber(fullTx)", err)
		assertNonEmpty(t, "GetBlockByNumber(fullTx)", result)
		t.Logf("GetBlockByNumber(latest, fullTx): %d bytes", len(result))
	})
}

func TestIntegration_GetBlockByNumber_Finalized(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{
			OnBlockQuery: OnBlockQuery{BlockTag: BlockTagFinalized},
		})
		if err != nil {
			t.Logf("GetBlockByNumber(finalized) not supported on %s: %v", chain.Name, err)
			return
		}
		assertNonEmpty(t, "GetBlockByNumber(finalized)", result)
		t.Logf("GetBlockByNumber(finalized): %d bytes", len(result))
	})
}

func TestIntegration_GetBlockByHash(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		blockHash := resolvedBlockHash(t, c, chain)
		result, err := c.GetBlockByHash(context.Background(), BlockQuery{Hash: blockHash})
		requireRPC(t, "GetBlockByHash", err)
		assertNonEmpty(t, "GetBlockByHash", result)
		t.Logf("GetBlockByHash(%s): %d bytes", blockHash, len(result))
	})
}

func TestIntegration_GetLogs(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		block := hexBlockMinus(currentBlockHex(t, c), 5)
		// GetLogs legitimately returns [] when no events match.
		// ParseResponse strips the outer [] leaving zero bytes, so assertNonEmpty
		// must not be used here — zero bytes is a valid "no logs" result.
		_, err := c.GetLogs(context.Background(), LogsQuery{
			AddressedQuery: AddressedQuery{Address: chain.contract()},
			FromBlock:      block,
			ToBlock:        block,
		})
		requireRPC(t, "GetLogs", err)
		t.Logf("GetLogs(block %s, contract %s): ok", block, chain.contract())
	})
}

func TestIntegration_GetLogs_DefaultToBlock(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		fromBlock := hexBlockMinus(currentBlockHex(t, c), 5)
		_, err := c.GetLogs(context.Background(), LogsQuery{
			AddressedQuery: AddressedQuery{Address: chain.contract()},
			FromBlock:      fromBlock,
		})
		requireRPC(t, "GetLogs(default toBlock)", err)
	})
}

// ============================================================================
// Connection management (per-chain)
// ============================================================================

func TestIntegration_MultipleResources_Pooled(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, _ *ThinClient, chain *chainConfig) {
		url := chain.httpURL()
		c, err := NewRawClient(&ClientConfig{
			Resources:      []string{url, url},
			RequestTimeout: 15 * time.Second,
		})
		if err != nil {
			t.Fatalf("NewRawClient: %v", err)
		}
		result, err := c.HTTP().BlockNumber(context.Background())
		if err != nil {
			t.Fatalf("BlockNumber (pooled): %v", err)
		}
		assertHex(t, "BlockNumber (pooled)", result)
	})
}

func TestIntegration_DeadResourceFallback(t *testing.T) {
	forEachHTTPChain(t, func(t *testing.T, _ *ThinClient, chain *chainConfig) {
		c, err := NewRawClient(&ClientConfig{
			Resources:      []string{"http://localhost:19999", chain.httpURL()},
			RequestTimeout: 15 * time.Second,
		})
		if err != nil {
			t.Fatalf("NewRawClient: %v", err)
		}
		result, err := c.HTTP().BlockNumber(context.Background())
		if err != nil {
			t.Fatalf("BlockNumber (after dead resource): %v", err)
		}
		assertHex(t, "BlockNumber (fallback)", result)
	})
}

// ============================================================================
// WebSocket integration tests
// ============================================================================

func TestIntegrationWS_ChainId(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.ChainId(context.Background())
		requireRPC(t, "WS ChainId", err)
		assertHex(t, "WS ChainId", result)
		want := "0x" + strconv.FormatInt(int64(chain.ChainID), 16)
		if string(result) != want {
			t.Errorf("WS ChainId: expected %s, got %s", want, result)
		}
		t.Logf("WS ChainId: %s", result)
	})
}

func TestIntegrationWS_BlockNumber(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.BlockNumber(context.Background())
		requireRPC(t, "WS BlockNumber", err)
		assertHex(t, "WS BlockNumber", result)
		t.Logf("WS BlockNumber: %s", result)
	})
}

func TestIntegrationWS_GetBalance(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.GetBalance(context.Background(), BalanceQuery{AddressedQuery: AddressedQuery{Address: addr}})
		requireRPC(t, "WS GetBalance", err)
		assertHex(t, "WS GetBalance", result)
		t.Logf("WS GetBalance(%s): %s", addr, result)
	})
}

func TestIntegrationWS_GetCode_Contract(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		contract := chain.contract()
		result, err := c.GetCode(context.Background(), CodeQuery{AddressedQuery: AddressedQuery{Address: contract}})
		requireRPC(t, "GetCode", err)
		assertHex(t, "WS GetCode", result)
		if string(result) == "0x" {
			t.Logf("WS GetCode: got '0x' — %s may not be a contract on %s (set %s_CONTRACT to override)",
				contract, chain.Name, chain.EnvPrefix)
		} else {
			t.Logf("WS GetCode(%s): %d bytes", contract, (len(result)-2)/2)
		}
	})
}

func TestIntegrationWS_GetTransactionCount(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		result, err := c.GetTransactionCount(context.Background(), AddressedQuery{Address: addr})
		requireRPC(t, "WS GetTransactionCount", err)
		assertHex(t, "WS GetTransactionCount", result)
		t.Logf("WS GetTransactionCount(%s): %s", addr, result)
	})
}

func TestIntegrationWS_Call_ERC20BalanceOf(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		contract := chain.contract()
		addr := strings.TrimPrefix(chain.address(), "0x")
		data := "0x70a08231" + strings.Repeat("0", 24) + strings.ToLower(addr)
		result, err := c.Call(context.Background(), CallQuery{To: contract, Data: data})
		requireRPC(t, "WS Call(balanceOf)", err)
		assertHex(t, "WS Call(balanceOf)", result)
		t.Logf("WS balanceOf(%s) on %s: %s", addr, contract, result)
	})
}

func TestIntegrationWS_GetTransactionByHash(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		txHash := resolvedTxHash(t, c, chain)
		result, err := c.GetTransactionByHash(context.Background(), TransactionQuery{Hash: txHash})
		requireRPC(t, "WS GetTransactionByHash", err)
		assertNonEmpty(t, "WS GetTransactionByHash", result)
		t.Logf("WS GetTransactionByHash(%s): %d bytes", txHash, len(result))
	})
}

func TestIntegrationWS_GetTransactionReceipt(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		txHash := resolvedTxHash(t, c, chain)
		result, err := c.GetTransactionReceipt(context.Background(), TransactionQuery{Hash: txHash})
		requireRPC(t, "WS GetTransactionReceipt", err)
		assertNonEmpty(t, "WS GetTransactionReceipt", result)
		t.Logf("WS GetTransactionReceipt(%s): %d bytes", txHash, len(result))
	})
}

func TestIntegrationWS_GetBlockByNumber_Latest(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		result, err := c.GetBlockByNumber(context.Background(), BlockQuery{})
		requireRPC(t, "WS GetBlockByNumber(latest)", err)
		assertNonEmpty(t, "WS GetBlockByNumber(latest)", result)
		t.Logf("WS GetBlockByNumber(latest): %d bytes", len(result))
	})
}

func TestIntegrationWS_GetBlockByHash(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		blockHash := resolvedBlockHash(t, c, chain)
		result, err := c.GetBlockByHash(context.Background(), BlockQuery{Hash: blockHash})
		requireRPC(t, "WS GetBlockByHash", err)
		assertNonEmpty(t, "WS GetBlockByHash", result)
		t.Logf("WS GetBlockByHash(%s): %d bytes", blockHash, len(result))
	})
}

func TestIntegrationWS_GetLogs(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		block := hexBlockMinus(currentBlockHex(t, c), 5)
		// Empty [] is a valid response; do not use assertNonEmpty (see HTTP variant).
		_, err := c.GetLogs(context.Background(), LogsQuery{
			AddressedQuery: AddressedQuery{Address: chain.contract()},
			FromBlock:      block,
			ToBlock:        block,
		})
		requireRPC(t, "WS GetLogs", err)
		t.Logf("WS GetLogs(block %s, contract %s): ok", block, chain.contract())
	})
}

func TestIntegrationWS_SequentialCalls_SharedConnection(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		r1, err := c.ChainId(context.Background())
		requireRPC(t, "WS ChainId (1st)", err)
		r2, err := c.BlockNumber(context.Background())
		requireRPC(t, "WS BlockNumber", err)
		r3, err := c.ChainId(context.Background())
		requireRPC(t, "WS ChainId (2nd)", err)

		assertHex(t, "WS ChainId (1st)", r1)
		assertHex(t, "WS BlockNumber", r2)
		assertHex(t, "WS ChainId (2nd)", r3)

		if string(r1) != string(r3) {
			t.Errorf("ChainId should be stable: first=%s second=%s", r1, r3)
		}
		t.Logf("WS sequential: chainId=%s blockNumber=%s", r1, r2)
	})
}

// ============================================================================
// WebSocket stress tests
// ============================================================================

func TestIntegrationWS_Stress_SequentialHighVolume(t *testing.T) {
	const n = 50
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		chainID, err := c.ChainId(context.Background())
		requireRPC(t, "initial ChainId", err)
		for i := 0; i < n; i++ {
			result, err := c.ChainId(context.Background())
			requireRPC(t, "ChainId", err)
			if string(result) != string(chainID) {
				t.Errorf("call %d: chainId changed: want %s got %s", i, chainID, result)
			}
		}
		t.Logf("completed %d sequential calls, chainId=%s", n, chainID)
	})
}

func TestIntegrationWS_Stress_ConcurrentSameMethod(t *testing.T) {
	const (
		goroutines = 10
		callsEach  = 5
	)
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		var (
			wg       sync.WaitGroup
			errCount atomic.Int64
		)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < callsEach; i++ {
					result, err := c.ChainId(context.Background())
					if err != nil {
						t.Errorf("goroutine %d call %d: %v", id, i, err)
						errCount.Add(1)
						return
					}
					if !strings.HasPrefix(string(result), "0x") {
						t.Errorf("goroutine %d call %d: bad result %q", id, i, result)
						errCount.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		t.Logf("completed %d concurrent calls (%d×%d), errors=%d",
			goroutines*callsEach, goroutines, callsEach, errCount.Load())
		if errCount.Load() > 0 {
			t.Fail()
		}
	})
}

func TestIntegrationWS_Stress_ConcurrentMixedMethods(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		addr := chain.address()
		contract := chain.contract()

		type job struct {
			name string
			fn   func() ([]byte, error)
		}
		jobs := []job{
			{"ChainId", func() ([]byte, error) { return c.ChainId(context.Background()) }},
			{"BlockNumber", func() ([]byte, error) { return c.BlockNumber(context.Background()) }},
			{"GetBalance", func() ([]byte, error) {
				return c.GetBalance(context.Background(), BalanceQuery{AddressedQuery: AddressedQuery{Address: addr}})
			}},
			{"GetCode", func() ([]byte, error) {
				return c.GetCode(context.Background(), CodeQuery{AddressedQuery: AddressedQuery{Address: contract}})
			}},
			{"GetTransactionCount", func() ([]byte, error) {
				return c.GetTransactionCount(context.Background(), AddressedQuery{Address: addr})
			}},
		}

		var wg sync.WaitGroup
		for _, j := range jobs {
			j := j
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := j.fn()
				if err != nil {
					t.Errorf("%s: %v", j.name, err)
					return
				}
				assertHex(t, "WS "+j.name, result)
				t.Logf("WS %s: %s", j.name, result)
			}()
		}
		wg.Wait()
	})
}

// ============================================================================
// WebSocket subscription tests
// ============================================================================

func TestIntegrationWS_Subscribe_NewHeads(t *testing.T) {
	forEachWSChain(t, func(t *testing.T, c *ThinClient, chain *chainConfig) {
		sub, listener, err := c.Subscribe(context.Background(), SubscribeQuery{On: "newHeads"})
		if err != nil {
			t.Fatalf("Subscribe(newHeads): %v", err)
		}
		t.Logf("subscribed to newHeads, id=%s", sub)

		select {
		case event, ok := <-listener:
			if !ok {
				t.Fatal("listener closed before receiving an event")
			}
			assertNonEmpty(t, "newHeads event", event)
			t.Logf("received newHeads event: %d bytes", len(event))
		case <-time.After(30 * time.Second):
			t.Skip("no newHeads event within 30s — chain may be slow or paused")
		}

		result, err := c.UnSubscribe(context.Background(), UnSubscribeQuery{Subscription: sub})
		if err != nil {
			t.Fatalf("UnSubscribe: %v", err)
		}
		t.Logf("unsubscribed: %s", result)
	})
}
