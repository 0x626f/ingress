package rpc

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0x626f/ingress/solana/model"
	"github.com/0x626f/ingress/solana/types"
	"github.com/0x626f/ingress/transport"
)

const (
	defaultPublicHTTPURL  = "https://api.mainnet-beta.solana.com"
	defaultPublicWSURL    = "wss://api.mainnet-beta.solana.com"
	defaultPublicRPCDelay = 350 * time.Millisecond

	systemProgramID = "11111111111111111111111111111111"
	wrappedSOLMint  = "So11111111111111111111111111111111111111112"
)

var publicRPCTestThrottle = struct {
	sync.Mutex
	last time.Time
}{}

func publicHTTPURL() string {
	if value := os.Getenv("SOLANA_HTTP_URL"); value != "" {
		return value
	}
	return defaultPublicHTTPURL
}

func publicWSURL() string {
	if value := os.Getenv("SOLANA_WS_URL"); value != "" {
		return value
	}
	return defaultPublicWSURL
}

func publicRPCDelay() time.Duration {
	if value := os.Getenv("SOLANA_RPC_TEST_DELAY"); value != "" {
		delay, err := time.ParseDuration(value)
		if err == nil {
			return delay
		}
	}
	return defaultPublicRPCDelay
}

func throttlePublicRPC(t *testing.T) {
	t.Helper()
	delay := publicRPCDelay()
	if delay <= 0 {
		return
	}

	publicRPCTestThrottle.Lock()
	defer publicRPCTestThrottle.Unlock()

	if !publicRPCTestThrottle.last.IsZero() {
		wait := delay - time.Since(publicRPCTestThrottle.last)
		if wait > 0 {
			time.Sleep(wait)
		}
	}
	publicRPCTestThrottle.last = time.Now()
}

func newPublicHTTPClient(t *testing.T) *Client {
	t.Helper()
	raw, err := NewRawClient(&ClientConfig{
		Resources:              []string{publicHTTPURL()},
		ErrorOnInvalidResource: true,
		RequestTimeout:         20 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	return raw.HTTP()
}

func newPublicWSClient(t *testing.T, ctx context.Context) *Client {
	t.Helper()
	raw, err := NewRawClientWithContext(ctx, &ClientConfig{
		Resources:              []string{publicWSURL()},
		ErrorOnInvalidResource: true,
		RequestTimeout:         20 * time.Second,
		SubscriptionStreamSize: 8,
	})
	if err != nil {
		t.Fatalf("NewRawClientWithContext: %v", err)
	}
	return raw.WS()
}

func requireRaw(t *testing.T, name string, values ...any) types.RawResult {
	t.Helper()
	if len(values) != 2 {
		t.Fatalf("%s: expected raw result and error, got %d values", name, len(values))
	}
	raw, ok := values[0].(types.RawResult)
	if !ok {
		t.Fatalf("%s: expected types.RawResult, got %T", name, values[0])
	}
	err, ok := values[1].(error)
	if values[1] != nil && !ok {
		t.Fatalf("%s: expected error, got %T", name, values[1])
	}
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(raw) == 0 {
		t.Fatalf("%s returned empty result", name)
	}
	return raw
}

func requireRawCall(t *testing.T, name string, call func() (types.RawResult, error)) types.RawResult {
	t.Helper()
	throttlePublicRPC(t)
	raw, err := call()
	return requireRaw(t, name, raw, err)
}

func requirePublicRawCall(t *testing.T, name string, call func() (types.RawResult, error)) types.RawResult {
	t.Helper()
	throttlePublicRPC(t)
	raw, err := call()
	if err != nil && strings.Contains(err.Error(), "429") {
		t.Skipf("%s rate limited by public RPC endpoint: %v", name, err)
	}
	return requireRaw(t, name, raw, err)
}

func requirePublicSlotCall(t *testing.T, name string, call func() (types.Slot, error)) types.Slot {
	t.Helper()
	throttlePublicRPC(t)
	slot, err := call()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if slot == 0 {
		t.Fatalf("%s returned zero", name)
	}
	return slot
}

func requirePublicSlotLeadersCall(t *testing.T, name string, call func() (types.SlotLeaders, error)) types.SlotLeaders {
	t.Helper()
	throttlePublicRPC(t)
	leaders, err := call()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(leaders) == 0 {
		t.Fatalf("%s returned no leaders", name)
	}
	return leaders
}

func requirePublicConfirmedSlotsCall(t *testing.T, name string, call func() (types.ConfirmedSlots, error)) types.ConfirmedSlots {
	t.Helper()
	throttlePublicRPC(t)
	slots, err := call()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(slots) == 0 {
		t.Fatalf("%s returned no slots", name)
	}
	return slots
}

func TestNewRawClient_PublicResources(t *testing.T) {
	raw, err := NewRawClient(&ClientConfig{
		Resources: []string{
			"ftp://invalid",
			publicHTTPURL(),
			publicWSURL(),
		},
		RequestTimeout:         20 * time.Second,
		SubscriptionStreamSize: 8,
	})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	if raw.HTTP() == nil {
		t.Fatal("expected HTTP thin client")
	}
	if raw.WS() == nil {
		t.Fatal("expected WS thin client")
	}
}

func TestNewRawClient_HTTPResourceAvailability(t *testing.T) {
	raw, err := NewRawClient(&ClientConfig{Resources: []string{"https://api.mainnet-beta.solana.com"}})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	if !raw.HasResourceByProtocol(transport.HTTP) {
		t.Fatal("expected HTTP resources to be available")
	}
	if raw.HasResourceByProtocol(transport.WS) {
		t.Fatal("expected WS resources to be unavailable")
	}
	if raw.HTTP() == nil {
		t.Fatal("expected HTTP thin client")
	}
	if raw.WS() != nil {
		t.Fatal("expected nil WS thin client without WS resources")
	}
}

func TestNewRawClient_WSResourceAvailability(t *testing.T) {
	raw, err := NewRawClient(&ClientConfig{Resources: []string{"wss://api.mainnet-beta.solana.com"}})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	if !raw.HasResourceByProtocol(transport.WS) {
		t.Fatal("expected WS resources to be available")
	}
	if raw.HasResourceByProtocol(transport.HTTP) {
		t.Fatal("expected HTTP resources to be unavailable")
	}
	if raw.WS() == nil {
		t.Fatal("expected WS thin client")
	}
	if raw.HTTP() != nil {
		t.Fatal("expected nil HTTP thin client without HTTP resources")
	}
}

func TestNewRawClient_UnknownResourceProtocolUnavailable(t *testing.T) {
	raw, err := NewRawClient(&ClientConfig{Resources: []string{"https://api.mainnet-beta.solana.com"}})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	if raw.HasResourceByProtocol(transport.ConnectionKind(255)) {
		t.Fatal("expected unknown protocol to be unavailable")
	}
}

func TestNewRawClient_NoValidResources(t *testing.T) {
	if _, err := NewRawClient(&ClientConfig{Resources: []string{"ftp://invalid"}}); err == nil {
		t.Fatal("expected error for no valid resources")
	}
}

func TestPublicRPC_HealthAndVersion(t *testing.T) {
	client := newPublicHTTPClient(t)

	health := requireRawCall(t, "GetHealth", func() (types.RawResult, error) { return client.GetHealth(context.Background()) })
	if string(health) != `"ok"` {
		t.Fatalf("unexpected health result: %s", health)
	}

	versionRaw := requireRawCall(t, "GetVersion", func() (types.RawResult, error) { return client.GetVersion(context.Background()) })
	var version model.Version
	if err := json.Unmarshal(versionRaw, &version); err != nil {
		t.Fatalf("unmarshal version: %v", err)
	}
	if version.SolanaCore == "" {
		t.Fatalf("missing solana-core in version: %s", versionRaw)
	}

	identityRaw := requireRawCall(t, "GetIdentity", func() (types.RawResult, error) { return client.GetIdentity(context.Background()) })
	var identity model.Identity
	if err := json.Unmarshal(identityRaw, &identity); err != nil {
		t.Fatalf("unmarshal identity: %v", err)
	}
	if identity.Identity == "" {
		t.Fatalf("missing identity: %s", identityRaw)
	}
}

func TestPublicRPC_SlotAndEpochMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	slot := requirePublicSlotCall(t, "GetSlot", func() (types.Slot, error) {
		return client.GetSlot(context.Background(), ConfirmedCommitment)
	})

	blockHeight := requireRawCall(t, "GetBlockHeight", func() (types.RawResult, error) {
		return client.GetBlockHeight(context.Background(), ConfirmedCommitment)
	})
	var height uint64
	if err := json.Unmarshal(blockHeight, &height); err != nil {
		t.Fatalf("unmarshal block height: %v", err)
	}
	if height == 0 {
		t.Fatal("GetBlockHeight returned zero")
	}

	epochRaw := requireRawCall(t, "GetEpochInfo", func() (types.RawResult, error) {
		return client.GetEpochInfo(context.Background(), ConfirmedCommitment)
	})
	var epoch model.EpochInfo
	if err := json.Unmarshal(epochRaw, &epoch); err != nil {
		t.Fatalf("unmarshal epoch info: %v", err)
	}
	if epoch.SlotsInEpoch == 0 {
		t.Fatalf("invalid epoch info: %s", epochRaw)
	}

	leaders := requirePublicSlotLeadersCall(t, "GetSlotLeaders", func() (types.SlotLeaders, error) {
		return client.GetSlotLeaders(context.Background(), slot, 2)
	})
	if len(leaders) != 2 {
		t.Fatalf("expected 2 slot leaders, got %d", len(leaders))
	}

	requireRawCall(t, "GetSlotLeader", func() (types.RawResult, error) {
		return client.GetSlotLeader(context.Background(), ConfirmedCommitment)
	})
	requireRawCall(t, "GetEpochSchedule", func() (types.RawResult, error) { return client.GetEpochSchedule(context.Background()) })
}

func TestPublicRPC_BlockAndLedgerMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	slot := requirePublicSlotCall(t, "GetSlot", func() (types.Slot, error) {
		return client.GetSlot(context.Background(), ConfirmedCommitment)
	})
	if slot < 10 {
		t.Fatalf("unexpected small slot: %d", slot)
	}

	requireRawCall(t, "GetFirstAvailableBlock", func() (types.RawResult, error) { return client.GetFirstAvailableBlock(context.Background()) })
	requireRawCall(t, "MinimumLedgerSlot", func() (types.RawResult, error) { return client.MinimumLedgerSlot(context.Background()) })
	requireRawCall(t, "GetMaxRetransmitSlot", func() (types.RawResult, error) { return client.GetMaxRetransmitSlot(context.Background()) })
	requireRawCall(t, "GetMaxShredInsertSlot", func() (types.RawResult, error) { return client.GetMaxShredInsertSlot(context.Background()) })
	requireRawCall(t, "GetRecentPerformanceSamples", func() (types.RawResult, error) {
		return client.GetRecentPerformanceSamples(context.Background(), 1)
	})

	blocksRaw := requireRawCall(t, "GetBlocksWithLimit", func() (types.RawResult, error) {
		return client.GetBlocksWithLimit(context.Background(), slot-10, 2, ConfirmedCommitment)
	})
	var blocks []types.Slot
	if err := json.Unmarshal(blocksRaw, &blocks); err != nil {
		t.Fatalf("unmarshal blocks: %v", err)
	}
	if len(blocks) == 0 {
		t.Fatalf("GetBlocksWithLimit returned no blocks near slot %d", slot)
	}

	confirmed := requirePublicConfirmedSlotsCall(t, "GetConfirmedSlots", func() (types.ConfirmedSlots, error) {
		return client.GetConfirmedSlots(context.Background(), slot-10, slot, ConfirmedCommitment)
	})

	requireRawCall(t, "GetBlockCommitment", func() (types.RawResult, error) {
		return client.GetBlockCommitment(context.Background(), confirmed[len(confirmed)-1])
	})
	requireRawCall(t, "GetBlockTime", func() (types.RawResult, error) {
		return client.GetBlockTime(context.Background(), confirmed[len(confirmed)-1])
	})
}

func TestPublicRPC_AccountMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	balanceRaw := requireRawCall(t, "GetBalance", func() (types.RawResult, error) {
		return client.GetBalance(context.Background(), systemProgramID, map[string]string{"commitment": string(ConfirmedCommitment)})
	})
	var balance model.Balance
	if err := json.Unmarshal(balanceRaw, &balance); err != nil {
		t.Fatalf("unmarshal balance: %v", err)
	}

	accountRaw := requireRawCall(t, "GetAccountInfo", func() (types.RawResult, error) {
		return client.GetAccountInfo(context.Background(), systemProgramID, map[string]string{
			"encoding":   "base64",
			"commitment": string(ConfirmedCommitment),
		})
	})
	var account model.AccountInfo
	if err := json.Unmarshal(accountRaw, &account); err != nil {
		t.Fatalf("unmarshal account info: %v", err)
	}
	if account.Value == nil {
		t.Fatalf("system program account not found: %s", accountRaw)
	}

	multipleRaw := requireRawCall(t, "GetMultipleAccounts", func() (types.RawResult, error) {
		return client.GetMultipleAccounts(context.Background(), []string{systemProgramID}, map[string]string{
			"encoding":   "base64",
			"commitment": string(ConfirmedCommitment),
		})
	})
	var multiple model.MultipleAccounts
	if err := json.Unmarshal(multipleRaw, &multiple); err != nil {
		t.Fatalf("unmarshal multiple accounts: %v", err)
	}
	if len(multiple.Value) != 1 || multiple.Value[0] == nil {
		t.Fatalf("unexpected multiple account result: %s", multipleRaw)
	}

	requirePublicRawCall(t, "GetLargestAccounts", func() (types.RawResult, error) {
		return client.GetLargestAccounts(context.Background(), map[string]string{"commitment": string(ConfirmedCommitment)})
	})
	requireRawCall(t, "GetMinimumBalanceForRentExemption", func() (types.RawResult, error) {
		return client.GetMinimumBalanceForRentExemption(context.Background(), 0, ConfirmedCommitment)
	})
}

func TestPublicRPC_TokenAndSupplyMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	supplyRaw := requireRawCall(t, "GetSupply", func() (types.RawResult, error) {
		return client.GetSupply(context.Background(), map[string]any{
			"commitment":                        ConfirmedCommitment,
			"excludeNonCirculatingAccountsList": true,
		})
	})
	var supply model.Supply
	if err := json.Unmarshal(supplyRaw, &supply); err != nil {
		t.Fatalf("unmarshal supply: %v", err)
	}
	if supply.Value.Total == 0 {
		t.Fatalf("invalid supply: %s", supplyRaw)
	}

	tokenSupplyRaw := requireRawCall(t, "GetTokenSupply", func() (types.RawResult, error) {
		return client.GetTokenSupply(context.Background(), wrappedSOLMint, ConfirmedCommitment)
	})
	var tokenSupply model.TokenSupply
	if err := json.Unmarshal(tokenSupplyRaw, &tokenSupply); err != nil {
		t.Fatalf("unmarshal token supply: %v", err)
	}
	if tokenSupply.Value.Amount == "" {
		t.Fatalf("missing token supply amount: %s", tokenSupplyRaw)
	}

	requirePublicRawCall(t, "GetTokenLargestAccounts", func() (types.RawResult, error) {
		return client.GetTokenLargestAccounts(context.Background(), wrappedSOLMint, ConfirmedCommitment)
	})
	requireRawCall(t, "GetStakeMinimumDelegation", func() (types.RawResult, error) {
		return client.GetStakeMinimumDelegation(context.Background(), ConfirmedCommitment)
	})
}

func TestPublicRPC_BlockhashAndTransactionLookupMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	blockhashRaw := requireRawCall(t, "GetLatestBlockhash", func() (types.RawResult, error) {
		return client.GetLatestBlockhash(context.Background(), ConfirmedCommitment)
	})
	var blockhash model.LatestBlockhash
	if err := json.Unmarshal(blockhashRaw, &blockhash); err != nil {
		t.Fatalf("unmarshal latest blockhash: %v", err)
	}
	if blockhash.Value.Blockhash == "" {
		t.Fatalf("missing blockhash: %s", blockhashRaw)
	}

	validRaw := requireRawCall(t, "IsBlockhashValid", func() (types.RawResult, error) {
		return client.IsBlockhashValid(context.Background(), blockhash.Value.Blockhash, ConfirmedCommitment)
	})
	var valid model.BlockhashValid
	if err := json.Unmarshal(validRaw, &valid); err != nil {
		t.Fatalf("unmarshal blockhash validity: %v", err)
	}
	if !valid.Value {
		t.Fatalf("fresh blockhash reported invalid: %s", validRaw)
	}

	requireRawCall(t, "GetRecentPrioritizationFees", func() (types.RawResult, error) {
		return client.GetRecentPrioritizationFees(context.Background())
	})
	requireRawCall(t, "GetTransactionCount", func() (types.RawResult, error) {
		return client.GetTransactionCount(context.Background(), ConfirmedCommitment)
	})
	requireRawCall(t, "GetSignaturesForAddress", func() (types.RawResult, error) {
		return client.GetSignaturesForAddress(context.Background(), systemProgramID, map[string]any{
			"limit":      1,
			"commitment": ConfirmedCommitment,
		})
	})
}

func TestPublicRPC_ClusterAndValidatorMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	nodesRaw := requireRawCall(t, "GetClusterNodes", func() (types.RawResult, error) { return client.GetClusterNodes(context.Background()) })
	var nodes model.ClusterNodes
	if err := json.Unmarshal(nodesRaw, &nodes); err != nil {
		t.Fatalf("unmarshal cluster nodes: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatalf("GetClusterNodes returned no nodes")
	}

	voteRaw := requireRawCall(t, "GetVoteAccounts", func() (types.RawResult, error) { return client.GetVoteAccounts(context.Background()) })
	var voteAccounts model.VoteAccounts
	if err := json.Unmarshal(voteRaw, &voteAccounts); err != nil {
		t.Fatalf("unmarshal vote accounts: %v", err)
	}
	if len(voteAccounts.Current) == 0 {
		t.Fatalf("GetVoteAccounts returned no current accounts")
	}

	requireRawCall(t, "GetInflationGovernor", func() (types.RawResult, error) {
		return client.GetInflationGovernor(context.Background(), ConfirmedCommitment)
	})
	requireRawCall(t, "GetInflationRate", func() (types.RawResult, error) { return client.GetInflationRate(context.Background()) })
	requireRawCall(t, "GetLeaderSchedule", func() (types.RawResult, error) {
		return client.GetLeaderSchedule(context.Background(), nil, map[string]string{"commitment": string(ConfirmedCommitment)})
	})
}

func TestPublicWS_SlotSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client := newPublicWSClient(t, ctx)
	throttlePublicRPC(t)
	subscription, err := client.SlotSubscribe(context.Background())
	if err != nil {
		t.Fatalf("SlotSubscribe: %v", err)
	}
	defer subscription.Unsubscribe()

	select {
	case event, ok := <-subscription.Events:
		if !ok {
			t.Fatal("subscription events closed")
		}
		if event.Error != nil {
			t.Fatalf("slot event error: %v", event.Error)
		}
		var slot model.SlotUpdate
		if err := json.Unmarshal(event.Data, &slot); err != nil {
			t.Fatalf("unmarshal slot event: %v; raw=%s", err, event.Data)
		}
		if slot.Slot == 0 {
			t.Fatalf("slot update has zero slot: %s", event.Data)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for slot event: %v", ctx.Err())
	}
}

func TestPublicWS_SubscribeSlotConvenience(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client := newPublicWSClient(t, ctx)
	throttlePublicRPC(t)
	events, err := client.SubscribeSlot(context.Background())
	if err != nil {
		t.Fatalf("SubscribeSlot: %v", err)
	}

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("slot events closed")
		}
		if event.Error != nil {
			t.Fatalf("slot event error: %v", event.Error)
		}
		if event.Data == 0 {
			t.Fatal("SubscribeSlot returned zero slot")
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for slot event: %v", ctx.Err())
	}
}
