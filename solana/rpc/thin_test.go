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
	"github.com/0x626f/ingress/transport"
)

const (
	defaultPublicHTTPURL       = "https://api.mainnet-beta.solana.com"
	defaultPublicWSURL         = "wss://api.mainnet-beta.solana.com"
	defaultPublicRPCDelay      = time.Second
	defaultPublicRPCRetryDelay = 5 * time.Second
	defaultPublicRPCRetries    = 3

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

func publicRPCRetryDelay() time.Duration {
	if value := os.Getenv("SOLANA_RPC_TEST_RETRY_DELAY"); value != "" {
		delay, err := time.ParseDuration(value)
		if err == nil {
			return delay
		}
	}
	return defaultPublicRPCRetryDelay
}

func isPublicRPCRateLimit(err error) bool {
	return err != nil && strings.Contains(err.Error(), "429")
}

func retryPublicRPCCall[T any](t *testing.T, name string, call func() (T, error)) (T, error) {
	t.Helper()

	var result T
	var err error
	for attempt := 1; attempt <= defaultPublicRPCRetries; attempt++ {
		throttlePublicRPC(t)
		result, err = call()
		if !isPublicRPCRateLimit(err) {
			return result, err
		}
		if attempt < defaultPublicRPCRetries {
			time.Sleep(publicRPCRetryDelay())
		}
	}

	t.Skipf("%s rate limited by public RPC endpoint after retries: %v", name, err)
	return result, err
}

func throttlePublicRPC(t *testing.T) {
	t.Helper()
	delay := publicRPCDelay()
	if delay <= 0 {
		return
	}

	publicRPCTestThrottle.Lock()
	defer publicRPCTestThrottle.Unlock()

	now := time.Now()
	if wait := publicRPCTestThrottle.last.Sub(now); wait > 0 {
		time.Sleep(wait)
	}
	publicRPCTestThrottle.last = time.Now().Add(delay)
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

func requireRaw(t *testing.T, name string, values ...any) model.RawResult {
	t.Helper()
	if len(values) != 2 {
		t.Fatalf("%s: expected raw result and error, got %d values", name, len(values))
	}
	raw, ok := values[0].(model.RawResult)
	if !ok {
		t.Fatalf("%s: expected model.RawResult, got %T", name, values[0])
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

func requireRawCall(t *testing.T, name string, call func() (model.RawResult, error)) model.RawResult {
	t.Helper()
	raw, err := retryPublicRPCCall(t, name, call)
	return requireRaw(t, name, raw, err)
}

func requirePublicRawCall(t *testing.T, name string, call func() (model.RawResult, error)) model.RawResult {
	t.Helper()
	raw, err := retryPublicRPCCall(t, name, call)
	return requireRaw(t, name, raw, err)
}

func requirePublicSlotCall(t *testing.T, name string, call func() (model.Slot, error)) model.Slot {
	t.Helper()
	slot, err := retryPublicRPCCall(t, name, call)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if slot == 0 {
		t.Fatalf("%s returned zero", name)
	}
	return slot
}

func requirePublicSlotLeadersCall(t *testing.T, name string, call func() (model.SlotLeaders, error)) model.SlotLeaders {
	t.Helper()
	leaders, err := retryPublicRPCCall(t, name, call)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(leaders) == 0 {
		t.Fatalf("%s returned no leaders", name)
	}
	return leaders
}

func requirePublicConfirmedSlotsCall(t *testing.T, name string, call func() (model.ConfirmedSlots, error)) model.ConfirmedSlots {
	t.Helper()
	slots, err := retryPublicRPCCall(t, name, call)
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
	if raw.HasResourceByProtocol(transport.Protocol(255)) {
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

	health := requireRawCall(t, "GetHealth", func() (model.RawResult, error) { return client.GetHealth(context.Background()) })
	if string(health) != `"ok"` {
		t.Fatalf("unexpected health result: %s", health)
	}

	versionRaw := requireRawCall(t, "GetVersion", func() (model.RawResult, error) { return client.GetVersion(context.Background()) })
	var version model.Version
	if err := json.Unmarshal(versionRaw, &version); err != nil {
		t.Fatalf("unmarshal version: %v", err)
	}
	if version.SolanaCore == "" {
		t.Fatalf("missing solana-core in version: %s", versionRaw)
	}

	identityRaw := requireRawCall(t, "GetIdentity", func() (model.RawResult, error) { return client.GetIdentity(context.Background()) })
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

	slot := requirePublicSlotCall(t, "GetSlot", func() (model.Slot, error) {
		return client.GetSlot(context.Background(), GetSlotQuery{Commitment: model.ConfirmedCommitment})
	})

	blockHeight := requireRawCall(t, "GetBlockHeight", func() (model.RawResult, error) {
		return client.GetBlockHeight(context.Background(), GetBlockHeightQuery{Commitment: model.ConfirmedCommitment})
	})
	var height uint64
	if err := json.Unmarshal(blockHeight, &height); err != nil {
		t.Fatalf("unmarshal block height: %v", err)
	}
	if height == 0 {
		t.Fatal("GetBlockHeight returned zero")
	}

	epochRaw := requireRawCall(t, "GetEpochInfo", func() (model.RawResult, error) {
		return client.GetEpochInfo(context.Background(), GetEpochInfoQuery{Commitment: model.ConfirmedCommitment})
	})
	var epoch model.EpochInfo
	if err := json.Unmarshal(epochRaw, &epoch); err != nil {
		t.Fatalf("unmarshal epoch info: %v", err)
	}
	if epoch.SlotsInEpoch == 0 {
		t.Fatalf("invalid epoch info: %s", epochRaw)
	}

	leaders := requirePublicSlotLeadersCall(t, "GetSlotLeaders", func() (model.SlotLeaders, error) {
		return client.GetSlotLeaders(context.Background(), GetSlotLeadersQuery{From: slot, Limit: 2})
	})
	if len(leaders) != 2 {
		t.Fatalf("expected 2 slot leaders, got %d", len(leaders))
	}

	requireRawCall(t, "GetSlotLeader", func() (model.RawResult, error) {
		return client.GetSlotLeader(context.Background(), GetSlotLeaderQuery{Commitment: model.ConfirmedCommitment})
	})
	requireRawCall(t, "GetEpochSchedule", func() (model.RawResult, error) { return client.GetEpochSchedule(context.Background()) })
}

func TestPublicRPC_BlockAndLedgerMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	slot := requirePublicSlotCall(t, "GetSlot", func() (model.Slot, error) {
		return client.GetSlot(context.Background(), GetSlotQuery{Commitment: model.ConfirmedCommitment})
	})
	if slot < 10 {
		t.Fatalf("unexpected small slot: %d", slot)
	}

	requireRawCall(t, "GetFirstAvailableBlock", func() (model.RawResult, error) { return client.GetFirstAvailableBlock(context.Background()) })
	requireRawCall(t, "MinimumLedgerSlot", func() (model.RawResult, error) { return client.MinimumLedgerSlot(context.Background()) })
	requireRawCall(t, "GetMaxRetransmitSlot", func() (model.RawResult, error) { return client.GetMaxRetransmitSlot(context.Background()) })
	requireRawCall(t, "GetMaxShredInsertSlot", func() (model.RawResult, error) { return client.GetMaxShredInsertSlot(context.Background()) })
	requireRawCall(t, "GetRecentPerformanceSamples", func() (model.RawResult, error) {
		return client.GetRecentPerformanceSamples(context.Background(), GetRecentPerformanceSamplesQuery{Limit: 1})
	})

	blocksRaw := requireRawCall(t, "GetBlocksWithLimit", func() (model.RawResult, error) {
		return client.GetBlocksWithLimit(context.Background(), GetBlocksWithLimitQuery{StartSlot: slot - 10, Limit: 2, Commitment: model.ConfirmedCommitment})
	})
	var blocks []model.Slot
	if err := json.Unmarshal(blocksRaw, &blocks); err != nil {
		t.Fatalf("unmarshal blocks: %v", err)
	}
	if len(blocks) == 0 {
		t.Fatalf("GetBlocksWithLimit returned no blocks near slot %d", slot)
	}

	confirmed := requirePublicConfirmedSlotsCall(t, "GetConfirmedSlots", func() (model.ConfirmedSlots, error) {
		return client.GetConfirmedSlots(context.Background(), GetConfirmedSlotsQuery{From: slot - 10, To: slot, Commitment: model.ConfirmedCommitment})
	})

	requireRawCall(t, "GetBlockCommitment", func() (model.RawResult, error) {
		return client.GetBlockCommitment(context.Background(), SlotQuery{Slot: confirmed[len(confirmed)-1]})
	})
	requireRawCall(t, "GetBlockTime", func() (model.RawResult, error) {
		return client.GetBlockTime(context.Background(), SlotQuery{Slot: confirmed[len(confirmed)-1]})
	})
}

func TestPublicRPC_AccountMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	balanceRaw := requireRawCall(t, "GetBalance", func() (model.RawResult, error) {
		return client.GetBalance(context.Background(), GetBalanceQuery{Pubkey: systemProgramID, Commitment: model.ConfirmedCommitment})
	})
	var balance model.Balance
	if err := json.Unmarshal(balanceRaw, &balance); err != nil {
		t.Fatalf("unmarshal balance: %v", err)
	}

	accountRaw := requireRawCall(t, "GetAccountInfo", func() (model.RawResult, error) {
		return client.GetAccountInfo(context.Background(), GetAccountInfoQuery{Pubkey: systemProgramID, Encoding: EncodingBase64, Commitment: model.ConfirmedCommitment})
	})
	var account model.AccountInfo
	if err := json.Unmarshal(accountRaw, &account); err != nil {
		t.Fatalf("unmarshal account info: %v", err)
	}
	if account.Value == nil {
		t.Fatalf("system program account not found: %s", accountRaw)
	}

	multipleRaw := requireRawCall(t, "GetMultipleAccounts", func() (model.RawResult, error) {
		return client.GetMultipleAccounts(context.Background(), GetMultipleAccountsQuery{Pubkeys: []string{systemProgramID}, Encoding: EncodingBase64, Commitment: model.ConfirmedCommitment})
	})
	var multiple model.MultipleAccounts
	if err := json.Unmarshal(multipleRaw, &multiple); err != nil {
		t.Fatalf("unmarshal multiple accounts: %v", err)
	}
	if len(multiple.Value) != 1 || multiple.Value[0] == nil {
		t.Fatalf("unexpected multiple account result: %s", multipleRaw)
	}

	requirePublicRawCall(t, "GetLargestAccounts", func() (model.RawResult, error) {
		return client.GetLargestAccounts(context.Background(), GetLargestAccountsQuery{Commitment: model.ConfirmedCommitment})
	})
	requireRawCall(t, "GetMinimumBalanceForRentExemption", func() (model.RawResult, error) {
		return client.GetMinimumBalanceForRentExemption(context.Background(), GetMinimumBalanceForRentExemptionQuery{DataSize: 0, Commitment: model.ConfirmedCommitment})
	})
}

func TestPublicRPC_TokenAndSupplyMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	supplyRaw := requireRawCall(t, "GetSupply", func() (model.RawResult, error) {
		return client.GetSupply(context.Background(), GetSupplyQuery{
			Commitment:                        model.ConfirmedCommitment,
			ExcludeNonCirculatingAccountsList: true,
		})
	})
	var supply model.Supply
	if err := json.Unmarshal(supplyRaw, &supply); err != nil {
		t.Fatalf("unmarshal supply: %v", err)
	}
	if supply.Value.Total == 0 {
		t.Fatalf("invalid supply: %s", supplyRaw)
	}

	tokenSupplyRaw := requireRawCall(t, "GetTokenSupply", func() (model.RawResult, error) {
		return client.GetTokenSupply(context.Background(), GetTokenSupplyQuery{Mint: wrappedSOLMint, Commitment: model.ConfirmedCommitment})
	})
	var tokenSupply model.TokenSupply
	if err := json.Unmarshal(tokenSupplyRaw, &tokenSupply); err != nil {
		t.Fatalf("unmarshal token supply: %v", err)
	}
	if tokenSupply.Value.Amount == "" {
		t.Fatalf("missing token supply amount: %s", tokenSupplyRaw)
	}

	requirePublicRawCall(t, "GetTokenLargestAccounts", func() (model.RawResult, error) {
		return client.GetTokenLargestAccounts(context.Background(), GetTokenLargestAccountsQuery{Mint: wrappedSOLMint, Commitment: model.ConfirmedCommitment})
	})
	requireRawCall(t, "GetStakeMinimumDelegation", func() (model.RawResult, error) {
		return client.GetStakeMinimumDelegation(context.Background(), GetStakeMinimumDelegationQuery{Commitment: model.ConfirmedCommitment})
	})
}

func TestPublicRPC_BlockhashAndTransactionLookupMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	blockhashRaw := requireRawCall(t, "GetLatestBlockhash", func() (model.RawResult, error) {
		return client.GetLatestBlockhash(context.Background(), GetLatestBlockhashQuery{Commitment: model.ConfirmedCommitment})
	})
	var blockhash model.LatestBlockhash
	if err := json.Unmarshal(blockhashRaw, &blockhash); err != nil {
		t.Fatalf("unmarshal latest blockhash: %v", err)
	}
	if blockhash.Value.Blockhash == "" {
		t.Fatalf("missing blockhash: %s", blockhashRaw)
	}

	validRaw := requireRawCall(t, "IsBlockhashValid", func() (model.RawResult, error) {
		return client.IsBlockhashValid(context.Background(), IsBlockhashValidQuery{Blockhash: blockhash.Value.Blockhash, Commitment: model.ConfirmedCommitment})
	})
	var valid model.BlockhashValid
	if err := json.Unmarshal(validRaw, &valid); err != nil {
		t.Fatalf("unmarshal blockhash validity: %v", err)
	}
	if !valid.Value {
		t.Fatalf("fresh blockhash reported invalid: %s", validRaw)
	}

	requireRawCall(t, "GetRecentPrioritizationFees", func() (model.RawResult, error) {
		return client.GetRecentPrioritizationFees(context.Background(), GetRecentPrioritizationFeesQuery{})
	})
	requireRawCall(t, "GetTransactionCount", func() (model.RawResult, error) {
		return client.GetTransactionCount(context.Background(), GetTransactionCountQuery{Commitment: model.ConfirmedCommitment})
	})
	requireRawCall(t, "GetSignaturesForAddress", func() (model.RawResult, error) {
		return client.GetSignaturesForAddress(context.Background(), GetSignaturesForAddressQuery{Address: systemProgramID, Limit: 1, Commitment: model.ConfirmedCommitment})
	})
}

func TestPublicRPC_ClusterAndValidatorMethods(t *testing.T) {
	client := newPublicHTTPClient(t)

	nodesRaw := requireRawCall(t, "GetClusterNodes", func() (model.RawResult, error) { return client.GetClusterNodes(context.Background()) })
	var nodes model.ClusterNodes
	if err := json.Unmarshal(nodesRaw, &nodes); err != nil {
		t.Fatalf("unmarshal cluster nodes: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatalf("GetClusterNodes returned no nodes")
	}

	voteRaw := requireRawCall(t, "GetVoteAccounts", func() (model.RawResult, error) { return client.GetVoteAccounts(context.Background()) })
	var voteAccounts model.VoteAccounts
	if err := json.Unmarshal(voteRaw, &voteAccounts); err != nil {
		t.Fatalf("unmarshal vote accounts: %v", err)
	}
	if len(voteAccounts.Current) == 0 {
		t.Fatalf("GetVoteAccounts returned no current accounts")
	}

	requireRawCall(t, "GetInflationGovernor", func() (model.RawResult, error) {
		return client.GetInflationGovernor(context.Background(), GetInflationGovernorQuery{Commitment: model.ConfirmedCommitment})
	})
	requireRawCall(t, "GetInflationRate", func() (model.RawResult, error) { return client.GetInflationRate(context.Background()) })
	requireRawCall(t, "GetLeaderSchedule", func() (model.RawResult, error) {
		return client.GetLeaderSchedule(context.Background(), GetLeaderScheduleQuery{Commitment: model.ConfirmedCommitment})
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
