package client

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/0x626f/ingress/solana/types"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// mockServer creates a test HTTP server that returns the provided response
func mockServer(t *testing.T, expectedMethod string, response interface{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}

		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type: application/json, got %s", r.Header.Get("Content-Type"))
		}

		// Decode request to verify method
		var req struct {
			JsonRpc string          `json:"jsonrpc"`
			Method  string          `json:"method"`
			Id      int             `json:"id"`
			Params  json.RawMessage `json:"params,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
		}

		if req.Method != expectedMethod {
			t.Errorf("Expected method %s, got %s", expectedMethod, req.Method)
		}

		if req.JsonRpc != "2.0" {
			t.Errorf("Expected jsonrpc 2.0, got %s", req.JsonRpc)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
}

// mockErrorServer creates a test HTTP server that returns an error
func mockErrorServer(t *testing.T, statusCode int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
	}))
}

// mockWSServer creates a test WebSocket server that sends the provided messages
// and validates the subscription request.
func mockWSServer(t *testing.T, expectedMethod string, messages []interface{}) (string, func()) {
	// Create a TCP listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Channel to signal server shutdown
	done := make(chan bool)

	// Start server in goroutine
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					return
				}

				go func(c net.Conn) {
					defer c.Close()

					// Upgrade connection to WebSocket
					_, err := ws.Upgrade(c)
					if err != nil {
						t.Logf("WebSocket upgrade failed: %v", err)
						return
					}

					// Read the subscription request
					data, _, err := wsutil.ReadClientData(c)
					if err != nil {
						t.Logf("Failed to read subscription request: %v", err)
						return
					}

					// Verify the subscription method
					var req struct {
						JsonRpc string          `json:"jsonrpc"`
						Method  string          `json:"method"`
						Id      int             `json:"id"`
						Params  json.RawMessage `json:"params,omitempty"`
					}
					if err := json.Unmarshal(data, &req); err != nil {
						t.Logf("Failed to decode subscription request: %v", err)
						return
					}

					if req.Method != expectedMethod {
						t.Errorf("Expected method %s, got %s", expectedMethod, req.Method)
						return
					}

					if req.JsonRpc != "2.0" {
						t.Errorf("Expected jsonrpc 2.0, got %s", req.JsonRpc)
						return
					}

					// Send subscription confirmation
					confirmation := map[string]interface{}{
						"jsonrpc": "2.0",
						"id":      1,
						"result":  1, // subscription ID
					}
					confirmData, _ := json.Marshal(confirmation)
					if err := wsutil.WriteServerText(c, confirmData); err != nil {
						return
					}

					// Send all messages
					for _, msg := range messages {
						msgData, _ := json.Marshal(msg)
						if err := wsutil.WriteServerText(c, msgData); err != nil {
							return
						}
						time.Sleep(10 * time.Millisecond) // Small delay between messages
					}

					// Keep connection open for a bit
					time.Sleep(100 * time.Millisecond)
				}(conn)
			}
		}
	}()

	cleanup := func() {
		close(done)
		listener.Close()
	}

	return "ws://" + listener.Addr().String(), cleanup
}

func mockWSSubscriptionServer(t *testing.T, expectedSubscribe, expectedUnsubscribe string, messages []interface{}) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					return
				}

				go func(c net.Conn) {
					defer c.Close()

					if _, err := ws.Upgrade(c); err != nil {
						t.Logf("WebSocket upgrade failed: %v", err)
						return
					}

					data, _, err := wsutil.ReadClientData(c)
					if err != nil {
						t.Logf("Failed to read subscription request: %v", err)
						return
					}

					var req struct {
						Method string `json:"method"`
					}
					if err := json.Unmarshal(data, &req); err != nil {
						t.Logf("Failed to decode subscription request: %v", err)
						return
					}
					if req.Method != expectedSubscribe {
						t.Errorf("Expected subscribe method %s, got %s", expectedSubscribe, req.Method)
						return
					}

					confirmData, _ := json.Marshal(map[string]interface{}{
						"jsonrpc": "2.0",
						"id":      1,
						"result":  7,
					})
					if err := wsutil.WriteServerText(c, confirmData); err != nil {
						return
					}

					for _, msg := range messages {
						msgData, _ := json.Marshal(msg)
						if err := wsutil.WriteServerText(c, msgData); err != nil {
							return
						}
					}

					data, _, err = wsutil.ReadClientData(c)
					if err != nil {
						return
					}
					if err := json.Unmarshal(data, &req); err != nil {
						t.Logf("Failed to decode unsubscribe request: %v", err)
						return
					}
					if req.Method != expectedUnsubscribe {
						t.Errorf("Expected unsubscribe method %s, got %s", expectedUnsubscribe, req.Method)
					}
					responseData, _ := json.Marshal(map[string]interface{}{
						"jsonrpc": "2.0",
						"id":      2,
						"result":  true,
					})
					_ = wsutil.WriteServerText(c, responseData)
				}(conn)
			}
		}
	}()

	cleanup := func() {
		close(done)
		listener.Close()
	}

	return "ws://" + listener.Addr().String(), cleanup
}

func newHTTPClientForTest(t *testing.T, resource string) *Client {
	t.Helper()
	raw, err := NewRawClient(&ClientConfig{
		Resources:              []string{resource},
		ErrorOnInvalidResource: true,
	})
	if err != nil {
		t.Fatalf("NewRawClient: %v", err)
	}
	return raw.HTTP()
}

func newWSClientForTest(t *testing.T, ctx context.Context, resource string, streamSize int) *Client {
	t.Helper()
	raw, err := NewRawClientWithContext(ctx, &ClientConfig{
		Resources:              []string{resource},
		ErrorOnInvalidResource: true,
		SubscriptionStreamSize: streamSize,
	})
	if err != nil {
		t.Fatalf("NewRawClientWithContext: %v", err)
	}
	return raw.WS()
}

func TestNewRawClient_MultiResourceConfig(t *testing.T) {
	httpServer := mockServer(t, RPCMethodGetHealth, Response{
		JsonRpc: "2.0",
		Id:      1,
		Result:  json.RawMessage(`"ok"`),
	})
	defer httpServer.Close()

	wsURL, cleanup := mockWSSubscriptionServer(t, RPCMethodSlotSubscribe, RPCMethodSlotUnsubscribe, []interface{}{
		map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "slotNotification",
			"params": map[string]interface{}{
				"subscription": 7,
				"result": map[string]interface{}{
					"slot": 1,
				},
			},
		},
	})
	defer cleanup()

	raw, err := NewRawClient(&ClientConfig{
		Resources: []string{
			"ftp://invalid",
			httpServer.URL,
			wsURL,
		},
		SubscriptionStreamSize: 10,
	})
	if err != nil {
		t.Fatalf("NewRawClient failed: %v", err)
	}
	if raw.HTTP() == nil {
		t.Fatal("expected HTTP thin client")
	}
	if raw.WS() == nil {
		t.Fatal("expected WS thin client")
	}

	if result, err := raw.HTTP().GetHealth(); err != nil {
		t.Fatalf("HTTP GetHealth failed: %v", err)
	} else if string(result) != `"ok"` {
		t.Fatalf("unexpected health result: %s", result)
	}

	subscription, err := raw.WS().SlotSubscribe()
	if err != nil {
		t.Fatalf("WS SlotSubscribe failed: %v", err)
	}
	if subscription.ID != 7 {
		t.Fatalf("expected subscription id 7, got %d", subscription.ID)
	}
	if err := subscription.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}
}

func TestNewRawClient_ResourcesFallbackOrder(t *testing.T) {
	server := mockServer(t, RPCMethodGetHealth, Response{
		JsonRpc: "2.0",
		Id:      1,
		Result:  json.RawMessage(`"ok"`),
	})
	defer server.Close()

	raw, err := NewRawClient(&ClientConfig{
		Resources: []string{
			"http://127.0.0.1:1",
			server.URL,
		},
	})
	if err != nil {
		t.Fatalf("NewRawClient failed: %v", err)
	}

	if result, err := raw.HTTP().GetHealth(); err != nil {
		t.Fatalf("fallback GetHealth failed: %v", err)
	} else if string(result) != `"ok"` {
		t.Fatalf("unexpected health result: %s", result)
	}
}

func TestNewRawClient_NoValidResources(t *testing.T) {
	if _, err := NewRawClient(&ClientConfig{Resources: []string{"ftp://invalid"}}); err == nil {
		t.Fatal("expected error for no valid resources")
	}
}

func mockServerWithRequest(t *testing.T, check func(req struct {
	JsonRpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Id      int             `json:"id"`
	Params  json.RawMessage `json:"params,omitempty"`
})) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			JsonRpc string          `json:"jsonrpc"`
			Method  string          `json:"method"`
			Id      int             `json:"id"`
			Params  json.RawMessage `json:"params,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("failed to decode request: %v", err)
		}
		check(req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case RPCMethodGetSlot:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":123}`))
			return
		case RPCMethodGetSlotLeaders:
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":["leader"]}`))
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
	}))
}

func TestClient_HTTPMethodWrappersBuildRequests(t *testing.T) {
	testSlot := types.Slot(12)
	filter := map[string]string{"mint": "So11111111111111111111111111111111111111112"}
	config := map[string]string{"encoding": "base64"}

	tests := []struct {
		name   string
		method string
		call   func(*Client) error
	}{
		{"RawCall", "customMethod", func(c *Client) error { _, err := c.RawCall("customMethod", "x"); return err }},
		{"GetAccountInfo", RPCMethodGetAccountInfo, func(c *Client) error { _, err := c.GetAccountInfo("acct", config); return err }},
		{"GetBalance", RPCMethodGetBalance, func(c *Client) error { _, err := c.GetBalance("acct", config); return err }},
		{"GetLargestAccounts", RPCMethodGetLargestAccounts, func(c *Client) error { _, err := c.GetLargestAccounts(config); return err }},
		{"GetMinimumBalanceForRentExemption", RPCMethodGetMinimumBalanceForRentExemption, func(c *Client) error {
			_, err := c.GetMinimumBalanceForRentExemption(165, FinalizedCommitment)
			return err
		}},
		{"GetMultipleAccounts", RPCMethodGetMultipleAccounts, func(c *Client) error { _, err := c.GetMultipleAccounts([]string{"a", "b"}, config); return err }},
		{"GetProgramAccounts", RPCMethodGetProgramAccounts, func(c *Client) error { _, err := c.GetProgramAccounts("program", config); return err }},
		{"GetTokenAccountBalance", RPCMethodGetTokenAccountBalance, func(c *Client) error { _, err := c.GetTokenAccountBalance("token", ConfirmedCommitment); return err }},
		{"GetTokenAccountsByDelegate", RPCMethodGetTokenAccountsByDelegate, func(c *Client) error { _, err := c.GetTokenAccountsByDelegate("delegate", filter, config); return err }},
		{"GetTokenAccountsByOwner", RPCMethodGetTokenAccountsByOwner, func(c *Client) error { _, err := c.GetTokenAccountsByOwner("owner", filter, config); return err }},
		{"GetTokenLargestAccounts", RPCMethodGetTokenLargestAccounts, func(c *Client) error { _, err := c.GetTokenLargestAccounts("mint", ConfirmedCommitment); return err }},
		{"GetTokenSupply", RPCMethodGetTokenSupply, func(c *Client) error { _, err := c.GetTokenSupply("mint", ConfirmedCommitment); return err }},
		{"GetFeeForMessage", RPCMethodGetFeeForMessage, func(c *Client) error { _, err := c.GetFeeForMessage("message", ConfirmedCommitment); return err }},
		{"GetLatestBlockhash", RPCMethodGetLatestBlockhash, func(c *Client) error { _, err := c.GetLatestBlockhash(ConfirmedCommitment); return err }},
		{"GetRecentPrioritizationFees", RPCMethodGetRecentPrioritizationFees, func(c *Client) error { _, err := c.GetRecentPrioritizationFees("acct"); return err }},
		{"GetSignaturesForAddress", RPCMethodGetSignaturesForAddress, func(c *Client) error { _, err := c.GetSignaturesForAddress("addr", config); return err }},
		{"GetSignatureStatuses", RPCMethodGetSignatureStatuses, func(c *Client) error { _, err := c.GetSignatureStatuses([]string{"sig"}, config); return err }},
		{"GetTransaction", RPCMethodGetTransaction, func(c *Client) error { _, err := c.GetTransaction("sig", config); return err }},
		{"GetTransactionCount", RPCMethodGetTransactionCount, func(c *Client) error { _, err := c.GetTransactionCount(ConfirmedCommitment); return err }},
		{"IsBlockhashValid", RPCMethodIsBlockhashValid, func(c *Client) error { _, err := c.IsBlockhashValid("hash", ConfirmedCommitment); return err }},
		{"RequestAirdrop", RPCMethodRequestAirdrop, func(c *Client) error { _, err := c.RequestAirdrop("acct", 1, ConfirmedCommitment); return err }},
		{"SendTransaction", RPCMethodSendTransaction, func(c *Client) error { _, err := c.SendTransaction([]byte("tx"), config); return err }},
		{"SendEncodedTransaction", RPCMethodSendTransaction, func(c *Client) error { _, err := c.SendEncodedTransaction("encoded", config); return err }},
		{"SimulateEncodedTransaction", RPCMethodSimulateTransaction, func(c *Client) error { _, err := c.SimulateEncodedTransaction("encoded", config); return err }},
		{"GetBlock", RPCMethodGetBlock, func(c *Client) error { _, err := c.GetBlock(1, ConfirmedCommitment); return err }},
		{"GetBlockCommitment", RPCMethodGetBlockCommitment, func(c *Client) error { _, err := c.GetBlockCommitment(1); return err }},
		{"GetBlockHeight", RPCMethodGetBlockHeight, func(c *Client) error { _, err := c.GetBlockHeight(ConfirmedCommitment); return err }},
		{"GetBlockProduction", RPCMethodGetBlockProduction, func(c *Client) error { _, err := c.GetBlockProduction(config); return err }},
		{"GetBlocks", RPCMethodGetBlocks, func(c *Client) error { _, err := c.GetBlocks(1, &testSlot, ConfirmedCommitment); return err }},
		{"GetBlocksWithLimit", RPCMethodGetBlocksWithLimit, func(c *Client) error { _, err := c.GetBlocksWithLimit(1, 2, ConfirmedCommitment); return err }},
		{"GetBlockTime", RPCMethodGetBlockTime, func(c *Client) error { _, err := c.GetBlockTime(1); return err }},
		{"GetFirstAvailableBlock", RPCMethodGetFirstAvailableBlock, func(c *Client) error { _, err := c.GetFirstAvailableBlock(); return err }},
		{"GetRecentPerformanceSamples", RPCMethodGetRecentPerformanceSamples, func(c *Client) error { _, err := c.GetRecentPerformanceSamples(1); return err }},
		{"MinimumLedgerSlot", RPCMethodMinimumLedgerSlot, func(c *Client) error { _, err := c.MinimumLedgerSlot(); return err }},
		{"GetClusterNodes", RPCMethodGetClusterNodes, func(c *Client) error { _, err := c.GetClusterNodes(); return err }},
		{"GetEpochInfo", RPCMethodGetEpochInfo, func(c *Client) error { _, err := c.GetEpochInfo(ConfirmedCommitment); return err }},
		{"GetEpochSchedule", RPCMethodGetEpochSchedule, func(c *Client) error { _, err := c.GetEpochSchedule(); return err }},
		{"GetGenesisHash", RPCMethodGetGenesisHash, func(c *Client) error { _, err := c.GetGenesisHash(); return err }},
		{"GetHealth", RPCMethodGetHealth, func(c *Client) error { _, err := c.GetHealth(); return err }},
		{"GetHighestSnapshotSlot", RPCMethodGetHighestSnapshotSlot, func(c *Client) error { _, err := c.GetHighestSnapshotSlot(); return err }},
		{"GetIdentity", RPCMethodGetIdentity, func(c *Client) error { _, err := c.GetIdentity(); return err }},
		{"GetLeaderSchedule", RPCMethodGetLeaderSchedule, func(c *Client) error { _, err := c.GetLeaderSchedule(&testSlot, config); return err }},
		{"GetMaxRetransmitSlot", RPCMethodGetMaxRetransmitSlot, func(c *Client) error { _, err := c.GetMaxRetransmitSlot(); return err }},
		{"GetMaxShredInsertSlot", RPCMethodGetMaxShredInsertSlot, func(c *Client) error { _, err := c.GetMaxShredInsertSlot(); return err }},
		{"GetSlot", RPCMethodGetSlot, func(c *Client) error { _, err := c.GetSlot(ConfirmedCommitment); return err }},
		{"GetSlotLeader", RPCMethodGetSlotLeader, func(c *Client) error { _, err := c.GetSlotLeader(ConfirmedCommitment); return err }},
		{"GetSlotLeaders", RPCMethodGetSlotLeaders, func(c *Client) error { _, err := c.GetSlotLeaders(1, 2); return err }},
		{"GetVersion", RPCMethodGetVersion, func(c *Client) error { _, err := c.GetVersion(); return err }},
		{"GetVoteAccounts", RPCMethodGetVoteAccounts, func(c *Client) error { _, err := c.GetVoteAccounts(); return err }},
		{"GetInflationGovernor", RPCMethodGetInflationGovernor, func(c *Client) error { _, err := c.GetInflationGovernor(ConfirmedCommitment); return err }},
		{"GetInflationRate", RPCMethodGetInflationRate, func(c *Client) error { _, err := c.GetInflationRate(); return err }},
		{"GetInflationReward", RPCMethodGetInflationReward, func(c *Client) error { _, err := c.GetInflationReward([]string{"acct"}, config); return err }},
		{"GetStakeMinimumDelegation", RPCMethodGetStakeMinimumDelegation, func(c *Client) error { _, err := c.GetStakeMinimumDelegation(ConfirmedCommitment); return err }},
		{"GetSupply", RPCMethodGetSupply, func(c *Client) error { _, err := c.GetSupply(config); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := mockServerWithRequest(t, func(req struct {
				JsonRpc string          `json:"jsonrpc"`
				Method  string          `json:"method"`
				Id      int             `json:"id"`
				Params  json.RawMessage `json:"params,omitempty"`
			}) {
				if req.JsonRpc != "2.0" {
					t.Errorf("expected jsonrpc 2.0, got %s", req.JsonRpc)
				}
				if req.Method != tt.method {
					t.Errorf("expected method %s, got %s", tt.method, req.Method)
				}
			})
			defer server.Close()

			client := newHTTPClientForTest(t, server.URL)
			if err := tt.call(client); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestClient_WSMethodWrappersBuildRequests(t *testing.T) {
	message := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notification",
		"params": map[string]interface{}{
			"subscription": 7,
			"result": map[string]interface{}{
				"ok": true,
			},
		},
	}

	tests := []struct {
		name        string
		subscribe   string
		unsubscribe string
		call        func(*Client) (*Subscription, error)
	}{
		{"RawSubscribe", "customSubscribe", "customUnsubscribe", func(c *Client) (*Subscription, error) {
			return c.RawSubscribe("customSubscribe", "customUnsubscribe", "x")
		}},
		{"AccountSubscribe", RPCMethodAccountSubscribe, RPCMethodAccountUnsubscribe, func(c *Client) (*Subscription, error) { return c.AccountSubscribe("acct") }},
		{"BlockSubscribe", RPCMethodBlockSubscribe, RPCMethodBlockUnsubscribe, func(c *Client) (*Subscription, error) { return c.BlockSubscribe("all") }},
		{"LogsSubscribe", RPCMethodLogsSubscribe, RPCMethodLogsUnsubscribe, func(c *Client) (*Subscription, error) { return c.LogsSubscribe("all") }},
		{"ProgramSubscribe", RPCMethodProgramSubscribe, RPCMethodProgramUnsubscribe, func(c *Client) (*Subscription, error) { return c.ProgramSubscribe("program") }},
		{"RootSubscribe", RPCMethodRootSubscribe, RPCMethodRootUnsubscribe, func(c *Client) (*Subscription, error) { return c.RootSubscribe() }},
		{"SignatureSubscribe", RPCMethodSignatureSubscribe, RPCMethodSignatureUnsubscribe, func(c *Client) (*Subscription, error) { return c.SignatureSubscribe("sig") }},
		{"SlotSubscribe", RPCMethodSlotSubscribe, RPCMethodSlotUnsubscribe, func(c *Client) (*Subscription, error) { return c.SlotSubscribe() }},
		{"SlotsUpdatesSubscribe", RPCMethodSlotsUpdatesSubscribe, RPCMethodSlotsUpdatesUnsubscribe, func(c *Client) (*Subscription, error) { return c.SlotsUpdatesSubscribe() }},
		{"VoteSubscribe", RPCMethodVoteSubscribe, RPCMethodVoteUnsubscribe, func(c *Client) (*Subscription, error) { return c.VoteSubscribe() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wsURL, cleanup := mockWSSubscriptionServer(t, tt.subscribe, tt.unsubscribe, []interface{}{message})
			defer cleanup()

			client := newWSClientForTest(t, context.Background(), wsURL, 10)

			subscription, err := tt.call(client)
			if err != nil {
				t.Fatalf("subscribe failed: %v", err)
			}
			if subscription.ID != 7 {
				t.Fatalf("expected subscription id 7, got %d", subscription.ID)
			}

			select {
			case event := <-subscription.Events:
				if event.Error != nil {
					t.Fatalf("unexpected event error: %v", event.Error)
				}
				if len(event.Data) == 0 {
					t.Fatal("expected raw event data")
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for event")
			}

			if err := subscription.Unsubscribe(); err != nil {
				t.Fatalf("unsubscribe failed: %v", err)
			}
		})
	}
}

func TestClient_LiveRPCActions(t *testing.T) {
	rpcURL := os.Getenv("SOLANA_RPC_HTTP_URL")
	if rpcURL == "" {
		t.Skip("set SOLANA_RPC_HTTP_URL to run live Solana RPC actions")
	}

	client := newHTTPClientForTest(t, rpcURL)

	if result, err := client.GetHealth(); err != nil {
		t.Fatalf("GetHealth: %v", err)
	} else if len(result) == 0 {
		t.Fatal("GetHealth returned empty result")
	}

	slot, err := client.GetSlot(ConfirmedCommitment)
	if err != nil {
		t.Fatalf("GetSlot: %v", err)
	}
	if slot == 0 {
		t.Fatal("GetSlot returned zero")
	}

	if result, err := client.GetLatestBlockhash(ConfirmedCommitment); err != nil {
		t.Fatalf("GetLatestBlockhash: %v", err)
	} else if len(result) == 0 {
		t.Fatal("GetLatestBlockhash returned empty result")
	}
}

func TestClient_GetSlot(t *testing.T) {
	t.Run("successful get slot", func(t *testing.T) {
		expectedSlot := types.Slot(123456789)
		server := mockServer(t, RPCMethodGetSlot, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`123456789`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		slot, err := client.GetSlot(ProcessedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if slot != expectedSlot {
			t.Errorf("Expected slot %d, got %d", expectedSlot, slot)
		}
	})

	t.Run("get slot with different commitments", func(t *testing.T) {
		commitments := []types.Commitment{ProcessedCommitment, ConfirmedCommitment, FinalizedCommitment}

		for _, commitment := range commitments {
			server := mockServer(t, RPCMethodGetSlot, Response{
				JsonRpc: "2.0",
				Id:      1,
				Result:  json.RawMessage(`100`),
			})

			client := newHTTPClientForTest(t, server.URL)
			slot, err := client.GetSlot(commitment)

			if err != nil {
				t.Errorf("Expected no error for commitment %s, got %v", commitment, err)
			}

			if slot != 100 {
				t.Errorf("Expected slot 100, got %d", slot)
			}

			server.Close()
		}
	})

	t.Run("failed get slot - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusInternalServerError)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetSlot(ProcessedCommitment)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetEpochInfo(t *testing.T) {
	t.Run("successful get epoch info", func(t *testing.T) {
		epochData := map[string]interface{}{
			"absoluteSlot": 166598,
			"epoch":        27,
			"slotIndex":    2790,
			"slotsInEpoch": 8192,
		}
		epochJSON, _ := json.Marshal(epochData)

		server := mockServer(t, RPCMethodGetEpochInfo, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(epochJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetEpochInfo(ConfirmedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected result, got nil")
		}

		// Verify we can parse the result
		var epochInfo types.EpochInfo
		if err := json.Unmarshal(result, &epochInfo); err != nil {
			t.Errorf("Failed to unmarshal epoch info: %v", err)
		}

		if epochInfo.AbsoluteSlot != 166598 {
			t.Errorf("Expected absoluteSlot 166598, got %d", epochInfo.AbsoluteSlot)
		}

		if epochInfo.Epoch != 27 {
			t.Errorf("Expected epoch 27, got %d", epochInfo.Epoch)
		}
	})

	t.Run("failed get epoch info - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusServiceUnavailable)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetEpochInfo(FinalizedCommitment)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetSlotLeaders(t *testing.T) {
	t.Run("successful get slot leaders", func(t *testing.T) {
		leaders := []string{
			"ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH15n",
			"ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH16n",
			"ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH17n",
		}
		leadersJSON, _ := json.Marshal(leaders)

		server := mockServer(t, RPCMethodGetSlotLeaders, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(leadersJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetSlotLeaders(100, 3)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 3 {
			t.Errorf("Expected 3 leaders, got %d", len(result))
		}

		for i, leader := range result {
			if leader != leaders[i] {
				t.Errorf("Expected leader %s at index %d, got %s", leaders[i], i, leader)
			}
		}
	})

	t.Run("get slot leaders with empty result", func(t *testing.T) {
		server := mockServer(t, RPCMethodGetSlotLeaders, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`[]`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetSlotLeaders(100, 10)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected empty slice, got nil")
		}

		if len(result) != 0 {
			t.Errorf("Expected 0 leaders, got %d", len(result))
		}
	})

	t.Run("failed get slot leaders - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusBadRequest)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetSlotLeaders(100, 5000)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetClusterNodes(t *testing.T) {
	t.Run("successful get cluster nodes", func(t *testing.T) {
		nodes := []map[string]interface{}{
			{
				"pubkey":  "Bx8D2AkvGQG3eSw6RkdPSyW8Zxw6o9EQW6Z8CJ4C6gqh",
				"gossip":  "127.0.0.1:8001",
				"tpu":     "127.0.0.1:8003",
				"rpc":     "127.0.0.1:8899",
				"version": "1.14.0",
			},
		}
		nodesJSON, _ := json.Marshal(nodes)

		server := mockServer(t, RPCMethodGetClusterNodes, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(nodesJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetClusterNodes()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected result, got nil")
		}

		// Verify we can parse the result
		var nodesList []map[string]interface{}
		if err := json.Unmarshal(result, &nodesList); err != nil {
			t.Errorf("Failed to unmarshal nodes: %v", err)
		}

		if len(nodesList) != 1 {
			t.Errorf("Expected 1 node, got %d", len(nodesList))
		}
	})

	t.Run("failed get cluster nodes - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusInternalServerError)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetClusterNodes()

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetVoteAccounts(t *testing.T) {
	t.Run("successful get vote accounts", func(t *testing.T) {
		voteAccounts := map[string]interface{}{
			"current": []map[string]interface{}{
				{
					"nodePubkey":     "B97CCUW3AEZFGy6uUg6zUdnNYvnVq5VG8PUtb2HayTDD",
					"activatedStake": 42,
					"commission":     0,
				},
			},
			"delinquent": []map[string]interface{}{},
		}
		accountsJSON, _ := json.Marshal(voteAccounts)

		server := mockServer(t, RPCMethodGetVoteAccounts, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(accountsJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetVoteAccounts()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected result, got nil")
		}

		// Verify we can parse the result
		var accounts types.VoteAccounts
		if err := json.Unmarshal(result, &accounts); err != nil {
			t.Errorf("Failed to unmarshal vote accounts: %v", err)
		}

		if len(accounts.Current) != 1 {
			t.Errorf("Expected 1 current vote account, got %d", len(accounts.Current))
		}
	})

	t.Run("failed get vote accounts - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusGatewayTimeout)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetVoteAccounts()

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_SimulateTransaction(t *testing.T) {
	t.Run("successful simulation", func(t *testing.T) {
		simulationResult := map[string]interface{}{
			"value": map[string]interface{}{
				"err":  nil,
				"logs": []string{"Program log: Hello, World!"},
			},
		}
		resultJSON, _ := json.Marshal(simulationResult)

		server := mockServer(t, RPCMethodSimulateTransaction, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(resultJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		txData := []byte("test-transaction-data")
		err := client.SimulateTransaction(txData, ProcessedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("simulation with transaction error", func(t *testing.T) {
		errorMsg := "InsufficientFundsForFee"
		simulationResult := map[string]interface{}{
			"value": map[string]interface{}{
				"err": errorMsg,
			},
		}
		resultJSON, _ := json.Marshal(simulationResult)

		server := mockServer(t, RPCMethodSimulateTransaction, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(resultJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		txData := []byte("test-transaction-data")
		err := client.SimulateTransaction(txData, ConfirmedCommitment)

		if err == nil {
			t.Error("Expected error for failed simulation, got nil")
		}

		if err.Error() != errorMsg {
			t.Errorf("Expected error message %s, got %s", errorMsg, err.Error())
		}
	})

	t.Run("failed simulation - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusInternalServerError)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		txData := []byte("test-transaction-data")
		err := client.SimulateTransaction(txData, FinalizedCommitment)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetTransaction(t *testing.T) {
	t.Run("successful get transaction", func(t *testing.T) {
		signature := "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"
		txData := map[string]interface{}{
			"slot":      123456,
			"blockTime": 1234567890,
			"meta": map[string]interface{}{
				"err": nil,
			},
		}
		txJSON, _ := json.Marshal(txData)

		server := mockServer(t, RPCMethodGetTransaction, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(txJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetTransaction(signature)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected result, got nil")
		}

		// Verify we can parse the result
		var tx map[string]interface{}
		if err := json.Unmarshal(result, &tx); err != nil {
			t.Errorf("Failed to unmarshal transaction: %v", err)
		}

		if tx["slot"] != float64(123456) {
			t.Errorf("Expected slot 123456, got %v", tx["slot"])
		}
	})

	t.Run("get transaction with null result", func(t *testing.T) {
		server := mockServer(t, RPCMethodGetTransaction, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`null`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetTransaction("invalid-signature")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Result should be the string "null"
		if string(result) != "null" {
			t.Errorf("Expected null result, got %s", string(result))
		}
	})

	t.Run("failed get transaction - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusNotFound)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetTransaction("some-signature")

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetBlock(t *testing.T) {
	t.Run("successful get block", func(t *testing.T) {
		blockData := map[string]interface{}{
			"blockhash":         "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG",
			"previousBlockhash": "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn",
			"parentSlot":        123456,
			"transactions":      []interface{}{},
			"rewards":           []interface{}{},
		}
		blockJSON, _ := json.Marshal(blockData)

		server := mockServer(t, RPCMethodGetBlock, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(blockJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetBlock(123456, ConfirmedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected result, got nil")
		}

		// Verify we can parse the result
		var block map[string]interface{}
		if err := json.Unmarshal(result, &block); err != nil {
			t.Errorf("Failed to unmarshal block: %v", err)
		}

		if block["blockhash"] != "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG" {
			t.Errorf("Expected specific blockhash, got %v", block["blockhash"])
		}

		if block["parentSlot"] != float64(123456) {
			t.Errorf("Expected parentSlot 123456, got %v", block["parentSlot"])
		}
	})

	t.Run("get block with different commitments", func(t *testing.T) {
		commitments := []types.Commitment{ConfirmedCommitment, FinalizedCommitment}

		for _, commitment := range commitments {
			blockData := map[string]interface{}{
				"blockhash":  "test-hash",
				"parentSlot": 100,
			}
			blockJSON, _ := json.Marshal(blockData)

			server := mockServer(t, RPCMethodGetBlock, Response{
				JsonRpc: "2.0",
				Id:      1,
				Result:  json.RawMessage(blockJSON),
			})

			client := newHTTPClientForTest(t, server.URL)
			result, err := client.GetBlock(100, commitment)

			if err != nil {
				t.Errorf("Expected no error for commitment %s, got %v", commitment, err)
			}

			if result == nil {
				t.Errorf("Expected result for commitment %s, got nil", commitment)
			}

			server.Close()
		}
	})

	t.Run("get block with null result", func(t *testing.T) {
		server := mockServer(t, RPCMethodGetBlock, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`null`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetBlock(999999, FinalizedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if string(result) != "null" {
			t.Errorf("Expected null result, got %s", string(result))
		}
	})

	t.Run("failed get block - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusInternalServerError)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetBlock(123456, ConfirmedCommitment)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})
}

func TestClient_GetConfirmedSlots(t *testing.T) {
	t.Run("successful get confirmed slots", func(t *testing.T) {
		slots := []uint64{100, 101, 102, 103, 104, 105}
		slotsJSON, _ := json.Marshal(slots)

		server := mockServer(t, RPCMethodGetBlocks, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(slotsJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetConfirmedSlots(100, 105, ConfirmedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 6 {
			t.Errorf("Expected 6 slots, got %d", len(result))
		}

		for i, expectedSlot := range slots {
			if result[i] != expectedSlot {
				t.Errorf("Slot %d: expected %d, got %d", i, expectedSlot, result[i])
			}
		}
	})

	t.Run("get confirmed slots with different commitments", func(t *testing.T) {
		commitments := []types.Commitment{ProcessedCommitment, ConfirmedCommitment, FinalizedCommitment}

		for _, commitment := range commitments {
			slots := []uint64{200, 201, 202}
			slotsJSON, _ := json.Marshal(slots)

			server := mockServer(t, RPCMethodGetBlocks, Response{
				JsonRpc: "2.0",
				Id:      1,
				Result:  json.RawMessage(slotsJSON),
			})

			client := newHTTPClientForTest(t, server.URL)
			result, err := client.GetConfirmedSlots(200, 202, commitment)

			if err != nil {
				t.Errorf("Expected no error for commitment %s, got %v", commitment, err)
			}

			if len(result) != 3 {
				t.Errorf("Expected 3 slots for commitment %s, got %d", commitment, len(result))
			}

			server.Close()
		}
	})

	t.Run("get confirmed slots with empty result", func(t *testing.T) {
		server := mockServer(t, RPCMethodGetBlocks, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`[]`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetConfirmedSlots(1000, 1100, FinalizedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if result == nil {
			t.Error("Expected empty slice, got nil")
		}

		if len(result) != 0 {
			t.Errorf("Expected 0 slots, got %d", len(result))
		}
	})

	t.Run("get confirmed slots with large range", func(t *testing.T) {
		// Simulate a larger range of slots
		var slots []uint64
		for i := uint64(5000); i <= 5100; i++ {
			slots = append(slots, i)
		}
		slotsJSON, _ := json.Marshal(slots)

		server := mockServer(t, RPCMethodGetBlocks, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(slotsJSON),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		result, err := client.GetConfirmedSlots(5000, 5100, ConfirmedCommitment)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 101 {
			t.Errorf("Expected 101 slots, got %d", len(result))
		}

		if result[0] != 5000 {
			t.Errorf("Expected first slot 5000, got %d", result[0])
		}

		if result[len(result)-1] != 5100 {
			t.Errorf("Expected last slot 5100, got %d", result[len(result)-1])
		}
	})

	t.Run("failed get confirmed slots - server error", func(t *testing.T) {
		server := mockErrorServer(t, http.StatusServiceUnavailable)
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetConfirmedSlots(100, 200, ConfirmedCommitment)

		if err == nil {
			t.Error("Expected error, got nil")
		}
	})

	t.Run("failed get confirmed slots - invalid JSON response", func(t *testing.T) {
		server := mockServer(t, RPCMethodGetBlocks, Response{
			JsonRpc: "2.0",
			Id:      1,
			Result:  json.RawMessage(`"invalid-not-an-array"`),
		})
		defer server.Close()

		client := newHTTPClientForTest(t, server.URL)
		_, err := client.GetConfirmedSlots(100, 200, ConfirmedCommitment)

		if err == nil {
			t.Error("Expected error for invalid JSON, got nil")
		}
	})
}

// WebSocket Subscription Tests

func TestClient_SubscribeSlot(t *testing.T) {
	t.Run("successful slot subscription with multiple updates", func(t *testing.T) {
		// Create mock slot update messages
		messages := []interface{}{
			map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "slotNotification",
				"params": map[string]interface{}{
					"result": map[string]interface{}{
						"slot":   100,
						"parent": 99,
						"root":   98,
					},
					"subscription": 1,
				},
			},
			map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "slotNotification",
				"params": map[string]interface{}{
					"result": map[string]interface{}{
						"slot":   101,
						"parent": 100,
						"root":   99,
					},
					"subscription": 1,
				},
			},
			map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "slotNotification",
				"params": map[string]interface{}{
					"result": map[string]interface{}{
						"slot":   102,
						"parent": 101,
						"root":   100,
					},
					"subscription": 1,
				},
			},
		}

		wsURL, cleanup := mockWSServer(t, RPCMethodSlotSubscribe, messages)
		defer cleanup()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		client := newWSClientForTest(t, ctx, wsURL, 10)

		events, err := client.SubscribeSlot()
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// Collect slot updates (skip confirmation message which returns slot 0)
		var slots []types.Slot
		timeout := time.After(1 * time.Second)
		expectedSlots := []types.Slot{100, 101, 102}

	collectLoop:
		for {
			select {
			case event, ok := <-events:
				if !ok {
					break collectLoop
				}
				if event.Error != nil {
					t.Logf("Received error event: %v", event.Error)
					continue
				}
				// Skip confirmation message (slot 0)
				if event.Data != 0 {
					slots = append(slots, event.Data)
				}
				if len(slots) >= len(expectedSlots) {
					break collectLoop
				}
			case <-timeout:
				break collectLoop
			}
		}

		if len(slots) < len(expectedSlots) {
			t.Errorf("Expected at least %d slot updates, got %d", len(expectedSlots), len(slots))
		}

		for i, expectedSlot := range expectedSlots {
			if i >= len(slots) {
				break
			}
			if slots[i] != expectedSlot {
				t.Errorf("ParentSlot %d: expected %d, got %d", i, expectedSlot, slots[i])
			}
		}
	})

	t.Run("slot subscription with context cancellation", func(t *testing.T) {
		// Create a long-running subscription
		messages := []interface{}{
			map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "slotNotification",
				"params": map[string]interface{}{
					"result": map[string]interface{}{
						"slot":   100,
						"parent": 99,
						"root":   98,
					},
					"subscription": 1,
				},
			},
		}

		wsURL, cleanup := mockWSServer(t, RPCMethodSlotSubscribe, messages)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())

		client := newWSClientForTest(t, ctx, wsURL, 10)

		events, err := client.SubscribeSlot()
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// Receive at least one event (skip confirmation if slot == 0)
		receivedActualEvent := false
		for !receivedActualEvent {
			event := <-events
			if event.Error != nil {
				t.Fatalf("Unexpected error: %v", event.Error)
			}
			if event.Data != 0 {
				receivedActualEvent = true
			}
		}

		// Cancel context
		cancel()

		// Wait a bit for goroutine to process cancellation
		time.Sleep(50 * time.Millisecond)

		// Try to receive from channel - should either get nothing or channel should close
		timeout := time.After(200 * time.Millisecond)
		channelClosed := false

		for !channelClosed {
			select {
			case _, ok := <-events:
				if !ok {
					channelClosed = true
				}
			case <-timeout:
				// Timeout reached, channel should be closed or closing
				channelClosed = true
			}
		}
	})

	t.Run("slot subscription with invalid websocket URL", func(t *testing.T) {
		ctx := context.Background()
		client := newWSClientForTest(t, ctx, "ws://invalid-url-does-not-exist-12345.com", 10)

		_, err := client.SubscribeSlot()
		if err == nil {
			t.Error("Expected error for invalid WebSocket URL, got nil")
		}
	})

	t.Run("slot subscription handles malformed JSON", func(t *testing.T) {
		// Create a listener that sends invalid JSON
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create listener: %v", err)
		}
		defer listener.Close()

		done := make(chan bool)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			// Upgrade to WebSocket
			_, err = ws.Upgrade(conn)
			if err != nil {
				return
			}

			// Read subscription request
			wsutil.ReadClientData(conn)

			// Send confirmation
			confirmation := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      1,
				"result":  1,
			}
			confirmData, _ := json.Marshal(confirmation)
			wsutil.WriteServerText(conn, confirmData)

			time.Sleep(10 * time.Millisecond)

			// Send invalid JSON
			wsutil.WriteServerText(conn, []byte("{invalid json}"))

			<-done
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		defer close(done)

		wsURL := "ws://" + listener.Addr().String()
		client := newWSClientForTest(t, ctx, wsURL, 10)

		events, err := client.SubscribeSlot()
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// Receive events - first might be confirmation, second should be error
		timeout := time.After(500 * time.Millisecond)
		gotError := false

	errorLoop:
		for {
			select {
			case event, ok := <-events:
				if !ok {
					break errorLoop
				}
				if event.Error != nil {
					gotError = true
					break errorLoop
				}
			case <-timeout:
				break errorLoop
			}
		}

		if !gotError {
			t.Error("Expected error event for malformed JSON")
		}
	})
}
