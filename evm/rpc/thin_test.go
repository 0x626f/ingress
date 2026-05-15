package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/0x626f/ingress/transport"
)

// ============================================================================
// Mock infrastructure
// ============================================================================

type mockConnection struct {
	kind        transport.ConnectionKind
	response    []byte
	err         error
	lastPayload []byte
	callCount   int
}

func (m *mockConnection) Kind() transport.ConnectionKind   { return m.kind }
func (m *mockConnection) Resource() string                 { return "" }
func (m *mockConnection) Timeout() time.Duration           { return 0 }
func (m *mockConnection) Stream() <-chan transport.Message { return nil }
func (m *mockConnection) Send(data []byte) ([]byte, error) {
	m.callCount++
	m.lastPayload = data
	return m.response, m.err
}

// rpcResp builds a JSON-RPC 2.0 string-result response.
func rpcResp(id uint, result string) []byte {
	return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"%s"}`, id, result))
}

// rpcErrResp builds a JSON-RPC 2.0 error response.
func rpcErrResp(id uint, code int, message string) []byte {
	return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":%d,"message":"%s"}}`, id, code, message))
}

type rpcRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      uint            `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func parseReq(t *testing.T, payload []byte) rpcRequest {
	t.Helper()
	var req rpcRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		t.Fatalf("failed to parse RPC request: %v\npayload: %s", err, payload)
	}
	return req
}

// withHTTP returns a ThinClient backed by a single mock HTTP connection.
func withHTTP(mock *mockConnection) *ThinClient {
	mgr := &transport.ConnectionManager{}
	mgr.AddConnection(mock)
	return newThinClient(transport.HTTP, mgr, new(transport.SequenceGenerator), 0)
}

// okHTTP returns a mock HTTP connection with a successful JSON-RPC response.
func okHTTP() *mockConnection {
	return &mockConnection{kind: transport.HTTP, response: rpcResp(1, "0x1")}
}

// failConn returns a mock connection that always fails with a transport error.
func failConn(k transport.ConnectionKind) *mockConnection {
	return &mockConnection{kind: k, err: errors.New("fail")}
}

// ============================================================================
// WS mock infrastructure
// ============================================================================

type mockWSConnection struct {
	kind        transport.ConnectionKind
	events      chan transport.Message
	lastPayload []byte
	callCount   int
	err         error
	result      string
	timeout     time.Duration
	noRespond   bool
}

func (m *mockWSConnection) Kind() transport.ConnectionKind   { return m.kind }
func (m *mockWSConnection) Resource() string                 { return "" }
func (m *mockWSConnection) Timeout() time.Duration           { return m.timeout }
func (m *mockWSConnection) Stream() <-chan transport.Message { return m.events }
func (m *mockWSConnection) Send(data []byte) ([]byte, error) {
	m.callCount++
	m.lastPayload = data
	if m.err != nil {
		return nil, m.err
	}
	if m.noRespond {
		return nil, nil
	}
	var req struct {
		ID uint `json:"id"`
	}
	json.Unmarshal(data, &req)
	resp := rpcResp(req.ID, m.result)
	go func() { m.events <- resp }()
	return nil, nil
}

// withWS returns a ThinClient backed by a single mock WS connection.
func withWS(mock *mockWSConnection) *ThinClient {
	mgr := &transport.ConnectionManager{}
	mgr.AddConnection(mock)
	return newThinClient(transport.WS, mgr, new(transport.SequenceGenerator), 10)
}

// okWS returns a mock WS connection that echoes a successful response.
func okWS() *mockWSConnection {
	return &mockWSConnection{
		kind:   transport.WS,
		events: make(chan transport.Message, 8),
		result: "0x1",
	}
}

// ============================================================================
// NewRawClient
// ============================================================================

func TestNewClient_NoResources_ReturnsError(t *testing.T) {
	if _, err := NewRawClient(&ClientConfig{}); err == nil {
		t.Error("expected error with no resources")
	}
}

func TestNewClient_InvalidResource_SkippedByDefault(t *testing.T) {
	_, err := NewRawClient(&ClientConfig{
		Resources: []string{"ftp://invalid", "ws://localhost:8546"},
	})
	if err != nil {
		t.Errorf("expected invalid resource to be skipped, got: %v", err)
	}
}

func TestNewClient_InvalidResource_ErrorWhenRequired(t *testing.T) {
	_, err := NewRawClient(&ClientConfig{
		Resources:              []string{"ftp://invalid"},
		ErrorOnInvalidResource: true,
	})
	if err == nil {
		t.Error("expected error for invalid resource with ErrorOnInvalidResource=true")
	}
}

func TestNewClient_InvalidResources_ReturnsErrorWhenNoneUsable(t *testing.T) {
	_, err := NewRawClient(&ClientConfig{
		Resources: []string{"ftp://invalid"},
	})
	if err == nil {
		t.Error("expected error when no valid resources remain")
	}
}

func TestNewClient_HTTPResource_CreatesHTTPManager(t *testing.T) {
	c, err := NewRawClient(&ClientConfig{Resources: []string{"https://rpc.example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.http == nil {
		t.Error("expected HTTP manager to be created")
	}
	if c.ws != nil {
		t.Error("expected no WS manager")
	}
	if !c.HasResourceByProtocol(transport.HTTP) {
		t.Error("expected HTTP resources to be available")
	}
	if c.HasResourceByProtocol(transport.WS) {
		t.Error("expected WS resources to be unavailable")
	}
	if c.HTTP() == nil {
		t.Error("expected HTTP thin client")
	}
	if c.WS() != nil {
		t.Error("expected nil WS thin client without WS resources")
	}
}

func TestNewClient_WSResource_CreatesWSManager(t *testing.T) {
	c, err := NewRawClient(&ClientConfig{Resources: []string{"ws://localhost:8546"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.ws == nil {
		t.Error("expected WS manager to be created")
	}
	if !c.HasResourceByProtocol(transport.WS) {
		t.Error("expected WS resources to be available")
	}
	if c.HasResourceByProtocol(transport.HTTP) {
		t.Error("expected HTTP resources to be unavailable")
	}
	if c.WS() == nil {
		t.Error("expected WS thin client")
	}
	if c.HTTP() != nil {
		t.Error("expected nil HTTP thin client without HTTP resources")
	}
}

func TestRawClient_HasResourceByProtocol_UnknownKindReturnsFalse(t *testing.T) {
	c, err := NewRawClient(&ClientConfig{Resources: []string{"ws://localhost:8546"}})
	if err != nil {
		t.Fatal(err)
	}

	if c.HasResourceByProtocol(transport.ConnectionKind(255)) {
		t.Error("expected unknown protocol to be unavailable")
	}
}

// ============================================================================
// ChainId
// ============================================================================

func TestClient_ChainId_Method(t *testing.T) {
	conn := okHTTP()
	if _, err := withHTTP(conn).ChainId(); err != nil {
		t.Fatal(err)
	}
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_chainId" {
		t.Errorf("expected eth_chainId, got %s", req.Method)
	}
	var params []any
	json.Unmarshal(req.Params, &params)
	if len(params) != 0 {
		t.Errorf("expected no params, got %v", params)
	}
}

// ============================================================================
// BlockNumber
// ============================================================================

func TestClient_BlockNumber_Method(t *testing.T) {
	conn := &mockConnection{kind: transport.HTTP, response: rpcResp(1, "0x1234")}
	_, err := withHTTP(conn).BlockNumber()
	if err != nil {
		t.Fatal(err)
	}
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_blockNumber" {
		t.Errorf("expected eth_blockNumber, got %s", req.Method)
	}
}

// ============================================================================
// GetBalance
// ============================================================================

func TestClient_GetBalance_DefaultBlockLatest(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBalance(BalanceQuery{AddressedQuery: AddressedQuery{Address: "0xABC"}})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getBalance" {
		t.Errorf("expected eth_getBalance, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 2 || params[0] != "0xABC" || params[1] != BlockTagLatest {
		t.Errorf("expected [0xABC, latest], got %v", params)
	}
}

func TestClient_GetBalance_CustomBlockTag(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBalance(BalanceQuery{AddressedQuery: AddressedQuery{
		OnBlockQuery: OnBlockQuery{BlockTag: BlockTagEarliest},
		Address:      "0xABC",
	}})
	var params []string
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	if params[1] != BlockTagEarliest {
		t.Errorf("expected 'earliest', got %q", params[1])
	}
}

// ============================================================================
// GetCode
// ============================================================================

func TestClient_GetCode_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetCode(CodeQuery{AddressedQuery: AddressedQuery{Address: "0xDEF"}})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getCode" {
		t.Errorf("expected eth_getCode, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 2 || params[0] != "0xDEF" || params[1] != BlockTagLatest {
		t.Errorf("expected [0xDEF, latest], got %v", params)
	}
}

// ============================================================================
// Call
// ============================================================================

func TestClient_Call_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).Call(CallQuery{To: "0xContract", Data: "0xdeadbeef"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_call" {
		t.Errorf("expected eth_call, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	if len(params) != 2 {
		t.Fatalf("expected 2 params, got %d", len(params))
	}
	var callObj map[string]string
	json.Unmarshal(params[0], &callObj)
	if callObj["to"] != "0xContract" || callObj["data"] != "0xdeadbeef" {
		t.Errorf("unexpected call object: %v", callObj)
	}
	var blockTag string
	json.Unmarshal(params[1], &blockTag)
	if blockTag != BlockTagLatest {
		t.Errorf("expected block tag 'latest', got %q", blockTag)
	}
}

func TestClient_Call_CustomBlockTag(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).Call(CallQuery{OnBlockQuery: OnBlockQuery{BlockTag: "pending"}, To: "0x0"})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var tag string
	json.Unmarshal(params[1], &tag)
	if tag != "pending" {
		t.Errorf("expected 'pending', got %q", tag)
	}
}

// ============================================================================
// EstimateGas
// ============================================================================

func TestClient_EstimateGas_RequiredFieldOnly(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).EstimateGas(EstimateGasQuery{To: "0xRecipient"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_estimateGas" {
		t.Errorf("expected eth_estimateGas, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	var callObj map[string]string
	json.Unmarshal(params[0], &callObj)
	if callObj["to"] != "0xRecipient" {
		t.Errorf("expected to='0xRecipient', got %q", callObj["to"])
	}
	for _, optional := range []string{"from", "data", "gas", "gasPrice", "value", "nonce"} {
		if _, ok := callObj[optional]; ok {
			t.Errorf("expected %q to be absent when empty", optional)
		}
	}
}

func TestClient_EstimateGas_AllFields_HexEncoded(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).EstimateGas(EstimateGasQuery{
		To:                   "0xTo",
		From:                 "0xFrom",
		Data:                 "0xData",
		Gas:                  "21000",
		GasPrice:             "1000000000",
		MaxFeePerGas:         "2000000000",
		MaxPriorityFeePerGas: "100000000",
		Value:                "1000000000000000000",
		Nonce:                "5",
	})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var obj map[string]string
	json.Unmarshal(params[0], &obj)

	cases := map[string]string{
		"from":                 "0xFrom",
		"data":                 "0xData",
		"gas":                  "0x5208",
		"gasPrice":             "0x3b9aca00",
		"maxFeePerGas":         "0x77359400",
		"maxPriorityFeePerGas": "0x5f5e100",
		"value":                "0xde0b6b3a7640000",
		"nonce":                "0x5",
	}
	for field, want := range cases {
		if got := obj[field]; got != want {
			t.Errorf("field %q: expected %q, got %q", field, want, got)
		}
	}
}

func TestClient_EstimateGas_DefaultBlockLatest(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).EstimateGas(EstimateGasQuery{To: "0x0"})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var tag string
	json.Unmarshal(params[1], &tag)
	if tag != BlockTagLatest {
		t.Errorf("expected 'latest', got %q", tag)
	}
}

// ============================================================================
// SendRawTransaction
// ============================================================================

func TestClient_SendRawTransaction_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).SendRawTransaction(TransactionQuery{Signed: "0xSignedTx"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_sendRawTransaction" {
		t.Errorf("expected eth_sendRawTransaction, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 1 || params[0] != "0xSignedTx" {
		t.Errorf("expected ['0xSignedTx'], got %v", params)
	}
}

// ============================================================================
// GetTransactionByHash
// ============================================================================

func TestClient_GetTransactionByHash_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetTransactionByHash(TransactionQuery{Hash: "0xHash"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getTransactionByHash" {
		t.Errorf("expected eth_getTransactionByHash, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 1 || params[0] != "0xHash" {
		t.Errorf("expected ['0xHash'], got %v", params)
	}
}

// ============================================================================
// GetTransactionReceipt
// ============================================================================

func TestClient_GetTransactionReceipt_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetTransactionReceipt(TransactionQuery{Hash: "0xHash"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getTransactionReceipt" {
		t.Errorf("expected eth_getTransactionReceipt, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 1 || params[0] != "0xHash" {
		t.Errorf("expected ['0xHash'], got %v", params)
	}
}

// ============================================================================
// GetTransactionCount
// ============================================================================

func TestClient_GetTransactionCount_MethodAndParams(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetTransactionCount(AddressedQuery{Address: "0xAddr"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getTransactionCount" {
		t.Errorf("expected eth_getTransactionCount, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) < 2 || params[0] != "0xAddr" || params[1] != BlockTagLatest {
		t.Errorf("expected ['0xAddr', 'latest'], got %v", params)
	}
}

// ============================================================================
// GetBlockByNumber
// ============================================================================

func TestClient_GetBlockByNumber_DefaultsToLatest(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByNumber(BlockQuery{})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getBlockByNumber" {
		t.Errorf("expected eth_getBlockByNumber, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	var tag string
	json.Unmarshal(params[0], &tag)
	if tag != BlockTagLatest {
		t.Errorf("expected 'latest', got %q", tag)
	}
}

func TestClient_GetBlockByNumber_NumberConvertedToHex(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByNumber(BlockQuery{Number: "1000"})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var tag string
	json.Unmarshal(params[0], &tag)
	if tag != "0x3e8" {
		t.Errorf("expected '0x3e8' (1000), got %q", tag)
	}
}

func TestClient_GetBlockByNumber_NumberOverridesBlockTag(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByNumber(BlockQuery{
		OnBlockQuery: OnBlockQuery{BlockTag: BlockTagLatest},
		Number:       "100",
	})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var tag string
	json.Unmarshal(params[0], &tag)
	if tag != "0x64" {
		t.Errorf("expected Number to override BlockTag, got %q", tag)
	}
}

func TestClient_GetBlockByNumber_BlockTagPropagated(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByNumber(BlockQuery{OnBlockQuery: OnBlockQuery{BlockTag: BlockTagFinalized}})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var tag string
	json.Unmarshal(params[0], &tag)
	if tag != BlockTagFinalized {
		t.Errorf("expected 'finalized', got %q", tag)
	}
}

func TestClient_GetBlockByNumber_FullTransactions(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByNumber(BlockQuery{FullTransactions: true})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	if len(params) < 2 {
		t.Fatal("expected 2 params")
	}
	var full bool
	json.Unmarshal(params[1], &full)
	if !full {
		t.Error("expected FullTransactions=true as second param")
	}
}

// ============================================================================
// GetBlockByHash
// ============================================================================

func TestClient_GetBlockByHash_UsesHash(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByHash(BlockQuery{Hash: "0xBlockHash"})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getBlockByHash" {
		t.Errorf("expected eth_getBlockByHash, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	var hash string
	json.Unmarshal(params[0], &hash)
	if hash != "0xBlockHash" {
		t.Errorf("expected hash='0xBlockHash', got %q", hash)
	}
}

func TestClient_GetBlockByHash_FallsBackToBlockTag(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetBlockByHash(BlockQuery{OnBlockQuery: OnBlockQuery{BlockTag: BlockTagSafe}})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getBlockByHash" {
		t.Errorf("expected eth_getBlockByHash, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	var tag string
	json.Unmarshal(params[0], &tag)
	if tag != BlockTagSafe {
		t.Errorf("expected 'safe', got %q", tag)
	}
}

// ============================================================================
// GetLogs
// ============================================================================

func TestClient_GetLogs_Method(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetLogs(LogsQuery{
		AddressedQuery: AddressedQuery{Address: "0xContract"},
		FromBlock:      "0x0",
		ToBlock:        "0x100",
		Topics:         []string{"0xTopic"},
	})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getLogs" {
		t.Errorf("expected eth_getLogs, got %s", req.Method)
	}
}

func TestClient_GetLogs_DefaultToBlockLatest(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetLogs(LogsQuery{
		AddressedQuery: AddressedQuery{Address: "0xContract"},
		FromBlock:      "0x0",
	})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var filterObj map[string]json.RawMessage
	json.Unmarshal(params[0], &filterObj)
	var toBlock string
	json.Unmarshal(filterObj["toBlock"], &toBlock)
	if toBlock != BlockTagLatest {
		t.Errorf("expected toBlock='latest', got %q", toBlock)
	}
}

func TestClient_GetLogs_AddressAndTopics(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).GetLogs(LogsQuery{
		AddressedQuery: AddressedQuery{Address: "0xContract"},
		Topics:         []string{"0xTopic1", "0xTopic2"},
	})
	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	var filterObj map[string]json.RawMessage
	json.Unmarshal(params[0], &filterObj)
	var address string
	json.Unmarshal(filterObj["address"], &address)
	if address != "0xContract" {
		t.Errorf("expected address='0xContract', got %q", address)
	}
	var topics []string
	json.Unmarshal(filterObj["topics"], &topics)
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}

// ============================================================================
// Subscribe / UnSubscribe
// ============================================================================

func TestClient_HTTP_Subscribe_ReturnsError(t *testing.T) {
	_, _, err := withHTTP(okHTTP()).Subscribe(SubscribeQuery{})
	if err == nil {
		t.Error("expected error: HTTP rpc does not support Subscribe")
	}
}

func TestClient_HTTP_UnSubscribe_ReturnsError(t *testing.T) {
	_, err := withHTTP(okHTTP()).UnSubscribe(UnSubscribeQuery{})
	if err == nil {
		t.Error("expected error: HTTP rpc does not support UnSubscribe")
	}
}

func TestWSClient_Subscribe_NewHeads_SendsSingleParam(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub1"}
	withWS(conn).Subscribe(SubscribeQuery{On: SubscriptionNewHeads})

	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_subscribe" {
		t.Errorf("expected eth_subscribe, got %s", req.Method)
	}
	var params []json.RawMessage
	json.Unmarshal(req.Params, &params)
	if len(params) != 1 {
		t.Errorf("newHeads must send exactly 1 param (no filter object), got %d", len(params))
	}
	var subType string
	json.Unmarshal(params[0], &subType)
	if subType != SubscriptionNewHeads {
		t.Errorf("expected 'newHeads', got %q", subType)
	}
}

func TestWSClient_Subscribe_Logs_IncludesFilterParams(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub2"}
	withWS(conn).Subscribe(SubscribeQuery{
		On:      SubscriptionLogs,
		Address: "0xContract",
		Topics:  []string{"0xTopic"},
	})

	var params []json.RawMessage
	json.Unmarshal(parseReq(t, conn.lastPayload).Params, &params)
	if len(params) != 2 {
		t.Fatalf("logs subscription must send 2 params, got %d", len(params))
	}
	var subType string
	json.Unmarshal(params[0], &subType)
	if subType != SubscriptionLogs {
		t.Errorf("expected 'logs', got %q", subType)
	}
	var filter map[string]json.RawMessage
	json.Unmarshal(params[1], &filter)
	if _, ok := filter["address"]; !ok {
		t.Error("expected 'address' in filter object")
	}
	if _, ok := filter["topics"]; !ok {
		t.Error("expected 'topics' in filter object")
	}
}

func TestWSClient_Subscribe_ReturnsSubscriptionIDAndListener(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub123"}
	subId, listener, err := withWS(conn).Subscribe(SubscribeQuery{On: SubscriptionNewHeads})
	if err != nil {
		t.Fatal(err)
	}
	if subId != "0xsub123" {
		t.Errorf("expected sub ID '0xsub123', got %q", subId)
	}
	if listener == nil {
		t.Error("expected non-nil listener channel")
	}
}

func TestWSClient_Subscribe_ListenerReceivesEvents(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub123"}
	_, listener, err := withWS(conn).Subscribe(SubscribeQuery{On: SubscriptionNewHeads})
	if err != nil {
		t.Fatal(err)
	}

	// Push a subscription notification directly onto the connection stream.
	event := []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0xsub123","result":"0xblockdata"}}`)
	conn.events <- event

	select {
	case data, ok := <-listener:
		if !ok {
			t.Fatal("listener closed unexpectedly")
		}
		// ParseSubscriptionResponse returns params.result as raw JSON.
		if string(data) != `"0xblockdata"` {
			t.Errorf("expected '\"0xblockdata\"', got %q", data)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for subscription event on listener")
	}
}

func TestWSClient_Subscribe_ListenerRegisteredInMap(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub999"}
	client := withWS(conn)

	if _, _, err := client.Subscribe(SubscribeQuery{On: SubscriptionNewHeads}); err != nil {
		t.Fatal(err)
	}

	client.mu.Lock()
	_, ok := client.listeners["0xsub999"]
	client.mu.Unlock()

	if !ok {
		t.Error("expected subscription listener registered in listeners map")
	}
}

func TestWSClient_UnSubscribe_RemovesAndClosesListener(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub999"}
	client := withWS(conn)

	sub, listener, err := client.Subscribe(SubscribeQuery{On: SubscriptionNewHeads})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.UnSubscribe(UnSubscribeQuery{Subscription: sub}); err != nil {
		t.Fatal(err)
	}

	client.mu.Lock()
	listenerCount := len(client.listeners)
	client.mu.Unlock()
	if listenerCount != 0 {
		t.Errorf("expected listeners to be empty, got %d", listenerCount)
	}

	if _, ok := <-listener; ok {
		t.Error("expected listener to be closed")
	}
}

// ============================================================================
// Transport error propagation
// ============================================================================

func TestClient_TransportError_ReturnsError(t *testing.T) {
	conn := failConn(transport.HTTP)
	if _, err := withHTTP(conn).ChainId(); err == nil {
		t.Error("expected error when transport fails")
	}
}

// ============================================================================
// Request ID sequencing
// ============================================================================

// The sequencer now always assigns IDs regardless of transport. Caller-supplied
// IDs in query structs are ignored at the ThinClient level.
func TestClient_RequestID_StartsAtOne(t *testing.T) {
	conn := okHTTP()
	withHTTP(conn).ChainId()
	if parseReq(t, conn.lastPayload).ID != 1 {
		t.Errorf("expected first sequenced ID to be 1, got %d", parseReq(t, conn.lastPayload).ID)
	}
}

func TestClient_RequestID_StrictlyIncreasing(t *testing.T) {
	conn := okHTTP()
	client := withHTTP(conn)

	client.ChainId()
	id1 := parseReq(t, conn.lastPayload).ID
	client.BlockNumber()
	id2 := parseReq(t, conn.lastPayload).ID

	if id2 <= id1 {
		t.Errorf("expected strictly increasing IDs, got %d then %d", id1, id2)
	}
}

// ============================================================================
// WS: sequence IDs
// ============================================================================

func TestWSClient_SequenceId_AutoAssigned(t *testing.T) {
	conn := okWS()
	if _, err := withWS(conn).ChainId(); err != nil {
		t.Fatal(err)
	}
	if parseReq(t, conn.lastPayload).ID == 0 {
		t.Error("expected non-zero sequence ID for WS call")
	}
}

func TestWSClient_SequenceIds_Increment(t *testing.T) {
	conn := okWS()
	client := withWS(conn)

	var ids []uint
	for i := 0; i < 3; i++ {
		if _, err := client.ChainId(); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		ids = append(ids, parseReq(t, conn.lastPayload).ID)
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("expected strictly increasing IDs, got %v", ids)
		}
	}
}

func TestWSClient_CallerProvidedId_IgnoredInFavourOfSequencer(t *testing.T) {
	conn := okWS()
	withWS(conn).GetBalance(BalanceQuery{AddressedQuery: AddressedQuery{
		OnBlockQuery: OnBlockQuery{IdentifiedQuery: IdentifiedQuery{Id: 9999}},
		Address:      "0xABC",
	}})
	id := parseReq(t, conn.lastPayload).ID
	if id == 9999 {
		t.Error("sequencer must override caller-supplied ID")
	}
	if id == 0 {
		t.Error("expected non-zero sequencer ID")
	}
}

// ============================================================================
// WS: method and params encoding
// ============================================================================

func TestWSClient_ChainId_CorrectMethod(t *testing.T) {
	conn := okWS()
	if _, err := withWS(conn).ChainId(); err != nil {
		t.Fatal(err)
	}
	if req := parseReq(t, conn.lastPayload); req.Method != "eth_chainId" {
		t.Errorf("expected eth_chainId, got %s", req.Method)
	}
}

func TestWSClient_BlockNumber_CorrectMethod(t *testing.T) {
	conn := okWS()
	if _, err := withWS(conn).BlockNumber(); err != nil {
		t.Fatal(err)
	}
	if req := parseReq(t, conn.lastPayload); req.Method != "eth_blockNumber" {
		t.Errorf("expected eth_blockNumber, got %s", req.Method)
	}
}

func TestWSClient_GetBalance_CorrectParams(t *testing.T) {
	conn := okWS()
	withWS(conn).GetBalance(BalanceQuery{AddressedQuery: AddressedQuery{Address: "0xABC"}})
	req := parseReq(t, conn.lastPayload)
	if req.Method != "eth_getBalance" {
		t.Errorf("expected eth_getBalance, got %s", req.Method)
	}
	var params []string
	json.Unmarshal(req.Params, &params)
	if len(params) != 2 || params[0] != "0xABC" || params[1] != BlockTagLatest {
		t.Errorf("expected [0xABC, latest], got %v", params)
	}
}

// ============================================================================
// WS: response routing by ID
// ============================================================================

func TestWSClient_ResponseRoutedById(t *testing.T) {
	events := make(chan transport.Message, 8)
	events <- rpcResp(1, "0xDEAD") // sequencer starts at 1

	conn := &mockWSConnection{
		kind:      transport.WS,
		events:    events,
		noRespond: true,
	}
	result, err := withWS(conn).ChainId()
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "0xDEAD" {
		t.Errorf("expected '0xDEAD', got %q", result)
	}
}

func TestWSClient_WrongIdInResponse_NotDelivered(t *testing.T) {
	events := make(chan transport.Message, 8)
	events <- rpcResp(99, "0xBAD") // ID 99 != sequencer ID 1

	conn := &mockWSConnection{
		kind:      transport.WS,
		events:    events,
		timeout:   20 * time.Millisecond,
		noRespond: true,
	}
	result, err := withWS(conn).ChainId()
	if result != nil {
		t.Errorf("expected nil result when response ID does not match, got %q", result)
	}
	_ = err
}

func TestWSClient_SequentialCalls_EachGetsOwnResponse(t *testing.T) {
	conn := &mockWSConnection{
		kind:   transport.WS,
		events: make(chan transport.Message, 8),
		result: "0x42",
	}
	client := withWS(conn)

	for i := 0; i < 3; i++ {
		result, err := client.BlockNumber()
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if string(result) != "0x42" {
			t.Errorf("call %d: expected '0x42', got %q", i, result)
		}
	}
}

// ============================================================================
// WS: transport error
// ============================================================================

func TestWSClient_TransportError_ReturnsError(t *testing.T) {
	conn := &mockWSConnection{
		kind:   transport.WS,
		events: make(chan transport.Message, 8),
		err:    errors.New("ws send failed"),
	}
	if _, err := withWS(conn).ChainId(); err == nil {
		t.Error("expected error when WS transport fails")
	}
}

// ============================================================================
// WS: multiple connections
// ============================================================================

func TestWSClient_MultipleConnections_UsesFirstHealthy(t *testing.T) {
	first := okWS()
	second := okWS()

	mgr := &transport.ConnectionManager{}
	mgr.AddConnection(first)
	mgr.AddConnection(second)

	client := newThinClient(transport.WS, mgr, new(transport.SequenceGenerator), 10)

	for i := 0; i < 3; i++ {
		if _, err := client.ChainId(); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}

	if second.callCount != 0 {
		t.Errorf("second connection must not be used while first is healthy, got %d calls", second.callCount)
	}
}

func TestWSClient_MultipleConnections_FallsBackToSecond(t *testing.T) {
	first := &mockWSConnection{
		kind:   transport.WS,
		events: make(chan transport.Message, 8),
		err:    errors.New("send failed"),
	}
	second := okWS()

	mgr := &transport.ConnectionManager{}
	mgr.AddConnection(first)
	mgr.AddConnection(second)

	client := newThinClient(transport.WS, mgr, new(transport.SequenceGenerator), 10)

	result, err := client.ChainId()
	if err != nil {
		t.Fatalf("expected fallback to second connection, got: %v", err)
	}
	if string(result) != "0x1" {
		t.Errorf("expected result from second connection, got %q", result)
	}
	if first.callCount != 1 {
		t.Errorf("expected first connection tried once, got %d", first.callCount)
	}
	if second.callCount != 1 {
		t.Errorf("expected second connection called once, got %d", second.callCount)
	}
}

func TestWSClient_MultipleConnections_AllFail_ReturnsError(t *testing.T) {
	first := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), err: errors.New("fail1")}
	second := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), err: errors.New("fail2")}

	mgr := &transport.ConnectionManager{}
	mgr.AddConnection(first)
	mgr.AddConnection(second)

	if _, err := newThinClient(transport.WS, mgr, new(transport.SequenceGenerator), 10).ChainId(); err == nil {
		t.Error("expected error when all connections fail")
	}
}

func TestWSClient_MultipleConnections_EachHasOwnStream(t *testing.T) {
	first := okWS()
	second := okWS()
	if first.Stream() == second.Stream() {
		t.Error("expected each mock connection to have its own independent stream channel")
	}
}

// ============================================================================
// WS: resource cleanup — pending channels
// ============================================================================

// TestWSClient_Resources_PendingChannel_ClosedWhenStreamDrops verifies that
// a caller blocked in postProcess is unblocked when the underlying stream
// closes, and that no open pending channels remain afterward.
func TestWSClient_Resources_PendingChannel_ClosedWhenStreamDrops(t *testing.T) {
	conn := &mockWSConnection{
		kind:      transport.WS,
		events:    make(chan transport.Message, 8),
		noRespond: true,
	}
	client := withWS(conn)

	done := make(chan error, 1)
	go func() {
		_, err := client.ChainId()
		done <- err
	}()

	// Wait for preProcess to register the pending channel before closing.
	time.Sleep(20 * time.Millisecond)
	close(conn.events)

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("pending caller not unblocked after stream drop")
	}

	// Every channel still referenced by pending must be closed.
	// Note: clearStream closes channels but does not delete the keys — a
	// separate memory-leak issue in the implementation.
	client.mu.Lock()
	for id, ch := range client.pending {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("pending[%d] channel still open after stream drop", id)
			}
		default:
			t.Errorf("pending[%d] channel is neither closed nor carries a value", id)
		}
	}
	client.mu.Unlock()
}

func TestWSClient_Resources_StreamsMap_ClearedWhenStreamDrops(t *testing.T) {
	conn := &mockWSConnection{
		kind:      transport.WS,
		events:    make(chan transport.Message, 8),
		noRespond: true,
	}
	client := withWS(conn)

	done := make(chan error, 1)
	go func() {
		_, err := client.ChainId()
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	stream := conn.Stream()
	close(conn.events)
	<-done

	client.mu.Lock()
	inner := client.streams[stream]
	client.mu.Unlock()

	if len(inner) != 0 {
		t.Errorf("expected streams[stream] inner map empty after clearStream, got %d entries", len(inner))
	}
}

// TestWSClient_Resources_RejectListener_ClosesPendingChannel verifies that
// rejectListener (called on postProcess timeout) removes the pending entry
// entirely — no leaked key unlike clearStream.
func TestWSClient_Resources_RejectListener_ClosesPendingChannel(t *testing.T) {
	conn := &mockWSConnection{
		kind:      transport.WS,
		events:    make(chan transport.Message, 8),
		timeout:   20 * time.Millisecond,
		noRespond: true,
	}
	client := withWS(conn)

	// Times out internally, triggering rejectListener.
	client.ChainId()

	client.mu.Lock()
	pendingCount := len(client.pending)
	client.mu.Unlock()

	if pendingCount != 0 {
		t.Errorf("expected pending map empty after timeout/rejectListener, got %d entries", pendingCount)
	}
}

// ============================================================================
// WS: resource cleanup — subscription listeners
// ============================================================================

func TestWSClient_Resources_SubscriptionListener_ClosedWhenStreamDrops(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub123"}
	_, listener, err := withWS(conn).Subscribe(SubscribeQuery{On: SubscriptionNewHeads})
	if err != nil {
		t.Fatal(err)
	}

	close(conn.events)

	select {
	case _, ok := <-listener:
		if ok {
			t.Error("expected listener closed after stream drop, got a value")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("listener not closed after stream drop")
	}
}

func TestWSClient_Resources_SubscriptionsMap_ClearedWhenStreamDrops(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub456"}
	client := withWS(conn)

	if _, _, err := client.Subscribe(SubscribeQuery{On: SubscriptionNewHeads}); err != nil {
		t.Fatal(err)
	}

	stream := conn.Stream()
	close(conn.events)
	time.Sleep(50 * time.Millisecond) // let clearStream run

	client.mu.Lock()
	inner := client.subscriptions[stream]
	client.mu.Unlock()

	if len(inner) != 0 {
		t.Errorf("expected subscriptions[stream] empty after stream drop, got %d entries", len(inner))
	}
}

// TestWSClient_Resources_RejectSubscription_ClosesListener verifies that
// rejectSubscription closes the listener channel and removes both the
// listeners and subscriptions map entries.
func TestWSClient_Resources_RejectSubscription_ClosesListener(t *testing.T) {
	conn := &mockWSConnection{kind: transport.WS, events: make(chan transport.Message, 8), result: "0xsub789"}
	client := withWS(conn)

	subId, listener, err := client.Subscribe(SubscribeQuery{On: SubscriptionNewHeads})
	if err != nil {
		t.Fatal(err)
	}

	stream := conn.Stream()
	client.rejectSubscription(stream, subId)

	select {
	case _, ok := <-listener:
		if ok {
			t.Error("expected listener closed by rejectSubscription")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("listener not closed after rejectSubscription")
	}

	client.mu.Lock()
	_, inListeners := client.listeners[subId]
	_, inSubscriptions := client.subscriptions[stream][subId]
	client.mu.Unlock()

	if inListeners {
		t.Error("expected listener key removed from listeners map")
	}
	if inSubscriptions {
		t.Error("expected subscription key removed from subscriptions map")
	}
}
