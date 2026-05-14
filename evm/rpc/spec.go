package rpc

import "github.com/0x626f/ingress/jsonrpc"

const (
	chainId               = "eth_chainId"
	blockNumber           = "eth_blockNumber"
	getBalance            = "eth_getBalance"
	getTransactionCount   = "eth_getTransactionCount"
	getCode               = "eth_getCode"
	getStorageAt          = "eth_getStorageAt"
	call                  = "eth_call"
	estimateGas           = "eth_estimateGas"
	sendRawTransaction    = "eth_sendRawTransaction"
	getTransactionByHash  = "eth_getTransactionByHash"
	getTransactionReceipt = "eth_getTransactionReceipt"
	getBlockByNumber      = "eth_getBlockByNumber"
	getBlockByHash        = "eth_getBlockByHash"
	getLogs               = "eth_getLogs"
	subscribe             = "eth_subscribe"
	unsubscribe           = "eth_unsubscribe"
)

// Block tag constants for use in query BlockTag fields.
const (
	BlockTagLatest    = "latest"
	BlockTagEarliest  = "earliest"
	BlockTagPending   = "pending"
	BlockTagSafe      = "safe"
	BlockTagFinalized = "finalized"
)

// Subscription event type constants for use in SubscribeQuery.On.
const (
	SubscriptionNewHeads               = "newHeads"
	SubscriptionLogs                   = "logs"
	SubscriptionNewPendingTransactions = "newPendingTransactions"
	SubscriptionSyncing                = "syncing"
)

// APISpec builds and parses JSON-RPC 2.0 messages for the Ethereum API.
type APISpec struct {
}

// APIError represents a JSON-RPC error object returned by the node.
type APIError = jsonrpc.Error

// BuildQuery serialises a JSON-RPC 2.0 request frame.
func (spec APISpec) BuildQuery(id uint, method string, params []any) ([]byte, error) {
	return jsonrpc.BuildRequest(id, method, params)
}

// ParseResponse deserialises a JSON-RPC 2.0 response frame and returns the
// inner value of the "result" field. JSON strings are returned without their
// quotes; objects, arrays, booleans, and numbers are returned as valid JSON.
// Returns nil without an error when the result field is absent or null.
// Returns an APIError when the response contains an error object.
func (spec APISpec) ParseResponse(response []byte) ([]byte, error) {
	return jsonrpc.ParseResponse(response)
}

// ParseSubscriptionResponse extracts the result payload from an eth_subscribe
// push notification (params.result field).
func (spec APISpec) ParseSubscriptionResponse(request []byte) ([]byte, error) {
	return jsonrpc.ParseSubscriptionResult(request)
}

// MessageId identifies a JSON-RPC message as either a regular response (Id > 0)
// or a subscription push notification (Subscription non-empty).
type MessageId = jsonrpc.MessageID

// ParseMessageId extracts the routing identity from an inbound WebSocket message.
// For ordinary responses it populates Id; for subscription notifications it
// populates Subscription.
func (spec APISpec) ParseMessageId(response []byte) (MessageId, error) {
	return jsonrpc.ParseMessageID(response)
}

// SupportedMethod returns the list of Ethereum JSON-RPC method names that
// this spec handles.
func (spec APISpec) SupportedMethod() []string {
	return []string{
		chainId,
		blockNumber,
		getBalance,
		getTransactionCount,
		getCode,
		call,
		estimateGas,
		sendRawTransaction,
		getTransactionByHash,
		getTransactionReceipt,
		getBlockByNumber,
		getBlockByHash,
		getLogs,
	}
}

// ChainId builds an eth_chainId request.
func (spec APISpec) ChainId(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(chainId, params)
}

// BlockNumber builds an eth_blockNumber request.
func (spec APISpec) BlockNumber(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(blockNumber, params)
}

// GetBalance builds an eth_getBalance request.
func (spec APISpec) GetBalance(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getBalance, params)
}

// GetTransactionCount builds an eth_getTransactionCount request.
func (spec APISpec) GetTransactionCount(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getTransactionCount, params)
}

// GetCode builds an eth_getCode request.
func (spec APISpec) GetCode(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getCode, params)
}

// Call builds an eth_call request.
func (spec APISpec) Call(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(call, params)
}

// GetStorageAt builds an eth_getStorageAt request.
func (spec APISpec) GetStorageAt(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getStorageAt, params)
}

// EstimateGas builds an eth_estimateGas request.
func (spec APISpec) EstimateGas(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(estimateGas, params)
}

// SendRawTransaction builds an eth_sendRawTransaction request.
func (spec APISpec) SendRawTransaction(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(sendRawTransaction, params)
}

// GetTransactionByHash builds an eth_getTransactionByHash request.
func (spec APISpec) GetTransactionByHash(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getTransactionByHash, params)
}

// GetTransactionReceipt builds an eth_getTransactionReceipt request.
func (spec APISpec) GetTransactionReceipt(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getTransactionReceipt, params)
}

// GetBlockByNumber builds an eth_getBlockByNumber request.
func (spec APISpec) GetBlockByNumber(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getBlockByNumber, params)
}

// GetBlockByHash builds an eth_getBlockByHash request.
func (spec APISpec) GetBlockByHash(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getBlockByHash, params)
}

// GetLogs builds an eth_getLogs request.
func (spec APISpec) GetLogs(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(getLogs, params)
}

// Subscribe builds an eth_subscribe request.
func (spec APISpec) Subscribe(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(subscribe, params)
}

// Unsubscribe builds an eth_unsubscribe request.
func (spec APISpec) Unsubscribe(params *QueryParams) ([]byte, error) {
	return spec.buildMethodCall(unsubscribe, params)
}

func (spec APISpec) buildMethodCall(method string, params *QueryParams) ([]byte, error) {
	if params == nil {
		params = DefaultQueryParams()
	}
	params.Adjust()
	return spec.BuildQuery(params.Id, method, params.Params)
}
