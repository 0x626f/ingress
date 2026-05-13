package rpc

import (
	"encoding/json"
	"fmt"
)

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
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

// Error implements the error interface.
func (err *APIError) Error() string {
	return fmt.Sprintf("RPC Error[%d]: Message: %s. Data: %v", err.Code, err.Message, err.Data)
}

// BuildQuery serialises a JSON-RPC 2.0 request frame.
func (spec APISpec) BuildQuery(id uint, method string, params []any) ([]byte, error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	return []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`,
		id,
		method,
		paramsJSON,
	)), nil
}

// ParseResponse deserialises a JSON-RPC 2.0 response frame and returns the
// inner value of the "result" field. JSON strings are returned without their
// quotes; objects, arrays, booleans, and numbers are returned as valid JSON.
// Returns nil without an error when the result field is absent or null.
// Returns an APIError when the response contains an error object.
func (spec APISpec) ParseResponse(response []byte) ([]byte, error) {
	if len(response) == 0 {
		return nil, nil
	}

	var summary struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      uint            `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *APIError       `json:"error"`
		Params  struct {
			Error *APIError `json:"error"`
		} `json:"params"`
	}
	err := json.Unmarshal(response, &summary)

	if err != nil {
		return nil, err
	}

	if summary.Error != nil {
		return nil, summary.Error
	}

	if summary.Params.Error != nil {
		return nil, summary.Params.Error
	}

	if summary.Result == nil || string(summary.Result) == "null" {
		return nil, nil
	}

	if summary.Result[0] == '"' {
		var value string
		if err := json.Unmarshal(summary.Result, &value); err != nil {
			return nil, err
		}
		return []byte(value), nil
	}

	return summary.Result, nil
}

// ParseSubscriptionResponse extracts the result payload from an eth_subscribe
// push notification (params.result field).
func (spec APISpec) ParseSubscriptionResponse(request []byte) ([]byte, error) {
	var summary struct {
		Params struct {
			Result json.RawMessage `json:"result,omitempty"`
		} `json:"params,omitempty"`
	}

	err := json.Unmarshal(request, &summary)
	if err != nil {
		return nil, err
	}

	return summary.Params.Result, nil
}

// MessageId identifies a JSON-RPC message as either a regular response (Id > 0)
// or a subscription push notification (Subscription non-empty).
type MessageId struct {
	Id           uint
	Subscription string
}

// ParseMessageId extracts the routing identity from an inbound WebSocket message.
// For ordinary responses it populates Id; for subscription notifications it
// populates Subscription.
func (spec APISpec) ParseMessageId(response []byte) (MessageId, error) {
	var summary struct {
		Id     uint `json:"id,omitempty"`
		Params struct {
			Subscription string `json:"subscription,omitempty"`
		} `json:"params,omitempty"`
	}

	err := json.Unmarshal(response, &summary)

	if err != nil {
		return MessageId{}, err
	}

	return MessageId{Id: summary.Id, Subscription: summary.Params.Subscription}, nil
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
