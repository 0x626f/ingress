package rpc

import "github.com/0x626f/ingress/evm"

// CoreClient is the top-level interface for interacting with an EVM-compatible
// JSON-RPC node. All methods return raw result bytes with the outer JSON
// delimiter stripped (see APISpec.ParseResponse). Callers are responsible for
// further deserialization.
type CoreClient interface {
	// ChainId returns the chain ID as a hex string (e.g. 0x1 for Ethereum mainnet).
	ChainId() ([]byte, error)
	// BlockNumber returns the current head block number as a hex string.
	BlockNumber() ([]byte, error)
	// GetBalance returns the native balance of the given address.
	GetBalance(BalanceQuery) ([]byte, error)
	// GetCode returns the bytecode deployed at the given address.
	GetCode(CodeQuery) ([]byte, error)
	// GetStorageAt returns the value stored at a specific storage slot.
	GetStorageAt(GetStorageQuery) ([]byte, error)
	// Call executes a read-only contract call without creating a transaction.
	Call(CallQuery) ([]byte, error)
	// EstimateGas estimates the gas required for the described transaction.
	EstimateGas(EstimateGasQuery) ([]byte, error)
	// SendRawTransaction broadcasts a signed, RLP-encoded transaction.
	SendRawTransaction(TransactionQuery) ([]byte, error)
	// GetTransactionByHash returns the transaction object for the given hash.
	GetTransactionByHash(TransactionQuery) ([]byte, error)
	// GetTransactionReceipt returns the receipt for the given transaction hash.
	GetTransactionReceipt(TransactionQuery) ([]byte, error)
	// GetTransactionCount returns the nonce (transaction count) for an address.
	GetTransactionCount(AddressedQuery) ([]byte, error)
	// GetBlockByNumber returns a block selected by tag or number.
	GetBlockByNumber(BlockQuery) ([]byte, error)
	// GetBlockByHash returns a block selected by hash.
	GetBlockByHash(BlockQuery) ([]byte, error)
	// GetLogs returns logs matching the given filter.
	GetLogs(LogsQuery) ([]byte, error)
	// Subscribe creates an eth_subscribe subscription (WebSocket only).
	// It returns the subscription ID and a channel on which push events are delivered.
	Subscribe(SubscribeQuery) (evm.Subscription, evm.RStream, error)
	// UnSubscribe cancels an active eth_subscribe subscription (WebSocket only).
	UnSubscribe(query UnSubscribeQuery) ([]byte, error)
}

// IdentifiedQuery carries an optional caller-supplied request ID.
// When Id is zero the sequencer assigns one automatically.
type IdentifiedQuery struct {
	Id uint
}

// OnBlockQuery extends IdentifiedQuery with a block tag (e.g. "latest", "earliest",
// a block number in hex, or a block hash). An empty BlockTag defaults to "latest".
type OnBlockQuery struct {
	IdentifiedQuery
	BlockTag string
}

// AddressedQuery extends OnBlockQuery with a target Ethereum address.
type AddressedQuery struct {
	OnBlockQuery
	Address string
}

// BalanceQuery is the input for eth_getBalance.
type BalanceQuery struct {
	AddressedQuery
}

// CodeQuery is the input for eth_getCode.
type CodeQuery struct {
	AddressedQuery
}

// CallQuery is the input for eth_call.
type CallQuery struct {
	OnBlockQuery
	// To is the contract address to call.
	To string
	// Data is the ABI-encoded call data.
	Data string
}

// GetStorageQuery is the input for eth_getStorageAt.
type GetStorageQuery struct {
	AddressedQuery
	// Slot is the storage position (decimal or 0x-prefixed hex).
	Slot string
}

// EstimateGasQuery is the input for eth_estimateGas.
// All numeric fields (Gas, GasPrice, MaxFeePerGas, MaxPriorityFeePerGas, Value, Nonce)
// may be supplied as decimal strings or 0x-prefixed hex strings.
type EstimateGasQuery struct {
	OnBlockQuery
	To                   string
	From                 string
	Data                 string
	Gas                  string
	GasPrice             string
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
	Value                string
	Nonce                string
}

// TransactionQuery is used for eth_sendRawTransaction, eth_getTransactionByHash,
// and eth_getTransactionReceipt.
type TransactionQuery struct {
	IdentifiedQuery
	// Signed is the RLP-encoded signed transaction (for SendRawTransaction).
	Signed string
	// Hash is the transaction hash (for GetTransactionByHash / GetTransactionReceipt).
	Hash string
}

// BlockQuery is the input for eth_getBlockByNumber and eth_getBlockByHash.
type BlockQuery struct {
	OnBlockQuery
	// Hash selects the block by hash (used by GetBlockByHash).
	Hash string
	// Number selects the block by number as a decimal string (used by GetBlockByNumber).
	Number string
	// FullTransactions controls whether full transaction objects or only hashes are returned.
	FullTransactions bool
}

// LogsQuery is the input for eth_getLogs.
type LogsQuery struct {
	AddressedQuery
	// FromBlock is the start of the block range (block tag or hex number).
	FromBlock string
	// ToBlock is the end of the block range. Defaults to "latest" when empty.
	ToBlock string
	// Topics is the ordered list of log topic filters.
	Topics []string
}

// SubscribeQuery is the input for eth_subscribe.
type SubscribeQuery struct {
	IdentifiedQuery
	// On is the subscription type, e.g. "newHeads", "logs", "newPendingTransactions".
	On string
	// Address filters log subscriptions to a specific contract address.
	Address string
	// Topics filters log subscriptions to specific topics.
	Topics []string
}

// UnSubscribeQuery is the input for eth_unsubscribe.
type UnSubscribeQuery struct {
	IdentifiedQuery
	// Subscription is the ID returned by a prior eth_subscribe call.
	Subscription string
}

// QueryParams holds the request ID and positional parameters for a JSON-RPC call.
type QueryParams struct {
	// Id is the JSON-RPC request identifier. Zero is replaced with 1 by Adjust.
	Id uint
	// Params is the ordered list of parameters sent in the "params" array.
	Params []any
}

// DefaultQueryParams returns a QueryParams with Id 1 and an empty params slice.
func DefaultQueryParams() *QueryParams {
	return &QueryParams{
		Id:     1,
		Params: []any{},
	}
}

// Query returns a QueryParams with no Id (assigned later by the sequencer) and
// the provided positional params.
func Query(params ...any) *QueryParams {
	return &QueryParams{
		Params: params,
	}
}

// QueryWithId returns a QueryParams with an explicit Id and the provided params.
func QueryWithId(id uint, params ...any) *QueryParams {
	return &QueryParams{
		Id:     id,
		Params: params,
	}
}

// Adjust normalizes the params before serialization:
// a zero Id is promoted to 1, and a nil Params slice is replaced with an empty slice.
func (params *QueryParams) Adjust() {
	if params.Id == 0 {
		params.Id = 1
	}

	if params.Params == nil {
		params.Params = []any{}
	}
}
