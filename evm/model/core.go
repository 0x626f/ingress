// Package model provides typed response structures for all EVM JSON-RPC calls
// and helpers to decode the raw bytes returned by [rpc.CoreClient] methods.
//
// CoreClient strips the outer JSON delimiter from every result
// (quotes from strings, braces from objects, brackets from arrays), so each
// Decode function re-adds the appropriate delimiter before unmarshaling.
package model

import "encoding/json"

// ─── Scalar helpers ──────────────────────────────────────────────────────────

// HexString is the decoded form of scalar hex results such as ChainId,
// BlockNumber, GetBalance, GetCode, GetStorageAt, Call, EstimateGas,
// SendRawTransaction, and GetTransactionCount.
// The raw bytes from CoreClient already represent the bare hex value
// (e.g. []byte("0x1")), so a simple string cast is sufficient:
//
//	hex := model.HexString(result)
type HexString = string

// DecodeHex converts a raw scalar result to a hex string.
func DecodeHex(data []byte) HexString {
	return string(data)
}

// ─── Log ─────────────────────────────────────────────────────────────────────

// Log is a single event log entry as returned by eth_getLogs or embedded
// inside a [Receipt].
type Log struct {
	// Address is the contract that emitted the event.
	Address string `json:"address"`
	// Topics is the indexed event signature and up to three indexed parameters.
	Topics []string `json:"topics"`
	// Data contains the ABI-encoded non-indexed parameters.
	Data string `json:"data"`
	// BlockNumber is the block in which the log was included.
	BlockNumber string `json:"blockNumber"`
	// TransactionHash is the hash of the transaction that produced this log.
	TransactionHash string `json:"transactionHash"`
	// TransactionIndex is the position of the transaction within its block.
	TransactionIndex string `json:"transactionIndex"`
	// BlockHash is the hash of the block containing this log.
	BlockHash string `json:"blockHash"`
	// LogIndex is the position of this log within the block.
	LogIndex string `json:"logIndex"`
	// Removed is true when the log was reverted due to a chain re-org.
	Removed bool `json:"removed"`
}

// DecodeLogs parses the raw result of eth_getLogs into a slice of Log values.
func DecodeLogs(data []byte) ([]Log, error) {
	var v []Log
	if err := decodeObject(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// ─── AccessListEntry ─────────────────────────────────────────────────────────

// AccessListEntry is a single entry in an EIP-2930 access list.
type AccessListEntry struct {
	// Address is the account to pre-warm.
	Address string `json:"address"`
	// StorageKeys is the list of storage slots to pre-warm for Address.
	StorageKeys []string `json:"storageKeys"`
}

// ─── Transaction ─────────────────────────────────────────────────────────────

// Transaction is the full transaction object returned by eth_getTransactionByHash
// or embedded in a [Block] when FullTransactions is true.
type Transaction struct {
	// BlockHash is the hash of the block that includes this transaction.
	// Empty for pending transactions.
	BlockHash string `json:"blockHash"`
	// BlockNumber is the number of the block that includes this transaction.
	// Empty for pending transactions.
	BlockNumber string `json:"blockNumber"`
	// From is the sender address.
	From string `json:"from"`
	// Gas is the gas limit provided by the sender.
	Gas string `json:"gas"`
	// GasPrice is the effective gas price (legacy and EIP-1559).
	GasPrice string `json:"gasPrice"`
	// MaxFeePerGas is the EIP-1559 maximum fee per gas unit. Omitted for legacy transactions.
	MaxFeePerGas string `json:"maxFeePerGas,omitempty"`
	// MaxPriorityFeePerGas is the EIP-1559 maximum miner tip per gas unit. Omitted for legacy transactions.
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas,omitempty"`
	// Hash is the transaction hash.
	Hash string `json:"hash"`
	// Input is the ABI-encoded call data or contract init code.
	Input string `json:"input"`
	// Nonce is the sender's transaction count at the time of inclusion.
	Nonce string `json:"nonce"`
	// To is the recipient address. Empty for contract-creation transactions.
	To string `json:"to"`
	// TransactionIndex is the position of this transaction within its block.
	TransactionIndex string `json:"transactionIndex"`
	// Value is the amount of native currency transferred in wei.
	Value string `json:"value"`
	// Type is the transaction type (0x0 legacy, 0x1 EIP-2930, 0x2 EIP-1559).
	Type string `json:"type"`
	// ChainId is the EIP-155 chain ID. Omitted for legacy transactions.
	ChainId string `json:"chainId,omitempty"`
	// AccessList is the EIP-2930 access list. Omitted when empty.
	AccessList []AccessListEntry `json:"accessList,omitempty"`
	// V is the ECDSA recovery identifier.
	V string `json:"v"`
	// R is the ECDSA signature R component.
	R string `json:"r"`
	// S is the ECDSA signature S component.
	S string `json:"s"`
}

// DecodeTransaction parses the raw result of eth_getTransactionByHash.
func DecodeTransaction(data []byte) (*Transaction, error) {
	var v Transaction
	if err := decodeObject(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ─── Receipt ─────────────────────────────────────────────────────────────────

// Receipt is the transaction receipt returned by eth_getTransactionReceipt.
type Receipt struct {
	// TransactionHash is the hash of the transaction.
	TransactionHash string `json:"transactionHash"`
	// TransactionIndex is the position of the transaction within its block.
	TransactionIndex string `json:"transactionIndex"`
	// BlockHash is the hash of the block containing the transaction.
	BlockHash string `json:"blockHash"`
	// BlockNumber is the number of the block containing the transaction.
	BlockNumber string `json:"blockNumber"`
	// From is the sender address.
	From string `json:"from"`
	// To is the recipient address. Empty for contract-creation transactions.
	To string `json:"to"`
	// CumulativeGasUsed is the total gas used in the block up to and including this transaction.
	CumulativeGasUsed string `json:"cumulativeGasUsed"`
	// EffectiveGasPrice is the actual gas price paid per unit.
	EffectiveGasPrice string `json:"effectiveGasPrice"`
	// GasUsed is the gas consumed by this transaction alone.
	GasUsed string `json:"gasUsed"`
	// ContractAddress is the address of the deployed contract for
	// contract-creation transactions. Empty otherwise.
	ContractAddress string `json:"contractAddress"`
	// Logs is the list of events emitted by this transaction.
	Logs []Log `json:"logs"`
	// LogsBloom is the 2048-bit bloom filter of all log topics and addresses.
	LogsBloom string `json:"logsBloom"`
	// Type is the transaction type (0x0 legacy, 0x1 EIP-2930, 0x2 EIP-1559).
	Type string `json:"type"`
	// Status is 0x1 for success and 0x0 for revert (post EIP-658).
	Status string `json:"status"`
}

// DecodeReceipt parses the raw result of eth_getTransactionReceipt.
func DecodeReceipt(data []byte) (*Receipt, error) {
	var v Receipt
	if err := decodeObject(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ─── Block ────────────────────────────────────────────────────────────────────

// Block is the block object returned by eth_getBlockByNumber and eth_getBlockByHash.
//
// The Transactions field is a raw JSON value because its element type depends
// on the FullTransactions flag used in the query: when false it is []string
// (transaction hashes); when true it is [][Transaction]. Use [Block.TxHashes]
// or [Block.TxObjects] to decode it.
type Block struct {
	// Number is the block number.
	Number string `json:"number"`
	// Hash is the block hash.
	Hash string `json:"hash"`
	// ParentHash is the hash of the parent block.
	ParentHash string `json:"parentHash"`
	// Nonce is the proof-of-work nonce. Zero for post-merge blocks.
	Nonce string `json:"nonce"`
	// Sha3Uncles is the SHA3 hash of the uncles list.
	Sha3Uncles string `json:"sha3Uncles"`
	// LogsBloom is the 2048-bit bloom filter for the logs in this block.
	LogsBloom string `json:"logsBloom"`
	// TransactionsRoot is the root of the transaction trie.
	TransactionsRoot string `json:"transactionsRoot"`
	// StateRoot is the root of the state trie after this block.
	StateRoot string `json:"stateRoot"`
	// ReceiptsRoot is the root of the receipts trie.
	ReceiptsRoot string `json:"receiptsRoot"`
	// Miner is the address of the block producer.
	Miner string `json:"miner"`
	// Difficulty is the proof-of-work difficulty. Zero for post-merge blocks.
	Difficulty string `json:"difficulty"`
	// TotalDifficulty is the cumulative difficulty of the chain up to this block.
	TotalDifficulty string `json:"totalDifficulty"`
	// ExtraData is an arbitrary byte array set by the miner.
	ExtraData string `json:"extraData"`
	// Size is the size of the block in bytes.
	Size string `json:"size"`
	// GasLimit is the maximum gas allowed in this block.
	GasLimit string `json:"gasLimit"`
	// GasUsed is the total gas consumed by all transactions in this block.
	GasUsed string `json:"gasUsed"`
	// Timestamp is the Unix timestamp at which the block was collated.
	Timestamp string `json:"timestamp"`
	// Transactions is either a JSON array of transaction hashes (strings) or
	// full transaction objects depending on the FullTransactions query flag.
	// Use TxHashes or TxObjects to decode.
	Transactions json.RawMessage `json:"transactions"`
	// Uncles is the list of uncle block hashes.
	Uncles []string `json:"uncles"`
	// BaseFeePerGas is the EIP-1559 base fee per gas. Omitted for pre-London blocks.
	BaseFeePerGas string `json:"baseFeePerGas,omitempty"`
	// WithdrawalsRoot is the EIP-4895 withdrawals trie root. Omitted for pre-Shanghai blocks.
	WithdrawalsRoot string `json:"withdrawalsRoot,omitempty"`
}

// TxHashes decodes the Transactions field as a slice of transaction hash strings.
// Use this when the block was fetched with FullTransactions = false.
func (b *Block) TxHashes() ([]string, error) {
	var hashes []string
	if err := json.Unmarshal(b.Transactions, &hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

// TxObjects decodes the Transactions field as a slice of full Transaction objects.
// Use this when the block was fetched with FullTransactions = true.
func (b *Block) TxObjects() ([]Transaction, error) {
	var txs []Transaction
	if err := json.Unmarshal(b.Transactions, &txs); err != nil {
		return nil, err
	}
	return txs, nil
}

// DecodeBlock parses the raw result of eth_getBlockByNumber or eth_getBlockByHash.
func DecodeBlock(data []byte) (*Block, error) {
	var v Block
	if err := decodeObject(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ─── internal helpers ─────────────────────────────────────────────────────────

// decodeObject unmarshals the result into v.
func decodeObject(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
