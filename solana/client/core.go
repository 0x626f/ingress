package client

import "github.com/0x626f/ingress/solana/types"

var _ CoreClient = (*ThinClient)(nil)

// CoreClient is the top-level interface for interacting with a Solana JSON-RPC node.
// It provides methods for common Solana RPC operations including slot and epoch
// information retrieval, cluster data access, transaction operations, and WebSocket
// subscriptions.
type CoreClient interface {
	RawCall(method string, params ...any) (types.RawResult, error)
	RawSubscribe(subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error)

	GetAccountInfo(pubkey string, config ...any) (types.RawResult, error)
	GetBalance(pubkey string, config ...any) (types.RawResult, error)
	GetLargestAccounts(config ...any) (types.RawResult, error)
	GetMinimumBalanceForRentExemption(dataSize uint64, commitment ...types.Commitment) (types.RawResult, error)
	GetMultipleAccounts(pubkeys []string, config ...any) (types.RawResult, error)
	GetProgramAccounts(programID string, config ...any) (types.RawResult, error)
	GetTokenAccountBalance(pubkey string, commitment ...types.Commitment) (types.RawResult, error)
	GetTokenAccountsByDelegate(delegate string, filter any, config ...any) (types.RawResult, error)
	GetTokenAccountsByOwner(owner string, filter any, config ...any) (types.RawResult, error)
	GetTokenLargestAccounts(mint string, commitment ...types.Commitment) (types.RawResult, error)
	GetTokenSupply(mint string, commitment ...types.Commitment) (types.RawResult, error)
	GetFeeForMessage(message string, commitment ...types.Commitment) (types.RawResult, error)
	GetLatestBlockhash(commitment ...types.Commitment) (types.RawResult, error)
	GetRecentPrioritizationFees(accounts ...string) (types.RawResult, error)
	GetSignaturesForAddress(address string, config ...any) (types.RawResult, error)
	GetSignatureStatuses(signatures []string, config ...any) (types.RawResult, error)
	GetTransactionCount(commitment ...types.Commitment) (types.RawResult, error)
	IsBlockhashValid(blockhash string, commitment ...types.Commitment) (types.RawResult, error)
	RequestAirdrop(pubkey string, lamports uint64, commitment ...types.Commitment) (types.RawResult, error)
	SendTransaction(serialized []byte, config ...any) (types.RawResult, error)
	SendEncodedTransaction(encoded string, config ...any) (types.RawResult, error)
	SimulateEncodedTransaction(encoded string, config ...any) (types.RawResult, error)
	GetBlockCommitment(slot types.Slot) (types.RawResult, error)
	GetBlockHeight(commitment ...types.Commitment) (types.RawResult, error)
	GetBlockProduction(config ...any) (types.RawResult, error)
	GetBlocks(startSlot types.Slot, endSlot *types.Slot, commitment ...types.Commitment) (types.RawResult, error)
	GetBlocksWithLimit(startSlot types.Slot, limit uint64, commitment ...types.Commitment) (types.RawResult, error)
	GetBlockTime(slot types.Slot) (types.RawResult, error)
	GetFirstAvailableBlock() (types.RawResult, error)
	GetRecentPerformanceSamples(limit ...uint64) (types.RawResult, error)
	MinimumLedgerSlot() (types.RawResult, error)
	GetEpochSchedule() (types.RawResult, error)
	GetGenesisHash() (types.RawResult, error)
	GetHealth() (types.RawResult, error)
	GetHighestSnapshotSlot() (types.RawResult, error)
	GetIdentity() (types.RawResult, error)
	GetLeaderSchedule(slot *types.Slot, config ...any) (types.RawResult, error)
	GetMaxRetransmitSlot() (types.RawResult, error)
	GetMaxShredInsertSlot() (types.RawResult, error)
	GetSlotLeader(commitment ...types.Commitment) (types.RawResult, error)
	GetVersion() (types.RawResult, error)
	GetInflationGovernor(commitment ...types.Commitment) (types.RawResult, error)
	GetInflationRate() (types.RawResult, error)
	GetInflationReward(addresses []string, config ...any) (types.RawResult, error)
	GetStakeMinimumDelegation(commitment ...types.Commitment) (types.RawResult, error)
	GetSupply(config ...any) (types.RawResult, error)

	// GetEpochInfo retrieves current epoch information with the specified commitment level
	GetEpochInfo(commitment types.Commitment) (types.RawResult, error)

	// GetSlot returns the current slot number with the specified commitment level
	GetSlot(types.Commitment) (types.Slot, error)

	// GetSlotLeaders returns the slot leaders starting from the specified slot up to the limit
	GetSlotLeaders(from types.Slot, limit uint16) (types.SlotLeaders, error)

	// GetClusterNodes retrieves information about all nodes in the cluster
	GetClusterNodes() (types.RawResult, error)

	// GetVoteAccounts returns information about all vote accounts in the cluster
	GetVoteAccounts() (types.RawResult, error)

	// SimulateTransaction simulates a transaction with the specified commitment level
	SimulateTransaction([]byte, types.Commitment) error

	// GetTransaction retrieves transaction information by signature
	GetTransaction(signature string, config ...any) (types.RawResult, error)

	GetBlock(slot types.Slot, commitment types.Commitment) (types.RawResult, error)

	GetConfirmedSlots(from, to types.Slot, commitment types.Commitment) (types.ConfirmedSlots, error)

	AccountSubscribe(pubkey string, config ...any) (*Subscription, error)
	BlockSubscribe(filter any, config ...any) (*Subscription, error)
	LogsSubscribe(filter any, config ...any) (*Subscription, error)
	ProgramSubscribe(programID string, config ...any) (*Subscription, error)
	RootSubscribe() (*Subscription, error)
	SignatureSubscribe(signature string, config ...any) (*Subscription, error)
	SlotSubscribe() (*Subscription, error)
	SlotsUpdatesSubscribe() (*Subscription, error)
	VoteSubscribe() (*Subscription, error)
	SubscribeSlot() (chan *Event[types.Slot], error)
}

// LiteRPC is kept as a compatibility alias for the previous interface name.
type LiteRPC = CoreClient
