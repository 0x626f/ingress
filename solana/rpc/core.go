package rpc

import (
	"context"

	"github.com/0x626f/ingress/solana/types"
)

var _ CoreClient = (*ThinClient)(nil)

// CoreClient is the top-level interface for interacting with a Solana JSON-RPC node.
// It provides methods for common Solana RPC operations including slot and epoch
// information retrieval, cluster data access, transaction operations, and WebSocket
// subscriptions.
type CoreClient interface {
	RawCall(ctx context.Context, method string, params ...any) (types.RawResult, error)
	RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error)

	GetAccountInfo(ctx context.Context, pubkey string, config ...any) (types.RawResult, error)
	GetBalance(ctx context.Context, pubkey string, config ...any) (types.RawResult, error)
	GetLargestAccounts(ctx context.Context, config ...any) (types.RawResult, error)
	GetMinimumBalanceForRentExemption(ctx context.Context, dataSize uint64, commitment ...types.Commitment) (types.RawResult, error)
	GetMultipleAccounts(ctx context.Context, pubkeys []string, config ...any) (types.RawResult, error)
	GetProgramAccounts(ctx context.Context, programID string, config ...any) (types.RawResult, error)
	GetTokenAccountBalance(ctx context.Context, pubkey string, commitment ...types.Commitment) (types.RawResult, error)
	GetTokenAccountsByDelegate(ctx context.Context, delegate string, filter any, config ...any) (types.RawResult, error)
	GetTokenAccountsByOwner(ctx context.Context, owner string, filter any, config ...any) (types.RawResult, error)
	GetTokenLargestAccounts(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error)
	GetTokenSupply(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error)
	GetFeeForMessage(ctx context.Context, message string, commitment ...types.Commitment) (types.RawResult, error)
	GetLatestBlockhash(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	GetRecentPrioritizationFees(ctx context.Context, accounts ...string) (types.RawResult, error)
	GetSignaturesForAddress(ctx context.Context, address string, config ...any) (types.RawResult, error)
	GetSignatureStatuses(ctx context.Context, signatures []string, config ...any) (types.RawResult, error)
	GetTransactionCount(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	IsBlockhashValid(ctx context.Context, blockhash string, commitment ...types.Commitment) (types.RawResult, error)
	RequestAirdrop(ctx context.Context, pubkey string, lamports uint64, commitment ...types.Commitment) (types.RawResult, error)
	SendTransaction(ctx context.Context, serialized []byte, config ...any) (types.RawResult, error)
	SendEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error)
	SimulateEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error)
	GetBlockCommitment(ctx context.Context, slot types.Slot) (types.RawResult, error)
	GetBlockHeight(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	GetBlockProduction(ctx context.Context, config ...any) (types.RawResult, error)
	GetBlocks(ctx context.Context, startSlot types.Slot, endSlot *types.Slot, commitment ...types.Commitment) (types.RawResult, error)
	GetBlocksWithLimit(ctx context.Context, startSlot types.Slot, limit uint64, commitment ...types.Commitment) (types.RawResult, error)
	GetBlockTime(ctx context.Context, slot types.Slot) (types.RawResult, error)
	GetFirstAvailableBlock(ctx context.Context) (types.RawResult, error)
	GetRecentPerformanceSamples(ctx context.Context, limit ...uint64) (types.RawResult, error)
	MinimumLedgerSlot(ctx context.Context) (types.RawResult, error)
	GetEpochSchedule(ctx context.Context) (types.RawResult, error)
	GetGenesisHash(ctx context.Context) (types.RawResult, error)
	GetHealth(ctx context.Context) (types.RawResult, error)
	GetHighestSnapshotSlot(ctx context.Context) (types.RawResult, error)
	GetIdentity(ctx context.Context) (types.RawResult, error)
	GetLeaderSchedule(ctx context.Context, slot *types.Slot, config ...any) (types.RawResult, error)
	GetMaxRetransmitSlot(ctx context.Context) (types.RawResult, error)
	GetMaxShredInsertSlot(ctx context.Context) (types.RawResult, error)
	GetSlotLeader(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	GetVersion(ctx context.Context) (types.RawResult, error)
	GetInflationGovernor(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	GetInflationRate(ctx context.Context) (types.RawResult, error)
	GetInflationReward(ctx context.Context, addresses []string, config ...any) (types.RawResult, error)
	GetStakeMinimumDelegation(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error)
	GetSupply(ctx context.Context, config ...any) (types.RawResult, error)

	// GetEpochInfo retrieves current epoch information with the specified commitment level
	GetEpochInfo(ctx context.Context, commitment types.Commitment) (types.RawResult, error)

	// GetSlot returns the current slot number with the specified commitment level
	GetSlot(ctx context.Context, commitment types.Commitment) (types.Slot, error)

	// GetSlotLeaders returns the slot leaders starting from the specified slot up to the limit
	GetSlotLeaders(ctx context.Context, from types.Slot, limit uint16) (types.SlotLeaders, error)

	// GetClusterNodes retrieves information about all nodes in the cluster
	GetClusterNodes(ctx context.Context) (types.RawResult, error)

	// GetVoteAccounts returns information about all vote accounts in the cluster
	GetVoteAccounts(ctx context.Context) (types.RawResult, error)

	// SimulateTransaction simulates a transaction with the specified commitment level
	SimulateTransaction(ctx context.Context, serialized []byte, commitment types.Commitment) error

	// GetTransaction retrieves transaction information by signature
	GetTransaction(ctx context.Context, signature string, config ...any) (types.RawResult, error)

	GetBlock(ctx context.Context, slot types.Slot, commitment types.Commitment) (types.RawResult, error)

	GetConfirmedSlots(ctx context.Context, from, to types.Slot, commitment types.Commitment) (types.ConfirmedSlots, error)

	AccountSubscribe(ctx context.Context, pubkey string, config ...any) (*Subscription, error)
	BlockSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error)
	LogsSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error)
	ProgramSubscribe(ctx context.Context, programID string, config ...any) (*Subscription, error)
	RootSubscribe(ctx context.Context) (*Subscription, error)
	SignatureSubscribe(ctx context.Context, signature string, config ...any) (*Subscription, error)
	SlotSubscribe(ctx context.Context) (*Subscription, error)
	SlotsUpdatesSubscribe(ctx context.Context) (*Subscription, error)
	VoteSubscribe(ctx context.Context) (*Subscription, error)
	SubscribeSlot(ctx context.Context) (chan *Event[types.Slot], error)
}

// LiteRPC is kept as a compatibility alias for the previous interface name.
type LiteRPC = CoreClient
