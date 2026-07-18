package rpc

import (
	"context"
	"fmt"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/model"
)

var _ CoreClient = (*ThinClient)(nil)

// CoreClient is the top-level interface for interacting with a Solana JSON-RPC node.
// It provides methods for common Solana RPC operations including slot and epoch
// information retrieval, cluster data access, transaction operations, and WebSocket
// subscriptions.
type CoreClient interface {
	RawCall(ctx context.Context, method string, params ...any) (model.RawResult, error)
	RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error)

	GetAccountInfo(ctx context.Context, query GetAccountInfoQuery) (model.RawResult, error)
	GetBalance(ctx context.Context, query GetBalanceQuery) (model.RawResult, error)
	GetLargestAccounts(ctx context.Context, query GetLargestAccountsQuery) (model.RawResult, error)
	GetMinimumBalanceForRentExemption(ctx context.Context, query GetMinimumBalanceForRentExemptionQuery) (model.RawResult, error)
	GetMultipleAccounts(ctx context.Context, query GetMultipleAccountsQuery) (model.RawResult, error)
	GetProgramAccounts(ctx context.Context, query GetProgramAccountsQuery) (model.RawResult, error)
	GetTokenAccountBalance(ctx context.Context, query GetTokenAccountBalanceQuery) (model.RawResult, error)
	GetTokenAccountsByDelegate(ctx context.Context, query GetTokenAccountsByDelegateQuery) (model.RawResult, error)
	GetTokenAccountsByOwner(ctx context.Context, query GetTokenAccountsByOwnerQuery) (model.RawResult, error)
	GetTokenLargestAccounts(ctx context.Context, query GetTokenLargestAccountsQuery) (model.RawResult, error)
	GetTokenSupply(ctx context.Context, query GetTokenSupplyQuery) (model.RawResult, error)
	GetFeeForMessage(ctx context.Context, query GetFeeForMessageQuery) (model.RawResult, error)
	GetLatestBlockhash(ctx context.Context, query GetLatestBlockhashQuery) (model.RawResult, error)
	GetRecentPrioritizationFees(ctx context.Context, query GetRecentPrioritizationFeesQuery) (model.RawResult, error)
	GetSignaturesForAddress(ctx context.Context, query GetSignaturesForAddressQuery) (model.RawResult, error)
	GetSignatureStatuses(ctx context.Context, query GetSignatureStatusesQuery) (model.RawResult, error)
	GetTransactionCount(ctx context.Context, query GetTransactionCountQuery) (model.RawResult, error)
	IsBlockhashValid(ctx context.Context, query IsBlockhashValidQuery) (model.RawResult, error)
	RequestAirdrop(ctx context.Context, query RequestAirdropQuery) (model.RawResult, error)
	SendTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error)
	SendEncodedTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error)
	SimulateEncodedTransaction(ctx context.Context, query SimulateTransactionQuery) (model.RawResult, error)
	GetBlockCommitment(ctx context.Context, query SlotQuery) (model.RawResult, error)
	GetBlockHeight(ctx context.Context, query GetBlockHeightQuery) (model.RawResult, error)
	GetBlockProduction(ctx context.Context, query GetBlockProductionQuery) (model.RawResult, error)
	GetBlocks(ctx context.Context, query GetBlocksQuery) (model.RawResult, error)
	GetBlocksWithLimit(ctx context.Context, query GetBlocksWithLimitQuery) (model.RawResult, error)
	GetBlockTime(ctx context.Context, query SlotQuery) (model.RawResult, error)
	GetFirstAvailableBlock(ctx context.Context) (model.RawResult, error)
	GetRecentPerformanceSamples(ctx context.Context, query GetRecentPerformanceSamplesQuery) (model.RawResult, error)
	MinimumLedgerSlot(ctx context.Context) (model.RawResult, error)
	GetEpochSchedule(ctx context.Context) (model.RawResult, error)
	GetGenesisHash(ctx context.Context) (model.RawResult, error)
	GetHealth(ctx context.Context) (model.RawResult, error)
	GetHighestSnapshotSlot(ctx context.Context) (model.RawResult, error)
	GetIdentity(ctx context.Context) (model.RawResult, error)
	GetLeaderSchedule(ctx context.Context, query GetLeaderScheduleQuery) (model.RawResult, error)
	GetMaxRetransmitSlot(ctx context.Context) (model.RawResult, error)
	GetMaxShredInsertSlot(ctx context.Context) (model.RawResult, error)
	GetSlotLeader(ctx context.Context, query GetSlotLeaderQuery) (model.RawResult, error)
	GetVersion(ctx context.Context) (model.RawResult, error)
	GetInflationGovernor(ctx context.Context, query GetInflationGovernorQuery) (model.RawResult, error)
	GetInflationRate(ctx context.Context) (model.RawResult, error)
	GetInflationReward(ctx context.Context, query GetInflationRewardQuery) (model.RawResult, error)
	GetStakeMinimumDelegation(ctx context.Context, query GetStakeMinimumDelegationQuery) (model.RawResult, error)
	GetSupply(ctx context.Context, query GetSupplyQuery) (model.RawResult, error)

	// GetEpochInfo retrieves current epoch information with the specified commitment level
	GetEpochInfo(ctx context.Context, query GetEpochInfoQuery) (model.RawResult, error)

	// GetSlot returns the current slot number with the specified commitment level
	GetSlot(ctx context.Context, query GetSlotQuery) (model.Slot, error)

	// GetSlotLeaders returns the slot leaders starting from the specified slot up to the limit
	GetSlotLeaders(ctx context.Context, query GetSlotLeadersQuery) (model.SlotLeaders, error)

	// GetClusterNodes retrieves information about all nodes in the cluster
	GetClusterNodes(ctx context.Context) (model.RawResult, error)

	// GetVoteAccounts returns information about all vote accounts in the cluster
	GetVoteAccounts(ctx context.Context) (model.RawResult, error)

	// SimulateTransaction simulates a transaction with the specified commitment level
	SimulateTransaction(ctx context.Context, query SimulateTransactionQuery) error

	// GetTransaction retrieves transaction information by signature
	GetTransaction(ctx context.Context, query GetTransactionQuery) (model.RawResult, error)

	GetBlock(ctx context.Context, query GetBlockQuery) (model.RawResult, error)

	GetConfirmedSlots(ctx context.Context, query GetConfirmedSlotsQuery) (model.ConfirmedSlots, error)

	AccountSubscribe(ctx context.Context, query AccountSubscribeQuery) (*Subscription, error)
	BlockSubscribe(ctx context.Context, query BlockSubscribeQuery) (*Subscription, error)
	LogsSubscribe(ctx context.Context, query LogsSubscribeQuery) (*Subscription, error)
	ProgramSubscribe(ctx context.Context, query ProgramSubscribeQuery) (*Subscription, error)
	RootSubscribe(ctx context.Context) (*Subscription, error)
	SignatureSubscribe(ctx context.Context, query SignatureSubscribeQuery) (*Subscription, error)
	SlotSubscribe(ctx context.Context) (*Subscription, error)
	SlotsUpdatesSubscribe(ctx context.Context) (*Subscription, error)
	VoteSubscribe(ctx context.Context) (*Subscription, error)
	SubscribeSlot(ctx context.Context) (chan *Event[model.Slot], error)
}

// LiteRPC is kept as a compatibility alias for the previous interface name.
type LiteRPC = CoreClient

// IdentifiedQuery carries an optional caller-supplied request ID.
// When Id is zero the sequencer assigns one automatically.
type IdentifiedQuery struct {
	Id uint `json:"-"`
}

// QueryParams holds the request ID and positional parameters for a JSON-RPC call.
type QueryParams struct {
	Id     uint
	Params []any
}

func DefaultQueryParams() *QueryParams {
	return &QueryParams{Id: 1, Params: []any{}}
}

func Query(params ...any) *QueryParams {
	return &QueryParams{Params: params}
}

func QueryWithId(id uint, params ...any) *QueryParams {
	return &QueryParams{Id: id, Params: params}
}

func (params *QueryParams) Adjust() {
	if params.Id == 0 {
		params.Id = 1
	}
	if params.Params == nil {
		params.Params = []any{}
	}
}

// Encoding selects how account, transaction, and instruction data is returned.
type Encoding string

const (
	EncodingBase58     Encoding = "base58"
	EncodingBase64     Encoding = "base64"
	EncodingBase64Zstd Encoding = "base64+zstd"
	EncodingJSON       Encoding = "json"
	EncodingJSONParsed Encoding = "jsonParsed"
)

// TransactionDetails selects how much transaction detail block responses include.
type TransactionDetails string

const (
	TransactionDetailsFull       TransactionDetails = "full"
	TransactionDetailsAccounts   TransactionDetails = "accounts"
	TransactionDetailsSignatures TransactionDetails = "signatures"
	TransactionDetailsNone       TransactionDetails = "none"
)

// DataSlice requests a byte range from account data.
type DataSlice struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// LargestAccountsFilter limits getLargestAccounts results.
type LargestAccountsFilter string

const (
	LargestAccountsFilterCirculating    LargestAccountsFilter = "circulating"
	LargestAccountsFilterNonCirculating LargestAccountsFilter = "nonCirculating"
)

// MemcmpFilter matches account data at an offset.
type MemcmpFilter struct {
	Offset   uint64   `json:"offset"`
	Bytes    string   `json:"bytes"`
	Encoding Encoding `json:"encoding,omitempty"`
}

// ProgramAccountsFilter configures a single getProgramAccounts/programSubscribe filter.
type ProgramAccountsFilter struct {
	DataSize uint64        `json:"dataSize,omitempty"`
	Memcmp   *MemcmpFilter `json:"memcmp,omitempty"`
	// DataSizeSet distinguishes an explicit dataSize of zero from an unset filter.
	DataSizeSet bool `json:"-"`
}

func (filter ProgramAccountsFilter) MarshalJSON() ([]byte, error) {
	hasDataSize := filter.DataSize != 0 || filter.DataSizeSet
	if hasDataSize == (filter.Memcmp != nil) {
		return nil, fmt.Errorf("program accounts filter must set exactly one of dataSize or memcmp")
	}
	if filter.Memcmp != nil {
		return jsonrpc.Marshal(struct {
			Memcmp *MemcmpFilter `json:"memcmp"`
		}{Memcmp: filter.Memcmp})
	}
	return jsonrpc.Marshal(struct {
		DataSize uint64 `json:"dataSize"`
	}{DataSize: filter.DataSize})
}

// TokenAccountsFilter selects token accounts by mint or token program id.
type TokenAccountsFilter struct {
	Mint      string `json:"mint,omitempty"`
	ProgramID string `json:"programId,omitempty"`
}

func (filter TokenAccountsFilter) MarshalJSON() ([]byte, error) {
	if (filter.Mint != "") == (filter.ProgramID != "") {
		return nil, fmt.Errorf("token accounts filter must set exactly one of mint or programId")
	}
	type tokenAccountsFilter TokenAccountsFilter
	return jsonrpc.Marshal(tokenAccountsFilter(filter))
}

// SimulateTransactionAccounts requests account snapshots from simulateTransaction.
type SimulateTransactionAccounts struct {
	Encoding  Encoding `json:"encoding,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

// BlockProductionRange limits getBlockProduction to a slot range.
type BlockProductionRange struct {
	FirstSlot model.Slot `json:"firstSlot"`
	LastSlot  model.Slot `json:"lastSlot,omitempty"`
	// LastSlotSet distinguishes an explicit lastSlot of zero from an unset lastSlot.
	LastSlotSet bool `json:"-"`
}

func (slotRange BlockProductionRange) MarshalJSON() ([]byte, error) {
	var lastSlot *model.Slot
	if slotRange.LastSlot != 0 || slotRange.LastSlotSet {
		lastSlot = &slotRange.LastSlot
	}
	return jsonrpc.Marshal(struct {
		FirstSlot model.Slot  `json:"firstSlot"`
		LastSlot  *model.Slot `json:"lastSlot,omitempty"`
	}{FirstSlot: slotRange.FirstSlot, LastSlot: lastSlot})
}

// BlockSubscribeFilterKind selects the stream scope for blockSubscribe.
type BlockSubscribeFilterKind string

const (
	BlockSubscribeAll BlockSubscribeFilterKind = "all"
	// BlockSubscribeAllWithVotes is retained for source compatibility but is not
	// accepted by Solana blockSubscribe. Use BlockSubscribeAll instead.
	BlockSubscribeAllWithVotes BlockSubscribeFilterKind = "allWithVotes"
)

// BlockSubscribeFilter configures the first blockSubscribe parameter.
type BlockSubscribeFilter struct {
	Kind                     BlockSubscribeFilterKind `json:"-"`
	MentionsAccountOrProgram string                   `json:"mentionsAccountOrProgram,omitempty"`
}

func (filter BlockSubscribeFilter) MarshalJSON() ([]byte, error) {
	if filter.Kind != "" && filter.MentionsAccountOrProgram != "" {
		return nil, fmt.Errorf("block subscribe filter must set either kind or mentionsAccountOrProgram, not both")
	}
	if filter.MentionsAccountOrProgram != "" {
		return jsonrpc.Marshal(struct {
			MentionsAccountOrProgram string `json:"mentionsAccountOrProgram"`
		}{MentionsAccountOrProgram: filter.MentionsAccountOrProgram})
	}
	if filter.Kind != BlockSubscribeAll {
		return nil, fmt.Errorf("block subscribe filter kind %q is invalid", filter.Kind)
	}
	return jsonrpc.Marshal(filter.Kind)
}

// LogsSubscribeFilterKind selects the stream scope for logsSubscribe.
type LogsSubscribeFilterKind string

const (
	LogsSubscribeAll          LogsSubscribeFilterKind = "all"
	LogsSubscribeAllWithVotes LogsSubscribeFilterKind = "allWithVotes"
)

// LogsSubscribeFilter configures the first logsSubscribe parameter.
type LogsSubscribeFilter struct {
	Kind     LogsSubscribeFilterKind `json:"-"`
	Mentions []string                `json:"mentions,omitempty"`
}

func (filter LogsSubscribeFilter) MarshalJSON() ([]byte, error) {
	if filter.Kind != "" && len(filter.Mentions) > 0 {
		return nil, fmt.Errorf("logs subscribe filter must set either kind or mentions, not both")
	}
	if len(filter.Mentions) > 0 {
		if len(filter.Mentions) != 1 || filter.Mentions[0] == "" {
			return nil, fmt.Errorf("logs subscribe mentions filter must contain exactly one non-empty pubkey")
		}
		return jsonrpc.Marshal(struct {
			Mentions []string `json:"mentions"`
		}{Mentions: filter.Mentions})
	}
	if filter.Kind != LogsSubscribeAll && filter.Kind != LogsSubscribeAllWithVotes {
		return nil, fmt.Errorf("logs subscribe filter kind %q is invalid", filter.Kind)
	}
	return jsonrpc.Marshal(filter.Kind)
}

type GetAccountInfoQuery struct {
	IdentifiedQuery
	Pubkey         string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	Encoding       Encoding         `json:"encoding,omitempty"`
	DataSlice      *DataSlice       `json:"dataSlice,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetBalanceQuery struct {
	IdentifiedQuery
	Pubkey         string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetLargestAccountsQuery struct {
	IdentifiedQuery
	Commitment model.Commitment      `json:"commitment,omitempty"`
	Filter     LargestAccountsFilter `json:"filter,omitempty"`
}

type GetMinimumBalanceForRentExemptionQuery struct {
	IdentifiedQuery
	DataSize   uint64
	Commitment model.Commitment
}

type GetMultipleAccountsQuery struct {
	IdentifiedQuery
	Pubkeys        []string         `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	Encoding       Encoding         `json:"encoding,omitempty"`
	DataSlice      *DataSlice       `json:"dataSlice,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetProgramAccountsQuery struct {
	IdentifiedQuery
	ProgramID      string                  `json:"-"`
	Commitment     model.Commitment        `json:"commitment,omitempty"`
	Encoding       Encoding                `json:"encoding,omitempty"`
	DataSlice      *DataSlice              `json:"dataSlice,omitempty"`
	Filters        []ProgramAccountsFilter `json:"filters,omitempty"`
	WithContext    bool                    `json:"withContext,omitempty"`
	MinContextSlot model.Slot              `json:"minContextSlot,omitempty"`
	SortResults    *bool                   `json:"sortResults,omitempty"`
}

type GetTokenAccountBalanceQuery struct {
	IdentifiedQuery
	Pubkey     string           `json:"-"`
	Commitment model.Commitment `json:"commitment,omitempty"`
}

type GetTokenAccountsByDelegateQuery struct {
	IdentifiedQuery
	Delegate       string              `json:"-"`
	Filter         TokenAccountsFilter `json:"-"`
	Commitment     model.Commitment    `json:"commitment,omitempty"`
	Encoding       Encoding            `json:"encoding,omitempty"`
	DataSlice      *DataSlice          `json:"dataSlice,omitempty"`
	MinContextSlot model.Slot          `json:"minContextSlot,omitempty"`
}

type GetTokenAccountsByOwnerQuery struct {
	IdentifiedQuery
	Owner          string              `json:"-"`
	Filter         TokenAccountsFilter `json:"-"`
	Commitment     model.Commitment    `json:"commitment,omitempty"`
	Encoding       Encoding            `json:"encoding,omitempty"`
	DataSlice      *DataSlice          `json:"dataSlice,omitempty"`
	MinContextSlot model.Slot          `json:"minContextSlot,omitempty"`
}

type GetTokenLargestAccountsQuery struct {
	IdentifiedQuery
	Mint       string           `json:"-"`
	Commitment model.Commitment `json:"commitment,omitempty"`
}

type GetTokenSupplyQuery struct {
	IdentifiedQuery
	Mint       string           `json:"-"`
	Commitment model.Commitment `json:"commitment,omitempty"`
}

type GetFeeForMessageQuery struct {
	IdentifiedQuery
	Message        string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetLatestBlockhashQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetRecentPrioritizationFeesQuery struct {
	IdentifiedQuery
	Accounts []string
}

type GetSignaturesForAddressQuery struct {
	IdentifiedQuery
	Address        string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
	Limit          uint64           `json:"limit,omitempty"`
	Before         string           `json:"before,omitempty"`
	Until          string           `json:"until,omitempty"`
}

type GetSignatureStatusesQuery struct {
	IdentifiedQuery
	Signatures               []string `json:"-"`
	SearchTransactionHistory bool     `json:"searchTransactionHistory,omitempty"`
}

type GetTransactionCountQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type IsBlockhashValidQuery struct {
	IdentifiedQuery
	Blockhash      string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type RequestAirdropQuery struct {
	IdentifiedQuery
	Pubkey     string           `json:"-"`
	Lamports   uint64           `json:"-"`
	Commitment model.Commitment `json:"commitment,omitempty"`
}

type SendTransactionQuery struct {
	IdentifiedQuery
	Serialized          []byte           `json:"-"`
	Encoded             string           `json:"-"`
	SkipPreflight       bool             `json:"skipPreflight,omitempty"`
	PreflightCommitment model.Commitment `json:"preflightCommitment,omitempty"`
	Encoding            Encoding         `json:"encoding,omitempty"`
	MaxRetries          uint64           `json:"maxRetries,omitempty"`
	MinContextSlot      model.Slot       `json:"minContextSlot,omitempty"`
	// MaxRetriesSet distinguishes an explicit maxRetries of zero from an unset value.
	MaxRetriesSet bool `json:"-"`
}

func (query SendTransactionQuery) MarshalJSON() ([]byte, error) {
	var maxRetries *uint64
	if query.MaxRetries != 0 || query.MaxRetriesSet {
		maxRetries = &query.MaxRetries
	}
	return jsonrpc.Marshal(struct {
		SkipPreflight       bool             `json:"skipPreflight,omitempty"`
		PreflightCommitment model.Commitment `json:"preflightCommitment,omitempty"`
		Encoding            Encoding         `json:"encoding,omitempty"`
		MaxRetries          *uint64          `json:"maxRetries,omitempty"`
		MinContextSlot      model.Slot       `json:"minContextSlot,omitempty"`
	}{
		SkipPreflight:       query.SkipPreflight,
		PreflightCommitment: query.PreflightCommitment,
		Encoding:            query.Encoding,
		MaxRetries:          maxRetries,
		MinContextSlot:      query.MinContextSlot,
	})
}

type SimulateTransactionQuery struct {
	IdentifiedQuery
	Serialized             []byte                       `json:"-"`
	Encoded                string                       `json:"-"`
	SigVerify              bool                         `json:"sigVerify,omitempty"`
	ReplaceRecentBlockhash bool                         `json:"replaceRecentBlockhash,omitempty"`
	Commitment             model.Commitment             `json:"commitment,omitempty"`
	Encoding               Encoding                     `json:"encoding,omitempty"`
	Accounts               *SimulateTransactionAccounts `json:"accounts,omitempty"`
	MinContextSlot         model.Slot                   `json:"minContextSlot,omitempty"`
	InnerInstructions      bool                         `json:"innerInstructions,omitempty"`
}

type SlotQuery struct {
	IdentifiedQuery
	Slot model.Slot
}

type GetBlockHeightQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetBlockProductionQuery struct {
	IdentifiedQuery
	Identity string `json:"identity,omitempty"`
	// FirstSlot and LastSlot are deprecated compatibility fields. New code should
	// set Range; these values are translated into Range and never serialized at
	// the top level.
	FirstSlot  model.Slot            `json:"-"`
	LastSlot   model.Slot            `json:"-"`
	Range      *BlockProductionRange `json:"range,omitempty"`
	Commitment model.Commitment      `json:"commitment,omitempty"`
}

func (query GetBlockProductionQuery) MarshalJSON() ([]byte, error) {
	normalized, err := normalizeGetBlockProductionQuery(query)
	if err != nil {
		return nil, err
	}
	return jsonrpc.Marshal(struct {
		Identity   string                `json:"identity,omitempty"`
		Range      *BlockProductionRange `json:"range,omitempty"`
		Commitment model.Commitment      `json:"commitment,omitempty"`
	}{Identity: normalized.Identity, Range: normalized.Range, Commitment: normalized.Commitment})
}

type GetBlocksQuery struct {
	IdentifiedQuery
	StartSlot  model.Slot
	EndSlot    *model.Slot
	Commitment model.Commitment
}

type GetBlocksWithLimitQuery struct {
	IdentifiedQuery
	StartSlot  model.Slot
	Limit      uint64
	Commitment model.Commitment
}

type GetRecentPerformanceSamplesQuery struct {
	IdentifiedQuery
	Limit uint64
}

type GetLeaderScheduleQuery struct {
	IdentifiedQuery
	Slot       *model.Slot      `json:"-"`
	Identity   string           `json:"identity,omitempty"`
	Commitment model.Commitment `json:"commitment,omitempty"`
}

type GetSlotLeaderQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetInflationGovernorQuery struct {
	IdentifiedQuery
	Commitment model.Commitment
}

type GetInflationRewardQuery struct {
	IdentifiedQuery
	Addresses      []string         `json:"-"`
	Epoch          uint64           `json:"epoch,omitempty"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
	// EpochSet distinguishes an explicit epoch of zero from an unset epoch.
	EpochSet bool `json:"-"`
}

func (query GetInflationRewardQuery) MarshalJSON() ([]byte, error) {
	var epoch *uint64
	if query.Epoch != 0 || query.EpochSet {
		epoch = &query.Epoch
	}
	return jsonrpc.Marshal(struct {
		Epoch          *uint64          `json:"epoch,omitempty"`
		Commitment     model.Commitment `json:"commitment,omitempty"`
		MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
	}{Epoch: epoch, Commitment: query.Commitment, MinContextSlot: query.MinContextSlot})
}

type GetStakeMinimumDelegationQuery struct {
	IdentifiedQuery
	Commitment model.Commitment
}

type GetSupplyQuery struct {
	IdentifiedQuery
	Commitment                        model.Commitment `json:"commitment,omitempty"`
	ExcludeNonCirculatingAccountsList bool             `json:"excludeNonCirculatingAccountsList,omitempty"`
}

type GetEpochInfoQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetSlotQuery struct {
	IdentifiedQuery
	Commitment     model.Commitment `json:"commitment,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type GetSlotLeadersQuery struct {
	IdentifiedQuery
	From  model.Slot
	Limit uint16
}

type GetTransactionQuery struct {
	IdentifiedQuery
	Signature                      string           `json:"-"`
	Commitment                     model.Commitment `json:"commitment,omitempty"`
	Encoding                       Encoding         `json:"encoding,omitempty"`
	MaxSupportedTransactionVersion *uint64          `json:"maxSupportedTransactionVersion,omitempty"`
}

type GetBlockQuery struct {
	IdentifiedQuery
	Slot                           model.Slot         `json:"-"`
	Commitment                     model.Commitment   `json:"commitment,omitempty"`
	Encoding                       Encoding           `json:"encoding,omitempty"`
	TransactionDetails             TransactionDetails `json:"transactionDetails,omitempty"`
	Rewards                        *bool              `json:"rewards,omitempty"`
	MaxSupportedTransactionVersion *uint64            `json:"maxSupportedTransactionVersion,omitempty"`
}

type GetConfirmedSlotsQuery struct {
	IdentifiedQuery
	From       model.Slot
	To         model.Slot
	Commitment model.Commitment
}

type AccountSubscribeQuery struct {
	IdentifiedQuery
	Pubkey         string           `json:"-"`
	Commitment     model.Commitment `json:"commitment,omitempty"`
	Encoding       Encoding         `json:"encoding,omitempty"`
	DataSlice      *DataSlice       `json:"dataSlice,omitempty"`
	MinContextSlot model.Slot       `json:"minContextSlot,omitempty"`
}

type BlockSubscribeQuery struct {
	IdentifiedQuery
	Filter                         BlockSubscribeFilter `json:"-"`
	Commitment                     model.Commitment     `json:"commitment,omitempty"`
	Encoding                       Encoding             `json:"encoding,omitempty"`
	TransactionDetails             TransactionDetails   `json:"transactionDetails,omitempty"`
	ShowRewards                    bool                 `json:"showRewards,omitempty"`
	MaxSupportedTransactionVersion *uint64              `json:"maxSupportedTransactionVersion,omitempty"`
}

type LogsSubscribeQuery struct {
	IdentifiedQuery
	Filter     LogsSubscribeFilter `json:"-"`
	Commitment model.Commitment    `json:"commitment,omitempty"`
}

type ProgramSubscribeQuery struct {
	IdentifiedQuery
	ProgramID      string                  `json:"-"`
	Commitment     model.Commitment        `json:"commitment,omitempty"`
	Encoding       Encoding                `json:"encoding,omitempty"`
	DataSlice      *DataSlice              `json:"dataSlice,omitempty"`
	Filters        []ProgramAccountsFilter `json:"filters,omitempty"`
	WithContext    bool                    `json:"withContext,omitempty"`
	MinContextSlot model.Slot              `json:"minContextSlot,omitempty"`
}

type SignatureSubscribeQuery struct {
	IdentifiedQuery
	Signature                  string           `json:"-"`
	Commitment                 model.Commitment `json:"commitment,omitempty"`
	EnableReceivedNotification bool             `json:"enableReceivedNotification,omitempty"`
}
