// Package model provides typed response structures for Solana JSON-RPC calls.
package model

import (
	"encoding/json"
	"time"
)

// Commitment represents the commitment level for Solana RPC requests.
type Commitment = string

const (
	// SupportedJsonRpcVersion is the JSON-RPC protocol version supported by this rpc.
	SupportedJsonRpcVersion = "2.0"

	// DefaultSlotWindow is the default time window for slot updates on Solana.
	DefaultSlotWindow = 400 * time.Millisecond

	// ProcessedCommitment means the node has processed the transaction but it
	// has not been confirmed by the cluster.
	ProcessedCommitment Commitment = "processed"

	// ConfirmedCommitment means the transaction has been confirmed by the
	// cluster with maximum lockout.
	ConfirmedCommitment Commitment = "confirmed"

	// FinalizedCommitment means the transaction has been finalized and cannot
	// be rolled back.
	FinalizedCommitment Commitment = "finalized"

	// MaxSlotLeadersRange is the maximum number of slot leaders per request.
	MaxSlotLeadersRange uint16 = 5000

	// MinSlotLeadersRange is the minimum number of slot leaders per request.
	MinSlotLeadersRange = 1
)

// RawResult represents raw JSON response data from RPC calls.
type RawResult []byte

// Slot represents a Solana slot number.
type Slot = uint64

// SlotLeaders represents validator public keys scheduled to produce consecutive slots.
type SlotLeaders = []string

// SlotLeaderTPUAddress represents a validator TPU endpoint address.
type SlotLeaderTPUAddress = string

// ConfirmedSlots represents a collection of confirmed slot numbers.
type ConfirmedSlots = []Slot

// UnixTimestamp is a Unix timestamp in seconds.
type UnixTimestamp = int64

// Lamports is an amount of Solana native currency.
type Lamports = uint64

// Context is the RPC context returned by many Solana endpoints.
type Context struct {
	Slot       Slot    `json:"slot"`
	APIVersion *string `json:"apiVersion,omitempty"`
}

// ContextResult wraps a value returned with an RPC context.
type ContextResult[T any] struct {
	Context Context `json:"context"`
	Value   T       `json:"value"`
}

// Account is a Solana account object. Data is raw because its JSON shape
// depends on the requested encoding.
type Account struct {
	Lamports   Lamports        `json:"lamports"`
	Owner      string          `json:"owner"`
	Data       json.RawMessage `json:"data"`
	Executable bool            `json:"executable"`
	RentEpoch  uint64          `json:"rentEpoch"`
	Space      uint64          `json:"space,omitempty"`
}

// AccountInfo is the result of getAccountInfo or accountSubscribe.
type AccountInfo = ContextResult[*Account]

// Balance is the result of getBalance.
type Balance = ContextResult[Lamports]

// LargestAccount is a single account returned by getLargestAccounts.
type LargestAccount struct {
	Address  string   `json:"address"`
	Lamports Lamports `json:"lamports"`
}

// LargestAccounts is the result of getLargestAccounts.
type LargestAccounts = ContextResult[[]LargestAccount]

// MultipleAccounts is the result of getMultipleAccounts.
type MultipleAccounts = ContextResult[[]*Account]

// ProgramAccount is a program-owned account returned by getProgramAccounts.
type ProgramAccount struct {
	Pubkey  string  `json:"pubkey"`
	Account Account `json:"account"`
}

// ProgramAccounts is the result of getProgramAccounts.
type ProgramAccounts []ProgramAccount

// TokenAmount is an SPL token amount response.
type TokenAmount struct {
	Amount         string   `json:"amount"`
	Decimals       uint8    `json:"decimals"`
	UIAmount       *float64 `json:"uiAmount"`
	UIAmountString string   `json:"uiAmountString"`
}

// TokenAccountBalance is the result of getTokenAccountBalance.
type TokenAccountBalance = ContextResult[TokenAmount]

// TokenSupply is the result of getTokenSupply.
type TokenSupply = ContextResult[TokenAmount]

// TokenLargestAccount is a single account returned by getTokenLargestAccounts.
type TokenLargestAccount struct {
	Address        string   `json:"address"`
	Amount         string   `json:"amount"`
	Decimals       uint8    `json:"decimals"`
	UIAmount       *float64 `json:"uiAmount"`
	UIAmountString string   `json:"uiAmountString"`
}

// TokenLargestAccounts is the result of getTokenLargestAccounts.
type TokenLargestAccounts = ContextResult[[]TokenLargestAccount]

// TokenAccount is a token account returned by owner/delegate token-account queries.
type TokenAccount struct {
	Pubkey  string  `json:"pubkey"`
	Account Account `json:"account"`
}

// TokenAccounts is the result of getTokenAccountsByOwner and getTokenAccountsByDelegate.
type TokenAccounts = ContextResult[[]TokenAccount]

// FeeForMessage is the result of getFeeForMessage.
type FeeForMessage = ContextResult[*uint64]

// StakeMinimumDelegation is the result of getStakeMinimumDelegation.
type StakeMinimumDelegation = ContextResult[Lamports]

// LatestBlockhashValue is the value returned by getLatestBlockhash.
type LatestBlockhashValue struct {
	Blockhash            string `json:"blockhash"`
	LastValidBlockHeight uint64 `json:"lastValidBlockHeight"`
}

// LatestBlockhash is the result of getLatestBlockhash.
type LatestBlockhash = ContextResult[LatestBlockhashValue]

// RecentPrioritizationFee is one entry returned by getRecentPrioritizationFees.
type RecentPrioritizationFee struct {
	Slot              Slot   `json:"slot"`
	PrioritizationFee uint64 `json:"prioritizationFee"`
}

// RecentPrioritizationFees is the result of getRecentPrioritizationFees.
type RecentPrioritizationFees []RecentPrioritizationFee

// SignatureInfo is one signature record returned by getSignaturesForAddress.
type SignatureInfo struct {
	Signature          string          `json:"signature"`
	Slot               Slot            `json:"slot"`
	Err                json.RawMessage `json:"err"`
	Memo               *string         `json:"memo"`
	BlockTime          *UnixTimestamp  `json:"blockTime"`
	ConfirmationStatus *string         `json:"confirmationStatus,omitempty"`
}

// SignaturesForAddress is the result of getSignaturesForAddress.
type SignaturesForAddress []SignatureInfo

// SignatureStatus is one status returned by getSignatureStatuses.
type SignatureStatus struct {
	Slot               Slot            `json:"slot"`
	Confirmations      *uint64         `json:"confirmations"`
	Err                json.RawMessage `json:"err"`
	ConfirmationStatus *string         `json:"confirmationStatus,omitempty"`
	Status             json.RawMessage `json:"status,omitempty"`
}

// SignatureStatuses is the result of getSignatureStatuses.
type SignatureStatuses = ContextResult[[]*SignatureStatus]

// BlockhashValid is the result of isBlockhashValid.
type BlockhashValid = ContextResult[bool]

// Transaction is a Solana transaction response. The Transaction and Meta fields
// are raw because their JSON shape varies with encoding and transaction version.
type Transaction struct {
	Slot        Slot            `json:"slot"`
	BlockTime   *UnixTimestamp  `json:"blockTime"`
	Transaction json.RawMessage `json:"transaction"`
	Meta        json.RawMessage `json:"meta"`
	Version     json.RawMessage `json:"version,omitempty"`
}

// SimulatedTransaction is the result of simulateTransaction.
type SimulatedTransaction = ContextResult[SimulatedTransactionValue]

// SimulatedTransactionValue contains transaction simulation output.
type SimulatedTransactionValue struct {
	Err                  json.RawMessage       `json:"err"`
	Logs                 []string              `json:"logs"`
	Accounts             []json.RawMessage     `json:"accounts,omitempty"`
	UnitsConsumed        *uint64               `json:"unitsConsumed,omitempty"`
	ReturnData           *ReturnData           `json:"returnData,omitempty"`
	InnerInstructions    json.RawMessage       `json:"innerInstructions,omitempty"`
	ReplacementBlockhash *LatestBlockhashValue `json:"replacementBlockhash,omitempty"`
}

// ReturnData is program return data emitted during transaction simulation.
type ReturnData struct {
	ProgramID string    `json:"programId"`
	Data      [2]string `json:"data"`
}

// Block is a Solana block returned by getBlock. Transactions are raw because
// they may be encoded, parsed, or signature-only depending on configuration.
type Block struct {
	Blockhash         string          `json:"blockhash"`
	PreviousBlockhash string          `json:"previousBlockhash"`
	ParentSlot        Slot            `json:"parentSlot"`
	Transactions      json.RawMessage `json:"transactions,omitempty"`
	Signatures        []string        `json:"signatures,omitempty"`
	Rewards           []Reward        `json:"rewards,omitempty"`
	BlockTime         *UnixTimestamp  `json:"blockTime"`
	BlockHeight       *uint64         `json:"blockHeight"`
}

// Reward describes a reward entry returned by getBlock and inflation endpoints.
type Reward struct {
	Pubkey      string   `json:"pubkey"`
	Lamports    int64    `json:"lamports"`
	PostBalance Lamports `json:"postBalance"`
	RewardType  *string  `json:"rewardType"`
	Commission  *uint8   `json:"commission,omitempty"`
}

// BlockCommitment is the result of getBlockCommitment.
type BlockCommitment struct {
	Commitment []uint64 `json:"commitment"`
	TotalStake Lamports `json:"totalStake"`
}

// BlockProduction is the result of getBlockProduction.
type BlockProduction = ContextResult[BlockProductionValue]

// BlockProductionValue contains per-leader block production counts.
type BlockProductionValue struct {
	ByIdentity map[string][2]uint64 `json:"byIdentity"`
	Range      SlotRange            `json:"range"`
}

// SlotRange is an inclusive slot range.
type SlotRange struct {
	FirstSlot Slot `json:"firstSlot"`
	LastSlot  Slot `json:"lastSlot"`
}

// PerformanceSample is one sample returned by getRecentPerformanceSamples.
type PerformanceSample struct {
	Slot             Slot   `json:"slot"`
	NumTransactions  uint64 `json:"numTransactions"`
	NumSlots         uint64 `json:"numSlots"`
	SamplePeriodSecs uint64 `json:"samplePeriodSecs"`
}

// RecentPerformanceSamples is the result of getRecentPerformanceSamples.
type RecentPerformanceSamples []PerformanceSample

// EpochInfo contains information about the current epoch.
type EpochInfo struct {
	AbsoluteSlot     Slot   `json:"absoluteSlot"`
	BlockHeight      uint64 `json:"blockHeight,omitempty"`
	Epoch            uint64 `json:"epoch"`
	SlotIndex        uint64 `json:"slotIndex"`
	SlotsInEpoch     uint64 `json:"slotsInEpoch"`
	TransactionCount uint64 `json:"transactionCount,omitempty"`
}

func (info *EpochInfo) CalculateUtilization(slot Slot) float64 {
	return float64(slot-info.EpochStartSlot()) / float64(info.SlotsInEpoch)
}

func (info *EpochInfo) EpochStartSlot() Slot {
	return info.AbsoluteSlot - info.SlotIndex
}

// EpochSchedule describes Solana epoch scheduling.
type EpochSchedule struct {
	SlotsPerEpoch            uint64 `json:"slotsPerEpoch"`
	LeaderScheduleSlotOffset uint64 `json:"leaderScheduleSlotOffset"`
	Warmup                   bool   `json:"warmup"`
	FirstNormalEpoch         uint64 `json:"firstNormalEpoch"`
	FirstNormalSlot          Slot   `json:"firstNormalSlot"`
}

// HighestSnapshotSlot is the result of getHighestSnapshotSlot.
type HighestSnapshotSlot struct {
	Full        Slot `json:"full"`
	Incremental Slot `json:"incremental,omitempty"`
}

// Identity is the result of getIdentity.
type Identity struct {
	Identity string `json:"identity"`
}

// LeaderSchedule maps validator identities to their leader slot indexes.
type LeaderSchedule map[string][]uint64

// ClusterNode is a node returned by getClusterNodes.
type ClusterNode struct {
	Pubkey       string  `json:"pubkey"`
	Gossip       *string `json:"gossip"`
	TPU          *string `json:"tpu"`
	TPUQUIC      *string `json:"tpuQuic,omitempty"`
	RPC          *string `json:"rpc"`
	Version      *string `json:"version"`
	FeatureSet   *uint32 `json:"featureSet,omitempty"`
	ShredVersion *uint16 `json:"shredVersion,omitempty"`
}

// ClusterNodes is the result of getClusterNodes.
type ClusterNodes []ClusterNode

// TPUQuick represents a validator's TPU QUIC endpoint information.
type TPUQuick struct {
	Pubkey string  `json:"pubkey"`
	URL    *string `json:"tpuQuic,omitempty"`
}

// ShortTransactionInfo contains basic transaction timing and slot information.
type ShortTransactionInfo struct {
	BlockTime UnixTimestamp `json:"blockTime"`
	Slot      Slot          `json:"slot"`
}

// ShortBlock contains basic block hash and parent slot information.
type ShortBlock struct {
	Hash       string `json:"blockhash"`
	ParentSlot Slot   `json:"parentSlot"`
}

// Version is the result of getVersion.
type Version struct {
	SolanaCore string  `json:"solana-core"`
	FeatureSet *uint32 `json:"feature-set,omitempty"`
}

// VoteAccounts is the result of getVoteAccounts.
type VoteAccounts struct {
	Current    []VoteAccount `json:"current"`
	Delinquent []VoteAccount `json:"delinquent"`
}

// VoteAccount represents a validator vote account.
type VoteAccount struct {
	VotePubkey       string      `json:"votePubkey"`
	NodePubkey       string      `json:"nodePubkey"`
	ActivatedStake   Lamports    `json:"activatedStake"`
	EpochVoteAccount bool        `json:"epochVoteAccount"`
	Commission       uint8       `json:"commission"`
	LastVote         Slot        `json:"lastVote"`
	RootSlot         *Slot       `json:"rootSlot"`
	EpochCredits     [][3]uint64 `json:"epochCredits"`
}

// InflationGovernor is the result of getInflationGovernor.
type InflationGovernor struct {
	Initial        float64 `json:"initial"`
	Terminal       float64 `json:"terminal"`
	Taper          float64 `json:"taper"`
	Foundation     float64 `json:"foundation"`
	FoundationTerm float64 `json:"foundationTerm"`
}

// InflationRate is the result of getInflationRate.
type InflationRate struct {
	Total      float64 `json:"total"`
	Validator  float64 `json:"validator"`
	Foundation float64 `json:"foundation"`
	Epoch      uint64  `json:"epoch"`
}

// InflationReward is one reward returned by getInflationReward.
type InflationReward struct {
	Epoch         uint64   `json:"epoch"`
	EffectiveSlot Slot     `json:"effectiveSlot"`
	Amount        Lamports `json:"amount"`
	PostBalance   Lamports `json:"postBalance"`
	Commission    *uint8   `json:"commission"`
}

// InflationRewards is the result of getInflationReward.
type InflationRewards []*InflationReward

// Supply is the result of getSupply.
type Supply = ContextResult[SupplyValue]

// SupplyValue contains Solana supply data.
type SupplyValue struct {
	Total                  Lamports `json:"total"`
	Circulating            Lamports `json:"circulating"`
	NonCirculating         Lamports `json:"nonCirculating"`
	NonCirculatingAccounts []string `json:"nonCirculatingAccounts"`
}

// SlotUpdate is emitted by slotSubscribe.
type SlotUpdate struct {
	Slot   Slot `json:"slot"`
	Parent Slot `json:"parent"`
	Root   Slot `json:"root"`
}

// RootUpdate is emitted by rootSubscribe.
type RootUpdate = Slot

// LogsUpdate is emitted by logsSubscribe.
type LogsUpdate = ContextResult[LogsValue]

// LogsValue contains a transaction log notification.
type LogsValue struct {
	Signature string          `json:"signature"`
	Err       json.RawMessage `json:"err"`
	Logs      []string        `json:"logs"`
}

// ProgramUpdate is emitted by programSubscribe.
type ProgramUpdate = ContextResult[ProgramAccount]

// SignatureUpdate is emitted by signatureSubscribe.
type SignatureUpdate = ContextResult[SignatureNotification]

// SignatureNotification contains the signature subscription status.
type SignatureNotification struct {
	Err json.RawMessage `json:"err"`
}

// SlotsUpdate is emitted by slotsUpdatesSubscribe.
type SlotsUpdate struct {
	Type      string  `json:"type"`
	Slot      Slot    `json:"slot"`
	Parent    *Slot   `json:"parent,omitempty"`
	Root      *Slot   `json:"root,omitempty"`
	Timestamp *int64  `json:"timestamp,omitempty"`
	Err       *string `json:"err,omitempty"`
}

// VoteUpdate is emitted by voteSubscribe.
type VoteUpdate struct {
	Hash      string `json:"hash"`
	Slots     []Slot `json:"slots"`
	Timestamp *int64 `json:"timestamp,omitempty"`
}
