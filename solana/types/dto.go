package types

import "time"

// TPUQuick represents a validator's TPU QUIC endpoint information.
// TPU (Transaction Processing Unit) is where validators receive transactions.
type TPUQuick struct {
	Pubkey string  `json:"pubkey"`
	URL    *string `json:"tpuQuic,omitempty"`
}

// EpochInfo contains information about a Solana epoch.
// An epoch is a period of slots during which the leader schedule is valid.
type EpochInfo struct {
	AbsoluteSlot uint64 `json:"absoluteSlot"`
	Epoch        uint64 `json:"epoch"`
	SlotIndex    uint64 `json:"slotIndex"`
	SlotsInEpoch uint64 `json:"slotsInEpoch"`
}

// CalculateUtilization calculates how far through the current epoch a given slot is,
// returning a value between 0.0 (epoch start) and 1.0 (epoch end).
func (info *EpochInfo) CalculateUtilization(slot Slot) float64 {
	return float64(slot-info.EpochStartSlot()) / float64(info.SlotsInEpoch)
}

// EpochStartSlot returns the first slot number of the current epoch.
func (info *EpochInfo) EpochStartSlot() Slot {
	return info.AbsoluteSlot - info.SlotIndex
}

// VoteAccount represents a validator's vote account information including
// its public key and activated stake amount.
type VoteAccount struct {
	Pubkey string `json:"nodePubkey,omitempty"`
	Stake  uint64 `json:"activatedStake,omitempty"`
}

// VoteAccounts contains collections of vote accounts in the cluster,
// separated into current (active) and delinquent validators.
type VoteAccounts struct {
	Current []*VoteAccount `json:"current,omitempty"`
}

// ShortTransactionInfo contains basic information about a transaction
// including its block time and the slot in which it was processed.
type ShortTransactionInfo struct {
	BlockTime time.Duration `json:"blockTime"`
	Slot      Slot          `json:"slot"`
}

// ShortBlock contains basic block information including the block hash
// and the parent slot number from which this block was derived.
type ShortBlock struct {
	Hash       string `json:"blockhash"`
	ParentSlot Slot   `json:"parentSlot"`
}

// ConfirmedSlots represents a collection of confirmed slot numbers.
// This type alias is used for slots that have reached the specified commitment level.
type ConfirmedSlots = []Slot
