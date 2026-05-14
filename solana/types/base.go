// Package types provides common type definitions and data structures
// used throughout the Solana RPC rpc library.
package types

// Commitment represents the commitment level for Solana RPC requests.
// Valid values are "processed", "confirmed", and "finalized".
type Commitment = string

// RawResult represents raw JSON response data from RPC calls.
// It is used to defer parsing until the caller needs to unmarshal
// the data into a specific type.
type RawResult []byte

// Slot represents a Solana slot number, which is a unit of time
// on the Solana blockchain (approximately 400ms per slot).
type Slot = uint64

// SlotLeaders represents a list of validator public keys (as base58 strings)
// that are scheduled to produce blocks for consecutive slots.
type SlotLeaders = []string

// SlotLeaderTPUAddress represents the TPU (Transaction Processing Unit)
// network address of a slot leader validator.
type SlotLeaderTPUAddress = string
