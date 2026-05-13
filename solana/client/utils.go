package client

import (
	"encoding/json"

	"github.com/0x626f/ingress/solana/types"
)

// ParseRawResultToArray is a generic utility function that unmarshals a RawResult
// into a slice of pointers to type T. It is useful for parsing array responses
// from RPC methods like GetClusterNodes or GetVoteAccounts.
//
// Example:
//
//	nodes, err := ParseRawResultToArray[types.TPUQuick](rawResult)
func ParseRawResultToArray[T any](data types.RawResult) ([]*T, error) {
	var result []*T

	err := json.Unmarshal(data, &result)

	return result, err
}

// ParseRawResult is a generic utility function that unmarshals a RawResult
// into a pointer to type T. It is useful for parsing single object responses
// from RPC methods like GetEpochInfo.
//
// Example:
//
//	epochInfo, err := ParseRawResult[types.EpochInfo](rawResult)
func ParseRawResult[T any](data types.RawResult) (*T, error) {
	var result *T

	err := json.Unmarshal(data, &result)

	return result, err
}
