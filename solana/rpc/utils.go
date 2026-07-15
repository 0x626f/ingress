package rpc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/0x626f/ingress/solana/model"
	"github.com/mr-tron/base58"
)

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// RawNumber is any Go numeric type that can be decoded from a raw JSON number.
type RawNumber interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// ParseRawResultToArray is a generic utility function that unmarshals a RawResult
// into a slice of pointers to type T. It is useful for parsing array responses
// from RPC methods like GetClusterNodes or GetVoteAccounts.
//
// Example:
//
//	nodes, err := ParseRawResultToArray[model.TPUQuick](rawResult)
func ParseRawResultToArray[T any](data model.RawResult) ([]*T, error) {
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
//	epochInfo, err := ParseRawResult[model.EpochInfo](rawResult)
func ParseRawResult[T any](data model.RawResult) (*T, error) {
	var result *T

	err := json.Unmarshal(data, &result)

	return result, err
}

// ParseRawValue unmarshals a RawResult into a concrete value. It works for
// objects, arrays, strings, booleans, and JSON numbers.
func ParseRawValue[T any](data model.RawResult) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}

// ParseRawNumber parses a RawResult containing a JSON number into the requested
// numeric type. It avoids float64 conversion for integer targets.
func ParseRawNumber[T RawNumber](data model.RawResult) (T, error) {
	var zero T
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return zero, fmt.Errorf("empty raw number")
	}

	switch any(zero).(type) {
	case float32:
		value, err := strconv.ParseFloat(raw, 32)
		return T(value), err
	case float64:
		value, err := strconv.ParseFloat(raw, 64)
		return T(value), err
	case int, int8, int16, int32, int64:
		value, err := strconv.ParseInt(raw, 10, bitSizeOf[T]())
		return T(value), err
	default:
		value, err := strconv.ParseUint(raw, 10, bitSizeOf[T]())
		return T(value), err
	}
}

// ParseRawString parses a RawResult containing a JSON string.
func ParseRawString(data model.RawResult) (string, error) {
	return ParseRawValue[string](data)
}

// ParseRawBool parses a RawResult containing a JSON boolean.
func ParseRawBool(data model.RawResult) (bool, error) {
	return ParseRawValue[bool](data)
}

// EncodeBase58 returns the Bitcoin/Solana base58 encoding for binary values
// such as public keys, signatures, and transaction identifiers.
func EncodeBase58(data []byte) string {
	return base58.Encode(data)
}

// DecodeBase58 decodes a Bitcoin/Solana base58 value.
func DecodeBase58(encoded string) ([]byte, error) {
	return base58.Decode(encoded)
}

// Base58EncodedValue is the common tuple shape for explicitly encoded Solana
// binary data: ["<base58 data>", "base58"].
type Base58EncodedValue [2]string

// NewBase58EncodedValue encodes data into a base58 encoded-data tuple.
func NewBase58EncodedValue(data []byte) Base58EncodedValue {
	return Base58EncodedValue{EncodeBase58(data), "base58"}
}

// Decode decodes the first tuple element as base58.
func (value Base58EncodedValue) Decode() ([]byte, error) {
	if value[1] != "" && value[1] != "base58" {
		return nil, fmt.Errorf("unsupported encoding %q", value[1])
	}
	return DecodeBase58(value[0])
}

func bitSizeOf[T RawNumber]() int {
	var zero T
	switch any(zero).(type) {
	case int, uint:
		return strconv.IntSize
	case int8, uint8:
		return 8
	case int16, uint16:
		return 16
	case int32, uint32, float32:
		return 32
	default:
		return 64
	}
}
