package rpc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/holiman/uint256"
)

// RawNumber is any Go numeric type that can be decoded from a raw JSON number
// or an EVM hex quantity.
type RawNumber interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

func getOrDefault(def, value string) string {
	if value != "" {
		return value
	}
	return def
}

func getFirstOrDefault(def string, values ...string) string {
	if len(values) > 0 {
		return values[0]
	}
	return def
}

func toHex(n uint64) string {
	return "0x" + strconv.FormatUint(n, 16)
}

func fromHex(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	return strconv.ParseUint(s, 16, 64)
}

func stringToHex(s string) string {
	var n uint256.Int
	if err := n.SetFromDecimal(s); err != nil {
		return ""
	}
	return n.Hex()
}

func stringToHexOrDefault(s string) string {
	if strings.HasPrefix(s, "0x") {
		return s
	}
	return stringToHex(s)
}

// ParseRawValue unmarshals raw JSON bytes into a concrete value. It is useful
// for structured EVM responses returned by methods such as GetLogs,
// GetTransactionByHash, GetTransactionReceipt, and GetBlockByNumber.
func ParseRawValue[T any](data []byte) (T, error) {
	var result T
	err := jsonrpc.Unmarshal(data, &result)
	return result, err
}

// ParseRawNumber parses raw bytes containing either a JSON number or an EVM
// hex quantity into the requested numeric type.
func ParseRawNumber[T RawNumber](data []byte) (T, error) {
	var zero T
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return zero, fmt.Errorf("empty raw number")
	}
	raw = strings.Trim(raw, `"`)

	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		return parseHexNumber[T](raw)
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

// ParseHexNumber parses an EVM hex quantity, with or without JSON quotes, into
// the requested numeric type.
func ParseHexNumber[T RawNumber](data []byte) (T, error) {
	raw := strings.Trim(strings.TrimSpace(string(data)), `"`)
	return parseHexNumber[T](raw)
}

// ParseRawString parses a raw JSON string or a bare scalar result. EVM scalar
// string results are often already unquoted by the RPC client.
func ParseRawString(data []byte) (string, error) {
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return "", nil
	}
	if raw[0] == '"' {
		return ParseRawValue[string](data)
	}
	return raw, nil
}

// ParseRawBool parses raw JSON boolean bytes.
func ParseRawBool(data []byte) (bool, error) {
	return ParseRawValue[bool](data)
}

// ToHexQuantity converts a uint64 to an EVM 0x-prefixed hex quantity.
func ToHexQuantity(value uint64) string {
	return toHex(value)
}

// DecimalStringToHexQuantity converts a decimal string to an EVM 0x-prefixed
// hex quantity. It returns an empty string when the decimal input is invalid
// or exceeds the EVM uint256 range.
func DecimalStringToHexQuantity(value string) string {
	return stringToHex(value)
}

// DecimalStringToHexQuantityOrDefault returns value unchanged when it already
// has a 0x prefix, otherwise it converts the decimal string to a hex quantity.
func DecimalStringToHexQuantityOrDefault(value string) string {
	return stringToHexOrDefault(value)
}

func parseHexNumber[T RawNumber](raw string) (T, error) {
	var zero T
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "0x"), "0X")
	if raw == "" {
		return zero, fmt.Errorf("empty hex number")
	}

	switch any(zero).(type) {
	case float32:
		value, err := strconv.ParseUint(raw, 16, 64)
		return T(float32(value)), err
	case float64:
		value, err := strconv.ParseUint(raw, 16, 64)
		return T(float64(value)), err
	case int, int8, int16, int32, int64:
		value, err := strconv.ParseInt(raw, 16, bitSizeOf[T]())
		return T(value), err
	default:
		value, err := strconv.ParseUint(raw, 16, bitSizeOf[T]())
		return T(value), err
	}
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
