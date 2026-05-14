package rpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/0x626f/ingress/solana/types"
)

func TestParseRawValue_Object(t *testing.T) {
	type payload struct {
		Slot types.Slot `json:"slot"`
	}

	result, err := ParseRawValue[payload](types.RawResult(`{"slot":42}`))
	if err != nil {
		t.Fatalf("ParseRawValue: %v", err)
	}
	if result.Slot != 42 {
		t.Fatalf("expected slot 42, got %d", result.Slot)
	}
}

func TestParseRawNumber_Uint64(t *testing.T) {
	result, err := ParseRawNumber[uint64](types.RawResult(`18446744073709551615`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != ^uint64(0) {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_Int64(t *testing.T) {
	result, err := ParseRawNumber[int64](types.RawResult(`-42`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != -42 {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_Float64(t *testing.T) {
	result, err := ParseRawNumber[float64](types.RawResult(`1.25`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != 1.25 {
		t.Fatalf("unexpected result: %f", result)
	}
}

func TestParseRawStringAndBool(t *testing.T) {
	text, err := ParseRawString(types.RawResult(`"ok"`))
	if err != nil {
		t.Fatalf("ParseRawString: %v", err)
	}
	if text != "ok" {
		t.Fatalf("unexpected string: %s", text)
	}

	value, err := ParseRawBool(types.RawResult(`true`))
	if err != nil {
		t.Fatalf("ParseRawBool: %v", err)
	}
	if !value {
		t.Fatal("expected true")
	}
}

func TestBase58Helpers(t *testing.T) {
	raw := []byte("solana transaction bytes")
	encoded := EncodeBase58(raw)

	decoded, err := DecodeBase58(encoded)
	if err != nil {
		t.Fatalf("DecodeBase58: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded bytes mismatch")
	}
}

func TestBase58Helpers_LeadingZeroes(t *testing.T) {
	raw := []byte{0, 0, 1, 2, 3, 4}
	encoded := EncodeBase58(raw)
	if encoded[:2] != "11" {
		t.Fatalf("expected leading zeroes encoded as 1s, got %q", encoded)
	}

	decoded, err := DecodeBase58(encoded)
	if err != nil {
		t.Fatalf("DecodeBase58: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded bytes mismatch")
	}
}

func TestBase58EncodedValue(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	value := NewBase58EncodedValue(raw)
	if value[1] != "base58" {
		t.Fatalf("expected base58 encoding, got %q", value[1])
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(encoded) != `["2VfUX","base58"]` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}

	decoded, err := value.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded bytes mismatch")
	}
}

func TestBase58EncodedValue_UnsupportedEncoding(t *testing.T) {
	value := Base58EncodedValue{"2VfUX", "base64"}
	if _, err := value.Decode(); err == nil {
		t.Fatal("expected unsupported encoding error")
	}
}

func TestDecodeBase58_InvalidCharacter(t *testing.T) {
	if _, err := DecodeBase58("0OIl"); err == nil {
		t.Fatal("expected invalid character error")
	}
}
