package rpc

import "testing"

func TestParseRawValue_Object(t *testing.T) {
	type payload struct {
		Hash string `json:"hash"`
	}

	result, err := ParseRawValue[payload]([]byte(`{"hash":"0xabc"}`))
	if err != nil {
		t.Fatalf("ParseRawValue: %v", err)
	}
	if result.Hash != "0xabc" {
		t.Fatalf("unexpected hash: %s", result.Hash)
	}
}

func TestParseRawNumber_DecimalUint64(t *testing.T) {
	result, err := ParseRawNumber[uint64]([]byte(`18446744073709551615`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != ^uint64(0) {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_HexUint64(t *testing.T) {
	result, err := ParseRawNumber[uint64]([]byte(`0xff`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != 255 {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_QuotedHexUint64(t *testing.T) {
	result, err := ParseRawNumber[uint64]([]byte(`"0x2a"`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != 42 {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_Int64(t *testing.T) {
	result, err := ParseRawNumber[int64]([]byte(`-42`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != -42 {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawNumber_Float64(t *testing.T) {
	result, err := ParseRawNumber[float64]([]byte(`1.25`))
	if err != nil {
		t.Fatalf("ParseRawNumber: %v", err)
	}
	if result != 1.25 {
		t.Fatalf("unexpected result: %f", result)
	}
}

func TestParseHexNumber(t *testing.T) {
	result, err := ParseHexNumber[uint16]([]byte(`"0x100"`))
	if err != nil {
		t.Fatalf("ParseHexNumber: %v", err)
	}
	if result != 256 {
		t.Fatalf("unexpected result: %d", result)
	}
}

func TestParseRawString_BareAndQuoted(t *testing.T) {
	bare, err := ParseRawString([]byte(`0xabc`))
	if err != nil {
		t.Fatalf("ParseRawString bare: %v", err)
	}
	if bare != "0xabc" {
		t.Fatalf("unexpected bare string: %s", bare)
	}

	quoted, err := ParseRawString([]byte(`"0xdef"`))
	if err != nil {
		t.Fatalf("ParseRawString quoted: %v", err)
	}
	if quoted != "0xdef" {
		t.Fatalf("unexpected quoted string: %s", quoted)
	}
}

func TestParseRawBool(t *testing.T) {
	result, err := ParseRawBool([]byte(`true`))
	if err != nil {
		t.Fatalf("ParseRawBool: %v", err)
	}
	if !result {
		t.Fatal("expected true")
	}
}

func TestHexQuantityHelpers(t *testing.T) {
	if got := ToHexQuantity(255); got != "0xff" {
		t.Fatalf("ToHexQuantity: %s", got)
	}
	if got := DecimalStringToHexQuantity("255"); got != "0xff" {
		t.Fatalf("DecimalStringToHexQuantity: %s", got)
	}
	if got := DecimalStringToHexQuantityOrDefault("0x10"); got != "0x10" {
		t.Fatalf("DecimalStringToHexQuantityOrDefault existing hex: %s", got)
	}
	if got := DecimalStringToHexQuantityOrDefault("16"); got != "0x10" {
		t.Fatalf("DecimalStringToHexQuantityOrDefault decimal: %s", got)
	}
}
