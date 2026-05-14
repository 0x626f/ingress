package rpc

import "testing"

func TestAPISpec_ParseResponse_StringResult_Unquotes(t *testing.T) {
	result, err := APISpec{}.ParseResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "0x1" {
		t.Fatalf("expected 0x1, got %q", result)
	}
}

func TestAPISpec_ParseResponse_ArrayResult_PreservesJSON(t *testing.T) {
	result, err := APISpec{}.ParseResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "[]" {
		t.Fatalf("expected [], got %q", result)
	}
}

func TestAPISpec_ParseResponse_BoolResult_PreservesJSON(t *testing.T) {
	result, err := APISpec{}.ParseResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "true" {
		t.Fatalf("expected true, got %q", result)
	}
}

func TestAPISpec_ParseResponse_NullResult_ReturnsNil(t *testing.T) {
	result, err := APISpec{}.ParseResponse([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %q", result)
	}
}
