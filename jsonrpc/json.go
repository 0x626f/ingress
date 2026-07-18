// Package json provides the project's JSON codec.
package jsonrpc

import (
	stdjson "encoding/json"

	"github.com/bytedance/sonic"
)

// RawMessage preserves the public encoding/json.RawMessage type while encoding
// and decoding are handled by Sonic.
type RawMessage = stdjson.RawMessage

// Marshal returns the JSON encoding of value using Sonic's standard-library
// compatible configuration.
func Marshal(value any) ([]byte, error) {
	return sonic.ConfigStd.Marshal(value)
}

// Unmarshal decodes JSON data into value using Sonic's standard-library
// compatible configuration.
func Unmarshal(data []byte, value any) error {
	return sonic.ConfigStd.Unmarshal(data, value)
}
