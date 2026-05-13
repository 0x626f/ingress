// Package client provides a lightweight Solana RPC client implementation
// for interacting with Solana blockchain nodes via JSON-RPC protocol.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/0x626f/ingress/evm"
	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/types"
)

// ThinClient implements CoreClient for a single transport kind (HTTP or WS).
// Obtain a ThinClient via RawClient.HTTP or RawClient.WS.
type ThinClient struct {
	ctx context.Context

	kind                evm.ConnectionKind
	manager             *evm.ConnectionManager
	sequencer           *evm.SequenceGenerator
	subscriptionBufSize int
}

// Client is kept as a compatibility alias for the previous Solana client API.
type Client = ThinClient

// Event represents a streaming event from a WebSocket subscription.
// It wraps both successful data and errors that occur during the subscription lifecycle.
type Event[T any] struct {
	// Data contains the deserialized subscription payload of type T.
	// This field is only valid when Error is nil.
	Data T
	// Error contains any error that occurred while reading or transforming the WebSocket message.
	// When non-nil, the Data field should not be used.
	Error error
}

// Response represents a JSON-RPC 2.0 response from the Solana node.
type Response struct {
	JsonRpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
}

func buildRequest(method string, params any) ([]byte, error) {
	return buildRequestWithID(1, method, params)
}

func buildRequestWithID(id uint, method string, params any) ([]byte, error) {
	switch value := params.(type) {
	case nil:
		return jsonrpc.BuildRequest(id, method, nil)
	case []byte:
		return jsonrpc.BuildRawRequest(id, method, value), nil
	case []any:
		return jsonrpc.BuildRequest(id, method, value)
	default:
		return jsonrpc.BuildRequest(id, method, []any{value})
	}
}

func (client *ThinClient) call(method string, params any) (*Response, error) {
	manager := client.manager
	return client.callWithManager(manager, method, params)
}

func (client *ThinClient) callWithManager(manager *evm.ConnectionManager, method string, params any) (*Response, error) {
	if manager == nil {
		return nil, fmt.Errorf("no %s connection manager configured", client.kind)
	}

	id := uint(1)
	if client.sequencer != nil {
		id = client.sequencer.Next()
	}
	data, err := buildRequestWithID(id, method, params)
	if err != nil {
		return nil, err
	}

	body, stream, timeout, err := manager.Send(data)
	if err != nil {
		return nil, err
	}
	if body == nil && stream != nil {
		var timer <-chan time.Time
		if timeout > 0 {
			timer = time.After(timeout)
		}
		select {
		case message, ok := <-stream:
			if !ok {
				return nil, fmt.Errorf("websocket stream closed")
			}
			body = message
		case <-timer:
			return nil, fmt.Errorf("rpc response timeout")
		}
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("empty rpc response")
	}

	result, err := jsonrpc.ParseRawResult(body)
	if err != nil {
		return nil, err
	}

	return &Response{Result: json.RawMessage(result)}, nil
}

func (client *ThinClient) SubscribeSlot() (chan *Event[types.Slot], error) {
	subscription, err := client.SlotSubscribe()
	if err != nil {
		return nil, err
	}

	channel := make(chan *Event[types.Slot], client.subscriptionBufferSize())
	go func() {
		defer close(channel)
		for event := range subscription.Events {
			if event.Error != nil {
				channel <- &Event[types.Slot]{Error: event.Error}
				continue
			}

			var update struct {
				Slot types.Slot `json:"slot"`
			}
			if err := json.Unmarshal(event.Data, &update); err != nil {
				channel <- &Event[types.Slot]{Error: err}
				continue
			}
			channel <- &Event[types.Slot]{Data: update.Slot}
		}
	}()
	return channel, nil
}
