// Package rpc provides a lightweight Solana RPC rpc implementation
// for interacting with Solana blockchain nodes via JSON-RPC protocol.
package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/model"
	"github.com/0x626f/ingress/transport"
)

// ThinClient implements CoreClient for a single transport kind (HTTP or WS).
// Obtain a ThinClient via RawClient.HTTP or RawClient.WS.
type ThinClient struct {
	ctx context.Context

	kind                transport.Protocol
	manager             *transport.ConnectionManager
	sequencer           *transport.SequenceGenerator
	subscriptionBufSize int

	mu            sync.Mutex
	streams       map[transport.RStream]map[uint]struct{}
	pending       map[uint]transport.RWStream
	subscriptions map[transport.RStream]map[string]struct{}
	listeners     map[string]transport.RWStream
}

// Client is kept as a compatibility alias for the previous Solana rpc API.
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
	JsonRpc string             `json:"jsonrpc"`
	Id      int                `json:"id"`
	Result  jsonrpc.RawMessage `json:"result"`
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

func (client *ThinClient) call(ctx context.Context, method string, params any) (*Response, error) {
	query := DefaultQueryParams()
	switch value := params.(type) {
	case nil:
	case []any:
		query.Params = value
	default:
		query.Params = []any{value}
	}
	result, _, err := client.handle(ctx, func(params *QueryParams) ([]byte, error) {
		return APISpec{}.BuildMethodCall(method, params)
	}, query)
	if err != nil {
		return nil, err
	}
	return &Response{Result: jsonrpc.RawMessage(result)}, nil
}

func (client *ThinClient) callWithManager(ctx context.Context, manager *transport.ConnectionManager, method string, params any) (*Response, error) {
	previous := client.manager
	client.manager = manager
	response, err := client.call(ctx, method, params)
	client.manager = previous
	return response, err
}

func (client *ThinClient) preProcess(params *QueryParams) {
	if client.sequencer != nil {
		params.Id = client.sequencer.Next()
	}
	if client.kind == transport.HTTP {
		return
	}
	client.mu.Lock()
	client.pending[params.Id] = make(chan transport.Message, 2)
	client.mu.Unlock()
}

func (client *ThinClient) postProcess(ctx context.Context, stream transport.RStream, timeout time.Duration, params *QueryParams, result []byte, failed bool) []byte {
	if ctx == nil {
		ctx = context.Background()
	}
	if client.kind == transport.HTTP {
		return result
	}

	client.mu.Lock()
	if failed {
		delete(client.pending, params.Id)
		client.mu.Unlock()
		return nil
	}

	if _, ok := client.streams[stream]; !ok {
		client.streams[stream] = make(map[uint]struct{})
		go client.listen(stream)
	}
	client.streams[stream][params.Id] = struct{}{}
	channel := client.pending[params.Id]
	client.mu.Unlock()

	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	select {
	case msg, ok := <-channel:
		if !ok {
			return nil
		}
		return msg
	case <-timer:
		client.rejectListener(stream, params.Id)
		return nil
	case <-ctx.Done():
		client.rejectListener(stream, params.Id)
		return nil
	}
}

func (client *ThinClient) listen(stream transport.RStream) {
	for {
		message, ok := <-stream
		if !ok {
			client.clearStream(stream)
			return
		}
		client.respond(stream, message)
	}
}

func (client *ThinClient) clearStream(stream transport.RStream) {
	client.mu.Lock()
	defer client.mu.Unlock()

	for id := range client.streams[stream] {
		close(client.pending[id])
		delete(client.pending, id)
	}
	delete(client.streams, stream)

	for subscription := range client.subscriptions[stream] {
		close(client.listeners[subscription])
		delete(client.listeners, subscription)
	}
	delete(client.subscriptions, stream)
}

func (client *ThinClient) rejectListener(stream transport.RStream, id uint) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if pending, ok := client.streams[stream]; ok {
		delete(pending, id)
	}
	if active, ok := client.pending[id]; ok {
		close(active)
		delete(client.pending, id)
	}
}

func (client *ThinClient) rejectSubscription(stream transport.RStream, id string) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if subscriptions, ok := client.subscriptions[stream]; ok {
		delete(subscriptions, id)
	}
	if active, ok := client.listeners[id]; ok {
		close(active)
		delete(client.listeners, id)
	}
}

func (client *ThinClient) removeSubscription(id string) {
	client.mu.Lock()
	defer client.mu.Unlock()

	for stream, subscriptions := range client.subscriptions {
		if _, ok := subscriptions[id]; ok {
			delete(subscriptions, id)
			if len(subscriptions) == 0 {
				delete(client.subscriptions, stream)
			}
			break
		}
	}
	if active, ok := client.listeners[id]; ok {
		close(active)
		delete(client.listeners, id)
	}
}

func sendSubscriptionMessage(stream transport.RWStream, data transport.Message) {
	defer func() {
		_ = recover()
	}()
	stream <- data
}

func (client *ThinClient) respond(source transport.RStream, message transport.Message) {
	messageID, err := APISpec{}.ParseMessageId(message)
	if err != nil {
		return
	}

	client.mu.Lock()
	if messageID.ID != 0 {
		if pending, ok := client.streams[source]; ok {
			delete(pending, messageID.ID)
		}
		if stream, ok := client.pending[messageID.ID]; ok {
			stream <- message
			delete(client.pending, messageID.ID)
		}
		client.mu.Unlock()
		return
	}

	if messageID.Subscription != "" {
		stream, ok := client.listeners[messageID.Subscription]
		client.mu.Unlock()
		if !ok {
			return
		}
		data, err := APISpec{}.ParseSubscriptionResponse(message)
		if err != nil {
			client.rejectSubscription(source, messageID.Subscription)
			return
		}
		sendSubscriptionMessage(stream, data)
		return
	}
	client.mu.Unlock()
}

func (client *ThinClient) handle(ctx context.Context, call func(*QueryParams) ([]byte, error), params *QueryParams) (result []byte, stream transport.RStream, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client.manager == nil {
		return nil, nil, fmt.Errorf("no %s connection manager configured", client.kind)
	}
	if params == nil {
		params = DefaultQueryParams()
	}
	client.preProcess(params)

	payload, err := call(params)
	if err != nil {
		return nil, nil, err
	}

	var timeout time.Duration
	if result, stream, timeout, err = client.manager.Send(ctx, payload); err != nil {
		_ = client.postProcess(ctx, stream, timeout, params, nil, true)
		return nil, nil, err
	}

	result = client.postProcess(ctx, stream, timeout, params, result, false)
	if err := ctx.Err(); err != nil {
		return nil, stream, err
	}
	if len(result) == 0 {
		return nil, stream, fmt.Errorf("empty rpc response")
	}

	result, err = APISpec{}.ParseResponse(result)
	if err != nil {
		return nil, nil, err
	}
	return result, stream, nil
}

func omitStream(result []byte, stream transport.RStream, err error) (model.RawResult, error) {
	_ = stream
	return model.RawResult(result), err
}
