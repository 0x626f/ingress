package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/types"
	"github.com/0x626f/ingress/transport"
)

type Subscription struct {
	ID     uint64
	Events chan *Event[types.RawResult]

	client            *ThinClient
	manager           *transport.ConnectionManager
	stream            transport.RStream
	unsubscribeMethod string
	closeOnce         sync.Once
	done              chan struct{}
}

func (subscription *Subscription) Unsubscribe() error {
	if subscription == nil {
		return nil
	}

	var err error
	subscription.closeOnce.Do(func() {
		close(subscription.done)
		if subscription.client != nil && subscription.unsubscribeMethod != "" {
			id := uint(1)
			if subscription.client.sequencer != nil {
				id = subscription.client.sequencer.Next()
			}
			var payload []byte
			payload, err = jsonrpc.BuildRequest(id, subscription.unsubscribeMethod, []any{subscription.ID})
			if err != nil {
				return
			}
			_, _, _, err = subscription.manager.Send(subscription.client.ctx, payload)
			return
		}
	})
	return err
}

func (client *ThinClient) RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	return client.rawSubscribeWithManager(ctx, subscribeMethod, unsubscribeMethod, params...)
}

func (client *ThinClient) rawSubscribeWithManager(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	manager := client.manager
	if manager == nil {
		return nil, fmt.Errorf("no %s connection manager configured", client.kind)
	}

	id := uint(1)
	if client.sequencer != nil {
		id = client.sequencer.Next()
	}
	payload, err := jsonrpc.BuildRequest(id, subscribeMethod, params)
	if err != nil {
		return nil, err
	}

	_, stream, timeout, err := manager.Send(ctx, payload)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, fmt.Errorf("no websocket stream available")
	}

	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	var data []byte
	select {
	case message, ok := <-stream:
		if !ok {
			return nil, fmt.Errorf("websocket stream closed")
		}
		data = message
	case <-timer:
		return nil, fmt.Errorf("subscription confirmation timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	subscriptionID, err := parseSubscriptionID(data)
	if err != nil {
		return nil, err
	}

	subscription := &Subscription{
		ID:                subscriptionID,
		Events:            make(chan *Event[types.RawResult], client.subscriptionBufferSize()),
		client:            client,
		manager:           manager,
		stream:            stream,
		unsubscribeMethod: unsubscribeMethod,
		done:              make(chan struct{}),
	}
	go subscription.listen(client)
	return subscription, nil
}

func (subscription *Subscription) listen(client *ThinClient) {
	defer close(subscription.Events)

	for {
		select {
		case <-client.ctx.Done():
			_ = subscription.Unsubscribe()
			return
		case <-subscription.done:
			return
		default:
			var data []byte
			message, ok := <-subscription.stream
			if !ok {
				select {
				case <-subscription.done:
					return
				default:
				}
				subscription.Events <- &Event[types.RawResult]{Error: fmt.Errorf("websocket stream closed")}
				return
			}
			data = message

			result, err := jsonrpc.ParseSubscriptionResult(data)
			if err != nil {
				subscription.Events <- &Event[types.RawResult]{Error: err}
				return
			}

			if result != nil {
				subscription.Events <- &Event[types.RawResult]{Data: types.RawResult(result)}
			}
		}
	}
}

func (client *ThinClient) subscriptionBufferSize() int {
	if client.subscriptionBufSize > 0 {
		return client.subscriptionBufSize
	}
	return 64
}

func parseSubscriptionID(data []byte) (uint64, error) {
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *jsonrpc.Error  `json:"error"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return 0, err
	}
	if response.Error != nil {
		return 0, response.Error
	}

	var number uint64
	if err := json.Unmarshal(response.Result, &number); err == nil {
		return number, nil
	}

	var text string
	if err := json.Unmarshal(response.Result, &text); err == nil {
		return 0, fmt.Errorf("unexpected string subscription id %q", text)
	}

	return 0, fmt.Errorf("missing subscription id")
}

func (client *ThinClient) AccountSubscribe(ctx context.Context, pubkey string, config ...any) (*Subscription, error) {
	return client.RawSubscribe(ctx,
		RPCMethodAccountSubscribe,
		RPCMethodAccountUnsubscribe,
		optionalParams([]any{pubkey}, firstOptional(config))...,
	)
}

func (client *ThinClient) BlockSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error) {
	return client.RawSubscribe(ctx,
		RPCMethodBlockSubscribe,
		RPCMethodBlockUnsubscribe,
		optionalParams([]any{filter}, firstOptional(config))...,
	)
}

func (client *ThinClient) LogsSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error) {
	return client.RawSubscribe(ctx,
		RPCMethodLogsSubscribe,
		RPCMethodLogsUnsubscribe,
		optionalParams([]any{filter}, firstOptional(config))...,
	)
}

func (client *ThinClient) ProgramSubscribe(ctx context.Context, programID string, config ...any) (*Subscription, error) {
	return client.RawSubscribe(ctx,
		RPCMethodProgramSubscribe,
		RPCMethodProgramUnsubscribe,
		optionalParams([]any{programID}, firstOptional(config))...,
	)
}

func (client *ThinClient) RootSubscribe(ctx context.Context) (*Subscription, error) {
	return client.RawSubscribe(ctx, RPCMethodRootSubscribe, RPCMethodRootUnsubscribe)
}

func (client *ThinClient) SignatureSubscribe(ctx context.Context, signature string, config ...any) (*Subscription, error) {
	return client.RawSubscribe(ctx,
		RPCMethodSignatureSubscribe,
		RPCMethodSignatureUnsubscribe,
		optionalParams([]any{signature}, firstOptional(config))...,
	)
}

func (client *ThinClient) SlotSubscribe(ctx context.Context) (*Subscription, error) {
	return client.RawSubscribe(ctx, RPCMethodSlotSubscribe, RPCMethodSlotUnsubscribe)
}

func (client *ThinClient) SlotsUpdatesSubscribe(ctx context.Context) (*Subscription, error) {
	return client.RawSubscribe(ctx, RPCMethodSlotsUpdatesSubscribe, RPCMethodSlotsUpdatesUnsubscribe)
}

func (client *ThinClient) VoteSubscribe(ctx context.Context) (*Subscription, error) {
	return client.RawSubscribe(ctx, RPCMethodVoteSubscribe, RPCMethodVoteUnsubscribe)
}
