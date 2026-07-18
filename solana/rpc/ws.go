package rpc

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/model"
	"github.com/0x626f/ingress/transport"
)

type Subscription struct {
	ID     uint64
	Events chan *Event[model.RawResult]

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
			subscription.client.removeSubscription(strconv.FormatUint(subscription.ID, 10))
			return
		}
	})
	return err
}

func (client *ThinClient) RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	return client.rawSubscribeWithManager(ctx, subscribeMethod, unsubscribeMethod, params...)
}

func (client *ThinClient) rawSubscribeWithManager(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	if client.kind != transport.WS {
		return nil, fmt.Errorf("%s rpc doesn't support subscribe method", client.kind)
	}

	result, stream, err := client.handle(ctx, func(query *QueryParams) ([]byte, error) {
		return APISpec{}.BuildMethodCall(subscribeMethod, query)
	}, Query(params...))
	if err != nil {
		return nil, err
	}

	subscriptionID, err := parseSubscriptionResultID(result)
	if err != nil {
		return nil, err
	}
	listener := make(transport.RWStream, client.subscriptionBufferSize())
	subscriptionKey := strconv.FormatUint(subscriptionID, 10)

	subscription := &Subscription{
		ID:                subscriptionID,
		Events:            make(chan *Event[model.RawResult], client.subscriptionBufferSize()),
		client:            client,
		manager:           client.manager,
		stream:            listener,
		unsubscribeMethod: unsubscribeMethod,
		done:              make(chan struct{}),
	}
	client.mu.Lock()
	if _, ok := client.subscriptions[stream]; !ok {
		client.subscriptions[stream] = make(map[string]struct{})
	}
	client.subscriptions[stream][subscriptionKey] = struct{}{}
	client.listeners[subscriptionKey] = listener
	client.mu.Unlock()

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
				subscription.Events <- &Event[model.RawResult]{Error: fmt.Errorf("websocket stream closed")}
				return
			}
			data = message
			if data != nil {
				subscription.Events <- &Event[model.RawResult]{Data: model.RawResult(data)}
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
		Result jsonrpc.RawMessage `json:"result"`
		Error  *jsonrpc.Error     `json:"error"`
	}
	if err := jsonrpc.Unmarshal(data, &response); err != nil {
		return 0, err
	}
	if response.Error != nil {
		return 0, response.Error
	}

	var number uint64
	if err := jsonrpc.Unmarshal(response.Result, &number); err == nil {
		return number, nil
	}

	var text string
	if err := jsonrpc.Unmarshal(response.Result, &text); err == nil {
		return 0, fmt.Errorf("unexpected string subscription id %q", text)
	}

	return 0, fmt.Errorf("missing subscription id")
}

func parseSubscriptionResultID(data []byte) (uint64, error) {
	var number uint64
	if err := jsonrpc.Unmarshal(data, &number); err == nil {
		return number, nil
	}

	var text string
	if err := jsonrpc.Unmarshal(data, &text); err == nil {
		return 0, fmt.Errorf("unexpected string subscription id %q", text)
	}

	return 0, fmt.Errorf("missing subscription id")
}

func (client *ThinClient) AccountSubscribe(ctx context.Context, query AccountSubscribeQuery) (*Subscription, error) {
	if err := requireString("account pubkey", query.Pubkey); err != nil {
		return nil, err
	}
	query = normalizeAccountSubscribeQuery(query)
	return client.RawSubscribe(ctx,
		RPCMethodAccountSubscribe,
		RPCMethodAccountUnsubscribe,
		optionalParams([]any{query.Pubkey}, optionalQueryConfig(query))...,
	)
}

func (client *ThinClient) BlockSubscribe(ctx context.Context, query BlockSubscribeQuery) (*Subscription, error) {
	if err := validateBlockSubscribeFilter(query.Filter); err != nil {
		return nil, err
	}
	return client.RawSubscribe(ctx,
		RPCMethodBlockSubscribe,
		RPCMethodBlockUnsubscribe,
		optionalParams([]any{query.Filter}, optionalQueryConfig(query))...,
	)
}

func (client *ThinClient) LogsSubscribe(ctx context.Context, query LogsSubscribeQuery) (*Subscription, error) {
	if err := validateLogsSubscribeFilter(query.Filter); err != nil {
		return nil, err
	}
	return client.RawSubscribe(ctx,
		RPCMethodLogsSubscribe,
		RPCMethodLogsUnsubscribe,
		optionalParams([]any{query.Filter}, optionalQueryConfig(query))...,
	)
}

func (client *ThinClient) ProgramSubscribe(ctx context.Context, query ProgramSubscribeQuery) (*Subscription, error) {
	if err := requireString("program id", query.ProgramID); err != nil {
		return nil, err
	}
	query = normalizeProgramSubscribeQuery(query)
	if err := validateProgramAccountFilters(query.Filters); err != nil {
		return nil, err
	}
	return client.RawSubscribe(ctx,
		RPCMethodProgramSubscribe,
		RPCMethodProgramUnsubscribe,
		optionalParams([]any{query.ProgramID}, optionalQueryConfig(query))...,
	)
}

func (client *ThinClient) RootSubscribe(ctx context.Context) (*Subscription, error) {
	return client.RawSubscribe(ctx, RPCMethodRootSubscribe, RPCMethodRootUnsubscribe)
}

func (client *ThinClient) SignatureSubscribe(ctx context.Context, query SignatureSubscribeQuery) (*Subscription, error) {
	if err := requireString("transaction signature", query.Signature); err != nil {
		return nil, err
	}
	return client.RawSubscribe(ctx,
		RPCMethodSignatureSubscribe,
		RPCMethodSignatureUnsubscribe,
		optionalParams([]any{query.Signature}, optionalQueryConfig(query))...,
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

func (client *ThinClient) SubscribeSlot(ctx context.Context) (chan *Event[model.Slot], error) {
	subscription, err := client.SlotSubscribe(ctx)
	if err != nil {
		return nil, err
	}

	channel := make(chan *Event[model.Slot], client.subscriptionBufferSize())
	go func() {
		defer close(channel)
		for event := range subscription.Events {
			if event.Error != nil {
				channel <- &Event[model.Slot]{Error: event.Error}
				continue
			}

			var update struct {
				Slot model.Slot `json:"slot"`
			}
			if err := jsonrpc.Unmarshal(event.Data, &update); err != nil {
				channel <- &Event[model.Slot]{Error: err}
				continue
			}
			channel <- &Event[model.Slot]{Data: update.Slot}
		}
	}()
	return channel, nil
}
