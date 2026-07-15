package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/0x626f/ingress/transport"
)

// ThinClient implements CoreClient for a single transport kind (HTTP or WS).
// For WebSocket it maintains per-request pending channels and routes incoming
// messages to the correct caller. Obtain a ThinClient via RawClient.HTTP or Client.WS.
type ThinClient struct {
	kind      transport.Protocol
	manager   *transport.ConnectionManager
	sequencer *transport.SequenceGenerator

	mu      sync.Mutex
	streams map[transport.RStream]map[uint]struct{} // manager stream to request id map
	pending map[uint]transport.RWStream             // request id to response channel

	subscriptionStreamSize int
	subscriptions          map[transport.RStream]map[string]struct{} // stream to subscriptions
	listeners              map[string]transport.RWStream             // subscription to listener
}

func newThinClient(kind transport.Protocol, manager *transport.ConnectionManager, sequencer *transport.SequenceGenerator, subStreamSize int) *ThinClient {
	if subStreamSize == 0 {
		subStreamSize = 64
	}

	client := &ThinClient{
		kind:      kind,
		manager:   manager,
		sequencer: sequencer,
	}

	if kind == transport.WS {
		client.pending = make(map[uint]transport.RWStream)
		client.streams = make(map[transport.RStream]map[uint]struct{})
		client.subscriptionStreamSize = subStreamSize
		client.subscriptions = make(map[transport.RStream]map[string]struct{})
		client.listeners = make(map[string]transport.RWStream)
	}

	return client
}

func (client *ThinClient) preProcess(params *QueryParams) {
	params.Id = client.sequencer.Next()

	if client.kind == transport.HTTP {
		return
	}

	client.mu.Lock()
	client.pending[params.Id] = make(chan transport.Message, 2) // set size 2 to avoid sync on proxy side
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

	for {
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
}

func (client *ThinClient) listen(stream transport.RStream) {
	for {
		select {
		case message, ok := <-stream:
			if !ok {
				client.clearStream(stream)
				return
			}
			client.respond(stream, message)
		}
	}
}

func (client *ThinClient) clearStream(stream transport.RStream) {
	client.mu.Lock()
	defer client.mu.Unlock()

	pending := client.streams[stream]
	for id := range pending {
		close(client.pending[id])
	}
	clear(pending)

	subscriptions := client.subscriptions[stream]
	for subscription, _ := range subscriptions {
		close(client.listeners[subscription])
	}
	clear(subscriptions)
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
	messageId, err := APISpec{}.ParseMessageId(message)
	if err != nil {
		return
	}

	client.mu.Lock()

	if messageId.ID != 0 {
		delete(client.streams[source], messageId.ID)
		if stream, ok := client.pending[messageId.ID]; ok {
			stream <- message
			delete(client.pending, messageId.ID)
		}
		client.mu.Unlock()
	} else if messageId.Subscription != "" {
		stream, ok := client.listeners[messageId.Subscription]
		client.mu.Unlock()
		if !ok {
			return
		}

		data, err := APISpec{}.ParseSubscriptionResponse(message)
		if err != nil {
			client.rejectSubscription(source, messageId.Subscription)
			return
		}

		sendSubscriptionMessage(stream, data)
		return
	} else {
		client.mu.Unlock()
	}
}

func (client *ThinClient) handle(ctx context.Context, call func(*QueryParams) ([]byte, error), params *QueryParams) (result []byte, stream transport.RStream, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if client.manager == nil {
		return nil, nil, fmt.Errorf("no %s connection manager configured", client.kind)
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

	result, err = APISpec{}.ParseResponse(result)
	if err != nil {
		return nil, nil, err
	}
	return result, stream, err
}

func omitStream(result []byte, stream transport.RStream, err error) ([]byte, error) {
	_ = stream
	return result, err
}

// ChainId calls eth_chainId.
func (client *ThinClient) ChainId(ctx context.Context) ([]byte, error) {
	return omitStream(client.handle(ctx, APISpec{}.ChainId, DefaultQueryParams()))
}

// BlockNumber calls eth_blockNumber.
func (client *ThinClient) BlockNumber(ctx context.Context) ([]byte, error) {
	return omitStream(client.handle(ctx, APISpec{}.BlockNumber, DefaultQueryParams()))
}

// GetBalance calls eth_getBalance.
func (client *ThinClient) GetBalance(ctx context.Context, query BalanceQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetBalance,
		QueryWithId(query.Id, query.Address, getOrDefault(BlockTagLatest, query.BlockTag)),
	))
}

// GetCode calls eth_getCode.
func (client *ThinClient) GetCode(ctx context.Context, query CodeQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetCode,
		QueryWithId(query.Id, query.Address, getOrDefault(BlockTagLatest, query.BlockTag)),
	))
}

// GetStorageAt calls eth_getStorageAt.
func (client *ThinClient) GetStorageAt(ctx context.Context, query GetStorageQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetStorageAt,
		QueryWithId(query.Id, query.Address, stringToHex(query.Slot), getOrDefault(BlockTagLatest, query.BlockTag)),
	))
}

// Call calls eth_call.
func (client *ThinClient) Call(ctx context.Context, query CallQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.Call,
		QueryWithId(
			query.Id,
			map[string]string{"to": query.To, "data": query.Data},
			getOrDefault(BlockTagLatest, query.BlockTag),
		),
	))
}

// EstimateGas calls eth_estimateGas.
func (client *ThinClient) EstimateGas(ctx context.Context, query EstimateGasQuery) ([]byte, error) {
	rpcCallData := make(map[string]string)

	rpcCallData["to"] = query.To

	if query.From != "" {
		rpcCallData["from"] = query.From
	}

	if query.Data != "" {
		rpcCallData["data"] = query.Data
	}

	if query.Gas != "" {
		rpcCallData["gas"] = stringToHexOrDefault(query.Gas)
	}

	if query.GasPrice != "" {
		rpcCallData["gasPrice"] = stringToHexOrDefault(query.GasPrice)
	}

	if query.MaxFeePerGas != "" {
		rpcCallData["maxFeePerGas"] = stringToHexOrDefault(query.MaxFeePerGas)
	}

	if query.MaxPriorityFeePerGas != "" {
		rpcCallData["maxPriorityFeePerGas"] = stringToHexOrDefault(query.MaxPriorityFeePerGas)
	}

	if query.Value != "" {
		rpcCallData["value"] = stringToHexOrDefault(query.Value)
	}

	if query.Nonce != "" {
		rpcCallData["nonce"] = stringToHexOrDefault(query.Nonce)
	}

	return omitStream(client.handle(ctx,
		APISpec{}.EstimateGas,
		QueryWithId(
			query.Id,
			rpcCallData,
			getOrDefault(BlockTagLatest, query.BlockTag),
		),
	))
}

// SendRawTransaction calls eth_sendRawTransaction.
func (client *ThinClient) SendRawTransaction(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.SendRawTransaction,
		QueryWithId(
			query.Id,
			query.Signed,
		),
	))
}

// GetTransactionByHash calls eth_getTransactionByHash.
func (client *ThinClient) GetTransactionByHash(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetTransactionByHash,
		QueryWithId(
			query.Id,
			query.Hash,
		),
	))
}

// GetTransactionReceipt calls eth_getTransactionReceipt.
func (client *ThinClient) GetTransactionReceipt(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetTransactionReceipt,
		QueryWithId(
			query.Id,
			query.Hash,
		),
	))
}

// GetTransactionCount calls eth_getTransactionCount (nonce).
func (client *ThinClient) GetTransactionCount(ctx context.Context, query AddressedQuery) ([]byte, error) {
	return omitStream(client.handle(ctx,
		APISpec{}.GetTransactionCount,
		QueryWithId(
			query.Id,
			query.Address,
			getOrDefault(BlockTagLatest, query.BlockTag),
		),
	))
}

// GetBlockByNumber calls eth_getBlockByNumber.
func (client *ThinClient) GetBlockByNumber(ctx context.Context, query BlockQuery) ([]byte, error) {
	tag := getOrDefault(BlockTagLatest, query.BlockTag)

	if query.Number != "" {
		tag = stringToHex(query.Number)
	}

	return omitStream(client.handle(ctx,
		APISpec{}.GetBlockByNumber,
		QueryWithId(query.Id, tag, query.FullTransactions),
	))
}

// GetBlockByHash calls eth_getBlockByHash.
func (client *ThinClient) GetBlockByHash(ctx context.Context, query BlockQuery) ([]byte, error) {
	tag := getOrDefault(BlockTagLatest, query.BlockTag)

	if query.Hash != "" {
		tag = query.Hash
	}

	return omitStream(client.handle(ctx,
		APISpec{}.GetBlockByHash,
		QueryWithId(query.Id, tag, query.FullTransactions),
	))
}

// GetLogs calls eth_getLogs.
func (client *ThinClient) GetLogs(ctx context.Context, query LogsQuery) ([]byte, error) {
	rpcCallData := map[string]any{}

	if query.FromBlock != "" {
		rpcCallData["fromBlock"] = query.FromBlock
	}

	rpcCallData["toBlock"] = getOrDefault(BlockTagLatest, query.ToBlock)

	if query.Address != "" {
		rpcCallData["address"] = query.Address
	}

	if len(query.Topics) != 0 {
		rpcCallData["topics"] = query.Topics
	}

	return omitStream(client.handle(ctx,
		APISpec{}.GetLogs,
		QueryWithId(query.Id, rpcCallData),
	))
}

// Subscribe calls eth_subscribe and registers a local listener channel.
// It is only supported on WebSocket clients. Returns the subscription ID and
// a channel on which push events are delivered until UnSubscribe is called.
func (client *ThinClient) Subscribe(ctx context.Context, query SubscribeQuery) (transport.Subscription, transport.RStream, error) {
	if client.kind != transport.WS {
		return "", nil, fmt.Errorf("%s rpc doesn't support subscribe method", client.kind)
	}

	var meta map[string]any
	if query.Address != "" || len(query.Topics) > 0 {
		meta = make(map[string]any)
		if query.Address != "" {
			meta["address"] = query.Address
		}

		if len(query.Topics) > 0 {
			meta["topics"] = query.Topics
		}
	}

	var subscription []byte
	var stream transport.RStream
	var err error

	if len(meta) > 0 {
		subscription, stream, err = client.handle(ctx,
			APISpec{}.Subscribe,
			QueryWithId(
				query.Id,
				query.On,
				meta,
			),
		)
	} else {
		subscription, stream, err = client.handle(ctx,
			APISpec{}.Subscribe,
			QueryWithId(
				query.Id,
				query.On,
			),
		)
	}

	if err != nil || subscription == nil {
		return "", nil, err
	}

	listener := make(transport.RWStream, client.subscriptionStreamSize)

	client.mu.Lock()
	if _, ok := client.subscriptions[stream]; !ok {
		client.subscriptions[stream] = make(map[string]struct{})
	}
	client.subscriptions[stream][string(subscription)] = struct{}{}
	client.listeners[string(subscription)] = listener
	client.mu.Unlock()

	return string(subscription), listener, nil
}

// UnSubscribe calls eth_unsubscribe and tears down the local listener.
// It is only supported on WebSocket clients.
func (client *ThinClient) UnSubscribe(ctx context.Context, query UnSubscribeQuery) ([]byte, error) {
	if client.kind != transport.WS {
		return nil, fmt.Errorf("%s rpc doesn't support subscribe method", client.kind)
	}

	result, err := omitStream(client.handle(ctx,
		APISpec{}.Unsubscribe,
		QueryWithId(
			query.Id,
			query.Subscription,
		),
	))
	if err == nil {
		client.removeSubscription(query.Subscription)
	}
	return result, err
}
