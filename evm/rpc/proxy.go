package rpc

import (
	"context"
	"fmt"

	"github.com/0x626f/ingress/transport"
	"github.com/bytedance/sonic"
	"golang.org/x/sync/singleflight"
)

var _ CoreClient = (*ProxyClient)(nil)

// ProxyPreprocessHook is called before a proxied RPC method is invoked.
type ProxyPreprocessHook func(ctx context.Context, method string, query any) error

// ProxyPostProcessHook is called after a proxied RPC method returns.
type ProxyPostProcessHook func(ctx context.Context, method string, query any, result any, err error) error

// ProxyContextFactory returns a context for proxied calls that receive nil.
type ProxyContextFactory func() context.Context

// ProxyClientOptions configures a ProxyClient.
type ProxyClientOptions struct {
	Client          CoreClient
	InflightCache   bool
	ContextFactory  ProxyContextFactory
	PreprocessHook  ProxyPreprocessHook
	PostProcessHook ProxyPostProcessHook
}

// ProxyClient wraps a CoreClient with optional in-flight request coalescing and hooks.
type ProxyClient struct {
	client CoreClient

	InflightCache   bool
	ContextFactory  ProxyContextFactory
	PreprocessHook  ProxyPreprocessHook
	PostProcessHook ProxyPostProcessHook

	inflight singleflight.Group
}

// NewProxyClient wraps client with proxy behavior.
func NewProxyClient(options ProxyClientOptions) *ProxyClient {
	return &ProxyClient{
		client:          options.Client,
		InflightCache:   options.InflightCache,
		ContextFactory:  options.ContextFactory,
		PreprocessHook:  options.PreprocessHook,
		PostProcessHook: options.PostProcessHook,
	}
}

type proxyCall struct {
	Method string `json:"method"`
	Query  any    `json:"query,omitempty"`
}

func proxyKey(method string, query any) string {
	data, err := sonic.Marshal(proxyCall{Method: method, Query: query})
	if err != nil {
		return fmt.Sprintf("%s:%#v", method, query)
	}
	return string(data)
}

func (client *ProxyClient) context(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if client != nil && client.ContextFactory != nil {
		if factoryCtx := client.ContextFactory(); factoryCtx != nil {
			return factoryCtx
		}
	}
	return context.Background()
}

func (client *ProxyClient) call(ctx context.Context, method string, query any, fn func(context.Context) (any, error)) (any, error) {
	if client == nil || client.client == nil {
		return nil, fmt.Errorf("nil proxy client")
	}
	ctx = client.context(ctx)
	if client.PreprocessHook != nil {
		if err := client.PreprocessHook(ctx, method, query); err != nil {
			return nil, err
		}
	}

	var run func() (any, error)
	if client.InflightCache {
		key := proxyKey(method, query)
		run = func() (any, error) {
			result, err, _ := client.inflight.Do(key, func() (any, error) {
				return fn(ctx)
			})
			return result, err
		}
	} else {
		run = func() (any, error) {
			return fn(ctx)
		}
	}

	result, err := run()
	if client.PostProcessHook != nil {
		if hookErr := client.PostProcessHook(ctx, method, query, result, err); hookErr != nil && err == nil {
			err = hookErr
		}
	}
	return result, err
}

func (client *ProxyClient) bytes(ctx context.Context, method string, query any, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	result, err := client.call(ctx, method, query, func(ctx context.Context) (any, error) {
		return fn(ctx)
	})
	if result == nil {
		return nil, err
	}
	return result.([]byte), err
}

func (client *ProxyClient) ChainId(ctx context.Context) ([]byte, error) {
	return client.bytes(ctx, "ChainId", nil, func(ctx context.Context) ([]byte, error) {
		return client.client.ChainId(ctx)
	})
}

func (client *ProxyClient) BlockNumber(ctx context.Context) ([]byte, error) {
	return client.bytes(ctx, "BlockNumber", nil, func(ctx context.Context) ([]byte, error) {
		return client.client.BlockNumber(ctx)
	})
}

func (client *ProxyClient) GetBalance(ctx context.Context, query BalanceQuery) ([]byte, error) {
	return client.bytes(ctx, "GetBalance", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetBalance(ctx, query)
	})
}

func (client *ProxyClient) GetCode(ctx context.Context, query CodeQuery) ([]byte, error) {
	return client.bytes(ctx, "GetCode", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetCode(ctx, query)
	})
}

func (client *ProxyClient) GetStorageAt(ctx context.Context, query GetStorageQuery) ([]byte, error) {
	return client.bytes(ctx, "GetStorageAt", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetStorageAt(ctx, query)
	})
}

func (client *ProxyClient) Call(ctx context.Context, query CallQuery) ([]byte, error) {
	return client.bytes(ctx, "Call", query, func(ctx context.Context) ([]byte, error) {
		return client.client.Call(ctx, query)
	})
}

func (client *ProxyClient) EstimateGas(ctx context.Context, query EstimateGasQuery) ([]byte, error) {
	return client.bytes(ctx, "EstimateGas", query, func(ctx context.Context) ([]byte, error) {
		return client.client.EstimateGas(ctx, query)
	})
}

func (client *ProxyClient) SendRawTransaction(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return client.bytes(ctx, "SendRawTransaction", query, func(ctx context.Context) ([]byte, error) {
		return client.client.SendRawTransaction(ctx, query)
	})
}

func (client *ProxyClient) GetTransactionByHash(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return client.bytes(ctx, "GetTransactionByHash", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetTransactionByHash(ctx, query)
	})
}

func (client *ProxyClient) GetTransactionReceipt(ctx context.Context, query TransactionQuery) ([]byte, error) {
	return client.bytes(ctx, "GetTransactionReceipt", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetTransactionReceipt(ctx, query)
	})
}

func (client *ProxyClient) GetTransactionCount(ctx context.Context, query AddressedQuery) ([]byte, error) {
	return client.bytes(ctx, "GetTransactionCount", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetTransactionCount(ctx, query)
	})
}

func (client *ProxyClient) GetBlockByNumber(ctx context.Context, query BlockQuery) ([]byte, error) {
	return client.bytes(ctx, "GetBlockByNumber", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetBlockByNumber(ctx, query)
	})
}

func (client *ProxyClient) GetBlockByHash(ctx context.Context, query BlockQuery) ([]byte, error) {
	return client.bytes(ctx, "GetBlockByHash", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetBlockByHash(ctx, query)
	})
}

func (client *ProxyClient) GetLogs(ctx context.Context, query LogsQuery) ([]byte, error) {
	return client.bytes(ctx, "GetLogs", query, func(ctx context.Context) ([]byte, error) {
		return client.client.GetLogs(ctx, query)
	})
}

func (client *ProxyClient) Subscribe(ctx context.Context, query SubscribeQuery) (transport.Subscription, transport.RStream, error) {
	result, err := client.call(ctx, "Subscribe", query, func(ctx context.Context) (any, error) {
		subscription, stream, err := client.client.Subscribe(ctx, query)
		return struct {
			subscription transport.Subscription
			stream       transport.RStream
		}{subscription: subscription, stream: stream}, err
	})
	if result == nil {
		return "", nil, err
	}
	value := result.(struct {
		subscription transport.Subscription
		stream       transport.RStream
	})
	return value.subscription, value.stream, err
}

func (client *ProxyClient) UnSubscribe(ctx context.Context, query UnSubscribeQuery) ([]byte, error) {
	return client.bytes(ctx, "UnSubscribe", query, func(ctx context.Context) ([]byte, error) {
		return client.client.UnSubscribe(ctx, query)
	})
}
