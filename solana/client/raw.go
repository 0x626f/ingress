package client

import (
	"context"
	"fmt"
	"time"

	"github.com/0x626f/ingress/evm"
)

type ClientConfig struct {
	// Resources is the list of endpoint URLs (http/https/ws/wss).
	// At least one must be provided.
	Resources []string
	// ErrorOnInvalidResource causes NewRawClient to return an error when a resource
	// URL cannot be parsed or has an unsupported scheme. When false, invalid
	// resources are silently skipped.
	ErrorOnInvalidResource bool
	// RequestTimeout is the per-request deadline forwarded to each connection.
	// Zero means no deadline.
	RequestTimeout time.Duration
	// KeepAlivePeriod is the idle interval after which a keep-alive probe is sent
	// on WebSocket connections. Defaults to 15 s when zero.
	KeepAlivePeriod time.Duration
	// SubscriptionStreamSize is the buffer depth of subscription event channels.
	SubscriptionStreamSize int
}

// RawClient manages HTTP and WebSocket connection pools for a Solana JSON-RPC node.
// Use HTTP() and WS() to obtain a ThinClient for the desired transport.
type RawClient struct {
	ctx    context.Context
	config *ClientConfig

	http *evm.ConnectionManager
	ws   *evm.ConnectionManager

	sequencer *evm.SequenceGenerator
}

// NewRawClient constructs a RawClient from the provided config, dialing and
// registering each resource URL in the appropriate connection pool (HTTP or WS).
// Returns an error if no resources are provided, or if a resource is invalid
// and ErrorOnInvalidResource is set.
func NewRawClient(config *ClientConfig) (*RawClient, error) {
	return NewRawClientWithContext(context.Background(), config)
}

// NewRawClientWithContext constructs a RawClient with an explicit context for
// subscription lifetimes.
func NewRawClientWithContext(ctx context.Context, config *ClientConfig) (*RawClient, error) {
	if config == nil {
		return nil, fmt.Errorf("nil config")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(config.Resources) == 0 {
		return nil, fmt.Errorf("no resources specified")
	}

	client := &RawClient{
		ctx:       ctx,
		config:    config,
		sequencer: new(evm.SequenceGenerator),
	}

	genKeepAliveMessage := func() []byte {
		payload, _ := buildRequestWithID(client.sequencer.Next(), RPCMethodGetHealth, nil)
		return payload
	}

	for _, resource := range config.Resources {
		conn, err := evm.NewRPCConnection(evm.ConnectionParams{
			Resource:         resource,
			Timeout:          config.RequestTimeout,
			KeepAlivePeriod:  config.KeepAlivePeriod,
			KeepAliveMessage: genKeepAliveMessage,
			StreamSize:       config.SubscriptionStreamSize,
		})
		if err != nil {
			if config.ErrorOnInvalidResource {
				return nil, err
			}
			continue
		}

		switch conn.Kind() {
		case evm.HTTP:
			if client.http == nil {
				client.http = &evm.ConnectionManager{}
			}
			client.http.AddConnection(conn)
		case evm.WS:
			if client.ws == nil {
				client.ws = &evm.ConnectionManager{}
			}
			client.ws.AddConnection(conn)
		default:
			if config.ErrorOnInvalidResource {
				return nil, fmt.Errorf("client does not support %s resources", conn.Kind())
			}
		}
	}

	if client.http == nil && client.ws == nil {
		return nil, fmt.Errorf("no valid resources specified")
	}

	return client, nil
}

func (client *RawClient) HTTP() *ThinClient {
	return newThinClientWithContext(client.ctx, evm.HTTP, client.http, client.sequencer, 0)
}

func (client *RawClient) WS() *ThinClient {
	return newThinClientWithContext(client.ctx, evm.WS, client.ws, client.sequencer, client.config.SubscriptionStreamSize)
}

func newThinClient(kind evm.ConnectionKind, manager *evm.ConnectionManager, sequencer *evm.SequenceGenerator, subscriptionBufSize int) *ThinClient {
	return newThinClientWithContext(context.Background(), kind, manager, sequencer, subscriptionBufSize)
}

func newThinClientWithContext(ctx context.Context, kind evm.ConnectionKind, manager *evm.ConnectionManager, sequencer *evm.SequenceGenerator, subscriptionBufSize int) *ThinClient {
	if subscriptionBufSize == 0 {
		subscriptionBufSize = 64
	}
	return &ThinClient{
		ctx:                 ctx,
		kind:                kind,
		manager:             manager,
		sequencer:           sequencer,
		subscriptionBufSize: subscriptionBufSize,
	}
}
