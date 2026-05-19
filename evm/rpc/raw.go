package rpc

import (
	"fmt"
	"time"

	"github.com/0x626f/ingress/transport"
)

// ClientConfig holds the configuration used to construct a RawClient.
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
	// SubscriptionStreamSize is the buffer depth of the channel returned by Subscribe.
	SubscriptionStreamSize int
}

// RawClient manages HTTP and WebSocket connection pools for an EVM JSON-RPC node.
// Use HTTP() and WS() to obtain a ThinClient for the desired transport.
type RawClient struct {
	config *ClientConfig

	http *transport.ConnectionManager
	ws   *transport.ConnectionManager

	sequencer *transport.SequenceGenerator
}

// NewRawClient constructs a RawClient from the provided config, dialling and
// registering each resource URL in the appropriate connection pool (HTTP or WS).
// Returns an error if no resources are provided, or if a resource is invalid
// and ErrorOnInvalidResource is set.
func NewRawClient(config *ClientConfig) (*RawClient, error) {
	client := &RawClient{
		config:    config,
		sequencer: new(transport.SequenceGenerator),
	}

	if len(client.config.Resources) == 0 {
		return nil, fmt.Errorf("no resources specified")
	}

	// use application level ping to maintain connection in case of keep-alive is required
	genKeepAliveMessage := func() []byte {
		keepAliveMessage, _ := APISpec{}.BlockNumber(QueryWithId(client.sequencer.Next()))
		return keepAliveMessage
	}

	for _, resource := range client.config.Resources {

		connectionParams := transport.ConnectionParams{
			Resource:         resource,
			Timeout:          config.RequestTimeout,
			KeepAlivePeriod:  config.KeepAlivePeriod,
			KeepAliveMessage: genKeepAliveMessage,
			StreamSize:       config.SubscriptionStreamSize,
		}

		conn, err := transport.NewRPCConnection(connectionParams)
		if err != nil {
			if client.config.ErrorOnInvalidResource {
				return nil, err
			}
			continue
		}

		var manager *transport.ConnectionManager

		switch conn.Kind() {
		case transport.HTTP:
			if client.http == nil {
				client.http = &transport.ConnectionManager{}
			}
			manager = client.http
		case transport.WS:
			if client.ws == nil {
				client.ws = &transport.ConnectionManager{}
			}
			manager = client.ws
		default:
			if client.config.ErrorOnInvalidResource {
				return nil, fmt.Errorf("rpc does not support %s resources", conn.Kind())
			}
			continue
		}

		manager.AddConnection(conn)
	}

	if client.http == nil && client.ws == nil {
		return nil, fmt.Errorf("no valid resources specified")
	}

	return client, nil
}

// HTTP returns a ThinClient backed by the HTTP connection pool.
func (client *RawClient) HTTP() *ThinClient {
	if !client.HasResourceByProtocol(transport.HTTP) {
		return nil
	}
	return newThinClient(transport.HTTP, client.http, client.sequencer, 0)
}

// WS returns a ThinClient backed by the WebSocket connection pool.
func (client *RawClient) WS() *ThinClient {
	if !client.HasResourceByProtocol(transport.WS) {
		return nil
	}
	return newThinClient(transport.WS, client.ws, client.sequencer, client.config.SubscriptionStreamSize)
}

// HasResourceByProtocol reports whether the RawClient has resources for kind.
func (client *RawClient) HasResourceByProtocol(kind transport.Protocol) bool {
	if client == nil {
		return false
	}
	switch kind {
	case transport.HTTP:
		return client.http != nil
	case transport.WS:
		return client.ws != nil
	default:
		return false
	}
}
