package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

const (
	defaultKeepAlivePeriod = 15 * time.Second
)

// Protocol distinguishes the transport protocol of a connection.
type Protocol uint8

const (
	// HTTP represents an HTTP or HTTPS JSON-RPC connection.
	HTTP Protocol = iota
	// WS represents a WebSocket or WebSocket-Secure JSON-RPC connection.
	WS
)

// String returns a human-readable name for the connection kind.
func (kind Protocol) String() string {
	switch kind {
	case HTTP:
		return "http"
	case WS:
		return "ws"
	}
	return "unknown"
}

// Message is a raw JSON-RPC message payload.
type Message = []byte

// RStream is a read-only channel of incoming messages from a connection.
type RStream = <-chan Message

// RWStream is a bidirectional channel used to buffer messages within the proxy layer.
type RWStream = chan Message

// Subscription is the opaque identifier returned by the node for an active eth_subscribe subscription.
type Subscription = string

// Connection is the interface that wraps a single JSON-RPC transport endpoint.
type Connection interface {
	// Resource returns the endpoint URL this connection targets.
	Resource() string
	// Kind returns whether this is an HTTP or WebSocket connection.
	Kind() Protocol

	// Send serializes and dispatches a JSON-RPC request.
	// For HTTP it returns the response body directly.
	// For WebSocket it returns nil and the response arrives on Stream.
	Send(context.Context, []byte) ([]byte, error)
	// Timeout returns the per-request deadline configured for this connection.
	Timeout() time.Duration
	// Stream returns the channel on which incoming WebSocket messages are delivered.
	// Returns nil for HTTP connections.
	Stream() RStream
}

// ConnectionManager holds a pool of connections of the same kind and routes
// outgoing requests to the first healthy one.
type ConnectionManager struct {
	mu          sync.Mutex
	connections []Connection
	active      int
}

// AddConnection appends conn to the pool and returns the manager for chaining.
func (manager *ConnectionManager) AddConnection(conn Connection) *ConnectionManager {
	manager.connections = append(manager.connections, conn)
	return manager
}

// Send dispatches data to the first available connection in the pool.
// It returns the raw response bytes (HTTP only), the connection's event stream
// (WebSocket only), the configured timeout, and any transport error.
func (manager *ConnectionManager) Send(ctx context.Context, data []byte) (result []byte, stream RStream, timeout time.Duration, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if len(manager.connections) == 0 {
		err = fmt.Errorf("no connections available")
		return
	}

	for index := manager.active; index < len(manager.connections); index++ {
		manager.active = index

		result, err = manager.connections[index].Send(ctx, data)
		if err == nil {
			stream = manager.connections[index].Stream()
			timeout = manager.connections[index].Timeout()
			return
		}
	}
	if err == nil {
		err = fmt.Errorf("no connections available")
	}
	return
}

// BaseConnection holds fields shared by all connection types.
type BaseConnection struct {
	kind Protocol

	timeout   time.Duration
	connected bool
	resource  string

	keepAlivePeriod         time.Duration
	keepAliveMessageFunctor func() []byte
}

// Kind returns the transport kind of this connection.
func (connection *BaseConnection) Kind() Protocol {
	return connection.kind
}

// Resource returns the endpoint URL of this connection.
func (connection *BaseConnection) Resource() string {
	return connection.resource
}

// Timeout returns the per-request deadline of this connection.
func (connection *BaseConnection) Timeout() time.Duration {
	return connection.timeout
}

// ConnectionParams is the configuration used to construct a new Connection.
type ConnectionParams struct {
	// Resource is the endpoint URL (http/https/ws/wss).
	Resource string
	// Timeout is the per-request deadline. Zero means no deadline.
	Timeout time.Duration
	// KeepAlivePeriod is the idle interval after which a keep-alive probe is sent.
	// Defaults to 15 s when zero.
	KeepAlivePeriod time.Duration
	// KeepAliveMessage is an optional factory that produces application-level
	// keep-alive payloads. When nil a WebSocket ping frame is sent instead.
	KeepAliveMessage func() []byte
	// StreamSize is the buffer depth of the WebSocket event channel.
	// Defaults to 64 when zero.
	StreamSize int
}

// NewRPCConnection creates a Connection for the given params.
// The transport is selected from the URL scheme: http/https → HttpConnection,
// ws/wss → WsConnection. Any other scheme returns an error.
func NewRPCConnection(params ConnectionParams) (Connection, error) {
	rpcUrl, err := url.Parse(params.Resource)
	if err != nil {
		return nil, err
	}

	if params.StreamSize == 0 {
		params.StreamSize = 64
	}

	if params.KeepAlivePeriod == 0 {
		params.KeepAlivePeriod = defaultKeepAlivePeriod
	}

	switch rpcUrl.Scheme {
	case "http", "https":
		return newHttpConnection(params), nil
	case "ws", "wss":
		return newWsConnection(params), nil
	}
	return nil, fmt.Errorf("unsupported resource schema: %s", rpcUrl.String())
}

// HttpConnection implements Connection over HTTP/HTTPS.
// Each Send call opens a new HTTP request.
type HttpConnection struct {
	BaseConnection
}

func newHttpConnection(params ConnectionParams) *HttpConnection {
	return &HttpConnection{
		BaseConnection: BaseConnection{
			resource:                params.Resource,
			kind:                    HTTP,
			timeout:                 params.Timeout,
			keepAlivePeriod:         params.KeepAlivePeriod,
			keepAliveMessageFunctor: params.KeepAliveMessage,
		},
	}
}

// Send posts payload as a JSON-RPC request and returns the raw response body.
func (connection *HttpConnection) Send(ctx context.Context, payload []byte) (result []byte, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, connection.resource, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := connection.client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if result, err = io.ReadAll(response.Body); err != nil {
		return nil, err
	}
	return
}

func (connection *HttpConnection) client() *http.Client {
	return &http.Client{
		Timeout: connection.timeout,
	}
}

// Stream always returns nil for HTTP connections; responses are returned by Send.
func (connection *HttpConnection) Stream() <-chan Message {
	return nil
}

// WsConnection implements Connection over WebSocket/WebSocket-Secure.
// A single persistent connection is shared across all Send calls.
// Incoming messages are delivered on the channel returned by Stream.
type WsConnection struct {
	BaseConnection

	connMu  sync.Mutex
	writeMu sync.Mutex
	ws      net.Conn

	done       chan struct{}
	activity   chan struct{}
	events     chan Message
	eventsSize int
}

func newWsConnection(params ConnectionParams) *WsConnection {
	return &WsConnection{
		BaseConnection: BaseConnection{
			resource:                params.Resource,
			kind:                    WS,
			timeout:                 params.Timeout,
			keepAlivePeriod:         params.KeepAlivePeriod,
			keepAliveMessageFunctor: params.KeepAliveMessage,
		},
		events:     make(chan Message, params.StreamSize),
		eventsSize: params.StreamSize,
	}
}

func (connection *WsConnection) connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	connection.connMu.Lock()
	defer connection.connMu.Unlock()

	if connection.connected {
		return nil
	}

	if connection.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, connection.timeout)
		defer cancel()
	}

	conn, _, _, err := ws.Dial(ctx, connection.resource)
	if err != nil {
		return err
	}

	connection.ws = conn
	connection.done = make(chan struct{})
	connection.activity = make(chan struct{}, connection.eventsSize)
	if connection.events == nil {
		connection.events = make(chan Message, connection.eventsSize)
	}
	connection.connected = true

	go connection.listen(conn)
	go connection.keepAlive(conn)

	return nil
}

func (connection *WsConnection) listen(wsConn net.Conn) {
	for {
		data, op, err := wsutil.ReadServerData(wsConn)
		if err != nil {
			connection.closeConn(wsConn)
			return
		}

		switch op {
		case ws.OpClose:
			connection.closeConn(wsConn)
			return
		case ws.OpPing:
			connection.writeMu.Lock()
			_ = wsutil.WriteClientMessage(wsConn, ws.OpPong, data)
			connection.writeMu.Unlock()
			continue
		case ws.OpPong:
			continue
		}

		select {
		case connection.events <- data:
		case <-connection.done:
			return
		}
	}
}

func (connection *WsConnection) keepAlive(wsConn net.Conn) {
	timer := time.NewTimer(connection.keepAlivePeriod)
	defer timer.Stop()

	for {
		select {
		case _, ok := <-connection.activity:
			if !ok {
				return
			}
			timer.Reset(connection.keepAlivePeriod)
		case <-timer.C:
			connection.writeMu.Lock()
			if connection.keepAliveMessageFunctor != nil {
				msg := connection.keepAliveMessageFunctor()
				_ = wsutil.WriteClientText(wsConn, msg)
			} else {
				_ = wsutil.WriteClientMessage(wsConn, ws.OpPing, nil)
			}
			timer.Reset(connection.keepAlivePeriod)
			connection.writeMu.Unlock()
		}
	}
}

func (connection *WsConnection) close() {
	connection.closeConn(nil)
}

func (connection *WsConnection) closeConn(expected net.Conn) {
	connection.connMu.Lock()
	if !connection.connected {
		connection.connMu.Unlock()
		return
	}
	if expected != nil && connection.ws != expected {
		connection.connMu.Unlock()
		return
	}
	defer connection.connMu.Unlock()

	connection.connected = false

	close(connection.events)
	close(connection.done)
	close(connection.activity)

	connection.events = nil
	connection.ws.Close()
}

// Send writes payload to the WebSocket connection, lazily establishing the
// connection on the first call. The response is not returned here; it arrives
// asynchronously on the channel returned by Stream.
func (connection *WsConnection) Send(ctx context.Context, payload []byte) ([]byte, error) {
	if err := connection.connect(ctx); err != nil {
		return nil, err
	}

	connection.writeMu.Lock()
	err := wsutil.WriteClientText(connection.ws, payload)
	connection.writeMu.Unlock()

	if err == nil {
		connection.connMu.Lock()
		defer connection.connMu.Unlock()
		if !connection.connected || connection.activity == nil {
			return nil, nil
		}
		select {
		case connection.activity <- struct{}{}:
		default:
		}
	}
	return nil, err
}

// Stream returns the channel on which all inbound WebSocket messages are delivered.
func (connection *WsConnection) Stream() RStream {
	connection.connMu.Lock()
	defer connection.connMu.Unlock()
	return connection.events
}
