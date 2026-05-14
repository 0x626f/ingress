package transport

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// wsURL converts an httptest server URL to its ws:// equivalent.
func wsURL(u string) string {
	return "ws" + strings.TrimPrefix(u, "http")
}

// newEchoServer starts a WebSocket server that echoes every rpc frame back.
func newEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			_ = wsutil.WriteServerMessage(conn, op, msg)
		}
	}))
}

// newPushServer starts a WebSocket server that sends messages once connected then closes.
func newPushServer(t *testing.T, messages [][]byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, msg := range messages {
			_ = wsutil.WriteServerText(conn, msg)
		}
	}))
}

func mustRecv(t *testing.T, ch <-chan Message, timeout time.Duration) (Message, bool) {
	t.Helper()
	select {
	case data, ok := <-ch:
		return data, ok
	case <-time.After(timeout):
		t.Error("timeout waiting for event")
		return nil, false
	}
}

// ============================================================================
// NewRPCConnection
// ============================================================================

func TestNewRPCConnection_HTTP(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "http://localhost:8545"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*HttpConnection); !ok {
		t.Fatalf("expected *HttpConnection, got %T", conn)
	}
	if conn.Kind() != HTTP {
		t.Errorf("expected HTTP kind, got %v", conn.Kind())
	}
	if conn.Resource() != "http://localhost:8545" {
		t.Errorf("unexpected resource: %s", conn.Resource())
	}
}

func TestNewRPCConnection_HTTPS(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "https://rpc.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*HttpConnection); !ok {
		t.Fatalf("expected *HttpConnection, got %T", conn)
	}
	if conn.Kind() != HTTP {
		t.Errorf("expected HTTP kind, got %v", conn.Kind())
	}
}

func TestNewRPCConnection_WS(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "ws://localhost:8546"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*WsConnection); !ok {
		t.Fatalf("expected *WsConnection, got %T", conn)
	}
	if conn.Kind() != WS {
		t.Errorf("expected WS kind, got %v", conn.Kind())
	}
}

func TestNewRPCConnection_WSS(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "wss://rpc.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if conn.Kind() != WS {
		t.Errorf("expected WS kind, got %v", conn.Kind())
	}
}

func TestNewRPCConnection_UnsupportedScheme(t *testing.T) {
	if _, err := NewRPCConnection(ConnectionParams{Resource: "ftp://bad"}); err == nil {
		t.Error("expected error for unsupported scheme")
	}
}

func TestNewRPCConnection_WS_DefaultBufferSize(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "ws://localhost:8546"})
	if err != nil {
		t.Fatal(err)
	}
	if cap(conn.Stream()) != 64 {
		t.Errorf("expected default stream capacity 64, got %d", cap(conn.Stream()))
	}
}

func TestNewRPCConnection_WS_CustomBufferSize(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "ws://localhost:8546", StreamSize: 128})
	if err != nil {
		t.Fatal(err)
	}
	if cap(conn.Stream()) != 128 {
		t.Errorf("expected stream capacity 128, got %d", cap(conn.Stream()))
	}
}

// ============================================================================
// ConnectionManager
// ============================================================================

type stubConn struct {
	response []byte
	err      error
}

func (s *stubConn) Kind() ConnectionKind        { return HTTP }
func (s *stubConn) Resource() string            { return "" }
func (s *stubConn) Timeout() time.Duration      { return 0 }
func (s *stubConn) Stream() <-chan Message      { return nil }
func (s *stubConn) Send([]byte) ([]byte, error) { return s.response, s.err }

func TestConnectionManager_Send_FirstSucceeds(t *testing.T) {
	mgr := &ConnectionManager{}
	mgr.AddConnection(&stubConn{response: []byte("first")})
	mgr.AddConnection(&stubConn{response: []byte("second")})

	result, _, _, err := mgr.Send(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "first" {
		t.Errorf("expected 'first', got %q", result)
	}
}

func TestConnectionManager_Send_FirstFails_FallsBackToSecond(t *testing.T) {
	mgr := &ConnectionManager{}
	mgr.AddConnection(&stubConn{err: errors.New("fail")})
	mgr.AddConnection(&stubConn{response: []byte("fallback")})

	result, _, _, err := mgr.Send(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "fallback" {
		t.Errorf("expected 'fallback', got %q", result)
	}
}

func TestConnectionManager_Send_AllFail_ReturnsError(t *testing.T) {
	mgr := &ConnectionManager{}
	mgr.AddConnection(&stubConn{err: errors.New("err1")})
	mgr.AddConnection(&stubConn{err: errors.New("err2")})

	if _, _, _, err := mgr.Send(nil); err == nil {
		t.Error("expected error when all connections fail")
	}
}

func TestConnectionManager_AddConnection_Chaining(t *testing.T) {
	mgr := (&ConnectionManager{}).
		AddConnection(&stubConn{}).
		AddConnection(&stubConn{})
	if len(mgr.connections) != 2 {
		t.Errorf("expected 2 connections, got %d", len(mgr.connections))
	}
}

// ============================================================================
// HttpConnection
// ============================================================================

func TestHttpConnection_Send_ReturnsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"result":"ok"}`))
	}))
	defer srv.Close()

	conn := &HttpConnection{BaseConnection{resource: srv.URL, kind: HTTP}}
	result, err := conn.Send([]byte(`{"id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != `{"result":"ok"}` {
		t.Errorf("unexpected body: %s", result)
	}
}

func TestHttpConnection_Send_ConnectionRefused(t *testing.T) {
	conn := &HttpConnection{BaseConnection{resource: "http://127.0.0.1:1", kind: HTTP}}
	if _, err := conn.Send([]byte(`{}`)); err == nil {
		t.Error("expected connection error")
	}
}

func TestHttpConnection_Stream_ReturnsNil(t *testing.T) {
	if (&HttpConnection{}).Stream() != nil {
		t.Error("expected nil stream for HTTP connection")
	}
}

// ============================================================================
// WsConnection — Send
// ============================================================================

func TestWsConnection_Send_WritesPayload(t *testing.T) {
	received := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		msg, _, _ := wsutil.ReadClientData(conn)
		received <- msg
	}))
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})
	if _, err := conn.Send([]byte(`{"id":1,"method":"test"}`)); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-received:
		if string(msg) != `{"id":1,"method":"test"}` {
			t.Errorf("server received unexpected payload: %s", msg)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for server to receive payload")
	}
}

func TestWsConnection_Send_ReturnsNilBytes(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})
	result, err := conn.Send([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil result bytes from async send, got %v", result)
	}
}

func TestWsConnection_Send_ConnectError(t *testing.T) {
	conn, _ := NewRPCConnection(ConnectionParams{Resource: "ws://127.0.0.1:1"})
	if _, err := conn.Send([]byte(`{}`)); err == nil {
		t.Error("expected error when connecting to unreachable endpoint")
	}
}

func TestWsConnection_Send_ConcurrentSafe(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.Send([]byte(`{"id":1}`))
		}()
	}
	wg.Wait()
}

// ============================================================================
// WsConnection — RStream
// ============================================================================

func TestWsConnection_Stream_AvailableBeforeConnect(t *testing.T) {
	conn, err := NewRPCConnection(ConnectionParams{Resource: "ws://localhost:8546"})
	if err != nil {
		t.Fatal(err)
	}
	if conn.Stream() == nil {
		t.Error("expected non-nil stream channel immediately after construction")
	}
}

func TestWsConnection_Stream_ReceivesServerEvents(t *testing.T) {
	payload := []byte(`{"id":1,"result":"0x1"}`)
	srv := newPushServer(t, [][]byte{payload})
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})

	// Obtain stream before Send so we hold the channel for this connection cycle
	// before the server closes and close nulls connection.events.
	stream := conn.Stream()
	conn.Send([]byte(`{}`))

	data, ok := mustRecv(t, stream, time.Second)
	if !ok {
		return
	}
	if string(data) != string(payload) {
		t.Errorf("expected %q, got %q", payload, data)
	}
}

func TestWsConnection_Stream_ClosedOnServerDisconnect(t *testing.T) {
	srv := newPushServer(t, nil) // upgrades then immediately closes
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})

	// Obtain stream before Send — same reasoning as above.
	stream := conn.Stream()
	conn.Send([]byte(`{}`))

	// Drain any buffered events; the channel must eventually close.
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-stream:
			if !ok {
				return // closed as expected
			}
		case <-deadline:
			t.Error("timeout waiting for stream to close after server disconnect")
			return
		}
	}
}

func TestWsConnection_Stream_DropsOldListenersOnReconnect(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv.URL)})

	// First connection cycle.
	conn.Send([]byte(`{}`))
	first := conn.Stream()

	// Force disconnect — old channel must close to signal previous listeners.
	conn.(*WsConnection).close()

	select {
	case _, ok := <-first:
		if ok {
			t.Error("expected first stream to be closed after disconnect")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for first stream to close")
	}

	// Reconnect and verify a distinct fresh channel is returned.
	conn.Send([]byte(`{}`))
	second := conn.Stream()

	if second == first {
		t.Error("expected a new stream channel after reconnect")
	}
	if second == nil {
		t.Error("expected non-nil stream after reconnect")
	}
}

// ============================================================================
// WsConnection — close
// ============================================================================

func TestWsConnection_CloseConn_Idempotent(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	conn := &WsConnection{
		BaseConnection: BaseConnection{resource: wsURL(srv.URL), kind: WS, keepAlivePeriod: time.Minute},
		events:         make(chan Message, 64),
		eventsSize:     64,
	}
	if err := conn.connect(); err != nil {
		t.Fatal(err)
	}

	// Must not panic on double close.
	conn.close()
	conn.close()
}

func TestWsConnection_CloseConn_ClosesStream(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	conn := &WsConnection{
		BaseConnection: BaseConnection{resource: wsURL(srv.URL), kind: WS, keepAlivePeriod: time.Minute},
		events:         make(chan Message, 64),
		eventsSize:     64,
	}
	if err := conn.connect(); err != nil {
		t.Fatal(err)
	}

	stream := conn.Stream()
	conn.close()

	select {
	case _, ok := <-stream:
		if ok {
			t.Error("expected stream to be closed")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for stream to close after close")
	}
}

// ============================================================================
// WsConnection — keepAlive
// ============================================================================

// newPingCountServer starts a WS server that counts ping frames and responds
// with pongs. It uses raw ws.ReadFrame so control frames are not swallowed by
// the wsutil high-level API.
func newPingCountServer(t *testing.T, pings *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			fr, err := ws.ReadFrame(conn)
			if err != nil {
				return
			}
			// Client frames are masked; unmask before inspecting.
			if fr.Header.Masked {
				ws.Cipher(fr.Payload, fr.Header.Mask, 0)
				fr.Header.Masked = false
			}
			switch fr.Header.OpCode {
			case ws.OpPing:
				pings.Add(1)
				_ = ws.WriteFrame(conn, ws.NewPongFrame(fr.Payload))
			case ws.OpClose:
				_ = ws.WriteFrame(conn, ws.NewCloseFrame(nil))
				return
			}
		}
	}))
}

func TestWsConnection_KeepAlive_SendsPingWhenIdle(t *testing.T) {
	var pings atomic.Int32
	srv := newPingCountServer(t, &pings)
	defer srv.Close()

	conn, _ := NewRPCConnection(ConnectionParams{
		Resource:        wsURL(srv.URL),
		KeepAlivePeriod: 30 * time.Millisecond,
	})
	conn.Send([]byte(`{}`)) // trigger connection

	// Wait for at least one ping.
	deadline := time.After(300 * time.Millisecond)
	for {
		if pings.Load() >= 1 {
			return
		}
		select {
		case <-deadline:
			t.Errorf("expected at least one ping when idle, got %d", pings.Load())
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestWsConnection_KeepAlive_TimerResetsOnActivity(t *testing.T) {
	var pings atomic.Int32
	srv := newPingCountServer(t, &pings)
	defer srv.Close()

	period := 60 * time.Millisecond
	conn, _ := NewRPCConnection(ConnectionParams{
		Resource:        wsURL(srv.URL),
		KeepAlivePeriod: period,
	})
	conn.Send([]byte(`{}`)) // trigger connection

	// Keep sending within the period — timer should keep resetting so no ping fires.
	end := time.After(period * 3)
	tick := time.NewTicker(period / 4)
	defer tick.Stop()
	for {
		select {
		case <-end:
			if got := pings.Load(); got > 1 {
				t.Errorf("expected at most 1 ping with continuous activity, got %d", got)
			}
			return
		case <-tick.C:
			conn.Send([]byte(`{}`))
		}
	}
}

func TestWsConnection_KeepAlive_ApplicationLevelMessage(t *testing.T) {
	received := make(chan []byte, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			if op == ws.OpText {
				select {
				case received <- msg:
				default:
				}
			}
		}
	}))
	defer srv.Close()

	keepAliveMsg := []byte(`{"jsonrpc":"2.0","id":99,"method":"eth_blockNumber","params":[]}`)
	conn, _ := NewRPCConnection(ConnectionParams{
		Resource:         wsURL(srv.URL),
		KeepAlivePeriod:  30 * time.Millisecond,
		KeepAliveMessage: func() []byte { return keepAliveMsg },
	})

	conn.Send([]byte(`{"id":1}`))
	<-received // consume the initial send

	select {
	case msg := <-received:
		if string(msg) != string(keepAliveMsg) {
			t.Errorf("expected keepalive message %q, got %q", keepAliveMsg, msg)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("timeout: no application-level keepalive sent while idle")
	}
}

// ============================================================================
// Multiple WsConnections — independence
// ============================================================================

func TestWsConnection_MultipleConnections_IndependentStreams(t *testing.T) {
	srv1 := newEchoServer(t)
	defer srv1.Close()
	srv2 := newEchoServer(t)
	defer srv2.Close()

	conn1, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv1.URL)})
	conn2, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv2.URL)})

	stream1 := conn1.Stream()
	stream2 := conn2.Stream()

	if stream1 == stream2 {
		t.Fatal("expected separate stream channels for independent connections")
	}

	// Send on conn1 only.
	conn1.Send([]byte(`{"id":1}`))

	select {
	case <-stream1:
		// echo from srv1 — expected
	case <-time.After(time.Second):
		t.Error("timeout waiting for echo on stream1")
	}

	// stream2 must remain silent.
	select {
	case msg := <-stream2:
		t.Errorf("unexpected message on stream2: %s", msg)
	case <-time.After(50 * time.Millisecond):
		// silence as expected
	}
}

func TestWsConnection_MultipleConnections_CloseOneDoesNotAffectOther(t *testing.T) {
	srv1 := newEchoServer(t)
	defer srv1.Close()
	srv2 := newEchoServer(t)
	defer srv2.Close()

	conn1, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv1.URL)})
	conn2, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv2.URL)})

	conn1.Send([]byte(`{}`))
	conn2.Send([]byte(`{}`))

	stream1 := conn1.Stream()
	stream2 := conn2.Stream()

	// Drain echo frames.
	mustRecv(t, stream1, time.Second)
	mustRecv(t, stream2, time.Second)

	// Close conn1.
	conn1.(*WsConnection).close()

	select {
	case _, ok := <-stream1:
		if ok {
			t.Error("expected stream1 closed after conn1.close()")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for stream1 to close")
	}

	// conn2 must still work.
	if _, err := conn2.Send([]byte(`{"id":2}`)); err != nil {
		t.Errorf("conn2 should still be functional: %v", err)
	}
	mustRecv(t, stream2, time.Second)
}

func TestConnectionManager_MultipleWS_FallbackOnSendError(t *testing.T) {
	// srv1 accepts connection but immediately closes — subsequent writes fail.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		conn.Close() // close immediately after upgrade
	}))
	defer srv1.Close()

	srv2 := newEchoServer(t)
	defer srv2.Close()

	conn1, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv1.URL)})
	conn2, _ := NewRPCConnection(ConnectionParams{Resource: wsURL(srv2.URL)})

	// Trigger conn1 to connect (and discover the broken socket) before the manager test.
	conn1.Send([]byte(`{}`))
	time.Sleep(50 * time.Millisecond) // let listen() detect the closed socket

	mgr := &ConnectionManager{}
	mgr.AddConnection(conn1)
	mgr.AddConnection(conn2)

	_, stream, _, err := mgr.Send([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("expected fallback to succeed, got: %v", err)
	}
	if stream != conn2.Stream() {
		t.Error("expected manager to have fallen back to conn2's stream")
	}
}
