package rpc

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x626f/ingress/solana/types"
)

type proxyTestContextKey string

func TestProxyKey_SerializesMethodAndQuery(t *testing.T) {
	key := proxyKey("GetBalance", proxyParams("pubkey", []any{map[string]string{"commitment": "confirmed"}}))
	want := `{"method":"GetBalance","query":["pubkey",[{"commitment":"confirmed"}]]}`
	if key != want {
		t.Fatalf("unexpected proxy key:\nwant: %s\n got: %s", want, key)
	}
}

func TestProxyClient_ContextFactoryUsedForNilContext(t *testing.T) {
	factoryCtx := context.WithValue(context.Background(), proxyTestContextKey("source"), "factory")

	proxy := NewProxyClient(ProxyClientOptions{
		Client: &ThinClient{},
		ContextFactory: func() context.Context {
			return factoryCtx
		},
		PreprocessHook: func(ctx context.Context, method string, query any) error {
			if ctx != factoryCtx {
				t.Fatal("preprocess did not receive factory context")
			}
			return nil
		},
		PostProcessHook: func(ctx context.Context, method string, query any, result any, err error) error {
			if ctx != factoryCtx {
				t.Fatal("postprocess did not receive factory context")
			}
			return nil
		},
	})

	result, err := proxy.raw(nil, "GetHealth", nil, func(ctx context.Context) (types.RawResult, error) {
		if ctx != factoryCtx {
			t.Fatal("proxied call did not receive factory context")
		}
		return types.RawResult("ok"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "ok" {
		t.Fatalf("unexpected result: %s", result)
	}
}

func TestProxyClient_HooksWrapCall(t *testing.T) {
	ctx := context.Background()
	query := proxyParams("pubkey")
	var order []string

	proxy := NewProxyClient(ProxyClientOptions{
		Client: &ThinClient{},
		PreprocessHook: func(gotCtx context.Context, method string, gotQuery any) error {
			if gotCtx != ctx {
				t.Fatal("preprocess received unexpected context")
			}
			if method != "GetBalance" {
				t.Fatalf("unexpected preprocess method: %s", method)
			}
			if !reflect.DeepEqual(gotQuery, query) {
				t.Fatalf("unexpected preprocess query: %#v", gotQuery)
			}
			order = append(order, "pre")
			return nil
		},
		PostProcessHook: func(gotCtx context.Context, method string, gotQuery any, result any, err error) error {
			if gotCtx != ctx {
				t.Fatal("postprocess received unexpected context")
			}
			if method != "GetBalance" {
				t.Fatalf("unexpected postprocess method: %s", method)
			}
			if !reflect.DeepEqual(gotQuery, query) {
				t.Fatalf("unexpected postprocess query: %#v", gotQuery)
			}
			if string(result.(types.RawResult)) != "ok" {
				t.Fatalf("unexpected postprocess result: %v", result)
			}
			if err != nil {
				t.Fatalf("unexpected postprocess error: %v", err)
			}
			order = append(order, "post")
			return nil
		},
	})

	result, err := proxy.raw(ctx, "GetBalance", query, func(ctx context.Context) (types.RawResult, error) {
		order = append(order, "call")
		return types.RawResult("ok"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "ok" {
		t.Fatalf("unexpected result: %s", result)
	}
	if !reflect.DeepEqual(order, []string{"pre", "call", "post"}) {
		t.Fatalf("unexpected hook order: %v", order)
	}
}

func TestProxyClient_PreprocessErrorSkipsCallAndPostprocess(t *testing.T) {
	preErr := errors.New("pre")
	var called atomic.Bool

	proxy := NewProxyClient(ProxyClientOptions{
		Client: &ThinClient{},
		PreprocessHook: func(context.Context, string, any) error {
			return preErr
		},
		PostProcessHook: func(context.Context, string, any, any, error) error {
			t.Fatal("postprocess should not run")
			return nil
		},
	})

	result, err := proxy.raw(context.Background(), "GetHealth", nil, func(ctx context.Context) (types.RawResult, error) {
		called.Store(true)
		return types.RawResult("ok"), nil
	})
	if !errors.Is(err, preErr) {
		t.Fatalf("expected preprocess error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %s", result)
	}
	if called.Load() {
		t.Fatal("proxied call should not run")
	}
}

func TestProxyClient_PostprocessErrorOverridesSuccessfulCall(t *testing.T) {
	postErr := errors.New("post")
	proxy := NewProxyClient(ProxyClientOptions{
		Client: &ThinClient{},
		PostProcessHook: func(context.Context, string, any, any, error) error {
			return postErr
		},
	})

	result, err := proxy.raw(context.Background(), "GetHealth", nil, func(ctx context.Context) (types.RawResult, error) {
		return types.RawResult("ok"), nil
	})
	if !errors.Is(err, postErr) {
		t.Fatalf("expected postprocess error, got %v", err)
	}
	if string(result) != "ok" {
		t.Fatalf("unexpected result: %s", result)
	}
}

func TestProxyClient_InflightCacheCoalescesIdenticalCalls(t *testing.T) {
	const workers = 8

	proxy := NewProxyClient(ProxyClientOptions{Client: &ThinClient{}, InflightCache: true})
	release := make(chan struct{})
	started := make(chan struct{})
	var calls atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := proxy.raw(context.Background(), "GetHealth", nil, func(ctx context.Context) (types.RawResult, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return types.RawResult("ok"), nil
			})
			if err != nil {
				t.Errorf("proxy call failed: %v", err)
				return
			}
			if string(result) != "ok" {
				t.Errorf("unexpected result: %s", result)
			}
		}()
	}

	<-started
	time.Sleep(10 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		close(release)
		t.Fatalf("expected one in-flight call, got %d", got)
	}
	close(release)
	wg.Wait()
}
