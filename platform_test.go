package ingress

import (
	"testing"

	evmrpc "github.com/0x626f/ingress/evm/rpc"
	solanarpc "github.com/0x626f/ingress/solana/rpc"
)

func TestSetupEVM_ConfiguresRawHTTPAndWS(t *testing.T) {
	previous := EVM
	defer func() { EVM = previous }()

	err := SetupEVM(&EVMPlatformConfig{
		ClientConfig: evmrpc.ClientConfig{
			Resources: []string{
				"https://evm.example.com",
				"wss://evm.example.com/ws",
			},
		},
	})
	if err != nil {
		t.Fatalf("SetupEVM: %v", err)
	}

	if EVM == nil || EVM.RawClient() == nil {
		t.Fatal("expected configured EVM platform")
	}
	if _, ok := EVM.HTTP().(*evmrpc.ThinClient); !ok {
		t.Fatalf("expected raw EVM HTTP thin client, got %T", EVM.HTTP())
	}
	if _, ok := EVM.WS().(*evmrpc.ThinClient); !ok {
		t.Fatalf("expected raw EVM WS thin client, got %T", EVM.WS())
	}
}

func TestSetupEVM_ConfiguresProxies(t *testing.T) {
	previous := EVM
	defer func() { EVM = previous }()

	err := SetupEVM(&EVMPlatformConfig{
		ClientConfig: evmrpc.ClientConfig{
			Resources: []string{
				"https://evm.example.com",
				"wss://evm.example.com/ws",
			},
		},
		RPCProxyEnabled:  true,
		RPCInflightCache: true,
	})
	if err != nil {
		t.Fatalf("SetupEVM: %v", err)
	}
	http, ok := EVM.HTTP().(*evmrpc.ProxyClient)
	if !ok {
		t.Fatalf("expected EVM HTTP proxy client, got %T", EVM.HTTP())
	}
	if !http.InflightCache {
		t.Fatal("expected EVM HTTP proxy inflight cache enabled")
	}
	if _, ok := EVM.WS().(*evmrpc.ProxyClient); !ok {
		t.Fatalf("expected EVM WS proxy client, got %T", EVM.WS())
	}
}

func TestSetupSolana_ConfiguresRawHTTPAndWS(t *testing.T) {
	previous := Solana
	defer func() { Solana = previous }()

	err := SetupSolana(&SolanaPlatformConfig{
		ClientConfig: solanarpc.ClientConfig{
			Resources: []string{
				"https://solana.example.com",
				"wss://solana.example.com",
			},
		},
	})
	if err != nil {
		t.Fatalf("SetupSolana: %v", err)
	}

	if Solana == nil || Solana.RawClient() == nil {
		t.Fatal("expected configured Solana platform")
	}
	if _, ok := Solana.HTTP().(*solanarpc.ThinClient); !ok {
		t.Fatalf("expected raw Solana HTTP thin client, got %T", Solana.HTTP())
	}
	if _, ok := Solana.WS().(*solanarpc.ThinClient); !ok {
		t.Fatalf("expected raw Solana WS thin client, got %T", Solana.WS())
	}
}

func TestSetupSolana_ConfiguresProxies(t *testing.T) {
	previous := Solana
	defer func() { Solana = previous }()

	err := SetupSolana(&SolanaPlatformConfig{
		ClientConfig: solanarpc.ClientConfig{
			Resources: []string{
				"https://solana.example.com",
				"wss://solana.example.com",
			},
		},
		RPCProxyEnabled:  true,
		RPCInflightCache: true,
	})
	if err != nil {
		t.Fatalf("SetupSolana: %v", err)
	}
	http, ok := Solana.HTTP().(*solanarpc.ProxyClient)
	if !ok {
		t.Fatalf("expected Solana HTTP proxy client, got %T", Solana.HTTP())
	}
	if !http.InflightCache {
		t.Fatal("expected Solana HTTP proxy inflight cache enabled")
	}
	if _, ok := Solana.WS().(*solanarpc.ProxyClient); !ok {
		t.Fatalf("expected Solana WS proxy client, got %T", Solana.WS())
	}
}

func TestSetupPlatform_NilConfigReturnsError(t *testing.T) {
	if err := SetupEVM(nil); err == nil {
		t.Fatal("expected SetupEVM nil config error")
	}
	if err := SetupSolana(nil); err == nil {
		t.Fatal("expected SetupSolana nil config error")
	}
}

func TestSetupPlatform_FailureDoesNotOverwriteGlobals(t *testing.T) {
	previousEVM := EVM
	previousSolana := Solana
	defer func() {
		EVM = previousEVM
		Solana = previousSolana
	}()

	evmSentinel := &EVMPlatform{}
	solanaSentinel := &SolanaPlatform{}
	EVM = evmSentinel
	Solana = solanaSentinel

	if err := SetupEVM(&EVMPlatformConfig{}); err == nil {
		t.Fatal("expected SetupEVM error for empty resources")
	}
	if EVM != evmSentinel {
		t.Fatal("SetupEVM failure overwrote global EVM")
	}

	if err := SetupSolana(&SolanaPlatformConfig{}); err == nil {
		t.Fatal("expected SetupSolana error for empty resources")
	}
	if Solana != solanaSentinel {
		t.Fatal("SetupSolana failure overwrote global Solana")
	}
}
