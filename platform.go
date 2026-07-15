package ingress

import (
	"fmt"

	evmrpc "github.com/0x626f/ingress/evm/rpc"
	solanarpc "github.com/0x626f/ingress/solana/rpc"
	"github.com/0x626f/ingress/transport"
)

type EVMPlatform struct {
	rpcClient    *evmrpc.RawClient
	rpcHTTPProxy *evmrpc.ProxyClient
	rpcWSProxy   *evmrpc.ProxyClient
}
type SolanaPlatform struct {
	rpcClient    *solanarpc.RawClient
	rpcHTTPProxy *solanarpc.ProxyClient
	rpcWSProxy   *solanarpc.ProxyClient
}

type EVMPlatformConfig struct {
	evmrpc.ClientConfig
	RPCProxyEnabled    bool
	RPCInflightCache   bool
	RPCContextFactory  evmrpc.ProxyContextFactory
	RPCPreprocessHook  evmrpc.ProxyPreprocessHook
	RPCPostProcessHook evmrpc.ProxyPostProcessHook
}
type SolanaPlatformConfig struct {
	solanarpc.ClientConfig
	RPCProxyEnabled    bool
	RPCInflightCache   bool
	RPCContextFactory  solanarpc.ProxyContextFactory
	RPCPreprocessHook  solanarpc.ProxyPreprocessHook
	RPCPostProcessHook solanarpc.ProxyPostProcessHook
}

var EVM *EVMPlatform
var Solana *SolanaPlatform

func SetupEVM(config *EVMPlatformConfig) (err error) {
	if config == nil {
		return fmt.Errorf("nil EVM platform config")
	}

	platform := &EVMPlatform{}
	if platform.rpcClient, err = evmrpc.NewRawClient(&config.ClientConfig); err != nil {
		return
	}

	if config.RPCProxyEnabled {
		if platform.rpcClient.HasResourceByProtocol(transport.HTTP) {
			platform.rpcHTTPProxy = proxyEvmRPC(platform.rpcClient.HTTP(), config)
		}

		if platform.rpcClient.HasResourceByProtocol(transport.WS) {
			platform.rpcWSProxy = proxyEvmRPC(platform.rpcClient.WS(), config)
		}
	}

	EVM = platform
	return
}

func SetupSolana(config *SolanaPlatformConfig) (err error) {
	if config == nil {
		return fmt.Errorf("nil Solana platform config")
	}

	platform := &SolanaPlatform{}
	if platform.rpcClient, err = solanarpc.NewRawClient(&config.ClientConfig); err != nil {
		return
	}

	if config.RPCProxyEnabled {
		if platform.rpcClient.HasResourceByProtocol(transport.HTTP) {
			platform.rpcHTTPProxy = proxySolanaRPC(platform.rpcClient.HTTP(), config)
		}

		if platform.rpcClient.HasResourceByProtocol(transport.WS) {
			platform.rpcWSProxy = proxySolanaRPC(platform.rpcClient.WS(), config)
		}
	}

	Solana = platform
	return
}

func (platform *EVMPlatform) RawClient() *evmrpc.RawClient {
	if platform == nil {
		return nil
	}

	return platform.rpcClient
}

func (platform *EVMPlatform) HTTP() evmrpc.CoreClient {
	if platform == nil || platform.rpcClient == nil {
		return nil
	}

	if platform.rpcHTTPProxy != nil {
		return platform.rpcHTTPProxy
	}

	return platform.rpcClient.HTTP()
}

func (platform *EVMPlatform) WS() evmrpc.CoreClient {
	if platform == nil || platform.rpcClient == nil {
		return nil
	}

	if platform.rpcWSProxy != nil {
		return platform.rpcWSProxy
	}

	return platform.rpcClient.WS()
}

func (platform *SolanaPlatform) RawClient() *solanarpc.RawClient {
	if platform == nil {
		return nil
	}

	return platform.rpcClient
}

func (platform *SolanaPlatform) HTTP() solanarpc.CoreClient {
	if platform == nil || platform.rpcClient == nil {
		return nil
	}

	if platform.rpcHTTPProxy != nil {
		return platform.rpcHTTPProxy
	}

	return platform.rpcClient.HTTP()
}

func (platform *SolanaPlatform) WS() solanarpc.CoreClient {
	if platform == nil || platform.rpcClient == nil {
		return nil
	}

	if platform.rpcWSProxy != nil {
		return platform.rpcWSProxy
	}

	return platform.rpcClient.WS()
}

func proxyEvmRPC(client evmrpc.CoreClient, config *EVMPlatformConfig) *evmrpc.ProxyClient {
	return evmrpc.NewProxyClient(evmrpc.ProxyClientOptions{
		Client:          client,
		InflightCache:   config.RPCInflightCache,
		ContextFactory:  config.RPCContextFactory,
		PreprocessHook:  config.RPCPreprocessHook,
		PostProcessHook: config.RPCPostProcessHook,
	})
}

func proxySolanaRPC(client solanarpc.CoreClient, config *SolanaPlatformConfig) *solanarpc.ProxyClient {
	return solanarpc.NewProxyClient(solanarpc.ProxyClientOptions{
		Client:          client,
		InflightCache:   config.RPCInflightCache,
		ContextFactory:  config.RPCContextFactory,
		PreprocessHook:  config.RPCPreprocessHook,
		PostProcessHook: config.RPCPostProcessHook,
	})
}
