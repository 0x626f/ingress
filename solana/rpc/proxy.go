package rpc

import (
	"context"
	"fmt"

	"github.com/0x626f/ingress/solana/types"
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

func proxyParams(values ...any) []any {
	return append([]any(nil), values...)
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

func (client *ProxyClient) raw(ctx context.Context, method string, query any, fn func(context.Context) (types.RawResult, error)) (types.RawResult, error) {
	result, err := client.call(ctx, method, query, func(ctx context.Context) (any, error) {
		return fn(ctx)
	})
	if result == nil {
		return nil, err
	}
	return result.(types.RawResult), err
}

func (client *ProxyClient) RawCall(ctx context.Context, method string, params ...any) (types.RawResult, error) {
	return client.raw(ctx, "RawCall", proxyParams(method, params), func(ctx context.Context) (types.RawResult, error) {
		return client.client.RawCall(ctx, method, params...)
	})
}

func (client *ProxyClient) RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	result, err := client.call(ctx, "RawSubscribe", proxyParams(subscribeMethod, unsubscribeMethod, params), func(ctx context.Context) (any, error) {
		return client.client.RawSubscribe(ctx, subscribeMethod, unsubscribeMethod, params...)
	})
	if result == nil {
		return nil, err
	}
	return result.(*Subscription), err
}

func (client *ProxyClient) GetAccountInfo(ctx context.Context, pubkey string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetAccountInfo", proxyParams(pubkey, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetAccountInfo(ctx, pubkey, config...)
	})
}

func (client *ProxyClient) GetBalance(ctx context.Context, pubkey string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetBalance", proxyParams(pubkey, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBalance(ctx, pubkey, config...)
	})
}

func (client *ProxyClient) GetLargestAccounts(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetLargestAccounts", config, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetLargestAccounts(ctx, config...)
	})
}

func (client *ProxyClient) GetMinimumBalanceForRentExemption(ctx context.Context, dataSize uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetMinimumBalanceForRentExemption", proxyParams(dataSize, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetMinimumBalanceForRentExemption(ctx, dataSize, commitment...)
	})
}

func (client *ProxyClient) GetMultipleAccounts(ctx context.Context, pubkeys []string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetMultipleAccounts", proxyParams(pubkeys, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetMultipleAccounts(ctx, pubkeys, config...)
	})
}

func (client *ProxyClient) GetProgramAccounts(ctx context.Context, programID string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetProgramAccounts", proxyParams(programID, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetProgramAccounts(ctx, programID, config...)
	})
}

func (client *ProxyClient) GetTokenAccountBalance(ctx context.Context, pubkey string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountBalance", proxyParams(pubkey, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTokenAccountBalance(ctx, pubkey, commitment...)
	})
}

func (client *ProxyClient) GetTokenAccountsByDelegate(ctx context.Context, delegate string, filter any, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountsByDelegate", proxyParams(delegate, filter, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTokenAccountsByDelegate(ctx, delegate, filter, config...)
	})
}

func (client *ProxyClient) GetTokenAccountsByOwner(ctx context.Context, owner string, filter any, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountsByOwner", proxyParams(owner, filter, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTokenAccountsByOwner(ctx, owner, filter, config...)
	})
}

func (client *ProxyClient) GetTokenLargestAccounts(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetTokenLargestAccounts", proxyParams(mint, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTokenLargestAccounts(ctx, mint, commitment...)
	})
}

func (client *ProxyClient) GetTokenSupply(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetTokenSupply", proxyParams(mint, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTokenSupply(ctx, mint, commitment...)
	})
}

func (client *ProxyClient) GetFeeForMessage(ctx context.Context, message string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetFeeForMessage", proxyParams(message, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetFeeForMessage(ctx, message, commitment...)
	})
}

func (client *ProxyClient) GetLatestBlockhash(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetLatestBlockhash", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetLatestBlockhash(ctx, commitment...)
	})
}

func (client *ProxyClient) GetRecentPrioritizationFees(ctx context.Context, accounts ...string) (types.RawResult, error) {
	return client.raw(ctx, "GetRecentPrioritizationFees", accounts, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetRecentPrioritizationFees(ctx, accounts...)
	})
}

func (client *ProxyClient) GetSignaturesForAddress(ctx context.Context, address string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetSignaturesForAddress", proxyParams(address, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetSignaturesForAddress(ctx, address, config...)
	})
}

func (client *ProxyClient) GetSignatureStatuses(ctx context.Context, signatures []string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetSignatureStatuses", proxyParams(signatures, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetSignatureStatuses(ctx, signatures, config...)
	})
}

func (client *ProxyClient) GetTransactionCount(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetTransactionCount", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTransactionCount(ctx, commitment...)
	})
}

func (client *ProxyClient) IsBlockhashValid(ctx context.Context, blockhash string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "IsBlockhashValid", proxyParams(blockhash, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.IsBlockhashValid(ctx, blockhash, commitment...)
	})
}

func (client *ProxyClient) RequestAirdrop(ctx context.Context, pubkey string, lamports uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "RequestAirdrop", proxyParams(pubkey, lamports, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.RequestAirdrop(ctx, pubkey, lamports, commitment...)
	})
}

func (client *ProxyClient) SendTransaction(ctx context.Context, serialized []byte, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "SendTransaction", proxyParams(serialized, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.SendTransaction(ctx, serialized, config...)
	})
}

func (client *ProxyClient) SendEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "SendEncodedTransaction", proxyParams(encoded, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.SendEncodedTransaction(ctx, encoded, config...)
	})
}

func (client *ProxyClient) SimulateEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "SimulateEncodedTransaction", proxyParams(encoded, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.SimulateEncodedTransaction(ctx, encoded, config...)
	})
}

func (client *ProxyClient) GetBlockCommitment(ctx context.Context, slot types.Slot) (types.RawResult, error) {
	return client.raw(ctx, "GetBlockCommitment", slot, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlockCommitment(ctx, slot)
	})
}

func (client *ProxyClient) GetBlockHeight(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetBlockHeight", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlockHeight(ctx, commitment...)
	})
}

func (client *ProxyClient) GetBlockProduction(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetBlockProduction", config, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlockProduction(ctx, config...)
	})
}

func (client *ProxyClient) GetBlocks(ctx context.Context, startSlot types.Slot, endSlot *types.Slot, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetBlocks", proxyParams(startSlot, endSlot, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlocks(ctx, startSlot, endSlot, commitment...)
	})
}

func (client *ProxyClient) GetBlocksWithLimit(ctx context.Context, startSlot types.Slot, limit uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetBlocksWithLimit", proxyParams(startSlot, limit, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlocksWithLimit(ctx, startSlot, limit, commitment...)
	})
}

func (client *ProxyClient) GetBlockTime(ctx context.Context, slot types.Slot) (types.RawResult, error) {
	return client.raw(ctx, "GetBlockTime", slot, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlockTime(ctx, slot)
	})
}

func (client *ProxyClient) GetFirstAvailableBlock(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetFirstAvailableBlock", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetFirstAvailableBlock(ctx)
	})
}

func (client *ProxyClient) GetRecentPerformanceSamples(ctx context.Context, limit ...uint64) (types.RawResult, error) {
	return client.raw(ctx, "GetRecentPerformanceSamples", limit, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetRecentPerformanceSamples(ctx, limit...)
	})
}

func (client *ProxyClient) MinimumLedgerSlot(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "MinimumLedgerSlot", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.MinimumLedgerSlot(ctx)
	})
}

func (client *ProxyClient) GetEpochSchedule(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetEpochSchedule", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetEpochSchedule(ctx)
	})
}

func (client *ProxyClient) GetGenesisHash(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetGenesisHash", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetGenesisHash(ctx)
	})
}

func (client *ProxyClient) GetHealth(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetHealth", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetHealth(ctx)
	})
}

func (client *ProxyClient) GetHighestSnapshotSlot(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetHighestSnapshotSlot", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetHighestSnapshotSlot(ctx)
	})
}

func (client *ProxyClient) GetIdentity(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetIdentity", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetIdentity(ctx)
	})
}

func (client *ProxyClient) GetLeaderSchedule(ctx context.Context, slot *types.Slot, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetLeaderSchedule", proxyParams(slot, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetLeaderSchedule(ctx, slot, config...)
	})
}

func (client *ProxyClient) GetMaxRetransmitSlot(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetMaxRetransmitSlot", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetMaxRetransmitSlot(ctx)
	})
}

func (client *ProxyClient) GetMaxShredInsertSlot(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetMaxShredInsertSlot", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetMaxShredInsertSlot(ctx)
	})
}

func (client *ProxyClient) GetSlotLeader(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetSlotLeader", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetSlotLeader(ctx, commitment...)
	})
}

func (client *ProxyClient) GetVersion(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetVersion", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetVersion(ctx)
	})
}

func (client *ProxyClient) GetInflationGovernor(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetInflationGovernor", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetInflationGovernor(ctx, commitment...)
	})
}

func (client *ProxyClient) GetInflationRate(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetInflationRate", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetInflationRate(ctx)
	})
}

func (client *ProxyClient) GetInflationReward(ctx context.Context, addresses []string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetInflationReward", proxyParams(addresses, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetInflationReward(ctx, addresses, config...)
	})
}

func (client *ProxyClient) GetStakeMinimumDelegation(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetStakeMinimumDelegation", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetStakeMinimumDelegation(ctx, commitment...)
	})
}

func (client *ProxyClient) GetSupply(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetSupply", config, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetSupply(ctx, config...)
	})
}

func (client *ProxyClient) GetEpochInfo(ctx context.Context, commitment types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetEpochInfo", commitment, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetEpochInfo(ctx, commitment)
	})
}

func (client *ProxyClient) GetSlot(ctx context.Context, commitment types.Commitment) (types.Slot, error) {
	result, err := client.call(ctx, "GetSlot", commitment, func(ctx context.Context) (any, error) {
		return client.client.GetSlot(ctx, commitment)
	})
	if result == nil {
		return 0, err
	}
	return result.(types.Slot), err
}

func (client *ProxyClient) GetSlotLeaders(ctx context.Context, from types.Slot, limit uint16) (types.SlotLeaders, error) {
	result, err := client.call(ctx, "GetSlotLeaders", proxyParams(from, limit), func(ctx context.Context) (any, error) {
		return client.client.GetSlotLeaders(ctx, from, limit)
	})
	if result == nil {
		return nil, err
	}
	return result.(types.SlotLeaders), err
}

func (client *ProxyClient) GetClusterNodes(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetClusterNodes", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetClusterNodes(ctx)
	})
}

func (client *ProxyClient) GetVoteAccounts(ctx context.Context) (types.RawResult, error) {
	return client.raw(ctx, "GetVoteAccounts", nil, func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetVoteAccounts(ctx)
	})
}

func (client *ProxyClient) SimulateTransaction(ctx context.Context, serialized []byte, commitment types.Commitment) error {
	_, err := client.call(ctx, "SimulateTransaction", proxyParams(serialized, commitment), func(ctx context.Context) (any, error) {
		return nil, client.client.SimulateTransaction(ctx, serialized, commitment)
	})
	return err
}

func (client *ProxyClient) GetTransaction(ctx context.Context, signature string, config ...any) (types.RawResult, error) {
	return client.raw(ctx, "GetTransaction", proxyParams(signature, config), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetTransaction(ctx, signature, config...)
	})
}

func (client *ProxyClient) GetBlock(ctx context.Context, slot types.Slot, commitment types.Commitment) (types.RawResult, error) {
	return client.raw(ctx, "GetBlock", proxyParams(slot, commitment), func(ctx context.Context) (types.RawResult, error) {
		return client.client.GetBlock(ctx, slot, commitment)
	})
}

func (client *ProxyClient) GetConfirmedSlots(ctx context.Context, from, to types.Slot, commitment types.Commitment) (types.ConfirmedSlots, error) {
	result, err := client.call(ctx, "GetConfirmedSlots", proxyParams(from, to, commitment), func(ctx context.Context) (any, error) {
		return client.client.GetConfirmedSlots(ctx, from, to, commitment)
	})
	if result == nil {
		return nil, err
	}
	return result.(types.ConfirmedSlots), err
}

func (client *ProxyClient) AccountSubscribe(ctx context.Context, pubkey string, config ...any) (*Subscription, error) {
	return client.subscription(ctx, "AccountSubscribe", proxyParams(pubkey, config), func(ctx context.Context) (*Subscription, error) {
		return client.client.AccountSubscribe(ctx, pubkey, config...)
	})
}

func (client *ProxyClient) BlockSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error) {
	return client.subscription(ctx, "BlockSubscribe", proxyParams(filter, config), func(ctx context.Context) (*Subscription, error) {
		return client.client.BlockSubscribe(ctx, filter, config...)
	})
}

func (client *ProxyClient) LogsSubscribe(ctx context.Context, filter any, config ...any) (*Subscription, error) {
	return client.subscription(ctx, "LogsSubscribe", proxyParams(filter, config), func(ctx context.Context) (*Subscription, error) {
		return client.client.LogsSubscribe(ctx, filter, config...)
	})
}

func (client *ProxyClient) ProgramSubscribe(ctx context.Context, programID string, config ...any) (*Subscription, error) {
	return client.subscription(ctx, "ProgramSubscribe", proxyParams(programID, config), func(ctx context.Context) (*Subscription, error) {
		return client.client.ProgramSubscribe(ctx, programID, config...)
	})
}

func (client *ProxyClient) RootSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "RootSubscribe", nil, func(ctx context.Context) (*Subscription, error) {
		return client.client.RootSubscribe(ctx)
	})
}

func (client *ProxyClient) SignatureSubscribe(ctx context.Context, signature string, config ...any) (*Subscription, error) {
	return client.subscription(ctx, "SignatureSubscribe", proxyParams(signature, config), func(ctx context.Context) (*Subscription, error) {
		return client.client.SignatureSubscribe(ctx, signature, config...)
	})
}

func (client *ProxyClient) SlotSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "SlotSubscribe", nil, func(ctx context.Context) (*Subscription, error) {
		return client.client.SlotSubscribe(ctx)
	})
}

func (client *ProxyClient) SlotsUpdatesSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "SlotsUpdatesSubscribe", nil, func(ctx context.Context) (*Subscription, error) {
		return client.client.SlotsUpdatesSubscribe(ctx)
	})
}

func (client *ProxyClient) VoteSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "VoteSubscribe", nil, func(ctx context.Context) (*Subscription, error) {
		return client.client.VoteSubscribe(ctx)
	})
}

func (client *ProxyClient) subscription(ctx context.Context, method string, query any, fn func(context.Context) (*Subscription, error)) (*Subscription, error) {
	result, err := client.call(ctx, method, query, func(ctx context.Context) (any, error) {
		return fn(ctx)
	})
	if result == nil {
		return nil, err
	}
	return result.(*Subscription), err
}

func (client *ProxyClient) SubscribeSlot(ctx context.Context) (chan *Event[types.Slot], error) {
	result, err := client.call(ctx, "SubscribeSlot", nil, func(ctx context.Context) (any, error) {
		return client.client.SubscribeSlot(ctx)
	})
	if result == nil {
		return nil, err
	}
	return result.(chan *Event[types.Slot]), err
}
