package rpc

import (
	"context"
	"fmt"

	"github.com/0x626f/ingress/solana/model"
	"github.com/bytedance/sonic"
	"golang.org/x/sync/singleflight"
)

var _ CoreClient = (*ProxyClient)(nil)

type ProxyPreprocessHook func(ctx context.Context, method string, query any) error

type ProxyPostProcessHook func(ctx context.Context, method string, query any, result any, err error) error

type ProxyContextFactory func() context.Context

type ProxyClientOptions struct {
	Client          CoreClient
	InflightCache   bool
	ContextFactory  ProxyContextFactory
	PreprocessHook  ProxyPreprocessHook
	PostProcessHook ProxyPostProcessHook
}

type ProxyClient struct {
	client CoreClient

	InflightCache   bool
	ContextFactory  ProxyContextFactory
	PreprocessHook  ProxyPreprocessHook
	PostProcessHook ProxyPostProcessHook

	inflight singleflight.Group
}

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
		run = func() (any, error) { return fn(ctx) }
	}

	result, err := run()
	if client.PostProcessHook != nil {
		if hookErr := client.PostProcessHook(ctx, method, query, result, err); hookErr != nil && err == nil {
			err = hookErr
		}
	}
	return result, err
}

func (client *ProxyClient) raw(ctx context.Context, method string, query any, fn func(context.Context) (model.RawResult, error)) (model.RawResult, error) {
	result, err := client.call(ctx, method, query, func(ctx context.Context) (any, error) { return fn(ctx) })
	if result == nil {
		return nil, err
	}
	return result.(model.RawResult), err
}

func (client *ProxyClient) subscription(ctx context.Context, method string, query any, fn func(context.Context) (*Subscription, error)) (*Subscription, error) {
	result, err := client.call(ctx, method, query, func(ctx context.Context) (any, error) { return fn(ctx) })
	if result == nil {
		return nil, err
	}
	return result.(*Subscription), err
}

func (client *ProxyClient) RawCall(ctx context.Context, method string, params ...any) (model.RawResult, error) {
	return client.raw(ctx, "RawCall", proxyParams(method, params), func(ctx context.Context) (model.RawResult, error) {
		return client.client.RawCall(ctx, method, params...)
	})
}

func (client *ProxyClient) RawSubscribe(ctx context.Context, subscribeMethod, unsubscribeMethod string, params ...any) (*Subscription, error) {
	return client.subscription(ctx, "RawSubscribe", proxyParams(subscribeMethod, unsubscribeMethod, params), func(ctx context.Context) (*Subscription, error) {
		return client.client.RawSubscribe(ctx, subscribeMethod, unsubscribeMethod, params...)
	})
}

func (client *ProxyClient) GetAccountInfo(ctx context.Context, query GetAccountInfoQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetAccountInfo", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetAccountInfo(ctx, query) })
}
func (client *ProxyClient) GetBalance(ctx context.Context, query GetBalanceQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBalance", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetBalance(ctx, query) })
}
func (client *ProxyClient) GetLargestAccounts(ctx context.Context, query GetLargestAccountsQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetLargestAccounts", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetLargestAccounts(ctx, query)
	})
}
func (client *ProxyClient) GetMinimumBalanceForRentExemption(ctx context.Context, query GetMinimumBalanceForRentExemptionQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetMinimumBalanceForRentExemption", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetMinimumBalanceForRentExemption(ctx, query)
	})
}
func (client *ProxyClient) GetMultipleAccounts(ctx context.Context, query GetMultipleAccountsQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetMultipleAccounts", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetMultipleAccounts(ctx, query)
	})
}
func (client *ProxyClient) GetProgramAccounts(ctx context.Context, query GetProgramAccountsQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetProgramAccounts", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetProgramAccounts(ctx, query)
	})
}
func (client *ProxyClient) GetTokenAccountBalance(ctx context.Context, query GetTokenAccountBalanceQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountBalance", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetTokenAccountBalance(ctx, query)
	})
}
func (client *ProxyClient) GetTokenAccountsByDelegate(ctx context.Context, query GetTokenAccountsByDelegateQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountsByDelegate", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetTokenAccountsByDelegate(ctx, query)
	})
}
func (client *ProxyClient) GetTokenAccountsByOwner(ctx context.Context, query GetTokenAccountsByOwnerQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTokenAccountsByOwner", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetTokenAccountsByOwner(ctx, query)
	})
}
func (client *ProxyClient) GetTokenLargestAccounts(ctx context.Context, query GetTokenLargestAccountsQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTokenLargestAccounts", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetTokenLargestAccounts(ctx, query)
	})
}
func (client *ProxyClient) GetTokenSupply(ctx context.Context, query GetTokenSupplyQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTokenSupply", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetTokenSupply(ctx, query) })
}
func (client *ProxyClient) GetFeeForMessage(ctx context.Context, query GetFeeForMessageQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetFeeForMessage", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetFeeForMessage(ctx, query) })
}
func (client *ProxyClient) GetLatestBlockhash(ctx context.Context, query GetLatestBlockhashQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetLatestBlockhash", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetLatestBlockhash(ctx, query)
	})
}
func (client *ProxyClient) GetRecentPrioritizationFees(ctx context.Context, query GetRecentPrioritizationFeesQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetRecentPrioritizationFees", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetRecentPrioritizationFees(ctx, query)
	})
}
func (client *ProxyClient) GetSignaturesForAddress(ctx context.Context, query GetSignaturesForAddressQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetSignaturesForAddress", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetSignaturesForAddress(ctx, query)
	})
}
func (client *ProxyClient) GetSignatureStatuses(ctx context.Context, query GetSignatureStatusesQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetSignatureStatuses", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetSignatureStatuses(ctx, query)
	})
}
func (client *ProxyClient) GetTransactionCount(ctx context.Context, query GetTransactionCountQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTransactionCount", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetTransactionCount(ctx, query)
	})
}
func (client *ProxyClient) IsBlockhashValid(ctx context.Context, query IsBlockhashValidQuery) (model.RawResult, error) {
	return client.raw(ctx, "IsBlockhashValid", query, func(ctx context.Context) (model.RawResult, error) { return client.client.IsBlockhashValid(ctx, query) })
}
func (client *ProxyClient) RequestAirdrop(ctx context.Context, query RequestAirdropQuery) (model.RawResult, error) {
	return client.raw(ctx, "RequestAirdrop", query, func(ctx context.Context) (model.RawResult, error) { return client.client.RequestAirdrop(ctx, query) })
}
func (client *ProxyClient) SendTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error) {
	return client.raw(ctx, "SendTransaction", query, func(ctx context.Context) (model.RawResult, error) { return client.client.SendTransaction(ctx, query) })
}
func (client *ProxyClient) SendEncodedTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error) {
	return client.raw(ctx, "SendEncodedTransaction", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.SendEncodedTransaction(ctx, query)
	})
}
func (client *ProxyClient) SimulateEncodedTransaction(ctx context.Context, query SimulateTransactionQuery) (model.RawResult, error) {
	return client.raw(ctx, "SimulateEncodedTransaction", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.SimulateEncodedTransaction(ctx, query)
	})
}
func (client *ProxyClient) GetBlockCommitment(ctx context.Context, query SlotQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlockCommitment", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetBlockCommitment(ctx, query)
	})
}
func (client *ProxyClient) GetBlockHeight(ctx context.Context, query GetBlockHeightQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlockHeight", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetBlockHeight(ctx, query) })
}
func (client *ProxyClient) GetBlockProduction(ctx context.Context, query GetBlockProductionQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlockProduction", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetBlockProduction(ctx, query)
	})
}
func (client *ProxyClient) GetBlocks(ctx context.Context, query GetBlocksQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlocks", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetBlocks(ctx, query) })
}
func (client *ProxyClient) GetBlocksWithLimit(ctx context.Context, query GetBlocksWithLimitQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlocksWithLimit", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetBlocksWithLimit(ctx, query)
	})
}
func (client *ProxyClient) GetBlockTime(ctx context.Context, query SlotQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlockTime", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetBlockTime(ctx, query) })
}
func (client *ProxyClient) GetFirstAvailableBlock(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetFirstAvailableBlock", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetFirstAvailableBlock(ctx) })
}
func (client *ProxyClient) GetRecentPerformanceSamples(ctx context.Context, query GetRecentPerformanceSamplesQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetRecentPerformanceSamples", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetRecentPerformanceSamples(ctx, query)
	})
}
func (client *ProxyClient) MinimumLedgerSlot(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "MinimumLedgerSlot", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.MinimumLedgerSlot(ctx) })
}
func (client *ProxyClient) GetEpochSchedule(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetEpochSchedule", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetEpochSchedule(ctx) })
}
func (client *ProxyClient) GetGenesisHash(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetGenesisHash", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetGenesisHash(ctx) })
}
func (client *ProxyClient) GetHealth(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetHealth", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetHealth(ctx) })
}
func (client *ProxyClient) GetHighestSnapshotSlot(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetHighestSnapshotSlot", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetHighestSnapshotSlot(ctx) })
}
func (client *ProxyClient) GetIdentity(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetIdentity", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetIdentity(ctx) })
}
func (client *ProxyClient) GetLeaderSchedule(ctx context.Context, query GetLeaderScheduleQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetLeaderSchedule", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetLeaderSchedule(ctx, query) })
}
func (client *ProxyClient) GetMaxRetransmitSlot(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetMaxRetransmitSlot", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetMaxRetransmitSlot(ctx) })
}
func (client *ProxyClient) GetMaxShredInsertSlot(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetMaxShredInsertSlot", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetMaxShredInsertSlot(ctx) })
}
func (client *ProxyClient) GetSlotLeader(ctx context.Context, query GetSlotLeaderQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetSlotLeader", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetSlotLeader(ctx, query) })
}
func (client *ProxyClient) GetVersion(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetVersion", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetVersion(ctx) })
}
func (client *ProxyClient) GetInflationGovernor(ctx context.Context, query GetInflationGovernorQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetInflationGovernor", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetInflationGovernor(ctx, query)
	})
}
func (client *ProxyClient) GetInflationRate(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetInflationRate", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetInflationRate(ctx) })
}
func (client *ProxyClient) GetInflationReward(ctx context.Context, query GetInflationRewardQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetInflationReward", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetInflationReward(ctx, query)
	})
}
func (client *ProxyClient) GetStakeMinimumDelegation(ctx context.Context, query GetStakeMinimumDelegationQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetStakeMinimumDelegation", query, func(ctx context.Context) (model.RawResult, error) {
		return client.client.GetStakeMinimumDelegation(ctx, query)
	})
}
func (client *ProxyClient) GetSupply(ctx context.Context, query GetSupplyQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetSupply", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetSupply(ctx, query) })
}
func (client *ProxyClient) GetEpochInfo(ctx context.Context, query GetEpochInfoQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetEpochInfo", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetEpochInfo(ctx, query) })
}

func (client *ProxyClient) GetSlot(ctx context.Context, query GetSlotQuery) (model.Slot, error) {
	result, err := client.call(ctx, "GetSlot", query, func(ctx context.Context) (any, error) { return client.client.GetSlot(ctx, query) })
	if result == nil {
		return 0, err
	}
	return result.(model.Slot), err
}

func (client *ProxyClient) GetSlotLeaders(ctx context.Context, query GetSlotLeadersQuery) (model.SlotLeaders, error) {
	result, err := client.call(ctx, "GetSlotLeaders", query, func(ctx context.Context) (any, error) { return client.client.GetSlotLeaders(ctx, query) })
	if result == nil {
		return nil, err
	}
	return result.(model.SlotLeaders), err
}

func (client *ProxyClient) GetClusterNodes(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetClusterNodes", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetClusterNodes(ctx) })
}
func (client *ProxyClient) GetVoteAccounts(ctx context.Context) (model.RawResult, error) {
	return client.raw(ctx, "GetVoteAccounts", nil, func(ctx context.Context) (model.RawResult, error) { return client.client.GetVoteAccounts(ctx) })
}

func (client *ProxyClient) SimulateTransaction(ctx context.Context, query SimulateTransactionQuery) error {
	_, err := client.call(ctx, "SimulateTransaction", query, func(ctx context.Context) (any, error) { return nil, client.client.SimulateTransaction(ctx, query) })
	return err
}

func (client *ProxyClient) GetTransaction(ctx context.Context, query GetTransactionQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetTransaction", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetTransaction(ctx, query) })
}
func (client *ProxyClient) GetBlock(ctx context.Context, query GetBlockQuery) (model.RawResult, error) {
	return client.raw(ctx, "GetBlock", query, func(ctx context.Context) (model.RawResult, error) { return client.client.GetBlock(ctx, query) })
}

func (client *ProxyClient) GetConfirmedSlots(ctx context.Context, query GetConfirmedSlotsQuery) (model.ConfirmedSlots, error) {
	result, err := client.call(ctx, "GetConfirmedSlots", query, func(ctx context.Context) (any, error) { return client.client.GetConfirmedSlots(ctx, query) })
	if result == nil {
		return nil, err
	}
	return result.(model.ConfirmedSlots), err
}

func (client *ProxyClient) AccountSubscribe(ctx context.Context, query AccountSubscribeQuery) (*Subscription, error) {
	return client.subscription(ctx, "AccountSubscribe", query, func(ctx context.Context) (*Subscription, error) { return client.client.AccountSubscribe(ctx, query) })
}
func (client *ProxyClient) BlockSubscribe(ctx context.Context, query BlockSubscribeQuery) (*Subscription, error) {
	return client.subscription(ctx, "BlockSubscribe", query, func(ctx context.Context) (*Subscription, error) { return client.client.BlockSubscribe(ctx, query) })
}
func (client *ProxyClient) LogsSubscribe(ctx context.Context, query LogsSubscribeQuery) (*Subscription, error) {
	return client.subscription(ctx, "LogsSubscribe", query, func(ctx context.Context) (*Subscription, error) { return client.client.LogsSubscribe(ctx, query) })
}
func (client *ProxyClient) ProgramSubscribe(ctx context.Context, query ProgramSubscribeQuery) (*Subscription, error) {
	return client.subscription(ctx, "ProgramSubscribe", query, func(ctx context.Context) (*Subscription, error) { return client.client.ProgramSubscribe(ctx, query) })
}
func (client *ProxyClient) RootSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "RootSubscribe", nil, func(ctx context.Context) (*Subscription, error) { return client.client.RootSubscribe(ctx) })
}
func (client *ProxyClient) SignatureSubscribe(ctx context.Context, query SignatureSubscribeQuery) (*Subscription, error) {
	return client.subscription(ctx, "SignatureSubscribe", query, func(ctx context.Context) (*Subscription, error) { return client.client.SignatureSubscribe(ctx, query) })
}
func (client *ProxyClient) SlotSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "SlotSubscribe", nil, func(ctx context.Context) (*Subscription, error) { return client.client.SlotSubscribe(ctx) })
}
func (client *ProxyClient) SlotsUpdatesSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "SlotsUpdatesSubscribe", nil, func(ctx context.Context) (*Subscription, error) { return client.client.SlotsUpdatesSubscribe(ctx) })
}
func (client *ProxyClient) VoteSubscribe(ctx context.Context) (*Subscription, error) {
	return client.subscription(ctx, "VoteSubscribe", nil, func(ctx context.Context) (*Subscription, error) { return client.client.VoteSubscribe(ctx) })
}

func (client *ProxyClient) SubscribeSlot(ctx context.Context) (chan *Event[model.Slot], error) {
	result, err := client.call(ctx, "SubscribeSlot", nil, func(ctx context.Context) (any, error) { return client.client.SubscribeSlot(ctx) })
	if result == nil {
		return nil, err
	}
	return result.(chan *Event[model.Slot]), err
}
