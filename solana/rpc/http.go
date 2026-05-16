package rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/0x626f/ingress/solana/types"
)

func optionalParams(required []any, optional ...any) []any {
	params := append([]any{}, required...)
	for _, value := range optional {
		if value != nil {
			params = append(params, value)
		}
	}
	return params
}

func firstOptional(optional []any) any {
	if len(optional) == 0 {
		return nil
	}
	return optional[0]
}

func (client *ThinClient) RawCall(ctx context.Context, method string, params ...any) (types.RawResult, error) {
	response, err := client.call(ctx, method, params)
	if err != nil {
		return nil, err
	}
	return types.RawResult(response.Result), nil
}

func (client *ThinClient) GetAccountInfo(ctx context.Context, pubkey string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetAccountInfo, optionalParams([]any{pubkey}, firstOptional(config))...)
}

func (client *ThinClient) GetBalance(ctx context.Context, pubkey string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBalance, optionalParams([]any{pubkey}, firstOptional(config))...)
}

func (client *ThinClient) GetLargestAccounts(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetLargestAccounts, optionalParams(nil, firstOptional(config))...)
}

func (client *ThinClient) GetMinimumBalanceForRentExemption(ctx context.Context, dataSize uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetMinimumBalanceForRentExemption, optionalParams([]any{dataSize}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetMultipleAccounts(ctx context.Context, pubkeys []string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetMultipleAccounts, optionalParams([]any{pubkeys}, firstOptional(config))...)
}

func (client *ThinClient) GetProgramAccounts(ctx context.Context, programID string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetProgramAccounts, optionalParams([]any{programID}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenAccountBalance(ctx context.Context, pubkey string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTokenAccountBalance, optionalParams([]any{pubkey}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetTokenAccountsByDelegate(ctx context.Context, delegate string, filter any, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTokenAccountsByDelegate, optionalParams([]any{delegate, filter}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenAccountsByOwner(ctx context.Context, owner string, filter any, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTokenAccountsByOwner, optionalParams([]any{owner, filter}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenLargestAccounts(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTokenLargestAccounts, optionalParams([]any{mint}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetTokenSupply(ctx context.Context, mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTokenSupply, optionalParams([]any{mint}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetFeeForMessage(ctx context.Context, message string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetFeeForMessage, optionalParams([]any{message}, commitmentConfig(commitment...))...)
}

// GetEpochInfo retrieves information about the current epoch with the specified commitment level.
// Returns the raw JSON result containing epoch details such as absolute slot, epoch number,
// slot index, and slots in epoch.
func (client *ThinClient) GetEpochInfo(ctx context.Context, commitment types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetEpochInfo, commitmentConfig(commitment))
}

func (client *ThinClient) GetLatestBlockhash(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetLatestBlockhash, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetRecentPrioritizationFees(ctx context.Context, accounts ...string) (types.RawResult, error) {
	if len(accounts) == 0 {
		return client.RawCall(ctx, RPCMethodGetRecentPrioritizationFees)
	}
	return client.RawCall(ctx, RPCMethodGetRecentPrioritizationFees, accounts)
}

func (client *ThinClient) GetSignaturesForAddress(ctx context.Context, address string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetSignaturesForAddress, optionalParams([]any{address}, firstOptional(config))...)
}

func (client *ThinClient) GetSignatureStatuses(ctx context.Context, signatures []string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetSignatureStatuses, optionalParams([]any{signatures}, firstOptional(config))...)
}

// GetTransaction retrieves information about a confirmed transaction by its signature.
// The signature parameter should be a base58-encoded transaction signature.
// Returns raw JSON containing transaction details including metadata and account keys.
func (client *ThinClient) GetTransaction(ctx context.Context, signature string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTransaction, optionalParams([]any{signature}, firstOptional(config))...)
}

func (client *ThinClient) GetTransactionCount(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetTransactionCount, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) IsBlockhashValid(ctx context.Context, blockhash string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodIsBlockhashValid, optionalParams([]any{blockhash}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) RequestAirdrop(ctx context.Context, pubkey string, lamports uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodRequestAirdrop, optionalParams([]any{pubkey, lamports}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) SendTransaction(ctx context.Context, serialized []byte, config ...any) (types.RawResult, error) {
	encoded := base64.StdEncoding.EncodeToString(serialized)
	return client.SendEncodedTransaction(ctx, encoded, config...)
}

func (client *ThinClient) SendEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodSendTransaction, optionalParams([]any{encoded}, firstOptional(config))...)
}

// SimulateTransaction simulates sending a transaction to the cluster with the specified commitment level.
// The serialized parameter should contain the base64-encoded transaction data.
// Returns an error if the simulation fails or if the transaction would fail on-chain.
func (client *ThinClient) SimulateTransaction(ctx context.Context, serialized []byte, commitment types.Commitment) error {
	encoded := base64.StdEncoding.EncodeToString(serialized)

	response, err := client.call(ctx, RPCMethodSimulateTransaction, []any{
		encoded,
		map[string]string{"encoding": "base64", "commitment": commitment},
	})

	if err != nil {
		return err
	}

	result := &struct {
		Value *struct {
			Err *string `json:"err,omitempty"`
		} `json:"value"`
	}{}

	if err := json.Unmarshal(response.Result, result); err != nil {
		return err
	}

	if result.Value != nil && result.Value.Err != nil {
		return errors.New(*result.Value.Err)
	}

	return nil
}

func (client *ThinClient) SimulateEncodedTransaction(ctx context.Context, encoded string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodSimulateTransaction, optionalParams([]any{encoded}, firstOptional(config))...)
}

// GetBlock retrieves detailed information about a confirmed block at the specified slot.
// The commitment parameter determines the level of finality required for the block data.
// Returns raw JSON containing block details including transactions, rewards, and block metadata.
func (client *ThinClient) GetBlock(ctx context.Context, slot types.Slot, commitment types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlock, slot, map[string]string{"commitment": commitment})
}

func (client *ThinClient) GetBlockCommitment(ctx context.Context, slot types.Slot) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlockCommitment, slot)
}

func (client *ThinClient) GetBlockHeight(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlockHeight, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlockProduction(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlockProduction, optionalParams(nil, firstOptional(config))...)
}

func (client *ThinClient) GetBlocks(ctx context.Context, startSlot types.Slot, endSlot *types.Slot, commitment ...types.Commitment) (types.RawResult, error) {
	if endSlot == nil {
		return client.RawCall(ctx, RPCMethodGetBlocks, optionalParams([]any{startSlot}, commitmentConfig(commitment...))...)
	}
	return client.RawCall(ctx, RPCMethodGetBlocks, optionalParams([]any{startSlot, *endSlot}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlocksWithLimit(ctx context.Context, startSlot types.Slot, limit uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlocksWithLimit, optionalParams([]any{startSlot, limit}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlockTime(ctx context.Context, slot types.Slot) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetBlockTime, slot)
}

// GetConfirmedSlots retrieves a list of confirmed slots between the specified range.
// The 'from' parameter specifies the starting slot (inclusive) and 'to' specifies the ending slot (inclusive).
// The commitment parameter determines the level of finality required for the slots.
// Returns a slice of slot numbers that have been confirmed in the specified range.
// Note: The range is limited by the RPC node's slot retention policy.
func (client *ThinClient) GetConfirmedSlots(ctx context.Context, from, to types.Slot, commitment types.Commitment) (types.ConfirmedSlots, error) {
	response, err := client.call(ctx, RPCMethodGetBlocks, []any{
		from,
		to,
		map[string]string{"commitment": commitment},
	})

	if err != nil {
		return nil, err
	}

	var slots types.ConfirmedSlots
	if err := json.Unmarshal(response.Result, &slots); err != nil {
		return nil, err
	}

	return slots, nil
}

func (client *ThinClient) GetFirstAvailableBlock(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetFirstAvailableBlock)
}

func (client *ThinClient) GetRecentPerformanceSamples(ctx context.Context, limit ...uint64) (types.RawResult, error) {
	if len(limit) == 0 {
		return client.RawCall(ctx, RPCMethodGetRecentPerformanceSamples)
	}
	return client.RawCall(ctx, RPCMethodGetRecentPerformanceSamples, limit[0])
}

func (client *ThinClient) MinimumLedgerSlot(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodMinimumLedgerSlot)
}

func (client *ThinClient) GetEpochSchedule(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetEpochSchedule)
}

func (client *ThinClient) GetGenesisHash(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetGenesisHash)
}

func (client *ThinClient) GetHealth(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetHealth)
}

func (client *ThinClient) GetHighestSnapshotSlot(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetHighestSnapshotSlot)
}

func (client *ThinClient) GetIdentity(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetIdentity)
}

func (client *ThinClient) GetLeaderSchedule(ctx context.Context, slot *types.Slot, config ...any) (types.RawResult, error) {
	if slot == nil {
		return client.RawCall(ctx, RPCMethodGetLeaderSchedule, optionalParams(nil, firstOptional(config))...)
	}
	return client.RawCall(ctx, RPCMethodGetLeaderSchedule, optionalParams([]any{*slot}, firstOptional(config))...)
}

func (client *ThinClient) GetMaxRetransmitSlot(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetMaxRetransmitSlot)
}

func (client *ThinClient) GetMaxShredInsertSlot(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetMaxShredInsertSlot)
}

// GetSlot retrieves the current slot number with the specified commitment level.
// Returns the slot as a uint64 value.
func (client *ThinClient) GetSlot(ctx context.Context, commitment types.Commitment) (types.Slot, error) {
	response, err := client.call(ctx, RPCMethodGetSlot, []any{
		map[string]string{"commitment": commitment},
	})

	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseUint(string(response.Result), 10, 64)
	return value, err
}

func (client *ThinClient) GetSlotLeader(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetSlotLeader, optionalParams(nil, commitmentConfig(commitment...))...)
}

// GetSlotLeaders retrieves the slot leaders (validator public keys) starting from the specified slot.
// The limit parameter specifies the maximum number of slot leaders to return (up to 5000).
// Returns a slice of validator public keys as base58-encoded strings.
func (client *ThinClient) GetSlotLeaders(ctx context.Context, from types.Slot, limit uint16) (types.SlotLeaders, error) {
	response, err := client.call(ctx, RPCMethodGetSlotLeaders, []any{from, limit})

	if err != nil {
		return nil, err
	}

	if len(response.Result) == 0 {
		return nil, nil
	}

	raw := response.Result[1 : len(response.Result)-1]

	leaders := make([]string, 0, limit)
	buffer := make([]byte, 0, 44)
	write := false
	for _, b := range raw {
		if b == ',' {
			continue
		}
		if b == '"' {
			if write {
				leaders = append(leaders, string(buffer))
				buffer = buffer[:0]
			}
			write = !write
			continue
		}
		buffer = append(buffer, b)
	}
	return leaders, err
}

func (client *ThinClient) GetVersion(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetVersion)
}

// GetClusterNodes retrieves information about all the nodes participating in the cluster.
// Returns raw JSON containing node details including public keys and network addresses.
func (client *ThinClient) GetClusterNodes(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetClusterNodes)
}

// GetVoteAccounts retrieves information about all vote accounts in the cluster.
// Returns raw JSON containing details about current and delinquent vote accounts.
func (client *ThinClient) GetVoteAccounts(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetVoteAccounts)
}

func (client *ThinClient) GetInflationGovernor(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetInflationGovernor, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetInflationRate(ctx context.Context) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetInflationRate)
}

func (client *ThinClient) GetInflationReward(ctx context.Context, addresses []string, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetInflationReward, optionalParams([]any{addresses}, firstOptional(config))...)
}

func (client *ThinClient) GetStakeMinimumDelegation(ctx context.Context, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetStakeMinimumDelegation, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetSupply(ctx context.Context, config ...any) (types.RawResult, error) {
	return client.RawCall(ctx, RPCMethodGetSupply, optionalParams(nil, firstOptional(config))...)
}

func commitmentConfig(commitment ...types.Commitment) any {
	if len(commitment) == 0 || commitment[0] == "" {
		return nil
	}
	return map[string]string{"commitment": commitment[0]}
}
