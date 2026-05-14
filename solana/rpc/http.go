package rpc

import (
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

func (client *ThinClient) RawCall(method string, params ...any) (types.RawResult, error) {
	response, err := client.call(method, params)
	if err != nil {
		return nil, err
	}
	return types.RawResult(response.Result), nil
}

func (client *ThinClient) GetAccountInfo(pubkey string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetAccountInfo, optionalParams([]any{pubkey}, firstOptional(config))...)
}

func (client *ThinClient) GetBalance(pubkey string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBalance, optionalParams([]any{pubkey}, firstOptional(config))...)
}

func (client *ThinClient) GetLargestAccounts(config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetLargestAccounts, optionalParams(nil, firstOptional(config))...)
}

func (client *ThinClient) GetMinimumBalanceForRentExemption(dataSize uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetMinimumBalanceForRentExemption, optionalParams([]any{dataSize}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetMultipleAccounts(pubkeys []string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetMultipleAccounts, optionalParams([]any{pubkeys}, firstOptional(config))...)
}

func (client *ThinClient) GetProgramAccounts(programID string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetProgramAccounts, optionalParams([]any{programID}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenAccountBalance(pubkey string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTokenAccountBalance, optionalParams([]any{pubkey}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetTokenAccountsByDelegate(delegate string, filter any, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTokenAccountsByDelegate, optionalParams([]any{delegate, filter}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenAccountsByOwner(owner string, filter any, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTokenAccountsByOwner, optionalParams([]any{owner, filter}, firstOptional(config))...)
}

func (client *ThinClient) GetTokenLargestAccounts(mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTokenLargestAccounts, optionalParams([]any{mint}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetTokenSupply(mint string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTokenSupply, optionalParams([]any{mint}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetFeeForMessage(message string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetFeeForMessage, optionalParams([]any{message}, commitmentConfig(commitment...))...)
}

// GetEpochInfo retrieves information about the current epoch with the specified commitment level.
// Returns the raw JSON result containing epoch details such as absolute slot, epoch number,
// slot index, and slots in epoch.
func (client *ThinClient) GetEpochInfo(commitment types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetEpochInfo, commitmentConfig(commitment))
}

func (client *ThinClient) GetLatestBlockhash(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetLatestBlockhash, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetRecentPrioritizationFees(accounts ...string) (types.RawResult, error) {
	if len(accounts) == 0 {
		return client.RawCall(RPCMethodGetRecentPrioritizationFees)
	}
	return client.RawCall(RPCMethodGetRecentPrioritizationFees, accounts)
}

func (client *ThinClient) GetSignaturesForAddress(address string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetSignaturesForAddress, optionalParams([]any{address}, firstOptional(config))...)
}

func (client *ThinClient) GetSignatureStatuses(signatures []string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetSignatureStatuses, optionalParams([]any{signatures}, firstOptional(config))...)
}

// GetTransaction retrieves information about a confirmed transaction by its signature.
// The signature parameter should be a base58-encoded transaction signature.
// Returns raw JSON containing transaction details including metadata and account keys.
func (client *ThinClient) GetTransaction(signature string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTransaction, optionalParams([]any{signature}, firstOptional(config))...)
}

func (client *ThinClient) GetTransactionCount(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetTransactionCount, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) IsBlockhashValid(blockhash string, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodIsBlockhashValid, optionalParams([]any{blockhash}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) RequestAirdrop(pubkey string, lamports uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodRequestAirdrop, optionalParams([]any{pubkey, lamports}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) SendTransaction(serialized []byte, config ...any) (types.RawResult, error) {
	encoded := base64.StdEncoding.EncodeToString(serialized)
	return client.SendEncodedTransaction(encoded, config...)
}

func (client *ThinClient) SendEncodedTransaction(encoded string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodSendTransaction, optionalParams([]any{encoded}, firstOptional(config))...)
}

// SimulateTransaction simulates sending a transaction to the cluster with the specified commitment level.
// The serialized parameter should contain the base64-encoded transaction data.
// Returns an error if the simulation fails or if the transaction would fail on-chain.
func (client *ThinClient) SimulateTransaction(serialized []byte, commitment types.Commitment) error {
	encoded := base64.StdEncoding.EncodeToString(serialized)

	response, err := client.call(RPCMethodSimulateTransaction, []any{
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

func (client *ThinClient) SimulateEncodedTransaction(encoded string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodSimulateTransaction, optionalParams([]any{encoded}, firstOptional(config))...)
}

// GetBlock retrieves detailed information about a confirmed block at the specified slot.
// The commitment parameter determines the level of finality required for the block data.
// Returns raw JSON containing block details including transactions, rewards, and block metadata.
func (client *ThinClient) GetBlock(slot types.Slot, commitment types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlock, slot, map[string]string{"commitment": commitment})
}

func (client *ThinClient) GetBlockCommitment(slot types.Slot) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlockCommitment, slot)
}

func (client *ThinClient) GetBlockHeight(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlockHeight, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlockProduction(config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlockProduction, optionalParams(nil, firstOptional(config))...)
}

func (client *ThinClient) GetBlocks(startSlot types.Slot, endSlot *types.Slot, commitment ...types.Commitment) (types.RawResult, error) {
	if endSlot == nil {
		return client.RawCall(RPCMethodGetBlocks, optionalParams([]any{startSlot}, commitmentConfig(commitment...))...)
	}
	return client.RawCall(RPCMethodGetBlocks, optionalParams([]any{startSlot, *endSlot}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlocksWithLimit(startSlot types.Slot, limit uint64, commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlocksWithLimit, optionalParams([]any{startSlot, limit}, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetBlockTime(slot types.Slot) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetBlockTime, slot)
}

// GetConfirmedSlots retrieves a list of confirmed slots between the specified range.
// The 'from' parameter specifies the starting slot (inclusive) and 'to' specifies the ending slot (inclusive).
// The commitment parameter determines the level of finality required for the slots.
// Returns a slice of slot numbers that have been confirmed in the specified range.
// Note: The range is limited by the RPC node's slot retention policy.
func (client *ThinClient) GetConfirmedSlots(from, to types.Slot, commitment types.Commitment) (types.ConfirmedSlots, error) {
	response, err := client.call(RPCMethodGetBlocks, []any{
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

func (client *ThinClient) GetFirstAvailableBlock() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetFirstAvailableBlock)
}

func (client *ThinClient) GetRecentPerformanceSamples(limit ...uint64) (types.RawResult, error) {
	if len(limit) == 0 {
		return client.RawCall(RPCMethodGetRecentPerformanceSamples)
	}
	return client.RawCall(RPCMethodGetRecentPerformanceSamples, limit[0])
}

func (client *ThinClient) MinimumLedgerSlot() (types.RawResult, error) {
	return client.RawCall(RPCMethodMinimumLedgerSlot)
}

func (client *ThinClient) GetEpochSchedule() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetEpochSchedule)
}

func (client *ThinClient) GetGenesisHash() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetGenesisHash)
}

func (client *ThinClient) GetHealth() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetHealth)
}

func (client *ThinClient) GetHighestSnapshotSlot() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetHighestSnapshotSlot)
}

func (client *ThinClient) GetIdentity() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetIdentity)
}

func (client *ThinClient) GetLeaderSchedule(slot *types.Slot, config ...any) (types.RawResult, error) {
	if slot == nil {
		return client.RawCall(RPCMethodGetLeaderSchedule, optionalParams(nil, firstOptional(config))...)
	}
	return client.RawCall(RPCMethodGetLeaderSchedule, optionalParams([]any{*slot}, firstOptional(config))...)
}

func (client *ThinClient) GetMaxRetransmitSlot() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetMaxRetransmitSlot)
}

func (client *ThinClient) GetMaxShredInsertSlot() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetMaxShredInsertSlot)
}

// GetSlot retrieves the current slot number with the specified commitment level.
// Returns the slot as a uint64 value.
func (client *ThinClient) GetSlot(commitment types.Commitment) (types.Slot, error) {
	response, err := client.call(RPCMethodGetSlot, []any{
		map[string]string{"commitment": commitment},
	})

	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseUint(string(response.Result), 10, 64)
	return value, err
}

func (client *ThinClient) GetSlotLeader(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetSlotLeader, optionalParams(nil, commitmentConfig(commitment...))...)
}

// GetSlotLeaders retrieves the slot leaders (validator public keys) starting from the specified slot.
// The limit parameter specifies the maximum number of slot leaders to return (up to 5000).
// Returns a slice of validator public keys as base58-encoded strings.
func (client *ThinClient) GetSlotLeaders(from types.Slot, limit uint16) (types.SlotLeaders, error) {
	response, err := client.call(RPCMethodGetSlotLeaders, []any{from, limit})

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

func (client *ThinClient) GetVersion() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetVersion)
}

// GetClusterNodes retrieves information about all the nodes participating in the cluster.
// Returns raw JSON containing node details including public keys and network addresses.
func (client *ThinClient) GetClusterNodes() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetClusterNodes)
}

// GetVoteAccounts retrieves information about all vote accounts in the cluster.
// Returns raw JSON containing details about current and delinquent vote accounts.
func (client *ThinClient) GetVoteAccounts() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetVoteAccounts)
}

func (client *ThinClient) GetInflationGovernor(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetInflationGovernor, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetInflationRate() (types.RawResult, error) {
	return client.RawCall(RPCMethodGetInflationRate)
}

func (client *ThinClient) GetInflationReward(addresses []string, config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetInflationReward, optionalParams([]any{addresses}, firstOptional(config))...)
}

func (client *ThinClient) GetStakeMinimumDelegation(commitment ...types.Commitment) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetStakeMinimumDelegation, optionalParams(nil, commitmentConfig(commitment...))...)
}

func (client *ThinClient) GetSupply(config ...any) (types.RawResult, error) {
	return client.RawCall(RPCMethodGetSupply, optionalParams(nil, firstOptional(config))...)
}

func commitmentConfig(commitment ...types.Commitment) any {
	if len(commitment) == 0 || commitment[0] == "" {
		return nil
	}
	return map[string]string{"commitment": commitment[0]}
}
