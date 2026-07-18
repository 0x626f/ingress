package rpc

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/model"
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

func optionalQueryConfig(config any) any {
	if config == nil {
		return nil
	}
	data, err := jsonrpc.Marshal(config)
	if err == nil && string(data) == "{}" {
		return nil
	}
	return config
}

func optionalCommitment(commitment model.Commitment) any {
	if commitment == "" {
		return nil
	}
	return map[string]string{"commitment": commitment}
}

func defaultEncoding(encoding Encoding) Encoding {
	if encoding == "" {
		return EncodingBase64
	}
	return encoding
}

func normalizeProgramAccountFilters(query []ProgramAccountsFilter) []ProgramAccountsFilter {
	if len(query) == 0 {
		return query
	}

	filters := make([]ProgramAccountsFilter, len(query))
	copy(filters, query)
	for index := range filters {
		if filters[index].Memcmp == nil || filters[index].Memcmp.Encoding != "" {
			continue
		}
		memcmp := *filters[index].Memcmp
		memcmp.Encoding = EncodingBase64
		filters[index].Memcmp = &memcmp
	}
	return filters
}

func normalizeGetAccountInfoQuery(query GetAccountInfoQuery) GetAccountInfoQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeGetProgramAccountsQuery(query GetProgramAccountsQuery) GetProgramAccountsQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	query.Filters = normalizeProgramAccountFilters(query.Filters)
	return query
}

func normalizeGetTokenAccountsByDelegateQuery(query GetTokenAccountsByDelegateQuery) GetTokenAccountsByDelegateQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeGetTokenAccountsByOwnerQuery(query GetTokenAccountsByOwnerQuery) GetTokenAccountsByOwnerQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeSendTransactionQuery(query SendTransactionQuery) SendTransactionQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeSimulateTransactionQuery(query SimulateTransactionQuery) SimulateTransactionQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeAccountSubscribeQuery(query AccountSubscribeQuery) AccountSubscribeQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	return query
}

func normalizeProgramSubscribeQuery(query ProgramSubscribeQuery) ProgramSubscribeQuery {
	query.Encoding = defaultEncoding(query.Encoding)
	query.Filters = normalizeProgramAccountFilters(query.Filters)
	return query
}

func methodCall(method string) func(*QueryParams) ([]byte, error) {
	return func(params *QueryParams) ([]byte, error) {
		return APISpec{}.BuildMethodCall(method, params)
	}
}

func rawCallParams(id uint, params ...any) *QueryParams {
	return QueryWithId(id, params...)
}

func getConfirmedSlotsQueryParams(query GetConfirmedSlotsQuery) *QueryParams {
	return rawCallParams(query.Id, optionalParams(
		[]any{query.From, query.To},
		optionalCommitment(query.Commitment),
	)...)
}

func getBlockProductionQueryParams(query GetBlockProductionQuery) (*QueryParams, error) {
	normalized, err := normalizeGetBlockProductionQuery(query)
	if err != nil {
		return nil, err
	}
	return rawCallParams(normalized.Id, optionalParams(nil, optionalQueryConfig(normalized))...), nil
}

func (client *ThinClient) rawMethod(ctx context.Context, method string, params *QueryParams) (model.RawResult, error) {
	return omitStream(client.handle(ctx, methodCall(method), params))
}

func (client *ThinClient) RawCall(ctx context.Context, method string, params ...any) (model.RawResult, error) {
	return client.rawMethod(ctx, method, Query(params...))
}

func (client *ThinClient) GetAccountInfo(ctx context.Context, query GetAccountInfoQuery) (model.RawResult, error) {
	if err := requireString("account pubkey", query.Pubkey); err != nil {
		return nil, err
	}
	query = normalizeGetAccountInfoQuery(query)
	return client.rawMethod(ctx, RPCMethodGetAccountInfo, rawCallParams(query.Id, optionalParams([]any{query.Pubkey}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetBalance(ctx context.Context, query GetBalanceQuery) (model.RawResult, error) {
	if err := requireString("account pubkey", query.Pubkey); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetBalance, rawCallParams(query.Id, optionalParams([]any{query.Pubkey}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetLargestAccounts(ctx context.Context, query GetLargestAccountsQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetLargestAccounts, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetMinimumBalanceForRentExemption(ctx context.Context, query GetMinimumBalanceForRentExemptionQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetMinimumBalanceForRentExemption, rawCallParams(query.Id, optionalParams([]any{query.DataSize}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetMultipleAccounts(ctx context.Context, query GetMultipleAccountsQuery) (model.RawResult, error) {
	if err := requireStrings("account pubkeys", query.Pubkeys); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetMultipleAccounts, rawCallParams(query.Id, optionalParams([]any{query.Pubkeys}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetProgramAccounts(ctx context.Context, query GetProgramAccountsQuery) (model.RawResult, error) {
	if err := requireString("program id", query.ProgramID); err != nil {
		return nil, err
	}
	query = normalizeGetProgramAccountsQuery(query)
	if err := validateProgramAccountFilters(query.Filters); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetProgramAccounts, rawCallParams(query.Id, optionalParams([]any{query.ProgramID}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetTokenAccountBalance(ctx context.Context, query GetTokenAccountBalanceQuery) (model.RawResult, error) {
	if err := requireString("token account pubkey", query.Pubkey); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetTokenAccountBalance, rawCallParams(query.Id, optionalParams([]any{query.Pubkey}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetTokenAccountsByDelegate(ctx context.Context, query GetTokenAccountsByDelegateQuery) (model.RawResult, error) {
	if err := requireString("token delegate", query.Delegate); err != nil {
		return nil, err
	}
	if err := validateTokenAccountsFilter(query.Filter); err != nil {
		return nil, err
	}
	query = normalizeGetTokenAccountsByDelegateQuery(query)
	return client.rawMethod(ctx, RPCMethodGetTokenAccountsByDelegate, rawCallParams(query.Id, optionalParams([]any{query.Delegate, query.Filter}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetTokenAccountsByOwner(ctx context.Context, query GetTokenAccountsByOwnerQuery) (model.RawResult, error) {
	if err := requireString("token owner", query.Owner); err != nil {
		return nil, err
	}
	if err := validateTokenAccountsFilter(query.Filter); err != nil {
		return nil, err
	}
	query = normalizeGetTokenAccountsByOwnerQuery(query)
	return client.rawMethod(ctx, RPCMethodGetTokenAccountsByOwner, rawCallParams(query.Id, optionalParams([]any{query.Owner, query.Filter}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetTokenLargestAccounts(ctx context.Context, query GetTokenLargestAccountsQuery) (model.RawResult, error) {
	if err := requireString("token mint", query.Mint); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetTokenLargestAccounts, rawCallParams(query.Id, optionalParams([]any{query.Mint}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetTokenSupply(ctx context.Context, query GetTokenSupplyQuery) (model.RawResult, error) {
	if err := requireString("token mint", query.Mint); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetTokenSupply, rawCallParams(query.Id, optionalParams([]any{query.Mint}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetFeeForMessage(ctx context.Context, query GetFeeForMessageQuery) (model.RawResult, error) {
	if err := requireString("encoded message", query.Message); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetFeeForMessage, rawCallParams(query.Id, optionalParams([]any{query.Message}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetEpochInfo(ctx context.Context, query GetEpochInfoQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetEpochInfo, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetLatestBlockhash(ctx context.Context, query GetLatestBlockhashQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetLatestBlockhash, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetRecentPrioritizationFees(ctx context.Context, query GetRecentPrioritizationFeesQuery) (model.RawResult, error) {
	if len(query.Accounts) == 0 {
		return client.rawMethod(ctx, RPCMethodGetRecentPrioritizationFees, rawCallParams(query.Id))
	}
	if err := requireStrings("account pubkeys", query.Accounts); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetRecentPrioritizationFees, rawCallParams(query.Id, query.Accounts))
}

func (client *ThinClient) GetSignaturesForAddress(ctx context.Context, query GetSignaturesForAddressQuery) (model.RawResult, error) {
	if err := requireString("account address", query.Address); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetSignaturesForAddress, rawCallParams(query.Id, optionalParams([]any{query.Address}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetSignatureStatuses(ctx context.Context, query GetSignatureStatusesQuery) (model.RawResult, error) {
	if err := requireStrings("transaction signatures", query.Signatures); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetSignatureStatuses, rawCallParams(query.Id, optionalParams([]any{query.Signatures}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetTransaction(ctx context.Context, query GetTransactionQuery) (model.RawResult, error) {
	if err := requireString("transaction signature", query.Signature); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetTransaction, rawCallParams(query.Id, optionalParams([]any{query.Signature}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetTransactionCount(ctx context.Context, query GetTransactionCountQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetTransactionCount, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) IsBlockhashValid(ctx context.Context, query IsBlockhashValidQuery) (model.RawResult, error) {
	if err := requireString("blockhash", query.Blockhash); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodIsBlockhashValid, rawCallParams(query.Id, optionalParams([]any{query.Blockhash}, optionalQueryConfig(query))...))
}

func (client *ThinClient) RequestAirdrop(ctx context.Context, query RequestAirdropQuery) (model.RawResult, error) {
	if err := requireString("account pubkey", query.Pubkey); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodRequestAirdrop, rawCallParams(query.Id, optionalParams([]any{query.Pubkey, query.Lamports}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) SendTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error) {
	encoded := query.Encoded
	if encoded == "" {
		if len(query.Serialized) == 0 {
			return nil, errors.New("solana rpc: encoded transaction is required")
		}
		encoded = base64.StdEncoding.EncodeToString(query.Serialized)
	}
	query.Encoded = encoded
	return client.SendEncodedTransaction(ctx, query)
}

func (client *ThinClient) SendEncodedTransaction(ctx context.Context, query SendTransactionQuery) (model.RawResult, error) {
	if err := requireString("encoded transaction", query.Encoded); err != nil {
		return nil, err
	}
	query = normalizeSendTransactionQuery(query)
	return client.rawMethod(ctx, RPCMethodSendTransaction, rawCallParams(query.Id, optionalParams([]any{query.Encoded}, optionalQueryConfig(query))...))
}

func (client *ThinClient) SimulateTransaction(ctx context.Context, query SimulateTransactionQuery) error {
	encoded := query.Encoded
	if encoded == "" {
		if len(query.Serialized) == 0 {
			return errors.New("solana rpc: encoded transaction is required")
		}
		encoded = base64.StdEncoding.EncodeToString(query.Serialized)
	}
	config := normalizeSimulateTransactionQuery(query)
	if err := validateSimulationAccounts(config.Accounts); err != nil {
		return err
	}

	response, err := client.rawMethod(ctx, RPCMethodSimulateTransaction, rawCallParams(query.Id, optionalParams([]any{encoded}, optionalQueryConfig(config))...))
	if err != nil {
		return err
	}

	result := &struct {
		Value *struct {
			Err *string `json:"err,omitempty"`
		} `json:"value"`
	}{}
	if err := jsonrpc.Unmarshal(response, result); err != nil {
		return err
	}
	if result.Value != nil && result.Value.Err != nil {
		return errors.New(*result.Value.Err)
	}
	return nil
}

func (client *ThinClient) SimulateEncodedTransaction(ctx context.Context, query SimulateTransactionQuery) (model.RawResult, error) {
	if err := requireString("encoded transaction", query.Encoded); err != nil {
		return nil, err
	}
	query = normalizeSimulateTransactionQuery(query)
	if err := validateSimulationAccounts(query.Accounts); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodSimulateTransaction, rawCallParams(query.Id, optionalParams([]any{query.Encoded}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetBlock(ctx context.Context, query GetBlockQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetBlock, rawCallParams(query.Id, optionalParams([]any{query.Slot}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetBlockCommitment(ctx context.Context, query SlotQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetBlockCommitment, rawCallParams(query.Id, query.Slot))
}

func (client *ThinClient) GetBlockHeight(ctx context.Context, query GetBlockHeightQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetBlockHeight, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetBlockProduction(ctx context.Context, query GetBlockProductionQuery) (model.RawResult, error) {
	params, err := getBlockProductionQueryParams(query)
	if err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetBlockProduction, params)
}

func (client *ThinClient) GetBlocks(ctx context.Context, query GetBlocksQuery) (model.RawResult, error) {
	if query.EndSlot == nil {
		return client.rawMethod(ctx, RPCMethodGetBlocks, rawCallParams(query.Id, optionalParams([]any{query.StartSlot}, optionalCommitment(query.Commitment))...))
	}
	return client.rawMethod(ctx, RPCMethodGetBlocks, rawCallParams(query.Id, optionalParams([]any{query.StartSlot, *query.EndSlot}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetBlocksWithLimit(ctx context.Context, query GetBlocksWithLimitQuery) (model.RawResult, error) {
	if query.Limit == 0 {
		return nil, errors.New("solana rpc: getBlocksWithLimit limit must be greater than zero")
	}
	return client.rawMethod(ctx, RPCMethodGetBlocksWithLimit, rawCallParams(query.Id, optionalParams([]any{query.StartSlot, query.Limit}, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetBlockTime(ctx context.Context, query SlotQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetBlockTime, rawCallParams(query.Id, query.Slot))
}

func (client *ThinClient) GetConfirmedSlots(ctx context.Context, query GetConfirmedSlotsQuery) (model.ConfirmedSlots, error) {
	response, err := client.rawMethod(ctx, RPCMethodGetBlocks, getConfirmedSlotsQueryParams(query))
	if err != nil {
		return nil, err
	}
	var slots model.ConfirmedSlots
	if err := jsonrpc.Unmarshal(response, &slots); err != nil {
		return nil, err
	}
	return slots, nil
}

func (client *ThinClient) GetFirstAvailableBlock(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetFirstAvailableBlock, DefaultQueryParams())
}

func (client *ThinClient) GetRecentPerformanceSamples(ctx context.Context, query GetRecentPerformanceSamplesQuery) (model.RawResult, error) {
	if query.Limit == 0 {
		return client.rawMethod(ctx, RPCMethodGetRecentPerformanceSamples, rawCallParams(query.Id))
	}
	return client.rawMethod(ctx, RPCMethodGetRecentPerformanceSamples, rawCallParams(query.Id, query.Limit))
}

func (client *ThinClient) MinimumLedgerSlot(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodMinimumLedgerSlot, DefaultQueryParams())
}

func (client *ThinClient) GetEpochSchedule(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetEpochSchedule, DefaultQueryParams())
}

func (client *ThinClient) GetGenesisHash(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetGenesisHash, DefaultQueryParams())
}

func (client *ThinClient) GetHealth(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetHealth, DefaultQueryParams())
}

func (client *ThinClient) GetHighestSnapshotSlot(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetHighestSnapshotSlot, DefaultQueryParams())
}

func (client *ThinClient) GetIdentity(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetIdentity, DefaultQueryParams())
}

func (client *ThinClient) GetLeaderSchedule(ctx context.Context, query GetLeaderScheduleQuery) (model.RawResult, error) {
	if query.Slot == nil {
		return client.rawMethod(ctx, RPCMethodGetLeaderSchedule, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
	}
	return client.rawMethod(ctx, RPCMethodGetLeaderSchedule, rawCallParams(query.Id, optionalParams([]any{*query.Slot}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetMaxRetransmitSlot(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetMaxRetransmitSlot, DefaultQueryParams())
}

func (client *ThinClient) GetMaxShredInsertSlot(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetMaxShredInsertSlot, DefaultQueryParams())
}

func (client *ThinClient) GetSlot(ctx context.Context, query GetSlotQuery) (model.Slot, error) {
	response, err := client.rawMethod(ctx, RPCMethodGetSlot, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(string(response), 10, 64)
	return value, err
}

func (client *ThinClient) GetSlotLeader(ctx context.Context, query GetSlotLeaderQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetSlotLeader, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetSlotLeaders(ctx context.Context, query GetSlotLeadersQuery) (model.SlotLeaders, error) {
	if query.Limit == 0 || query.Limit > 5000 {
		return nil, errors.New("solana rpc: getSlotLeaders limit must be between 1 and 5000")
	}
	response, err := client.rawMethod(ctx, RPCMethodGetSlotLeaders, rawCallParams(query.Id, query.From, query.Limit))
	if err != nil {
		return nil, err
	}
	var leaders []string
	if err := jsonrpc.Unmarshal(response, &leaders); err != nil {
		return nil, err
	}
	return leaders, nil
}

func (client *ThinClient) GetVersion(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetVersion, DefaultQueryParams())
}

func (client *ThinClient) GetClusterNodes(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetClusterNodes, DefaultQueryParams())
}

func (client *ThinClient) GetVoteAccounts(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetVoteAccounts, DefaultQueryParams())
}

func (client *ThinClient) GetInflationGovernor(ctx context.Context, query GetInflationGovernorQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetInflationGovernor, rawCallParams(query.Id, optionalParams(nil, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetInflationRate(ctx context.Context) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetInflationRate, DefaultQueryParams())
}

func (client *ThinClient) GetInflationReward(ctx context.Context, query GetInflationRewardQuery) (model.RawResult, error) {
	if err := requireStrings("inflation reward addresses", query.Addresses); err != nil {
		return nil, err
	}
	return client.rawMethod(ctx, RPCMethodGetInflationReward, rawCallParams(query.Id, optionalParams([]any{query.Addresses}, optionalQueryConfig(query))...))
}

func (client *ThinClient) GetStakeMinimumDelegation(ctx context.Context, query GetStakeMinimumDelegationQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetStakeMinimumDelegation, rawCallParams(query.Id, optionalParams(nil, optionalCommitment(query.Commitment))...))
}

func (client *ThinClient) GetSupply(ctx context.Context, query GetSupplyQuery) (model.RawResult, error) {
	return client.rawMethod(ctx, RPCMethodGetSupply, rawCallParams(query.Id, optionalParams(nil, optionalQueryConfig(query))...))
}
