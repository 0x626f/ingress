package rpc

import (
	"encoding/json"
	"fmt"

	"github.com/0x626f/ingress/jsonrpc"
)

const (
	RPCMethodGetAccountInfo                    = "getAccountInfo"
	RPCMethodGetBalance                        = "getBalance"
	RPCMethodGetLargestAccounts                = "getLargestAccounts"
	RPCMethodGetMinimumBalanceForRentExemption = "getMinimumBalanceForRentExemption"
	RPCMethodGetMultipleAccounts               = "getMultipleAccounts"
	RPCMethodGetProgramAccounts                = "getProgramAccounts"
	RPCMethodGetTokenAccountBalance            = "getTokenAccountBalance"
	RPCMethodGetTokenAccountsByDelegate        = "getTokenAccountsByDelegate"
	RPCMethodGetTokenAccountsByOwner           = "getTokenAccountsByOwner"
	RPCMethodGetTokenLargestAccounts           = "getTokenLargestAccounts"
	RPCMethodGetTokenSupply                    = "getTokenSupply"
	RPCMethodGetFeeForMessage                  = "getFeeForMessage"
	RPCMethodGetLatestBlockhash                = "getLatestBlockhash"
	RPCMethodGetRecentPrioritizationFees       = "getRecentPrioritizationFees"
	RPCMethodGetSignaturesForAddress           = "getSignaturesForAddress"
	RPCMethodGetSignatureStatuses              = "getSignatureStatuses"
	RPCMethodGetTransaction                    = "getTransaction"
	RPCMethodGetTransactionCount               = "getTransactionCount"
	RPCMethodIsBlockhashValid                  = "isBlockhashValid"
	RPCMethodRequestAirdrop                    = "requestAirdrop"
	RPCMethodSendTransaction                   = "sendTransaction"
	RPCMethodSimulateTransaction               = "simulateTransaction"
	RPCMethodGetBlock                          = "getBlock"
	RPCMethodGetBlockCommitment                = "getBlockCommitment"
	RPCMethodGetBlockHeight                    = "getBlockHeight"
	RPCMethodGetBlockProduction                = "getBlockProduction"
	RPCMethodGetBlocks                         = "getBlocks"
	RPCMethodGetBlocksWithLimit                = "getBlocksWithLimit"
	RPCMethodGetBlockTime                      = "getBlockTime"
	RPCMethodGetFirstAvailableBlock            = "getFirstAvailableBlock"
	RPCMethodGetRecentPerformanceSamples       = "getRecentPerformanceSamples"
	RPCMethodMinimumLedgerSlot                 = "minimumLedgerSlot"
	RPCMethodGetClusterNodes                   = "getClusterNodes"
	RPCMethodGetEpochInfo                      = "getEpochInfo"
	RPCMethodGetEpochSchedule                  = "getEpochSchedule"
	RPCMethodGetGenesisHash                    = "getGenesisHash"
	RPCMethodGetHealth                         = "getHealth"
	RPCMethodGetHighestSnapshotSlot            = "getHighestSnapshotSlot"
	RPCMethodGetIdentity                       = "getIdentity"
	RPCMethodGetLeaderSchedule                 = "getLeaderSchedule"
	RPCMethodGetMaxRetransmitSlot              = "getMaxRetransmitSlot"
	RPCMethodGetMaxShredInsertSlot             = "getMaxShredInsertSlot"
	RPCMethodGetSlot                           = "getSlot"
	RPCMethodGetSlotLeader                     = "getSlotLeader"
	RPCMethodGetSlotLeaders                    = "getSlotLeaders"
	RPCMethodGetVersion                        = "getVersion"
	RPCMethodGetVoteAccounts                   = "getVoteAccounts"
	RPCMethodGetInflationGovernor              = "getInflationGovernor"
	RPCMethodGetInflationRate                  = "getInflationRate"
	RPCMethodGetInflationReward                = "getInflationReward"
	RPCMethodGetStakeMinimumDelegation         = "getStakeMinimumDelegation"
	RPCMethodGetSupply                         = "getSupply"
	RPCMethodAccountSubscribe                  = "accountSubscribe"
	RPCMethodAccountUnsubscribe                = "accountUnsubscribe"
	RPCMethodBlockSubscribe                    = "blockSubscribe"
	RPCMethodBlockUnsubscribe                  = "blockUnsubscribe"
	RPCMethodLogsSubscribe                     = "logsSubscribe"
	RPCMethodLogsUnsubscribe                   = "logsUnsubscribe"
	RPCMethodProgramSubscribe                  = "programSubscribe"
	RPCMethodProgramUnsubscribe                = "programUnsubscribe"
	RPCMethodRootSubscribe                     = "rootSubscribe"
	RPCMethodRootUnsubscribe                   = "rootUnsubscribe"
	RPCMethodSignatureSubscribe                = "signatureSubscribe"
	RPCMethodSignatureUnsubscribe              = "signatureUnsubscribe"
	RPCMethodSlotSubscribe                     = "slotSubscribe"
	RPCMethodSlotUnsubscribe                   = "slotUnsubscribe"
	RPCMethodSlotsUpdatesSubscribe             = "slotsUpdatesSubscribe"
	RPCMethodSlotsUpdatesUnsubscribe           = "slotsUpdatesUnsubscribe"
	RPCMethodVoteSubscribe                     = "voteSubscribe"
	RPCMethodVoteUnsubscribe                   = "voteUnsubscribe"
)

type APISpec struct{}

type APIError = jsonrpc.Error

type MessageId = jsonrpc.MessageID

func (spec APISpec) BuildQuery(id uint, method string, params []any) ([]byte, error) {
	return jsonrpc.BuildRequest(id, method, params)
}

func (spec APISpec) ParseResponse(response []byte) ([]byte, error) {
	return jsonrpc.ParseRawResult(response)
}

func (spec APISpec) ParseSubscriptionResponse(request []byte) ([]byte, error) {
	return jsonrpc.ParseSubscriptionResult(request)
}

func (spec APISpec) ParseMessageId(response []byte) (MessageId, error) {
	var summary struct {
		ID     uint `json:"id,omitempty"`
		Params struct {
			Subscription json.RawMessage `json:"subscription,omitempty"`
		} `json:"params,omitempty"`
	}

	if err := json.Unmarshal(response, &summary); err != nil {
		return MessageId{}, err
	}

	if len(summary.Params.Subscription) == 0 {
		return MessageId{ID: summary.ID}, nil
	}

	var number uint64
	if err := json.Unmarshal(summary.Params.Subscription, &number); err == nil {
		return MessageId{ID: summary.ID, Subscription: fmt.Sprint(number)}, nil
	}

	var text string
	if err := json.Unmarshal(summary.Params.Subscription, &text); err == nil {
		return MessageId{ID: summary.ID, Subscription: text}, nil
	}

	return MessageId{ID: summary.ID}, nil
}

func (spec APISpec) BuildMethodCall(method string, params *QueryParams) ([]byte, error) {
	if params == nil {
		params = DefaultQueryParams()
	}
	params.Adjust()
	return spec.BuildQuery(params.Id, method, params.Params)
}
