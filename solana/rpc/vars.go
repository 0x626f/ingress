package rpc

import (
	"time"

	"github.com/0x626f/ingress/solana/types"
)

const (
	// SupportedJsonRpcVersion is the JSON-RPC protocol version supported by this rpc
	SupportedJsonRpcVersion = "2.0"

	// DefaultSlotWindow is the default time window for slot updates on Solana
	DefaultSlotWindow = 400 * time.Millisecond

	// ProcessedCommitment represents the processed commitment level - the node has processed
	// the transaction but it has not been confirmed by the cluster
	ProcessedCommitment types.Commitment = "processed"

	// ConfirmedCommitment represents the confirmed commitment level - the transaction
	// has been confirmed by the cluster with maximum lockout
	ConfirmedCommitment types.Commitment = "confirmed"

	// FinalizedCommitment represents the finalized commitment level - the transaction
	// has been finalized by the cluster and cannot be rolled back
	FinalizedCommitment types.Commitment = "finalized"

	// MaxSlotLeadersRange is the maximum number of slot leaders that can be requested in a single call
	MaxSlotLeadersRange uint16 = 5000

	// MinSlotLeadersRange is the minimum number of slot leaders that can be requested
	MinSlotLeadersRange = 1

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
