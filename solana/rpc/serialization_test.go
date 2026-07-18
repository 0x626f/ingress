package rpc

import (
	"context"
	"strings"
	"testing"

	"github.com/0x626f/ingress/jsonrpc"
	"github.com/0x626f/ingress/solana/model"
)

func mustBuildMethodCall(t *testing.T, method string, params *QueryParams) string {
	t.Helper()
	payload, err := APISpec{}.BuildMethodCall(method, params)
	if err != nil {
		t.Fatalf("BuildMethodCall: %v", err)
	}
	return string(payload)
}

func TestGetConfirmedSlots_SerializesOptionalCommitment(t *testing.T) {
	tests := []struct {
		name  string
		query GetConfirmedSlotsQuery
		want  string
	}{
		{
			name:  "unset commitment omits config",
			query: GetConfirmedSlotsQuery{IdentifiedQuery: IdentifiedQuery{Id: 7}, From: 0, To: 9},
			want:  `{"jsonrpc":"2.0","id":7,"method":"getBlocks","params":[0,9]}`,
		},
		{
			name: "explicit commitment includes config",
			query: GetConfirmedSlotsQuery{
				IdentifiedQuery: IdentifiedQuery{Id: 7},
				From:            0,
				To:              9,
				Commitment:      model.ConfirmedCommitment,
			},
			want: `{"jsonrpc":"2.0","id":7,"method":"getBlocks","params":[0,9,{"commitment":"confirmed"}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := mustBuildMethodCall(t, RPCMethodGetBlocks, getConfirmedSlotsQueryParams(test.query))
			if got != test.want {
				t.Fatalf("unexpected payload:\nwant: %s\n got: %s", test.want, got)
			}
		})
	}
}

func TestGetBlockProduction_NestsRangeAndPreservesZero(t *testing.T) {
	tests := []struct {
		name  string
		query GetBlockProductionQuery
		want  string
	}{
		{
			name: "replacement range",
			query: GetBlockProductionQuery{
				IdentifiedQuery: IdentifiedQuery{Id: 3},
				Range:           &BlockProductionRange{FirstSlot: 0, LastSlot: 9887},
			},
			want: `{"jsonrpc":"2.0","id":3,"method":"getBlockProduction","params":[{"range":{"firstSlot":0,"lastSlot":9887}}]}`,
		},
		{
			name: "legacy range translated",
			query: GetBlockProductionQuery{
				IdentifiedQuery: IdentifiedQuery{Id: 3},
				FirstSlot:       10,
				LastSlot:        20,
			},
			want: `{"jsonrpc":"2.0","id":3,"method":"getBlockProduction","params":[{"range":{"firstSlot":10,"lastSlot":20}}]}`,
		},
		{
			name: "explicit zero last slot",
			query: GetBlockProductionQuery{
				IdentifiedQuery: IdentifiedQuery{Id: 3},
				Range:           &BlockProductionRange{FirstSlot: 0, LastSlotSet: true},
			},
			want: `{"jsonrpc":"2.0","id":3,"method":"getBlockProduction","params":[{"range":{"firstSlot":0,"lastSlot":0}}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params, err := getBlockProductionQueryParams(test.query)
			if err != nil {
				t.Fatalf("getBlockProductionQueryParams: %v", err)
			}
			got := mustBuildMethodCall(t, RPCMethodGetBlockProduction, params)
			if got != test.want {
				t.Fatalf("unexpected payload:\nwant: %s\n got: %s", test.want, got)
			}
		})
	}
}

func TestGetBlockProduction_LegacyConflictsReturnError(t *testing.T) {
	tests := []GetBlockProductionQuery{
		{FirstSlot: 5, Range: &BlockProductionRange{FirstSlot: 6}},
		{LastSlot: 9, Range: &BlockProductionRange{FirstSlot: 0, LastSlot: 8}},
	}
	for _, query := range tests {
		if _, err := getBlockProductionQueryParams(query); err == nil || !strings.Contains(err.Error(), "conflicting getBlockProduction") {
			t.Fatalf("expected conflict error for %#v, got %v", query, err)
		}
	}
}

func TestOptionalNumericFields_PreserveExplicitZero(t *testing.T) {
	tests := []struct {
		name   string
		config any
		field  string
		set    bool
	}{
		{name: "maxRetries unset", config: SendTransactionQuery{}, field: "maxRetries"},
		{name: "maxRetries zero", config: SendTransactionQuery{MaxRetriesSet: true}, field: "maxRetries", set: true},
		{name: "epoch unset", config: GetInflationRewardQuery{}, field: "epoch"},
		{name: "epoch zero", config: GetInflationRewardQuery{EpochSet: true}, field: "epoch", set: true},
		{name: "dataSize zero", config: ProgramAccountsFilter{DataSizeSet: true}, field: "dataSize", set: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := jsonrpc.Marshal(test.config)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var config map[string]jsonrpc.RawMessage
			if err := jsonrpc.Unmarshal(data, &config); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			value, ok := config[test.field]
			if ok != test.set {
				t.Fatalf("field %q presence = %v, want %v in %s", test.field, ok, test.set, data)
			}
			if test.set && string(value) != "0" {
				t.Fatalf("field %q = %s, want 0", test.field, value)
			}
		})
	}
}

func TestOptionalNumericFields_ExactRequestPayloads(t *testing.T) {
	sendQuery := normalizeSendTransactionQuery(SendTransactionQuery{MaxRetriesSet: true})
	send := mustBuildMethodCall(t, RPCMethodSendTransaction, rawCallParams(1,
		optionalParams([]any{"transaction"}, optionalQueryConfig(sendQuery))...,
	))
	if want := `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["transaction",{"encoding":"base64","maxRetries":0}]}`; send != want {
		t.Fatalf("unexpected sendTransaction payload:\nwant: %s\n got: %s", want, send)
	}

	inflationQuery := GetInflationRewardQuery{Addresses: []string{systemProgramID}, EpochSet: true}
	inflation := mustBuildMethodCall(t, RPCMethodGetInflationReward, rawCallParams(1,
		optionalParams([]any{inflationQuery.Addresses}, optionalQueryConfig(inflationQuery))...,
	))
	if want := `{"jsonrpc":"2.0","id":1,"method":"getInflationReward","params":[["11111111111111111111111111111111"],{"epoch":0}]}`; inflation != want {
		t.Fatalf("unexpected getInflationReward payload:\nwant: %s\n got: %s", want, inflation)
	}

	programQuery := normalizeGetProgramAccountsQuery(GetProgramAccountsQuery{
		ProgramID: systemProgramID,
		Filters:   []ProgramAccountsFilter{{DataSizeSet: true}},
	})
	program := mustBuildMethodCall(t, RPCMethodGetProgramAccounts, rawCallParams(1,
		optionalParams([]any{programQuery.ProgramID}, optionalQueryConfig(programQuery))...,
	))
	if want := `{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":["11111111111111111111111111111111",{"encoding":"base64","filters":[{"dataSize":0}]}]}`; program != want {
		t.Fatalf("unexpected getProgramAccounts payload:\nwant: %s\n got: %s", want, program)
	}
}

func TestMinContextSlot_SerializesForSupportedMethods(t *testing.T) {
	tests := []struct {
		method string
		params []any
	}{
		{RPCMethodGetLatestBlockhash, optionalParams(nil, optionalQueryConfig(GetLatestBlockhashQuery{MinContextSlot: 11}))},
		{RPCMethodGetBlockHeight, optionalParams(nil, optionalQueryConfig(GetBlockHeightQuery{MinContextSlot: 11}))},
		{RPCMethodIsBlockhashValid, optionalParams([]any{"blockhash"}, optionalQueryConfig(IsBlockhashValidQuery{MinContextSlot: 11}))},
		{RPCMethodGetFeeForMessage, optionalParams([]any{"message"}, optionalQueryConfig(GetFeeForMessageQuery{MinContextSlot: 11}))},
		{RPCMethodGetTransactionCount, optionalParams(nil, optionalQueryConfig(GetTransactionCountQuery{MinContextSlot: 11}))},
		{RPCMethodGetEpochInfo, optionalParams(nil, optionalQueryConfig(GetEpochInfoQuery{MinContextSlot: 11}))},
		{RPCMethodGetSlot, optionalParams(nil, optionalQueryConfig(GetSlotQuery{MinContextSlot: 11}))},
		{RPCMethodGetSlotLeader, optionalParams(nil, optionalQueryConfig(GetSlotLeaderQuery{MinContextSlot: 11}))},
	}

	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			payload := []byte(mustBuildMethodCall(t, test.method, rawCallParams(1, test.params...)))
			params := requestParams(t, payload)
			var config map[string]jsonrpc.RawMessage
			if err := jsonrpc.Unmarshal(params[len(params)-1], &config); err != nil {
				t.Fatalf("unmarshal config: %v", err)
			}
			if string(config["minContextSlot"]) != "11" {
				t.Fatalf("minContextSlot missing from %s", payload)
			}
		})
	}
}

func TestGetBlock_SerializesCompleteConfig(t *testing.T) {
	rewards := false
	version := uint64(0)
	query := GetBlockQuery{
		IdentifiedQuery:                IdentifiedQuery{Id: 4},
		Slot:                           0,
		Commitment:                     model.FinalizedCommitment,
		Encoding:                       EncodingJSON,
		TransactionDetails:             TransactionDetailsFull,
		Rewards:                        &rewards,
		MaxSupportedTransactionVersion: &version,
	}
	got := mustBuildMethodCall(t, RPCMethodGetBlock, rawCallParams(query.Id,
		optionalParams([]any{query.Slot}, optionalQueryConfig(query))...,
	))
	want := `{"jsonrpc":"2.0","id":4,"method":"getBlock","params":[0,{"commitment":"finalized","encoding":"json","transactionDetails":"full","rewards":false,"maxSupportedTransactionVersion":0}]}`
	if got != want {
		t.Fatalf("unexpected payload:\nwant: %s\n got: %s", want, got)
	}

	empty := mustBuildMethodCall(t, RPCMethodGetBlock, rawCallParams(4,
		optionalParams([]any{model.Slot(0)}, optionalQueryConfig(GetBlockQuery{}))...,
	))
	if want := `{"jsonrpc":"2.0","id":4,"method":"getBlock","params":[0]}`; empty != want {
		t.Fatalf("empty config was not omitted:\nwant: %s\n got: %s", want, empty)
	}
}

func TestRequiredParameters_ReturnLocalErrors(t *testing.T) {
	client := &ThinClient{}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "account pubkey", call: func() error { _, err := client.GetBalance(context.Background(), GetBalanceQuery{}); return err }},
		{name: "account pubkeys", call: func() error {
			_, err := client.GetMultipleAccounts(context.Background(), GetMultipleAccountsQuery{})
			return err
		}},
		{name: "program id", call: func() error {
			_, err := client.GetProgramAccounts(context.Background(), GetProgramAccountsQuery{})
			return err
		}},
		{name: "token filter", call: func() error {
			_, err := client.GetTokenAccountsByOwner(context.Background(), GetTokenAccountsByOwnerQuery{Owner: systemProgramID})
			return err
		}},
		{name: "signature", call: func() error { _, err := client.GetTransaction(context.Background(), GetTransactionQuery{}); return err }},
		{name: "signature list", call: func() error {
			_, err := client.GetSignatureStatuses(context.Background(), GetSignatureStatusesQuery{})
			return err
		}},
		{name: "blockhash", call: func() error {
			_, err := client.IsBlockhashValid(context.Background(), IsBlockhashValidQuery{})
			return err
		}},
		{name: "encoded transaction", call: func() error {
			_, err := client.SendTransaction(context.Background(), SendTransactionQuery{})
			return err
		}},
		{name: "program filter", call: func() error {
			_, err := client.GetProgramAccounts(context.Background(), GetProgramAccountsQuery{ProgramID: systemProgramID, Filters: []ProgramAccountsFilter{{}}})
			return err
		}},
		{name: "simulation accounts", call: func() error {
			_, err := client.SimulateEncodedTransaction(context.Background(), SimulateTransactionQuery{Encoded: "transaction", Accounts: &SimulateTransactionAccounts{}})
			return err
		}},
		{name: "block subscribe filter", call: func() error { _, err := client.BlockSubscribe(context.Background(), BlockSubscribeQuery{}); return err }},
		{name: "logs subscribe filter", call: func() error { _, err := client.LogsSubscribe(context.Background(), LogsSubscribeQuery{}); return err }},
		{name: "subscription signature", call: func() error {
			_, err := client.SignatureSubscribe(context.Background(), SignatureSubscribeQuery{})
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || strings.Contains(err.Error(), "connection manager") {
				t.Fatalf("expected local validation error, got %v", err)
			}
		})
	}
}

func TestHTTPAndWebSocket_AccountConfigSerializationMatches(t *testing.T) {
	httpQuery := normalizeGetAccountInfoQuery(GetAccountInfoQuery{
		Pubkey: systemProgramID, Commitment: model.ConfirmedCommitment, Encoding: EncodingJSONParsed, MinContextSlot: 12,
	})
	wsQuery := normalizeAccountSubscribeQuery(AccountSubscribeQuery{
		Pubkey: systemProgramID, Commitment: model.ConfirmedCommitment, Encoding: EncodingJSONParsed, MinContextSlot: 12,
	})

	httpPayload := []byte(mustBuildMethodCall(t, RPCMethodGetAccountInfo, rawCallParams(1,
		optionalParams([]any{httpQuery.Pubkey}, optionalQueryConfig(httpQuery))...,
	)))
	wsPayload := []byte(mustBuildMethodCall(t, RPCMethodAccountSubscribe, rawCallParams(1,
		optionalParams([]any{wsQuery.Pubkey}, optionalQueryConfig(wsQuery))...,
	)))
	httpParams := requestParams(t, httpPayload)
	wsParams := requestParams(t, wsPayload)
	if len(httpParams) != 2 || len(wsParams) != 2 {
		t.Fatalf("unexpected params: HTTP=%s WS=%s", httpParams, wsParams)
	}
	if string(httpParams[1]) != string(wsParams[1]) {
		t.Fatalf("config mismatch:\nHTTP: %s\n  WS: %s", httpParams[1], wsParams[1])
	}
}
