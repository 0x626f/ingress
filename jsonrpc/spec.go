package jsonrpc

import (
	"fmt"
)

const Version = "2.0"

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func (err *Error) Error() string {
	return fmt.Sprintf("RPC Error[%d]: Message: %s. Data: %v", err.Code, err.Message, err.Data)
}

type MessageID struct {
	ID           uint
	Subscription string
}

func BuildRequest(id uint, method string, params []any) ([]byte, error) {
	if params == nil {
		params = []any{}
	}

	paramsJSON, err := Marshal(params)
	if err != nil {
		return nil, err
	}

	return BuildRawRequest(id, method, paramsJSON), nil
}

func BuildRawRequest(id uint, method string, paramsJSON []byte) []byte {
	if len(paramsJSON) == 0 {
		paramsJSON = []byte("[]")
	}

	return []byte(fmt.Sprintf(
		`{"jsonrpc":"%s","id":%d,"method":%q,"params":%s}`,
		Version,
		id,
		method,
		paramsJSON,
	))
}

func ParseResponse(response []byte) ([]byte, error) {
	return parseResult(response, true, true)
}

func ParseRawResult(response []byte) ([]byte, error) {
	return parseResult(response, false, false)
}

func parseResult(response []byte, stripString bool, nullAsNil bool) ([]byte, error) {
	if len(response) == 0 {
		return nil, nil
	}

	var summary struct {
		Result RawMessage `json:"result"`
		Error  *Error     `json:"error"`
		Params struct {
			Error *Error `json:"error"`
		} `json:"params"`
	}
	if err := Unmarshal(response, &summary); err != nil {
		return nil, err
	}

	if summary.Error != nil {
		return nil, summary.Error
	}

	if summary.Params.Error != nil {
		return nil, summary.Params.Error
	}

	if summary.Result == nil {
		return nil, nil
	}

	if string(summary.Result) == "null" {
		if nullAsNil {
			return nil, nil
		}
		return summary.Result, nil
	}

	if stripString && summary.Result[0] == '"' {
		var value string
		if err := Unmarshal(summary.Result, &value); err != nil {
			return nil, err
		}
		return []byte(value), nil
	}

	return summary.Result, nil
}

func ParseSubscriptionResult(request []byte) ([]byte, error) {
	var summary struct {
		Params struct {
			Result RawMessage `json:"result,omitempty"`
		} `json:"params,omitempty"`
	}

	if err := Unmarshal(request, &summary); err != nil {
		return nil, err
	}

	return summary.Params.Result, nil
}

func ParseMessageID(response []byte) (MessageID, error) {
	var summary struct {
		ID     uint `json:"id,omitempty"`
		Params struct {
			Subscription string `json:"subscription,omitempty"`
		} `json:"params,omitempty"`
	}

	if err := Unmarshal(response, &summary); err != nil {
		return MessageID{}, err
	}

	return MessageID{ID: summary.ID, Subscription: summary.Params.Subscription}, nil
}
