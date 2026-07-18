package jsonrpc

func MapResponse[T any](raw []byte, err error) (T, error) {
	var out T
	if err != nil {
		return out, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return out, nil
	}
	if err := Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}
