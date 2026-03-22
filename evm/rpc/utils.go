package rpc

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

func getOrDefault(def, value string) string {
	if value != "" {
		return value
	}
	return def
}

func getFirstOrDefault(def string, values ...string) string {
	if len(values) > 0 {
		return values[0]
	}
	return def
}

func toHex(n uint64) string {
	return "0x" + strconv.FormatUint(n, 16)
}

func fromHex(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	return strconv.ParseUint(s, 16, 64)
}

func stringToHex(s string) string {
	n := new(big.Int)
	_, ok := n.SetString(s, 10)
	if !ok {
		return ""
	}
	return "0x" + fmt.Sprintf("%x", n)
}

func stringToHexOrDefault(s string) string {
	if strings.HasPrefix(s, "0x") {
		return s
	}
	return stringToHex(s)
}
