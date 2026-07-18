package rpc

import (
	"fmt"

	"github.com/0x626f/ingress/jsonrpc"
)

func requireString(name, value string) error {
	if value == "" {
		return fmt.Errorf("solana rpc: %s is required", name)
	}
	return nil
}

func requireStrings(name string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("solana rpc: %s must contain at least one value", name)
	}
	for index, value := range values {
		if value == "" {
			return fmt.Errorf("solana rpc: %s[%d] is required", name, index)
		}
	}
	return nil
}

func validateTokenAccountsFilter(filter TokenAccountsFilter) error {
	if _, err := jsonrpc.Marshal(filter); err != nil {
		return fmt.Errorf("solana rpc: %w", err)
	}
	return nil
}

func validateProgramAccountFilters(filters []ProgramAccountsFilter) error {
	for index, filter := range filters {
		if _, err := jsonrpc.Marshal(filter); err != nil {
			return fmt.Errorf("solana rpc: filters[%d]: %w", index, err)
		}
		if filter.Memcmp != nil && filter.Memcmp.Bytes == "" {
			return fmt.Errorf("solana rpc: filters[%d].memcmp.bytes is required", index)
		}
	}
	return nil
}

func validateSimulationAccounts(accounts *SimulateTransactionAccounts) error {
	if accounts == nil {
		return nil
	}
	return requireStrings("simulation accounts addresses", accounts.Addresses)
}

func validateBlockSubscribeFilter(filter BlockSubscribeFilter) error {
	if _, err := jsonrpc.Marshal(filter); err != nil {
		return fmt.Errorf("solana rpc: %w", err)
	}
	return nil
}

func validateLogsSubscribeFilter(filter LogsSubscribeFilter) error {
	if _, err := jsonrpc.Marshal(filter); err != nil {
		return fmt.Errorf("solana rpc: %w", err)
	}
	return nil
}

func normalizeGetBlockProductionQuery(query GetBlockProductionQuery) (GetBlockProductionQuery, error) {
	hasLegacyRange := query.FirstSlot != 0 || query.LastSlot != 0
	if !hasLegacyRange {
		return query, nil
	}

	if query.Range == nil {
		query.Range = &BlockProductionRange{
			FirstSlot:   query.FirstSlot,
			LastSlot:    query.LastSlot,
			LastSlotSet: query.LastSlot != 0,
		}
		return query, nil
	}

	if query.FirstSlot != 0 && query.Range.FirstSlot != query.FirstSlot {
		return query, fmt.Errorf("solana rpc: conflicting getBlockProduction firstSlot values: legacy=%d range=%d", query.FirstSlot, query.Range.FirstSlot)
	}
	if query.LastSlot != 0 && (!query.Range.LastSlotSet && query.Range.LastSlot == 0 || query.Range.LastSlot != query.LastSlot) {
		return query, fmt.Errorf("solana rpc: conflicting getBlockProduction lastSlot values: legacy=%d range=%d", query.LastSlot, query.Range.LastSlot)
	}
	return query, nil
}
