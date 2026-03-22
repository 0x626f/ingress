package evm

import (
	"math"
	"sync/atomic"
)

// SequenceGenerator produces monotonically increasing request IDs.
// It wraps from MaxUint back to 1 and is safe for concurrent use.
type SequenceGenerator struct {
	current atomic.Uint64
}

// Next returns the next unique ID in the sequence.
// It wraps around to 1 after reaching MaxUint.
func (generator *SequenceGenerator) Next() uint {
	for {
		current := generator.current.Load()
		next := current + 1
		if next > math.MaxUint {
			next = 1
		}
		if generator.current.CompareAndSwap(current, next) {
			return uint(next)
		}
	}
}
