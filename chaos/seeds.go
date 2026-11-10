package chaos

import "errors"

// errNotImplemented marks the T5.6 stubs; the feat commits replace every use.
var errNotImplemented = errors.New("chaos: not implemented")

// ParseSeeds expands a seed specification such as "1-200", "7" or "1,3,5-9".
func ParseSeeds(_ string) ([]int64, error) {
	return nil, errNotImplemented
}
