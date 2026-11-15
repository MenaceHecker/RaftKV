package client

import (
	"math"
	"time"
)

// backoffCap returns the upper bound of the nth consecutive delay (n >= 1):
// Initial * Multiplier^(n-1), never above Max.
func (b Backoff) backoffCap(n int) time.Duration {
	capf := float64(b.Initial) * math.Pow(b.Multiplier, float64(n-1))
	if capf >= float64(b.Max) || math.IsInf(capf, 0) || math.IsNaN(capf) {
		return b.Max
	}
	if capf < 1 {
		return 1
	}
	return time.Duration(capf)
}

// delay returns the nth consecutive delay with full jitter: uniform in
// (0, cap]. It is never zero, so a retry loop cannot spin.
func (b Backoff) delay(n int, r Rand) time.Duration {
	limit := int64(b.backoffCap(n))
	return time.Duration(r.Int63n(limit) + 1)
}
