// Package gateway is the public HTTP front of a RaftKV cluster: key-value
// endpoints, a cached cluster view, abuse controls and admin auth, all in
// front of the Cluster interface.
package gateway

import "time"

// Clock is the only source of time the gateway uses, so tests run on a fake
// clock and never sleep.
type Clock interface {
	Now() time.Time
	// After delivers the time once d has elapsed on this clock.
	After(d time.Duration) <-chan time.Time
}

// Rand is the only source of randomness the gateway uses (request ids).
type Rand interface {
	Uint64() uint64
}
