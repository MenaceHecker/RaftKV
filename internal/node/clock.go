package node

import "time"

// Clock supplies the node's time so tests can drive ticks and timers by hand.
// A nil Config.Clock means the real clock.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
	AfterFunc(d time.Duration, f func()) Timer
}

// Ticker is the subset of time.Ticker the run loop uses.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Timer is the subset of time.Timer the node uses.
type Timer interface {
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
