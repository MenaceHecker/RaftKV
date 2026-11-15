package client

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// Clock supplies time to the retry logic so tests can drive it by hand. The
// retry logic never calls time.Now or time.Sleep.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer is the subset of time.Timer the client uses.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Rand supplies jitter. Int63n returns a value in [0, n); n is always > 0.
type Rand interface {
	Int63n(n int64) int64
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns the wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer returns a timer backed by time.NewTimer.
func (RealClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// RealRand draws from the operating system's CSPRNG. It is safe for concurrent
// use and does not touch the math/rand global.
type RealRand struct{}

// Int63n returns a uniform value in [0, n).
func (RealRand) Int63n(n int64) int64 {
	if n <= 1 {
		return 0
	}
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic("client: crypto/rand failed: " + err.Error())
		}
		v := int64(binary.BigEndian.Uint64(b[:]) >> 1)
		// Reject the biased tail so the result is uniform.
		if limit := int64((1<<63 - 1) - (1<<63)%uint64(n)); v > limit {
			continue
		}
		return v % n
	}
}
