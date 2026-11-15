package fakecluster

import (
	"sync"
	"time"
)

// Clock is a manually advanced clock that satisfies gateway.Clock.
type Clock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// NewClock starts at a fixed instant so tests are reproducible.
func NewClock() *Clock {
	c := &Clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Now is part of the clock and random-source interfaces the gateway takes.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After is part of the clock and random-source interfaces the gateway takes.
func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{at: c.now.Add(d), ch: ch})
	c.cond.Broadcast()
	return ch
}

// Advance moves time forward and fires every timer that came due.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

// BlockUntil returns once at least n timers are waiting, so a test can
// advance time without guessing when a goroutine reached its timer.
func (c *Clock) BlockUntil(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.waiters) < n {
		c.cond.Wait()
	}
}

// Rand is a deterministic gateway.Rand.
type Rand struct {
	mu sync.Mutex
	s  uint64
}

// NewRand seeds a splitmix64 generator.
func NewRand(seed uint64) *Rand { return &Rand{s: seed} }

// Uint64 is part of the clock and random-source interfaces the gateway takes.
func (r *Rand) Uint64() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
