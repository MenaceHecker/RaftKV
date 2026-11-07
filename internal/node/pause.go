package node

import (
	"sync"
	"sync/atomic"
	"time"
)

// MaxPause is the longest a pause may last (T4.7 decision 5).
const MaxPause = 60 * time.Second

// pauseState is the pause flag. Pausing never blocks the run loop: the loop
// only reads the atomic flag, and mu guards the timer bookkeeping of callers,
// never held across a wait.
type pauseState struct {
	paused atomic.Bool

	mu    sync.Mutex
	timer Timer
	gen   uint64 // identifies the current pause, so a stale timer cannot lift a newer one
}

// Pause stops ticks for d, clamped to MaxPause, and returns the duration
// applied. A d of zero or less is a Resume. The node lifts the pause by itself
// when d elapses on Config.Clock; pausing again replaces the earlier deadline.
//
// The pause stops ticks only. Committed entries and snapshots keep being
// applied (processReady and Advance are untouched), and Status stays served.
// Dropping peer traffic and failing client RPCs while paused is done by the
// transport layer, which reads Paused.
func (n *Node) Pause(d time.Duration) time.Duration {
	if d <= 0 {
		n.Resume()
		return 0
	}
	d = min(d, MaxPause)

	p := &n.pause
	p.mu.Lock()
	if p.timer != nil {
		p.timer.Stop()
	}
	p.gen++
	gen := p.gen
	p.paused.Store(true)
	p.timer = n.cfg.Clock.AfterFunc(d, func() { n.lift(gen) })
	p.mu.Unlock()
	return d
}

// Resume ends a pause early and wakes the run loop.
func (n *Node) Resume() { n.lift(0) }

// Paused reports whether a pause is in effect.
func (n *Node) Paused() bool { return n.pause.paused.Load() }

// lift ends the pause identified by gen, or any pause when gen is 0. A timer
// from a pause that was replaced or resumed finds a newer gen and does nothing.
func (n *Node) lift(gen uint64) {
	p := &n.pause
	p.mu.Lock()
	if gen != 0 && gen != p.gen {
		p.mu.Unlock()
		return
	}
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	p.gen++
	p.paused.Store(false)
	p.mu.Unlock()

	// Wake the run loop so work that became pending while paused is not left
	// waiting for the next unrelated event. The channel holds one signal.
	select {
	case n.applyc <- struct{}{}:
	default:
	}
}
