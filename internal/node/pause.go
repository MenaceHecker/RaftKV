package node

import "time"

// MaxPause is the longest a pause may last (T4.7 decision 5).
const MaxPause = 60 * time.Second

// Pause stops ticks for d, clamped to MaxPause, and returns the duration
// applied. The node resumes by itself when it elapses. STUB: does nothing.
func (n *Node) Pause(_ time.Duration) time.Duration { return 0 }

// Resume ends a pause early. STUB: does nothing.
func (n *Node) Resume() {}

// Paused reports whether a pause is in effect. STUB: always false.
func (n *Node) Paused() bool { return false }
