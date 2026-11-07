package node

import (
	"errors"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

const (
	// MaxSubscribers bounds concurrent event subscriptions (T4.7 decision 4).
	MaxSubscribers = 16

	// SubscriberBuffer is the per-subscriber event buffer. A subscriber that
	// falls this far behind is ended with ErrSubscriberOverflow.
	SubscriberBuffer = 256
)

var (
	// ErrTooManySubscribers is returned by Subscribe at the cap. The admin
	// layer maps it to codes.ResourceExhausted.
	ErrTooManySubscribers = errors.New("node: too many event subscribers")

	// ErrSubscriberOverflow ends a subscription whose buffer filled.
	ErrSubscriberOverflow = errors.New("node: event subscriber too slow")
)

// Event is one observed change of (term, role, leader).
type Event struct {
	// Seq starts at 1 and rises by exactly 1 per event.
	Seq      uint64
	UnixNano int64

	Node   raft.NodeID
	Term   raft.Term
	Role   raft.State
	Leader raft.NodeID

	Commit        raft.Index
	Applied       raft.Index
	SnapshotIndex raft.Index
}

// Subscription delivers events from the moment of subscription. C is closed
// when the subscription ends; Err then says why (nil after Close).
type Subscription struct {
	C <-chan Event
}

// Err reports why C was closed.
func (s *Subscription) Err() error { return nil }

// Close ends the subscription and frees its slot.
func (s *Subscription) Close() {}

// Subscribe registers an event subscriber. STUB: the subscription never
// delivers and the cap is not enforced.
func (n *Node) Subscribe() (*Subscription, error) {
	return &Subscription{C: make(chan Event)}, nil
}
