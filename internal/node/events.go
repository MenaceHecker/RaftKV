package node

import (
	"errors"
	"sync"

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
	// C delivers events. It is closed when the subscription ends.
	C <-chan Event

	ch  chan Event
	bus *eventBus
	err error // guarded by bus.mu
}

// Err reports why C was closed: ErrSubscriberOverflow if the subscriber fell
// SubscriberBuffer events behind, nil after Close or while still open.
func (s *Subscription) Err() error {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	return s.err
}

// Close ends the subscription and frees its slot. It is safe to call twice and
// after the subscription was dropped for being slow.
func (s *Subscription) Close() { s.bus.remove(s, nil) }

// eventBus fans events out to a bounded set of bounded subscribers. Its mutex
// is held only for non-blocking work, so a slow subscriber never stalls the
// node's run loop.
type eventBus struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
	seq  uint64
}

// Subscribe registers an event subscriber, or returns ErrTooManySubscribers at
// MaxSubscribers.
func (n *Node) Subscribe() (*Subscription, error) {
	b := &n.events
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subs) >= MaxSubscribers {
		return nil, ErrTooManySubscribers
	}
	ch := make(chan Event, SubscriberBuffer)
	s := &Subscription{C: ch, ch: ch, bus: b}
	if b.subs == nil {
		b.subs = make(map[*Subscription]struct{})
	}
	b.subs[s] = struct{}{}
	return s, nil
}

// remove ends s with the given error if it is still registered.
func (b *eventBus) remove(s *Subscription, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(s, err)
}

func (b *eventBus) removeLocked(s *Subscription, err error) {
	if _, ok := b.subs[s]; !ok {
		return
	}
	delete(b.subs, s)
	s.err = err
	close(s.ch)
}

// publish stamps ev with the next Seq and offers it to every subscriber without
// blocking. A subscriber whose buffer is full is ended with ErrSubscriberOverflow.
func (b *eventBus) publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	ev.Seq = b.seq
	for s := range b.subs {
		select {
		case s.ch <- ev:
		default:
			b.removeLocked(s, ErrSubscriberOverflow)
		}
	}
}
