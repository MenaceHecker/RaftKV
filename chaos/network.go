// Package chaos injects faults into a Raft cluster and checks that the results
// are still correct.
//
// It is the reason the consensus core has no clock, no goroutines, and no
// sockets. Because a whole cluster advances only when something here calls
// Tick, and because every message passes through a scheduler seeded from a
// single source of randomness, a scenario that fails does so identically every
// time it is run. A chaos suite that cannot reproduce its own failures is
// worse than none: it teaches you to re-run until green.
package chaos

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Faults describes how badly the network behaves.
//
// The defaults are all zero, which is a perfect network. Every field makes
// things worse, and they compose: a partitioned link never delivers however
// low the loss rate is.
type Faults struct {
	// LossRate is the probability in [0,1] that a message is dropped.
	//
	// Raft is supposed to tolerate this completely, because every message is
	// retried by the next heartbeat. What loss actually tests is whether
	// anything in the implementation quietly assumed a message arrived.
	LossRate float64

	// MinDelay and MaxDelay bound how many ticks a message waits before it is
	// delivered. Zero for both means immediate delivery.
	//
	// Variable delay is where reordering comes from, and reordering is the
	// interesting part: two messages sent in one order arriving in another is
	// what breaks implementations that treat a response as being about the
	// most recent request.
	MinDelay int
	MaxDelay int

	// DuplicateRate is the probability in [0,1] that a message is delivered
	// twice.
	//
	// Real networks and real retry logic both produce duplicates, and Raft's
	// correctness depends on handlers being idempotent in ways that are easy
	// to get subtly wrong — a repeated vote request must get the same answer,
	// a repeated append must not truncate entries accepted since.
	DuplicateRate float64
}

func (f Faults) validate() error {
	if f.LossRate < 0 || f.LossRate > 1 {
		return fmt.Errorf("chaos: LossRate %v is not a probability", f.LossRate)
	}
	if f.DuplicateRate < 0 || f.DuplicateRate > 1 {
		return fmt.Errorf("chaos: DuplicateRate %v is not a probability", f.DuplicateRate)
	}
	if f.MinDelay < 0 || f.MaxDelay < 0 {
		return fmt.Errorf("chaos: delays must not be negative")
	}
	if f.MaxDelay < f.MinDelay {
		return fmt.Errorf("chaos: MaxDelay %d is below MinDelay %d", f.MaxDelay, f.MinDelay)
	}
	return nil
}

// Stats counts what the network did, so a scenario can report the conditions
// it actually ran under rather than the ones it asked for.
//
// The distinction matters: a run configured for 20% loss that happened to drop
// nothing proves much less than the configuration suggests, and without
// counting there is no way to tell the two apart.
type Stats struct {
	Sent       int
	Delivered  int
	Dropped    int
	Partitions int
	Duplicated int
	Delayed    int
}

// scheduled is one message waiting to be delivered.
type scheduled struct {
	msg raft.Message
	at  int64
	// seq orders messages that come due on the same tick. Go randomizes map
	// iteration and slices grow unpredictably, so without an explicit
	// tiebreaker the delivery order would vary between runs of the same seed
	// and the whole harness would stop being reproducible.
	seq uint64
}

// Network is a deterministic, fault-injecting message scheduler.
//
// It holds no nodes and knows nothing about Raft beyond the address on a
// message. That separation keeps it usable for any scenario: partitions and
// delays are properties of the network, not of the cluster running on it.
type Network struct {
	rng *rand.Rand

	// now is the current tick. Nothing here reads a real clock.
	now int64

	faults Faults

	// queue holds messages not yet due, in no particular order; Deliver sorts
	// what it takes out.
	queue []scheduled
	seq   uint64

	// group maps a node to its partition group. Messages between different
	// groups are dropped. A nil map means the network is whole.
	group map[raft.NodeID]int

	stats Stats
}

// NewNetwork creates a network seeded for reproducibility.
//
// The seed is the whole point: a scenario that fails can be re-run with the
// same seed and will fail the same way, which is what makes a chaos failure
// something to debug rather than something to shrug at.
func NewNetwork(seed int64, faults Faults) (*Network, error) {
	if err := faults.validate(); err != nil {
		return nil, err
	}
	return &Network{
		rng:    rand.New(rand.NewSource(seed)),
		faults: faults,
	}, nil
}

// Now returns the current tick.
func (n *Network) Now() int64 { return n.now }

// Advance moves the clock forward by one tick.
func (n *Network) Advance() { n.now++ }

// Stats returns what the network has done so far.
func (n *Network) Stats() Stats { return n.stats }

// SetFaults changes the fault configuration mid-run, which is how a scenario
// models a network that degrades and then recovers.
func (n *Network) SetFaults(f Faults) error {
	if err := f.validate(); err != nil {
		return err
	}
	n.faults = f
	return nil
}

// Partition splits the cluster into isolated groups.
//
// Messages within a group flow normally; messages crossing a boundary are
// dropped, which is what a partition looks like from inside a node. Nodes not
// named in any group are left in a group of their own, so a caller can isolate
// one node without listing everyone else.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.group = make(map[raft.NodeID]int)
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i
		}
	}
}

// Heal restores full connectivity.
//
// Messages already in flight are unaffected: one dropped by a partition is
// gone for good, and Raft's own retries are what recover from that. Delivering
// them retroactively would model a network that buffers indefinitely, which is
// both unrealistic and much easier to survive.
func (n *Network) Heal() { n.group = nil }

// reachable reports whether a message can cross between two nodes.
func (n *Network) reachable(from, to raft.NodeID) bool {
	if n.group == nil {
		return true
	}

	// A node nobody placed in a group is isolated from everything except
	// itself. That makes "isolate node 3" expressible as a single group
	// rather than a full enumeration of the others.
	gf, okf := n.group[from]
	gt, okt := n.group[to]
	if !okf || !okt {
		return from == to
	}
	return gf == gt
}

// Send offers messages to the network. Each is dropped, delayed, duplicated,
// or scheduled for immediate delivery according to the current faults.
func (n *Network) Send(msgs []raft.Message) {
	for _, m := range msgs {
		n.stats.Sent++

		if !n.reachable(m.From, m.To) {
			n.stats.Partitions++
			continue
		}
		if n.faults.LossRate > 0 && n.rng.Float64() < n.faults.LossRate {
			n.stats.Dropped++
			continue
		}

		n.schedule(m)

		if n.faults.DuplicateRate > 0 && n.rng.Float64() < n.faults.DuplicateRate {
			// A duplicate is scheduled independently, so it usually arrives at
			// a different time than the original. A copy delivered back to
			// back would be a much weaker test than one that turns up several
			// ticks later, after the receiver has moved on.
			n.stats.Duplicated++
			n.schedule(m)
		}
	}
}

// schedule places one message in the queue at its delivery time.
func (n *Network) schedule(m raft.Message) {
	delay := n.faults.MinDelay
	if n.faults.MaxDelay > n.faults.MinDelay {
		delay += n.rng.Intn(n.faults.MaxDelay - n.faults.MinDelay + 1)
	}
	if delay > 0 {
		n.stats.Delayed++
	}

	n.seq++
	n.queue = append(n.queue, scheduled{msg: m, at: n.now + int64(delay), seq: n.seq})
}

// Deliver returns every message due at or before the current tick, in a
// deterministic order, and removes them from the queue.
//
// Ordering is by delivery time and then by the sequence in which messages were
// sent. That is not the same as the order they were sent in: a message with a
// longer delay overtakes one sent later with a shorter one, which is exactly
// the reordering these scenarios exist to produce.
func (n *Network) Deliver() []raft.Message {
	if len(n.queue) == 0 {
		return nil
	}

	var due []scheduled
	remaining := n.queue[:0]
	for _, s := range n.queue {
		if s.at <= n.now {
			due = append(due, s)
			continue
		}
		remaining = append(remaining, s)
	}
	n.queue = remaining

	sort.Slice(due, func(i, j int) bool {
		if due[i].at != due[j].at {
			return due[i].at < due[j].at
		}
		return due[i].seq < due[j].seq
	})

	out := make([]raft.Message, len(due))
	for i, s := range due {
		out[i] = s.msg
	}
	n.stats.Delivered += len(out)
	return out
}

// InFlight reports how many messages are waiting to be delivered, which lets a
// scenario tell a quiet network from a stalled one.
func (n *Network) InFlight() int { return len(n.queue) }

// DropAllInFlight discards everything waiting.
//
// This models the moment a node crashes: whatever was on its way to it is
// gone, and nothing will redeliver it except Raft itself.
func (n *Network) DropAllInFlight(to raft.NodeID) {
	remaining := n.queue[:0]
	for _, s := range n.queue {
		if s.msg.To == to {
			n.stats.Dropped++
			continue
		}
		remaining = append(remaining, s)
	}
	n.queue = remaining
}
