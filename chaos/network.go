package chaos

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

type Faults struct {
	LossRate float64

	MinDelay int
	MaxDelay int

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

type Stats struct {
	Sent       int
	Delivered  int
	Dropped    int
	Partitions int
	Duplicated int
	Delayed    int
}

type scheduled struct {
	msg raft.Message
	at  int64
	seq uint64
}

type Network struct {
	rng *rand.Rand

	now int64

	faults Faults

	queue []scheduled
	seq   uint64

	group map[raft.NodeID]int

	stats Stats
}

func NewNetwork(seed int64, faults Faults) (*Network, error) {
	if err := faults.validate(); err != nil {
		return nil, err
	}
	return &Network{
		rng:    rand.New(rand.NewSource(seed)),
		faults: faults,
	}, nil
}

func (n *Network) Now() int64 { return n.now }

func (n *Network) Advance() { n.now++ }

func (n *Network) Stats() Stats { return n.stats }

func (n *Network) SetFaults(f Faults) error {
	if err := f.validate(); err != nil {
		return err
	}
	n.faults = f
	return nil
}

func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.group = make(map[raft.NodeID]int)
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i
		}
	}
}

func (n *Network) Heal() { n.group = nil }

func (n *Network) reachable(from, to raft.NodeID) bool {
	if n.group == nil {
		return true
	}

	gf, okf := n.group[from]
	gt, okt := n.group[to]
	if !okf || !okt {
		return from == to
	}
	return gf == gt
}

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
			n.stats.Duplicated++
			n.schedule(m)
		}
	}
}

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

func (n *Network) InFlight() int { return len(n.queue) }

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
