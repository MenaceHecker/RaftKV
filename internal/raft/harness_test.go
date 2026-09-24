package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

const (
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
)

type cluster struct {
	t *testing.T

	ids      []NodeID
	nodes    map[NodeID]*Node
	storages map[NodeID]*MemoryStorage

	applied map[NodeID][]Entry

	readStates map[NodeID][]ReadState

	snapshots map[NodeID][]*Snapshot

	undeliverable int

	inflight []Message

	filter func(m Message) bool
}

type clusterOpts struct {
	electionTick   int
	heartbeatTick  int
	seed           int64
	maxAppendBytes int
	checkQuorum    bool
	preVote        bool
}

func newCluster(t *testing.T, size int, opts clusterOpts) *cluster {
	t.Helper()

	if opts.electionTick == 0 {
		opts.electionTick = defaultElectionTick
	}
	if opts.heartbeatTick == 0 {
		opts.heartbeatTick = defaultHeartbeatTick
	}

	ids := make([]NodeID, size)
	for i := range ids {
		ids[i] = NodeID(i + 1)
	}

	c := &cluster{
		t:          t,
		ids:        ids,
		nodes:      make(map[NodeID]*Node, size),
		storages:   make(map[NodeID]*MemoryStorage, size),
		applied:    make(map[NodeID][]Entry, size),
		readStates: make(map[NodeID][]ReadState, size),
		snapshots:  make(map[NodeID][]*Snapshot, size),
	}

	for _, id := range ids {
		storage := NewMemoryStorage()
		rng := rand.New(rand.NewSource(opts.seed + int64(id)*7919))

		node, err := NewNode(Config{
			ID:             id,
			Peers:          ids,
			ElectionTick:   opts.electionTick,
			HeartbeatTick:  opts.heartbeatTick,
			PreVote:        opts.preVote,
			CheckQuorum:    opts.checkQuorum,
			MaxAppendBytes: opts.maxAppendBytes,
			Storage:        storage,
			Rand:           rng,
		})
		if err != nil {
			t.Fatalf("creating node %d: %v", id, err)
		}

		c.nodes[id] = node
		c.storages[id] = storage
	}

	return c
}

func (c *cluster) node(id NodeID) *Node {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("no node %d in cluster", id)
	}
	return n
}

func (c *cluster) tick() {
	c.t.Helper()
	for _, id := range c.ids {
		if err := c.nodes[id].Tick(); err != nil {
			c.t.Fatalf("node %d tick: %v", id, err)
		}
	}
	c.deliverAll()
}

func (c *cluster) tickN(n int) {
	c.t.Helper()
	for range n {
		c.tick()
	}
}

func (c *cluster) campaign(id NodeID) {
	c.t.Helper()
	if err := c.node(id).Step(Message{Type: MsgCampaign}); err != nil {
		c.t.Fatalf("node %d campaign: %v", id, err)
	}
	c.deliverAll()
}

func (c *cluster) propose(id NodeID, data string) error {
	c.t.Helper()
	err := c.node(id).Propose([]byte(data))
	c.deliverAll()
	return err
}

func (c *cluster) collect() {
	c.t.Helper()
	for _, id := range c.ids {
		n := c.nodes[id]
		rd := n.Ready()
		if rd.IsEmpty() {
			continue
		}
		c.inflight = append(c.inflight, rd.Messages...)
		c.applied[id] = append(c.applied[id], rd.CommittedEntries...)
		c.readStates[id] = append(c.readStates[id], rd.ReadStates...)
		if rd.Snapshot != nil {
			c.snapshots[id] = append(c.snapshots[id], rd.Snapshot)
		}
		n.Advance(rd)
	}
}

func (c *cluster) deliverAll() {
	c.t.Helper()

	const maxRounds = 100
	for round := 0; ; round++ {
		if round > maxRounds {
			c.t.Fatalf("network did not settle after %d rounds (%d messages still in flight)",
				maxRounds, len(c.inflight))
		}

		c.collect()
		if len(c.inflight) == 0 {
			return
		}

		batch := c.inflight
		c.inflight = nil

		for _, m := range batch {
			if c.filter != nil && !c.filter(m) {
				continue
			}
			dst, ok := c.nodes[m.To]
			if !ok {
				c.undeliverable++
				continue
			}
			if err := dst.Step(m); err != nil {
				c.t.Fatalf("node %d stepping %s from %d: %v", m.To, m.Type, m.From, err)
			}
		}
	}
}

func (c *cluster) partition(groups ...[]NodeID) {
	c.t.Helper()

	group := make(map[NodeID]int, len(c.ids))
	for i, g := range groups {
		for _, id := range g {
			if _, dup := group[id]; dup {
				c.t.Fatalf("node %d appears in more than one partition group", id)
			}
			group[id] = i
		}
	}
	for _, id := range c.ids {
		if _, ok := group[id]; !ok {
			c.t.Fatalf("node %d is missing from the partition groups", id)
		}
	}

	c.filter = func(m Message) bool {
		return group[m.From] == group[m.To]
	}
}

func (c *cluster) heal() {
	c.filter = nil
}

func (c *cluster) restart(id NodeID, opts clusterOpts) {
	c.t.Helper()

	if opts.electionTick == 0 {
		opts.electionTick = defaultElectionTick
	}
	if opts.heartbeatTick == 0 {
		opts.heartbeatTick = defaultHeartbeatTick
	}

	node, err := NewNode(Config{
		ID:             id,
		Peers:          c.ids,
		ElectionTick:   opts.electionTick,
		HeartbeatTick:  opts.heartbeatTick,
		PreVote:        opts.preVote,
		CheckQuorum:    opts.checkQuorum,
		MaxAppendBytes: opts.maxAppendBytes,
		Storage:        c.storages[id],
		Rand:           rand.New(rand.NewSource(opts.seed + int64(id)*7919)),
	})
	if err != nil {
		c.t.Fatalf("restarting node %d: %v", id, err)
	}
	c.nodes[id] = node
}

func (c *cluster) leader() (NodeID, bool) {
	c.t.Helper()

	var best NodeID
	var bestTerm Term
	found := false

	for _, id := range c.ids {
		n := c.nodes[id]
		if n.State() != Leader {
			continue
		}
		switch {
		case !found || n.Term() > bestTerm:
			best, bestTerm, found = id, n.Term(), true
		case n.Term() == bestTerm:
			c.t.Fatalf("two leaders in term %d: nodes %d and %d", bestTerm, best, id)
		}
	}
	return best, found
}

func (c *cluster) mustLeader() NodeID {
	c.t.Helper()
	id, ok := c.leader()
	if !ok {
		c.t.Fatalf("expected a leader, but none exists\n%s", c.dump())
	}
	return id
}

func (c *cluster) awaitLeader(maxTicks int) NodeID {
	c.t.Helper()
	for range maxTicks {
		if id, ok := c.leader(); ok {
			return id
		}
		c.tick()
	}
	c.t.Fatalf("no leader elected within %d ticks\n%s", maxTicks, c.dump())
	return None
}

func (c *cluster) commands(id NodeID) []string {
	out := []string{}
	for _, e := range c.applied[id] {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func (c *cluster) assertAppliedConsistent() {
	c.t.Helper()

	for i := 1; i < len(c.ids); i++ {
		a, b := c.ids[0], c.ids[i]
		ea, eb := c.applied[a], c.applied[b]

		n := min(len(ea), len(eb))
		for j := range n {
			if ea[j].Index != eb[j].Index || ea[j].Term != eb[j].Term ||
				string(ea[j].Data) != string(eb[j].Data) {
				c.t.Fatalf("nodes %d and %d applied different entries at position %d: %+v vs %+v\n%s",
					a, b, j, ea[j], eb[j], c.dump())
			}
		}
	}
}

func (c *cluster) assertCommitted(id NodeID, i Index) {
	c.t.Helper()
	if got := c.node(id).CommitIndex(); got < i {
		c.t.Fatalf("node %d committed through %d, expected at least %d\n%s",
			id, got, i, c.dump())
	}
}

func (c *cluster) countCommitted(i Index) int {
	count := 0
	for _, id := range c.ids {
		if c.nodes[id].CommitIndex() >= i {
			count++
		}
	}
	return count
}

func (c *cluster) logEntries(id NodeID) []Entry {
	c.t.Helper()
	s := c.storages[id]
	entries, err := s.Entries(s.FirstIndex(), s.LastIndex()+1)
	if err != nil {
		c.t.Fatalf("reading log of node %d: %v", id, err)
	}
	return entries
}

func (c *cluster) dump() string {
	var b []byte
	b = append(b, "cluster state:\n"...)

	ids := append([]NodeID(nil), c.ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		n := c.nodes[id]
		hs, _ := c.storages[id].InitialState()
		b = append(b, fmt.Sprintf(
			"  node %d: state=%-9s term=%d vote=%d leader=%d last=%d commit=%d applied=%v\n",
			id, n.State(), n.Term(), hs.VotedFor, n.Leader(),
			n.LastIndex(), n.CommitIndex(), c.commands(id),
		)...)
	}
	if len(c.inflight) > 0 {
		b = append(b, fmt.Sprintf("  %d messages still in flight\n", len(c.inflight))...)
	}
	return string(b)
}
