package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// A deterministic, single-process test harness for whole clusters.
//
// Everything the real system would get from the outside world is supplied here
// instead: logical time comes from Tick, the network is a slice of in-flight
// messages, and the randomized election timeouts come from a seeded source. No
// goroutines, no sockets, no wall clock. A run is therefore reproducible — a
// failure replays exactly, which is the property that makes the Phase 5 chaos
// scenarios worth trusting.
//
// The network is honest by default: every message is delivered, exactly once,
// in the order it was produced. Tests that want partitions or loss install a
// filter.

// defaultElectionTick and defaultHeartbeatTick are the timings used unless a
// test overrides them. The ratio matters more than the values: a leader must
// get several heartbeats out inside the shortest possible election timeout.
const (
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
)

// cluster is a set of nodes wired to a simulated network.
type cluster struct {
	t *testing.T

	ids      []NodeID
	nodes    map[NodeID]*Node
	storages map[NodeID]*MemoryStorage

	// applied records the committed entries each node has handed to its
	// state machine, in order. Comparing these across nodes is how the tests
	// check State Machine Safety: every node must apply the same commands in
	// the same order.
	applied map[NodeID][]Entry

	// inflight holds messages produced but not yet delivered.
	inflight []Message

	// filter decides whether a message is delivered. Returning false drops
	// it, which is how partitions and message loss are simulated. Nil means
	// deliver everything.
	filter func(m Message) bool
}

// clusterOpts configures a test cluster.
type clusterOpts struct {
	electionTick  int
	heartbeatTick int
	// seed drives the randomized election timeouts. A fixed seed makes the
	// whole run reproducible; changing it explores different timings.
	seed int64
}

// newCluster builds a cluster of size nodes with IDs 1..size.
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
		t:        t,
		ids:      ids,
		nodes:    make(map[NodeID]*Node, size),
		storages: make(map[NodeID]*MemoryStorage, size),
		applied:  make(map[NodeID][]Entry, size),
	}

	for _, id := range ids {
		storage := NewMemoryStorage()
		// Each node draws from its own source, seeded distinctly, so their
		// election timeouts differ the way real clocks would. Deriving the
		// seed from the run seed keeps that reproducible.
		rng := rand.New(rand.NewSource(opts.seed + int64(id)*7919))

		node, err := NewNode(Config{
			ID:            id,
			Peers:         ids,
			ElectionTick:  opts.electionTick,
			HeartbeatTick: opts.heartbeatTick,
			Storage:       storage,
			Rand:          rng,
		})
		if err != nil {
			t.Fatalf("creating node %d: %v", id, err)
		}

		c.nodes[id] = node
		c.storages[id] = storage
	}

	return c
}

// node returns a node by ID, failing the test if it does not exist.
func (c *cluster) node(id NodeID) *Node {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("no node %d in cluster", id)
	}
	return n
}

// tick advances every node by one unit of logical time, then runs the network
// until it goes quiet.
//
// Nodes are ticked in ID order rather than map order, because Go randomizes map
// iteration and that would make runs irreproducible.
func (c *cluster) tick() {
	c.t.Helper()
	for _, id := range c.ids {
		if err := c.nodes[id].Tick(); err != nil {
			c.t.Fatalf("node %d tick: %v", id, err)
		}
	}
	c.deliverAll()
}

// tickN advances the cluster by n ticks.
func (c *cluster) tickN(n int) {
	c.t.Helper()
	for range n {
		c.tick()
	}
}

// campaign forces a node to start an election immediately, instead of waiting
// out its timeout. Tests use it to control exactly who campaigns and when.
func (c *cluster) campaign(id NodeID) {
	c.t.Helper()
	if err := c.node(id).Step(Message{Type: MsgCampaign}); err != nil {
		c.t.Fatalf("node %d campaign: %v", id, err)
	}
	c.deliverAll()
}

// propose submits a command to a node and runs the network until quiet. It
// returns the node's error, since a test may be checking for ErrNotLeader.
func (c *cluster) propose(id NodeID, data string) error {
	c.t.Helper()
	err := c.node(id).Propose([]byte(data))
	c.deliverAll()
	return err
}

// collect drains every node's Ready, queues the outbound messages, and records
// the newly applied entries.
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
		// Acknowledging only after recording mirrors the real contract: a
		// caller advances once the entries are safely applied.
		n.Advance(rd)
	}
}

// deliverAll runs the network to quiescence: collect what the nodes produced,
// deliver it, collect what that produced, and so on until nothing is left.
//
// The bound is a safety net for a bug that makes two nodes talk forever. Real
// convergence takes a handful of rounds, so tripping it means something is
// wrong rather than merely slow.
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

		// Take the whole batch and deliver it in production order. Messages
		// generated during delivery go to the next round, which keeps the
		// ordering deterministic.
		batch := c.inflight
		c.inflight = nil

		for _, m := range batch {
			if c.filter != nil && !c.filter(m) {
				continue
			}
			dst, ok := c.nodes[m.To]
			if !ok {
				c.t.Fatalf("message addressed to unknown node %d", m.To)
			}
			if err := dst.Step(m); err != nil {
				c.t.Fatalf("node %d stepping %s from %d: %v", m.To, m.Type, m.From, err)
			}
		}
	}
}

// partition splits the cluster into isolated groups. Messages within a group
// flow normally; messages crossing a group boundary are dropped, which is what
// a network partition looks like from a node's point of view.
//
// Every node must appear in exactly one group.
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

// heal removes any partition, restoring full connectivity.
func (c *cluster) heal() {
	c.filter = nil
}

// restart rebuilds a node from its persisted storage, simulating a crash and
// recovery. Volatile state is lost; the hard state and log survive, since
// those are what Storage is responsible for.
func (c *cluster) restart(id NodeID, opts clusterOpts) {
	c.t.Helper()

	if opts.electionTick == 0 {
		opts.electionTick = defaultElectionTick
	}
	if opts.heartbeatTick == 0 {
		opts.heartbeatTick = defaultHeartbeatTick
	}

	node, err := NewNode(Config{
		ID:            id,
		Peers:         c.ids,
		ElectionTick:  opts.electionTick,
		HeartbeatTick: opts.heartbeatTick,
		Storage:       c.storages[id],
		Rand:          rand.New(rand.NewSource(opts.seed + int64(id)*7919)),
	})
	if err != nil {
		c.t.Fatalf("restarting node %d: %v", id, err)
	}
	c.nodes[id] = node
}

// leader returns the single node that considers itself leader.
//
// It fails the test if there is more than one leader in the same term, which
// would be a direct violation of Election Safety. Two leaders in *different*
// terms is legitimate and transient: a deposed leader that has not yet heard
// about the new term still believes it is in charge, so the highest term wins.
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

// mustLeader returns the current leader, failing the test if there is none.
func (c *cluster) mustLeader() NodeID {
	c.t.Helper()
	id, ok := c.leader()
	if !ok {
		c.t.Fatalf("expected a leader, but none exists\n%s", c.dump())
	}
	return id
}

// awaitLeader ticks until a leader emerges, up to a bound.
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

// commands returns the data of the normal entries a node has applied, skipping
// the no-op entries leaders append on election. This is the node's view of the
// replicated state machine's input.
func (c *cluster) commands(id NodeID) []string {
	out := []string{}
	for _, e := range c.applied[id] {
		if e.Type == EntryNormal {
			out = append(out, string(e.Data))
		}
	}
	return out
}

// assertAppliedConsistent checks State Machine Safety across the cluster: no
// two nodes may apply different entries at the same index.
//
// Nodes are allowed to be at different points in the log — a slow follower has
// simply applied less — so this compares only the overlapping prefix.
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

// assertCommitted checks that a node has committed at least through index i.
func (c *cluster) assertCommitted(id NodeID, i Index) {
	c.t.Helper()
	if got := c.node(id).CommitIndex(); got < i {
		c.t.Fatalf("node %d committed through %d, expected at least %d\n%s",
			id, got, i, c.dump())
	}
}

// countCommitted reports how many nodes have committed through index i, which
// is how a test checks that a majority — not merely the leader — holds an entry.
func (c *cluster) countCommitted(i Index) int {
	count := 0
	for _, id := range c.ids {
		if c.nodes[id].CommitIndex() >= i {
			count++
		}
	}
	return count
}

// logEntries returns everything in a node's log, for assertions and dumps.
func (c *cluster) logEntries(id NodeID) []Entry {
	c.t.Helper()
	s := c.storages[id]
	entries, err := s.Entries(s.FirstIndex(), s.LastIndex()+1)
	if err != nil {
		c.t.Fatalf("reading log of node %d: %v", id, err)
	}
	return entries
}

// dump renders the whole cluster as a table. It is attached to failure
// messages, so a broken invariant is diagnosable from the test output alone
// rather than needing a re-run under a debugger.
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
