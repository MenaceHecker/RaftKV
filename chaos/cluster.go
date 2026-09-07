package chaos

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
)

// A cluster of real Raft nodes driven over the fault-injecting network.
//
// The nodes are the same ones the rest of the system uses. Nothing here is a
// model or a stand-in: the consensus core, the log semantics, and the state
// machine are the production code, and only the clock and the network are
// simulated. A harness that tested a simplified reimplementation would prove
// something about the reimplementation.

// OpKind distinguishes a read from a write in a recorded history.
type OpKind int

const (
	// OpWrite sets a key to a value.
	OpWrite OpKind = iota
	// OpRead returns whatever the key holds.
	OpRead
)

func (k OpKind) String() string {
	if k == OpWrite {
		return "write"
	}
	return "read"
}

// OpStatus is what became of an operation, and it is the distinction the
// linearizability checker depends on most.
//
// A client that never hears back cannot tell a request that was lost from one
// that was applied and whose reply was lost. Recording that as a failure would
// let the checker rule out an ordering that actually happened; recording it as
// a success would invent one that did not. It has to be its own answer.
type OpStatus int

const (
	// StatusPending means the operation has been invoked and has not returned.
	StatusPending OpStatus = iota
	// StatusOK means it completed and its effect definitely happened.
	StatusOK
	// StatusFailed means it definitely did not happen — refused before it
	// reached the log at all.
	StatusFailed
	// StatusUnknown means it may or may not have happened. The checker must
	// consider both.
	StatusUnknown
)

func (s OpStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusOK:
		return "ok"
	case StatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Op is one client operation as observed from outside the cluster.
//
// The times are the only thing the checker may assume about ordering: an
// operation that returned before another was invoked must be ordered before
// it, and operations that overlap may be ordered either way.
type Op struct {
	Kind   OpKind
	Client int
	Key    string

	// Value is what a write wrote, or what a read observed.
	Value string
	// Found reports whether a read found the key at all.
	Found bool

	Invoked  int64
	Returned int64
	Status   OpStatus

	// index and term identify the log entry a write is waiting on. A different
	// entry appearing at that index means the write was overwritten by a new
	// leader, and its outcome becomes unknown.
	index raft.Index
	term  raft.Term

	// readCtx identifies an in-flight read-index round.
	readCtx string

	// readIndex is the index a read must observe before it may be answered.
	readIndex raft.Index
	readReady bool

	node raft.NodeID
}

// Config describes a chaos cluster.
type Config struct {
	// Nodes is how many members the cluster has.
	Nodes int
	// Seed drives both the network and the nodes' election timeouts, so a
	// whole run is reproducible from this one number.
	Seed int64
	// Faults is the initial network behaviour.
	Faults Faults
	// ElectionTick and HeartbeatTick are in simulated ticks.
	ElectionTick  int
	HeartbeatTick int
}

func (c *Config) applyDefaults() {
	if c.Nodes == 0 {
		c.Nodes = 3
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
}

// Cluster is a set of Raft nodes, their state machines, and the client
// operations in flight against them.
type Cluster struct {
	cfg Config
	net *Network

	ids      []raft.NodeID
	nodes    map[raft.NodeID]*raft.Node
	machines map[raft.NodeID]*statemachine.KV

	// storages outlive a crash. They stand in for the write-ahead log: a
	// crashed node loses its in-memory state and rebuilds from here, which is
	// exactly what a real restart does.
	storages map[raft.NodeID]*raft.MemoryStorage

	// down records which nodes are crashed. A crashed node neither ticks nor
	// receives, and anything already on its way to it is discarded.
	down map[raft.NodeID]bool

	// pending holds operations awaiting an answer, and history holds every
	// operation ever invoked, in invocation order.
	pending []*Op
	history []*Op

	seq int
}

// NewCluster starts a cluster on a fresh network.
func NewCluster(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()

	net, err := NewNetwork(cfg.Seed, cfg.Faults)
	if err != nil {
		return nil, err
	}

	c := &Cluster{
		cfg:      cfg,
		net:      net,
		nodes:    make(map[raft.NodeID]*raft.Node, cfg.Nodes),
		machines: make(map[raft.NodeID]*statemachine.KV, cfg.Nodes),
		storages: make(map[raft.NodeID]*raft.MemoryStorage, cfg.Nodes),
		down:     make(map[raft.NodeID]bool, cfg.Nodes),
	}
	for i := range cfg.Nodes {
		c.ids = append(c.ids, raft.NodeID(i+1))
	}

	for _, id := range c.ids {
		c.storages[id] = raft.NewMemoryStorage()
		if err := c.start(id); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// start builds a node over its existing storage. It is used both for the
// initial launch and for a restart after a crash.
func (c *Cluster) start(id raft.NodeID) error {
	n, err := raft.NewNode(raft.Config{
		ID:            id,
		Peers:         c.ids,
		ElectionTick:  c.cfg.ElectionTick,
		HeartbeatTick: c.cfg.HeartbeatTick,
		Storage:       c.storages[id],
		// Each node draws from its own source so their election timeouts
		// differ the way real clocks would, while staying derived from the run
		// seed so the whole thing remains reproducible.
		Rand: rand.New(rand.NewSource(c.cfg.Seed + int64(id)*7919)),
	})
	if err != nil {
		return fmt.Errorf("chaos: starting node %d: %w", id, err)
	}

	c.nodes[id] = n
	// The state machine is volatile. A restarted node rebuilds it by replaying
	// the log, which is what makes a crash a real test of recovery rather than
	// a pause.
	c.machines[id] = statemachine.New()
	c.down[id] = false
	return nil
}

// Network exposes the network, so a scenario can partition, heal, or change
// the fault configuration mid-run.
func (c *Cluster) Network() *Network { return c.net }

// IDs returns the member IDs.
func (c *Cluster) IDs() []raft.NodeID { return c.ids }

// Now returns the current tick.
func (c *Cluster) Now() int64 { return c.net.Now() }

// History returns every operation invoked so far, in invocation order.
func (c *Cluster) History() []Op {
	out := make([]Op, 0, len(c.history))
	for _, op := range c.history {
		out = append(out, *op)
	}
	return out
}

// Tick advances the cluster by one unit of simulated time.
//
// The order within a tick is fixed: deliver what is due, let every live node
// act on it, collect what they produced, then resolve any client operations
// that completed. Fixing it is what makes a run reproducible; any dependence
// on map iteration or arrival order would not be.
func (c *Cluster) Tick() error {
	for _, m := range c.net.Deliver() {
		n, ok := c.nodes[m.To]
		if !ok || c.down[m.To] {
			// The destination is crashed. Its messages are lost, which is what
			// happens to anything in flight to a process that has died.
			continue
		}
		// A message the core rejects is not fatal here: a node that has moved
		// on legitimately refuses stale traffic, and the sender retries.
		_ = n.Step(m)
	}

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if err := c.nodes[id].Tick(); err != nil {
			return fmt.Errorf("chaos: node %d tick: %w", id, err)
		}
	}

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if err := c.drain(id); err != nil {
			return err
		}
	}

	c.resolve()
	c.net.Advance()
	return nil
}

// TickN advances the cluster by n ticks.
func (c *Cluster) TickN(n int) error {
	for range n {
		if err := c.Tick(); err != nil {
			return err
		}
	}
	return nil
}

// drain collects one node's outbound effects and applies its committed entries.
func (c *Cluster) drain(id raft.NodeID) error {
	n := c.nodes[id]
	rd := n.Ready()
	if rd.IsEmpty() {
		return nil
	}

	c.net.Send(rd.Messages)

	if rd.Snapshot != nil {
		if err := c.machines[id].Restore(rd.Snapshot.Data); err != nil {
			return fmt.Errorf("chaos: node %d restoring snapshot: %w", id, err)
		}
	}

	for _, e := range rd.CommittedEntries {
		if err := c.machines[id].Apply(e); err != nil {
			return fmt.Errorf("chaos: node %d applying entry %d: %w", id, e.Index, err)
		}
	}

	for _, rs := range rd.ReadStates {
		c.noteReadIndex(id, rs)
	}

	n.Advance(rd)
	return nil
}

// Crash stops a node abruptly.
//
// Its in-memory state is discarded and everything in flight to it is dropped,
// which is what a kill leaves behind. Its storage survives, so the restart has
// something real to recover from.
func (c *Cluster) Crash(id raft.NodeID) {
	if c.down[id] {
		return
	}
	c.down[id] = true
	delete(c.nodes, id)
	delete(c.machines, id)
	c.net.DropAllInFlight(id)

	// Anything this node was waiting on will never be answered by it. The
	// outcome is unknown rather than failed: a write it had already replicated
	// can still commit through the others.
	for _, op := range c.pending {
		if op.node == id && op.Status == StatusPending {
			c.finish(op, StatusUnknown)
		}
	}
}

// Restart brings a crashed node back, rebuilding it from its storage.
func (c *Cluster) Restart(id raft.NodeID) error {
	if !c.down[id] {
		return nil
	}
	return c.start(id)
}

// IsDown reports whether a node is crashed.
func (c *Cluster) IsDown(id raft.NodeID) bool { return c.down[id] }

// Leader returns the node that considers itself leader in the highest term.
//
// Two leaders in the same term would be an Election Safety violation, so that
// is reported as an error rather than resolved. Two leaders in different terms
// is legitimate and transient: a deposed leader that has not yet heard about
// the new term still believes it leads.
func (c *Cluster) Leader() (raft.NodeID, bool, error) {
	var best raft.NodeID
	var bestTerm raft.Term
	found := false

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.nodes[id]
		if n.State() != raft.Leader {
			continue
		}
		switch {
		case !found || n.Term() > bestTerm:
			best, bestTerm, found = id, n.Term(), true
		case n.Term() == bestTerm:
			return 0, false, fmt.Errorf(
				"chaos: election safety violated: nodes %d and %d both lead term %d",
				best, id, bestTerm)
		}
	}
	return best, found, nil
}

// AwaitLeader ticks until a leader emerges, up to a bound.
func (c *Cluster) AwaitLeader(maxTicks int) (raft.NodeID, error) {
	for range maxTicks {
		id, ok, err := c.Leader()
		if err != nil {
			return 0, err
		}
		if ok {
			return id, nil
		}
		if err := c.Tick(); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("chaos: no leader within %d ticks\n%s", maxTicks, c.Dump())
}

// Write invokes a write and returns immediately.
//
// The operation completes during a later Tick, which is what lets several be
// in flight at once. Concurrency is the whole point: a history where every
// operation returned before the next was invoked has only one possible
// ordering and proves nothing about linearizability.
func (c *Cluster) Write(client int, key, value string) *Op {
	op := &Op{
		Kind:    OpWrite,
		Client:  client,
		Key:     key,
		Value:   value,
		Invoked: c.net.Now(),
		Status:  StatusPending,
	}
	c.history = append(c.history, op)

	id, ok, err := c.Leader()
	if err != nil || !ok || c.down[id] {
		// Nobody could have accepted it, so it definitely did not happen.
		c.finish(op, StatusFailed)
		return op
	}

	n := c.nodes[id]
	c.seq++
	cmd := statemachine.Command{
		ClientID: uint64(client),
		Seq:      uint64(c.seq),
		Op:       statemachine.OpPut,
		Key:      key,
		Value:    []byte(value),
	}

	before := n.LastIndex()
	if err := n.Propose(cmd.Encode()); err != nil {
		c.finish(op, StatusFailed)
		return op
	}
	if n.LastIndex() == before {
		c.finish(op, StatusFailed)
		return op
	}

	op.node = id
	op.index = n.LastIndex()
	op.term = n.Term()
	c.pending = append(c.pending, op)
	return op
}

// ReadFrom invokes a read against one specific node.
//
// This exists because Read always picks the node with the highest term, which
// is not what a client does. A real client remembers where the leader was and
// keeps asking there until it is redirected — so it can and does end up
// talking to a node that still believes it leads but has been partitioned
// away. That node is exactly where a stale read would come from, and steering
// every read to the best available leader hides the case entirely.
//
// A node that knows it is not the leader refuses, which is the redirect a real
// client would follow.
func (c *Cluster) ReadFrom(client int, node raft.NodeID, key string) *Op {
	op := &Op{
		Kind:    OpRead,
		Client:  client,
		Key:     key,
		Invoked: c.net.Now(),
		Status:  StatusPending,
	}
	c.history = append(c.history, op)

	n, up := c.nodes[node]
	if !up || c.down[node] {
		c.finish(op, StatusFailed)
		return op
	}

	c.seq++
	ctx := fmt.Sprintf("r%d", c.seq)
	if err := n.ReadIndex([]byte(ctx)); err != nil {
		// Refused: not the leader, or leading a term it has not committed in.
		c.finish(op, StatusFailed)
		return op
	}

	op.node = node
	op.readCtx = ctx
	c.pending = append(c.pending, op)
	return op
}

// WriteTo invokes a write against one specific node, for the same reason
// ReadFrom exists.
func (c *Cluster) WriteTo(client int, node raft.NodeID, key, value string) *Op {
	op := &Op{
		Kind:    OpWrite,
		Client:  client,
		Key:     key,
		Value:   value,
		Invoked: c.net.Now(),
		Status:  StatusPending,
	}
	c.history = append(c.history, op)

	n, up := c.nodes[node]
	if !up || c.down[node] {
		c.finish(op, StatusFailed)
		return op
	}

	c.seq++
	cmd := statemachine.Command{
		ClientID: uint64(client),
		Seq:      uint64(c.seq),
		Op:       statemachine.OpPut,
		Key:      key,
		Value:    []byte(value),
	}

	before := n.LastIndex()
	if err := n.Propose(cmd.Encode()); err != nil {
		c.finish(op, StatusFailed)
		return op
	}
	if n.LastIndex() == before {
		c.finish(op, StatusFailed)
		return op
	}

	op.node = node
	op.index = n.LastIndex()
	op.term = n.Term()
	c.pending = append(c.pending, op)
	return op
}

// Read invokes a linearizable read and returns immediately.
//
// It goes through the read-index protocol, so it is answered only once a
// majority has confirmed the serving node is still leader and the state
// machine has applied through the recorded index.
func (c *Cluster) Read(client int, key string) *Op {
	op := &Op{
		Kind:    OpRead,
		Client:  client,
		Key:     key,
		Invoked: c.net.Now(),
		Status:  StatusPending,
	}
	c.history = append(c.history, op)

	id, ok, err := c.Leader()
	if err != nil || !ok || c.down[id] {
		c.finish(op, StatusFailed)
		return op
	}

	c.seq++
	ctx := fmt.Sprintf("r%d", c.seq)
	if err := c.nodes[id].ReadIndex([]byte(ctx)); err != nil {
		// A leader that has not yet committed in its term cannot serve a read.
		// That is transient and the read simply did not happen.
		c.finish(op, StatusFailed)
		return op
	}

	op.node = id
	op.readCtx = ctx
	c.pending = append(c.pending, op)
	return op
}

// noteReadIndex records a confirmed read index against the operation waiting
// for it.
func (c *Cluster) noteReadIndex(id raft.NodeID, rs raft.ReadState) {
	for _, op := range c.pending {
		if op.Kind == OpRead && op.node == id && op.readCtx == string(rs.Context) {
			op.readIndex = rs.Index
			op.readReady = true
			return
		}
	}
}

// resolve completes every pending operation whose outcome is now known.
func (c *Cluster) resolve() {
	remaining := c.pending[:0]

	for _, op := range c.pending {
		if op.Status != StatusPending {
			continue
		}

		// The node serving this operation may have crashed, in which case
		// nothing will ever answer it here.
		n, up := c.nodes[op.node]
		if !up || c.down[op.node] {
			c.finish(op, StatusUnknown)
			continue
		}

		if op.Kind == OpWrite {
			if !c.resolveWrite(op, n) {
				remaining = append(remaining, op)
			}
			continue
		}
		if !c.resolveRead(op, n) {
			remaining = append(remaining, op)
		}
	}

	c.pending = remaining
}

// resolveWrite completes a write once its entry has been applied, and reports
// whether it is now settled.
func (c *Cluster) resolveWrite(op *Op, n *raft.Node) bool {
	kv := c.machines[op.node]
	if kv.Applied() < op.index {
		return false
	}

	// The entry at that index must still be the one this write appended. A
	// different term there means a new leader overwrote it, and the write's
	// outcome is genuinely unknown: it may have been applied elsewhere before
	// being replaced, or never at all.
	term, err := n.TermAt(op.index)
	if err != nil || term != op.term {
		c.finish(op, StatusUnknown)
		return true
	}

	c.finish(op, StatusOK)
	return true
}

// resolveRead completes a read once its index is confirmed and applied.
func (c *Cluster) resolveRead(op *Op, n *raft.Node) bool {
	if !op.readReady {
		return false
	}
	kv := c.machines[op.node]
	if kv.Applied() < op.readIndex {
		return false
	}

	value, found := kv.Get(op.Key)
	op.Value = string(value)
	op.Found = found
	c.finish(op, StatusOK)
	return true
}

// finish stamps an operation's outcome.
func (c *Cluster) finish(op *Op, status OpStatus) {
	op.Status = status
	op.Returned = c.net.Now()
}

// FailPending marks every operation still in flight as unknown.
//
// A scenario calls this when it stops ticking: an operation that never
// returned is not a failure, and treating it as one would let the checker rule
// out an ordering that really happened.
func (c *Cluster) FailPending() {
	for _, op := range c.pending {
		if op.Status == StatusPending {
			c.finish(op, StatusUnknown)
		}
	}
	c.pending = nil
}

// Converged reports whether every live node has applied the same entries.
//
// The state machines are compared by snapshot, which is a deterministic
// function of the state — so equal bytes mean genuinely equal state rather
// than merely equal indexes.
func (c *Cluster) Converged() (bool, error) {
	var reference []byte
	var refID raft.NodeID

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		snap, err := c.machines[id].Snapshot()
		if err != nil {
			return false, fmt.Errorf("chaos: snapshotting node %d: %w", id, err)
		}
		if reference == nil {
			reference, refID = snap, id
			continue
		}
		if string(snap) != string(reference) {
			return false, nil
		}
		_ = refID
	}
	return true, nil
}

// writeCommand encodes a state machine write, for tests that need to inject
// state directly.
func writeCommand(key, value string) []byte {
	return statemachine.Command{
		Op: statemachine.OpPut, Key: key, Value: []byte(value),
	}.Encode()
}

// Dump renders the cluster for a failure message.
func (c *Cluster) Dump() string {
	ids := append([]raft.NodeID(nil), c.ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := fmt.Sprintf("cluster at tick %d:\n", c.net.Now())
	for _, id := range ids {
		if c.down[id] {
			out += fmt.Sprintf("  node %d: CRASHED\n", id)
			continue
		}
		n := c.nodes[id]
		out += fmt.Sprintf("  node %d: state=%-9s term=%d leader=%d last=%d commit=%d applied=%d keys=%d\n",
			id, n.State(), n.Term(), n.Leader(), n.LastIndex(), n.CommitIndex(),
			c.machines[id].Applied(), c.machines[id].Len())
	}

	st := c.net.Stats()
	out += fmt.Sprintf("  network: sent=%d delivered=%d dropped=%d partitioned=%d duplicated=%d delayed=%d inflight=%d\n",
		st.Sent, st.Delivered, st.Dropped, st.Partitions, st.Duplicated, st.Delayed, c.net.InFlight())
	out += fmt.Sprintf("  operations: %d invoked, %d pending\n", len(c.history), len(c.pending))
	return out
}
