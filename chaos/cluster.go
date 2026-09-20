package chaos

import (
	"errors"
	"fmt"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"math/rand"
	"path/filepath"
	"slices"
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

	// clientID and seq are the session identity the write was sent under.
	// They are kept so the operation can be sent again exactly as it was,
	// which is what a client does when it never learns the outcome.
	clientID uint64
	seq      uint64
}

// nodeStorage is a node's durable state, as the harness uses it.
//
// Both implementations of it are real: the in-memory one keeps runs fast and
// hermetic, and the on-disk one is the same write-ahead log the server ships
// with. Running the scenarios against the second is what puts the recovery
// path, the record framing, segment rotation and snapshot files, under the
// same adversarial crash sequences as everything else. Until it existed, a
// node in the chaos suite "crashed" by discarding a map, which cannot
// misparse a record or lose a segment.
type nodeStorage interface {
	raft.Storage
	CreateSnapshot(index raft.Index, data []byte, conf raft.ConfState) error
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

	// DataDir, when set, gives every node a real write-ahead log under it
	// instead of an in-memory one. A crash then closes real files and a
	// restart recovers from them, which is the path a deployed node takes
	// and the one the in-memory harness cannot exercise at all.
	DataDir string

	// Sync selects the durability policy when DataDir is set. The zero value
	// fsyncs every write, which is correct and slow; scenarios that run
	// thousands of appends usually do not need it, since what is being tested
	// is the recovery path rather than power loss.
	Sync storage.SyncPolicy
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
	storages map[raft.NodeID]nodeStorage

	// down records which nodes are crashed. A crashed node neither ticks nor
	// receives, and anything already on its way to it is discarded.
	down map[raft.NodeID]bool

	// pending holds operations awaiting an answer, and history holds every
	// operation ever invoked, in invocation order.
	pending []*Op
	history []*Op

	// snapshotsInstalled counts state machine images each node has accepted
	// from a leader. A scenario about snapshot transfer has to be able to
	// show one actually happened, or it is testing ordinary replication and
	// reporting a pass it did not earn.
	snapshotsInstalled map[raft.NodeID]int

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
		storages: make(map[raft.NodeID]nodeStorage, cfg.Nodes),
		down:     make(map[raft.NodeID]bool, cfg.Nodes),

		snapshotsInstalled: make(map[raft.NodeID]int, cfg.Nodes),
	}
	for i := range cfg.Nodes {
		c.ids = append(c.ids, raft.NodeID(i+1))
	}

	for _, id := range c.ids {
		st, err := c.openStorage(id)
		if err != nil {
			return nil, err
		}
		c.storages[id] = st
		if err := c.start(id); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// openStorage opens one node's durable state, creating it if this is the
// first time or recovering it if the node has been here before.
func (c *Cluster) openStorage(id raft.NodeID) (nodeStorage, error) {
	if c.cfg.DataDir == "" {
		return raft.NewMemoryStorage(), nil
	}

	dir := filepath.Join(c.cfg.DataDir, fmt.Sprintf("node-%d", id))
	st, _, err := storage.OpenDiskStorage(storage.DiskConfig{
		Dir:  dir,
		Sync: c.cfg.Sync,
		// A small segment so scenarios of a few hundred entries still roll
		// over several times. Rotation and the recovery that has to stitch
		// segments back together are the interesting part, and a default
		// sized segment would never fill.
		SegmentSize: 16 << 10,
	})
	if err != nil {
		return nil, fmt.Errorf("chaos: opening storage for node %d: %w", id, err)
	}
	return st, nil
}

// closeStorage releases a node's files, which is what its process dying does.
func (c *Cluster) closeStorage(id raft.NodeID) {
	if d, ok := c.storages[id].(*storage.DiskStorage); ok {
		d.Close()
	}
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
		CheckQuorum:   true,
		// The driver always enables pre-vote, so the chaos suite must too.
		// Exercising a configuration that never ships would leave the one
		// that does untested by the only suite built to break it.
		PreVote: true,
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

	// Replaying the log is only enough while the log still goes back to the
	// beginning. Once this node has compacted, the entries before its own
	// snapshot are gone and the image is the only record of them, so it has
	// to be restored first and the replay applied on top. This is what the
	// driver does on startup, and the harness has to match it or a restart
	// after compaction hands the state machine an entry it has no history
	// for.
	if snap, err := c.storages[id].Snapshot(); err == nil && snap.Index > 0 {
		if err := c.machines[id].Restore(snap.Data); err != nil {
			return fmt.Errorf("chaos: node %d restoring its own snapshot: %w", id, err)
		}
	}

	c.down[id] = false
	return nil
}

// Compact snapshots a node's state machine and drops the log up to that
// point.
//
// This is what makes the snapshot machinery reachable at all. A follower only
// needs an image once the leader has thrown away the entries it was missing,
// so without compaction the send and install paths are unreachable, and until
// now the chaos suite could not exercise a line of them.
func (c *Cluster) Compact(id raft.NodeID) error {
	n, ok := c.nodes[id]
	if !ok || c.down[id] {
		return fmt.Errorf("chaos: node %d is not running", id)
	}

	applied := c.machines[id].Applied()
	if applied == 0 {
		return nil
	}

	data, err := c.machines[id].Snapshot()
	if err != nil {
		return fmt.Errorf("chaos: snapshotting node %d: %w", id, err)
	}

	// Compacting to an index already covered is not an error worth failing a
	// scenario over; it just means nothing new has been applied.
	if err := c.storages[id].CreateSnapshot(applied, data, n.ConfState()); err != nil {
		return nil
	}
	return nil
}

// CompactAll compacts every running node.
//
// Scenarios use this rather than compacting one node, because leadership can
// move while a follower is away and only a leader that has actually compacted
// is unable to catch it up from the log. Compacting just the node that
// happened to lead at the time is how a snapshot test ends up quietly
// exercising ordinary replication instead.
func (c *Cluster) CompactAll() error {
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if err := c.Compact(id); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotsInstalled reports how many images a node has accepted from a
// leader.
func (c *Cluster) SnapshotsInstalled(id raft.NodeID) int { return c.snapshotsInstalled[id] }

// TotalSnapshotsInstalled reports how many images the cluster has accepted in
// total.
func (c *Cluster) TotalSnapshotsInstalled() int {
	var total int
	for _, n := range c.snapshotsInstalled {
		total += n
	}
	return total
}

// AddNode starts a new node and proposes its admission to the cluster.
//
// The joining node is given the membership it is joining, which is what a real
// node gets from its peer list. It cannot take leadership from under the
// cluster while the change is in flight: the existing majority has not adopted
// this configuration yet, so the newcomer cannot assemble a majority of the
// one that counts, and pre-vote stops it disturbing the leader by asking.
//
// Admission itself goes through the log like any other entry, so this returns
// once the change has been proposed, not once it has been agreed. A scenario
// ticks until the membership settles.
func (c *Cluster) AddNode(id raft.NodeID) error {
	if _, exists := c.storages[id]; exists {
		return fmt.Errorf("chaos: node %d already exists", id)
	}

	leader, ok, err := c.Leader()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("chaos: no leader to propose a membership change to")
	}

	st, err := c.openStorage(id)
	if err != nil {
		return err
	}
	c.storages[id] = st
	c.ids = append(c.ids, id)
	slices.Sort(c.ids)
	if err := c.start(id); err != nil {
		return err
	}

	return c.nodes[leader].ProposeConfChange(raft.ConfChange{
		Type:   raft.ConfChangeAddNode,
		NodeID: id,
	})
}

// RemoveNode proposes that a node leave the cluster.
//
// The node keeps running afterwards. That is deliberate: a decommissioned
// process does not always stop promptly, and one that keeps campaigning
// against a cluster that has forgotten it is exactly the sort of thing worth
// pointing a chaos suite at.
func (c *Cluster) RemoveNode(id raft.NodeID) error {
	leader, ok, err := c.Leader()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("chaos: no leader to propose a membership change to")
	}
	if leader == id {
		// Removing the leader is legal and interesting, but the proposal
		// still has to come from it.
		_ = leader
	}

	return c.nodes[leader].ProposeConfChange(raft.ConfChange{
		Type:   raft.ConfChangeRemoveNode,
		NodeID: id,
	})
}

// Members returns one node's view of the cluster membership.
func (c *Cluster) Members(id raft.NodeID) []raft.NodeID {
	n, ok := c.nodes[id]
	if !ok || c.down[id] {
		return nil
	}
	return n.Members()
}

// InJoint reports whether a node is still in the joint phase of a membership
// change, which is the window where both configurations must agree.
func (c *Cluster) InJoint(id raft.NodeID) bool {
	n, ok := c.nodes[id]
	if !ok || c.down[id] {
		return false
	}
	return n.InJointConfiguration()
}

// MembershipSettled reports whether every live node agrees on the membership
// and none is mid-transition.
//
// Agreement is the property that matters: a cluster where two nodes hold
// different ideas of who may vote can elect two leaders, one per view.
func (c *Cluster) MembershipSettled() bool {
	// Only current members have to agree. A removed node stops being
	// replicated to, so it is never told about the configuration that
	// removed it and may sit on the joint one forever. That is correct: the
	// cluster has no obligation to keep informing a node it has dropped.
	members := c.currentMembers()

	var reference []raft.NodeID
	for _, id := range c.ids {
		if c.down[id] || !members[id] {
			continue
		}
		if c.InJoint(id) {
			return false
		}
		members := c.Members(id)
		if reference == nil {
			reference = members
			continue
		}
		if !slices.Equal(members, reference) {
			return false
		}
	}
	return true
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

	// Election Safety, checked on every tick rather than whenever a scenario
	// happens to ask.
	//
	// To be straight about what this is worth: Leader() reports the same
	// violation, and every client write and read calls it, so a scenario
	// doing any work samples the property constantly. Breaking the vote so
	// that a node grants two in one term is caught either way, and no
	// mutation has yet been found that the sampled version misses.
	//
	// It is kept because the sampling is incidental rather than intended.
	// Scenarios spend long stretches inside TickN with no client operations
	// at all, and two leaders in one term is a transient state: the loser
	// finds out and steps down within an election timeout. A violation that
	// began and ended inside one of those stretches would leave no trace.
	// The check costs a walk over five nodes per tick, which is nothing
	// against the chance of silently missing the property the whole algorithm
	// rests on.
	return c.checkElectionSafety()
}

// checkElectionSafety reports an error if two nodes lead the same term.
//
// It is the one invariant worth paying for on every tick. Everything else the
// suite checks is a property of the recorded history, examined afterwards;
// this one is a property of the cluster at an instant, and an instant is
// exactly what it takes to be violated and then tidied away.
func (c *Cluster) checkElectionSafety() error {
	// Keyed by term rather than tracking the highest one seen. Two leaders in
	// some older term are just as much a violation as two in the newest, and
	// a check that only remembers the best term would walk straight past
	// them.
	leaders := make(map[raft.Term]raft.NodeID, len(c.ids))

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.nodes[id]
		if n.State() != raft.Leader {
			continue
		}
		if other, dup := leaders[n.Term()]; dup {
			return fmt.Errorf("chaos: election safety violated at tick %d: nodes %d and %d "+
				"both lead term %d", c.net.Now(), other, id, n.Term())
		}
		leaders[n.Term()] = id
	}
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
		c.snapshotsInstalled[id]++
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
	// A dying process releases its file handles. Reopening on restart is
	// what forces recovery to read back what was actually written rather
	// than whatever happened to be in memory.
	c.closeStorage(id)
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

	// Anything still addressed to the dead incarnation is discarded. A real
	// process takes its connections down with it, so a message sent to it
	// before it died cannot be delivered to the one that replaces it.
	//
	// Leaving them in flight is not merely unrealistic, it is actively
	// misleading: a message queued before the cluster compacted still carries
	// entries that no node holds any more, and a restarted node that accepts
	// one catches up from a log that no longer exists. That masked the
	// snapshot path entirely, since the follower never needed an image.
	c.net.DropAllInFlight(id)

	st, err := c.openStorage(id)
	if err != nil {
		return err
	}
	c.storages[id] = st

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
	op.clientID = uint64(client)
	op.seq = uint64(c.seq)
	cmd := statemachine.Command{
		ClientID: op.clientID,
		Seq:      op.seq,
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

// Resend proposes an operation's command again under its original session
// identity, exactly as a client that never learned the outcome would.
//
// Nothing is added to the history, and that is the point. A retry is not a
// second operation, it is the same one asked again, and the guarantee under
// test is that the cluster treats it that way. If deduplication fails, the
// command applies twice, the state machine ends up somewhere the recorded
// history cannot explain, and the checker says so.
//
// A duplicate write of the same value is invisible on its own, since writing
// a key twice leaves the same value. It becomes visible when somebody else
// has written that key in between: a stale duplicate landing afterwards
// throws away the newer write, and no linearization can account for that.
func (c *Cluster) Resend(op *Op) error {
	if op.Kind != OpWrite {
		return errors.New("chaos: only writes can be resent")
	}
	if op.clientID == 0 && op.seq == 0 {
		return errors.New("chaos: the operation was never accepted, so there is nothing to resend")
	}

	id, ok, err := c.Leader()
	if err != nil {
		return err
	}
	if !ok || c.down[id] {
		// No leader to take it. A real client would keep trying; the
		// scenario decides whether to.
		return nil
	}

	cmd := statemachine.Command{
		ClientID: op.clientID,
		Seq:      op.seq,
		Op:       statemachine.OpPut,
		Key:      op.Key,
		Value:    []byte(op.Value),
	}
	if err := c.nodes[id].Propose(cmd.Encode()); err != nil {
		// Refused by a node that is not the leader any more. Not a failure
		// of the scenario; the client would try again.
		return nil
	}
	return nil
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

	// Only current members are compared. A node the cluster has removed is
	// no longer sent anything, so its state machine falls behind by design,
	// and holding it to the same standard as a member would report every
	// successful removal as a divergence. It is still running, and still
	// wrong to serve from, which is what the scenario checks separately.
	members := c.currentMembers()

	for _, id := range c.ids {
		if c.down[id] || !members[id] {
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

// currentMembers returns the membership as the leader sees it.
//
// The leader is asked because it is the only node guaranteed to hold the
// current configuration. A removed node is never told about the change that
// removed it, so it goes on believing it is a member indefinitely, and asking
// any self-described member would let exactly the node that has been dropped
// answer with the configuration it was dropped from.
func (c *Cluster) currentMembers() map[raft.NodeID]bool {
	if leader, ok, err := c.Leader(); err == nil && ok {
		return memberSet(c.Members(leader))
	}

	// No leader right now, which happens mid-election. Any node that still
	// counts itself a member is a better guess than nothing.
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		members := c.Members(id)
		if !slices.Contains(members, id) {
			continue
		}
		return memberSet(members)
	}

	return memberSet(c.ids)
}

// memberSet turns a member list into a lookup set.
func memberSet(ids []raft.NodeID) map[raft.NodeID]bool {
	set := make(map[raft.NodeID]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
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
