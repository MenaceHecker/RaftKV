// Package node drives a Raft peer: it owns the consensus core, the durable
// storage, and the key-value state machine, and runs the loop that connects
// them.
//
// Everything below this package is a pure state machine with no clock, no
// goroutines, and no network. This is where those arrive. The design keeps
// that boundary intact: a single goroutine owns the raft.Node and is the only
// thing that ever touches it, while callers interact through channels. That
// preserves the property the whole system is built on — the consensus logic
// stays deterministic and testable, and all the concurrency lives in one
// reviewable loop.
package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

var (
	// ErrNotLeader means this node cannot serve the request. Status().Leader
	// names who can, so the server layer can redirect rather than fail.
	ErrNotLeader = raft.ErrNotLeader

	// ErrLostLeadership means a request was accepted but leadership changed
	// before it committed, so its outcome is unknown. A client should retry;
	// deduplication makes that safe.
	ErrLostLeadership = errors.New("node: leadership changed before the request committed")

	// ErrStopped means the node is shutting down.
	ErrStopped = errors.New("node: stopped")

	// ErrTimeout means a request did not complete before its context expired.
	ErrTimeout = errors.New("node: request timed out")
)

// Transport delivers Raft messages to other nodes.
//
// It is deliberately fire-and-forget with no error return. Raft already treats
// the network as unreliable — every message is retried by the next heartbeat,
// and correctness never depends on a particular one arriving — so surfacing
// send failures would add error handling that has nothing useful to do.
type Transport interface {
	// Send delivers messages to their destinations. It must not block the
	// caller: the Raft loop calls it inline, and a slow peer must not stall
	// consensus with the rest.
	Send(msgs []raft.Message)
}

// Config describes one node.
type Config struct {
	// ID is this node's identifier, non-zero and present in Peers.
	ID raft.NodeID

	// Peers is the full cluster membership including this node.
	Peers []raft.NodeID

	// DataDir holds this node's write-ahead log and snapshots.
	DataDir string

	// Transport sends messages to peers.
	Transport Transport

	// TickInterval is how much wall time one logical tick represents.
	// Election and heartbeat timeouts are counted in ticks, so this is what
	// converts them into real durations.
	TickInterval time.Duration

	// ElectionTick and HeartbeatTick are in units of TickInterval.
	ElectionTick  int
	HeartbeatTick int

	// SnapshotThreshold is how many entries may be applied past the last
	// snapshot before another is taken. Zero means the default.
	SnapshotThreshold uint64

	// Sync selects the WAL durability policy. The zero value fsyncs every
	// write, which is what Raft's guarantees assume.
	Sync storage.SyncPolicy
}

// Defaults for a node that does not specify otherwise.
const (
	DefaultTickInterval      = 100 * time.Millisecond
	DefaultElectionTick      = 10
	DefaultHeartbeatTick     = 1
	DefaultSnapshotThreshold = 10000
)

func (c *Config) applyDefaults() {
	if c.TickInterval == 0 {
		c.TickInterval = DefaultTickInterval
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = DefaultElectionTick
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = DefaultHeartbeatTick
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = DefaultSnapshotThreshold
	}
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return errors.New("node: DataDir must not be empty")
	}
	if c.Transport == nil {
		return errors.New("node: Transport must not be nil")
	}
	if c.TickInterval <= 0 {
		return fmt.Errorf("node: TickInterval must be positive, got %v", c.TickInterval)
	}
	return nil
}

// Status is a snapshot of what a node currently believes about the cluster.
type Status struct {
	ID      raft.NodeID
	Leader  raft.NodeID
	Term    raft.Term
	State   raft.State
	Commit  raft.Index
	Applied raft.Index
}

// proposal is a client write waiting for its entry to commit.
type proposal struct {
	// term is the term the entry was appended in. If an entry with a
	// different term turns up at the same index, this proposal's entry was
	// overwritten by a new leader and the outcome is unknown.
	term raft.Term
	done chan error
}

// read is a linearizable read waiting for its read index to be confirmed and
// then applied.
type read struct {
	// index is filled in once the read index is confirmed. Until then the
	// read is waiting on the leadership round.
	index    raft.Index
	resolved bool
	done     chan error
}

// Node is a running Raft peer.
//
// The raft.Node inside is owned exclusively by the run loop goroutine and is
// never touched from anywhere else. Every public method here works by sending
// the loop a request over a channel and waiting for an answer, which is what
// lets the consensus core stay a single-threaded state machine.
type Node struct {
	cfg Config

	raft    *raft.Node
	storage *storage.DiskStorage
	kv      *statemachine.KV

	// Inbound work for the run loop.
	recvc    chan raft.Message
	proposec chan proposalRequest
	readc    chan readRequest
	statusc  chan chan Status

	stopc chan struct{}
	donec chan struct{}
	// stopOnce guards Stop so that closing stopc twice cannot panic.
	stopOnce sync.Once

	// pending tracks in-flight proposals by log index, and in-flight reads by
	// context. Both are owned by the run loop.
	pending map[raft.Index]*proposal
	reads   map[string]*read

	// deferred holds reads that arrived while the leader had not yet
	// committed an entry in its own term. They are retried rather than
	// failed; see startRead.
	deferred []readRequest

	// readSeq mints unique read contexts. It is atomic because the context is
	// generated by the calling goroutine, before the request reaches the loop.
	readSeq atomic.Uint64

	// lastSnapshot is the index the most recent snapshot was taken at.
	lastSnapshot raft.Index
}

type proposalRequest struct {
	data []byte
	done chan error
}

type readRequest struct {
	context []byte
	done    chan error
}

// Start opens a node's durable state, restores its state machine, and begins
// running.
func Start(cfg Config) (*Node, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	store, snap, err := storage.OpenDiskStorage(storage.DiskConfig{
		Dir:  cfg.DataDir,
		Sync: cfg.Sync,
	})
	if err != nil {
		return nil, err
	}

	kv := statemachine.New()

	// Restore the snapshot before anything else. The log entries that follow
	// it are replayed on top; replaying from the beginning would be correct
	// but needlessly slow, and skipping the snapshot would be silently wrong.
	if snap.Meta.Index > 0 {
		if err := kv.Restore(snap.Data); err != nil {
			store.Close()
			return nil, fmt.Errorf("node: restoring snapshot: %w", err)
		}
	}

	rn, err := raft.NewNode(raft.Config{
		ID:            cfg.ID,
		Peers:         cfg.Peers,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		Storage:       store,
	})
	if err != nil {
		store.Close()
		return nil, err
	}

	n := &Node{
		cfg:          cfg,
		raft:         rn,
		storage:      store,
		kv:           kv,
		recvc:        make(chan raft.Message, 256),
		proposec:     make(chan proposalRequest),
		readc:        make(chan readRequest),
		statusc:      make(chan chan Status),
		stopc:        make(chan struct{}),
		donec:        make(chan struct{}),
		pending:      make(map[raft.Index]*proposal),
		reads:        make(map[string]*read),
		lastSnapshot: snap.Meta.Index,
	}

	go n.run()
	return n, nil
}

// Stop shuts the node down and releases its files. It is safe to call more
// than once.
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopc) })
	<-n.donec
	return n.storage.Close()
}

// Step delivers a message received from another node. It never blocks the
// caller: if the loop is behind, the message is dropped, which Raft already
// tolerates because the sender retries on its next heartbeat.
func (n *Node) Step(m raft.Message) {
	select {
	case n.recvc <- m:
	case <-n.stopc:
	default:
		// The loop is saturated. Dropping is better than blocking a
		// transport goroutine, and the sender will try again.
	}
}

// Status returns what this node currently believes about the cluster. The
// server layer uses Leader to redirect clients.
func (n *Node) Status() Status {
	reply := make(chan Status, 1)
	select {
	case n.statusc <- reply:
		return <-reply
	case <-n.donec:
		return Status{ID: n.cfg.ID}
	}
}

// Propose submits a command and waits for it to commit and apply.
//
// It returns ErrNotLeader if this node cannot accept writes, and
// ErrLostLeadership if the entry was appended but leadership changed before it
// committed. In the second case the write may or may not have taken effect —
// which is exactly why commands carry a client ID and sequence number, so the
// retry is deduplicated rather than double-applied.
func (n *Node) Propose(ctx context.Context, cmd statemachine.Command) error {
	req := proposalRequest{data: cmd.Encode(), done: make(chan error, 1)}

	select {
	case n.proposec <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}
}

// Get performs a linearizable read.
//
// It establishes a read index, waits for a majority to confirm this node is
// still leader, and only then reads local state. A read served without that
// confirmation could come from a leader that has already been deposed, and
// would be stale with nothing to detect it.
func (n *Node) Get(ctx context.Context, key string) ([]byte, bool, error) {
	// A unique context per read: it is what attributes a leadership
	// acknowledgement to this specific request.
	seq := n.readSeq.Add(1)
	rctx := make([]byte, 8)
	for i := range rctx {
		rctx[i] = byte(seq >> (8 * i))
	}

	req := readRequest{context: rctx, done: make(chan error, 1)}

	select {
	case n.readc <- req:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-n.donec:
		return nil, false, ErrStopped
	}

	select {
	case err := <-req.done:
		if err != nil {
			return nil, false, err
		}
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-n.donec:
		return nil, false, ErrStopped
	}

	// The read index has been confirmed and applied, so local state now
	// reflects everything committed when the read arrived.
	value, ok := n.kv.Get(key)
	return value, ok, nil
}

// run is the node's single goroutine. Everything that touches the raft.Node
// happens here, in the order the Ready contract requires: deliver inputs,
// drain effects, send messages, apply entries, acknowledge.
func (n *Node) run() {
	defer close(n.donec)

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopc:
			n.failAllPending(ErrStopped)
			return

		case <-ticker.C:
			if err := n.raft.Tick(); err != nil {
				// A tick only fails when a term change could not be
				// persisted, which means durability is gone. Continuing
				// would risk the safety properties that rest on it.
				n.failAllPending(fmt.Errorf("node: %w", err))
				return
			}

		case m := <-n.recvc:
			if err := n.raft.Step(m); err != nil {
				// A malformed or unexpected message is not fatal; the
				// cluster carries on without it.
				continue
			}

		case req := <-n.proposec:
			n.handleProposal(req)

		case req := <-n.readc:
			n.handleRead(req)

		case reply := <-n.statusc:
			reply <- n.status()
		}

		n.processReady()
		n.retryDeferredReads()
	}
}

// status builds a Status from the loop goroutine, where reading the raft.Node
// is safe.
func (n *Node) status() Status {
	return Status{
		ID:      n.raft.ID(),
		Leader:  n.raft.Leader(),
		Term:    n.raft.Term(),
		State:   n.raft.State(),
		Commit:  n.raft.CommitIndex(),
		Applied: n.kv.Applied(),
	}
}

// handleProposal appends a client command and registers a waiter for it.
func (n *Node) handleProposal(req proposalRequest) {
	if err := n.raft.Propose(req.data); err != nil {
		req.done <- err
		return
	}

	// The entry the core just appended is at the end of the log. Recording
	// its term as well as its index is what lets the loop tell "committed" from
	// "overwritten by a new leader at the same index".
	index := n.raft.LastIndex()
	n.pending[index] = &proposal{term: n.raft.Term(), done: req.done}
}

// handleRead starts a read-index round, or defers it if the leader is not yet
// able to produce a read index.
func (n *Node) handleRead(req readRequest) {
	n.startRead(req)
}

// startRead attempts to begin a read-index round.
//
// A leader that has just been elected cannot hand out a read index until it
// has committed an entry in its own term (§5.4.2). That window is brief and
// self-resolving — the no-op appended on election closes it within a
// heartbeat — so the read is held and retried rather than failed. Surfacing
// the condition to the client would turn an ordinary leader change into a
// visible error for a request that was always going to succeed.
func (n *Node) startRead(req readRequest) {
	err := n.raft.ReadIndex(req.context)
	switch {
	case err == nil:
		n.reads[string(req.context)] = &read{done: req.done}
	case errors.Is(err, raft.ErrLeaderNotReady):
		n.deferred = append(n.deferred, req)
	default:
		req.done <- err
	}
}

// retryDeferredReads re-attempts reads that were held back, once the leader
// can serve them. If leadership was lost in the meantime they are failed, so
// the client can be redirected instead of waiting.
func (n *Node) retryDeferredReads() {
	if len(n.deferred) == 0 {
		return
	}

	held := n.deferred
	n.deferred = nil
	for _, req := range held {
		n.startRead(req)
	}
}

// processReady drains the effects the core produced and acts on them.
func (n *Node) processReady() {
	rd := n.raft.Ready()
	if rd.IsEmpty() {
		return
	}

	// Messages first. The core has already persisted anything they promise,
	// so they are safe to send before the entries are applied.
	if len(rd.Messages) > 0 {
		n.cfg.Transport.Send(rd.Messages)
	}

	for _, e := range rd.CommittedEntries {
		n.applyEntry(e)
	}

	// Read indexes are recorded before resolving waiters, because a read may
	// have been confirmed at an index this batch has only just applied.
	for _, rs := range rd.ReadStates {
		if r, ok := n.reads[string(rs.Context)]; ok {
			r.index = rs.Index
			r.resolved = true
		}
	}
	n.resolveReads()

	n.raft.Advance(rd)

	// Leadership changes invalidate everything in flight: a follower cannot
	// commit proposals or confirm reads.
	if n.raft.State() != raft.Leader {
		n.failAllPending(ErrNotLeader)
	}

	n.maybeSnapshot()
}

// applyEntry hands one committed entry to the state machine and completes the
// proposal waiting on it, if any.
func (n *Node) applyEntry(e raft.Entry) {
	err := n.kv.Apply(e)

	p, waiting := n.pending[e.Index]
	if !waiting {
		return
	}
	delete(n.pending, e.Index)

	switch {
	case err != nil:
		p.done <- err
	case e.Term != p.term:
		// A different entry reached this index, so the proposal's own entry
		// was overwritten by a later leader. The client must retry; its
		// sequence number keeps that safe.
		p.done <- ErrLostLeadership
	default:
		p.done <- nil
	}
}

// resolveReads completes every read whose index has been both confirmed and
// applied.
func (n *Node) resolveReads() {
	applied := n.kv.Applied()
	for key, r := range n.reads {
		if !r.resolved || r.index > applied {
			continue
		}
		delete(n.reads, key)
		r.done <- nil
	}
}

// failAllPending completes every in-flight request with an error, so callers
// learn the outcome instead of waiting for a context deadline.
func (n *Node) failAllPending(err error) {
	for index, p := range n.pending {
		delete(n.pending, index)
		p.done <- err
	}
	for key, r := range n.reads {
		delete(n.reads, key)
		r.done <- err
	}
	for _, req := range n.deferred {
		req.done <- err
	}
	n.deferred = nil
}

// maybeSnapshot compacts the log once enough entries have been applied past
// the last snapshot.
//
// A failure here is deliberately not fatal. Snapshotting is an optimization:
// it bounds the log and speeds up recovery, but the node is entirely correct
// without it, so a full disk should slow the system rather than stop it.
func (n *Node) maybeSnapshot() {
	applied := n.kv.Applied()
	if applied < n.lastSnapshot+raft.Index(n.cfg.SnapshotThreshold) {
		return
	}

	data, err := n.kv.Snapshot()
	if err != nil {
		return
	}
	if err := n.storage.CreateSnapshot(applied, data); err != nil {
		return
	}
	n.lastSnapshot = applied
}
