package transport

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Fault injection for the admin surface (T4.7).
//
// These faults are application-level. They drop or delay Raft messages inside
// the process, in a wrapper around node.Transport and in the Raft ingress
// handlers. They do not touch the kernel network stack. Dropping or delaying
// a message is something the network may do to any message, so Raft tolerates
// it (paper §5, "Raft safety does not depend on timing"); the wrapper never
// alters a message.

const (
	// FaultExpiry is how long an injected fault lasts at the node before it is
	// removed, measured from the call that set it. Each fault has its own timer.
	FaultExpiry = 120 * time.Second

	// MaxDelayQueue bounds the messages held for one peer while a delay is set.
	MaxDelayQueue = 1024
)

// Timer is the part of time.Timer the wrapper needs.
type Timer interface {
	Stop() bool
}

// Clock supplies time to the wrapper so tests can control it.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// Rand supplies the draws behind percentage loss so tests can script them.
type Rand interface {
	// Intn returns a value in [0, n).
	Intn(n int) int
}

// RealClock is the production Clock.
type RealClock struct{}

// Now returns the wall clock time.
func (RealClock) Now() time.Time { return time.Now() }

// AfterFunc schedules f after d on the wall clock.
func (RealClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// RealRand is the production Rand.
type RealRand struct{}

// Intn draws from the process-wide generator. The draw only decides which
// messages a test fault drops, so it needs no cryptographic strength.
func (RealRand) Intn(n int) int { return rand.IntN(n) } //nolint:gosec // loss injection, not a security decision

// FaultConfig configures a FaultTransport.
type FaultConfig struct {
	// Self is this node. Messages addressed to it are never dropped or delayed.
	Self raft.NodeID

	// Inner carries the messages that pass.
	Inner node.Transport

	Clock Clock
	Rand  Rand

	// Paused, if set, reports whether the node is paused. While it is true,
	// messages to and from peers are dropped.
	Paused func() bool
}

// FaultStats counts what the wrapper has done to messages.
type FaultStats struct {
	// Dropped counts messages dropped by a partition, by percentage loss or
	// by a pause.
	Dropped uint64
	// QueueOverflow counts messages dropped because a peer's delay queue was
	// full.
	QueueOverflow uint64
	// Discarded counts queued messages thrown away when a delay expired or
	// was cleared.
	Discarded uint64
}

// rule is one injected delay or loss setting with its own expiry timer. The
// generation lets a timer callback that fires after the rule was replaced or
// cleared recognise that it is stale, whether or not Stop managed to cancel it.
type rule struct {
	delay time.Duration
	pct   int
	gen   uint64
	timer Timer
}

type queued struct {
	m   raft.Message
	due time.Time
}

// peerQueue holds the delayed messages for one peer in send order. due is
// non-decreasing along the queue, so a shorter delay set later cannot let a
// newer message overtake an older one.
type peerQueue struct {
	items []queued
	timer Timer
	tgen  uint64
}

// FaultTransport wraps a node.Transport and applies injected faults.
//
// Faults only drop or delay messages. They are never altered, and messages the
// node addresses to itself are never touched. Send never blocks: delayed
// messages wait in a bounded per-peer queue and a clock timer releases them.
type FaultTransport struct {
	cfg   FaultConfig
	clock Clock
	rnd   Rand

	// flushMu serializes releases of delayed messages so that two timers for
	// the same peer cannot hand them to the inner transport out of order.
	flushMu sync.Mutex

	mu      sync.Mutex
	gen     uint64
	blocked map[raft.NodeID]struct{}
	partGen uint64
	partTmr Timer
	delays  map[raft.NodeID]*rule
	drops   map[raft.NodeID]*rule
	queues  map[raft.NodeID]*peerQueue
	stats   FaultStats
}

// NewFaultTransport returns a wrapper with no faults set.
func NewFaultTransport(cfg FaultConfig) *FaultTransport {
	f := &FaultTransport{
		cfg:    cfg,
		clock:  cfg.Clock,
		rnd:    cfg.Rand,
		delays: make(map[raft.NodeID]*rule),
		drops:  make(map[raft.NodeID]*rule),
		queues: make(map[raft.NodeID]*peerQueue),
	}
	if f.clock == nil {
		f.clock = RealClock{}
	}
	if f.rnd == nil {
		f.rnd = RealRand{}
	}
	return f
}

func (f *FaultTransport) paused() bool { return f.cfg.Paused != nil && f.cfg.Paused() }

// effective returns the rule for peer, falling back to the all-peers rule.
func effective(rules map[raft.NodeID]*rule, peer raft.NodeID) *rule {
	if r, ok := rules[peer]; ok {
		return r
	}
	return rules[0]
}

func (f *FaultTransport) effectiveDelay(peer raft.NodeID) time.Duration {
	if r := effective(f.delays, peer); r != nil {
		return r.delay
	}
	return 0
}

// Send applies the faults to msgs and passes the survivors to the inner
// transport in their original order.
func (f *FaultTransport) Send(msgs []raft.Message) {
	var pass []raft.Message
	paused := f.paused()

	f.mu.Lock()
	for _, m := range msgs {
		if m.To == f.cfg.Self {
			pass = append(pass, m)
			continue
		}
		if paused {
			f.stats.Dropped++
			continue
		}
		if _, cut := f.blocked[m.To]; cut {
			f.stats.Dropped++
			continue
		}
		if r := effective(f.drops, m.To); r != nil && f.dropLocked(r.pct) {
			f.stats.Dropped++
			continue
		}
		if d := f.effectiveDelay(m.To); d > 0 {
			f.enqueueLocked(m, d)
			continue
		}
		pass = append(pass, m)
	}
	f.mu.Unlock()

	if len(pass) > 0 {
		f.cfg.Inner.Send(pass)
	}
}

// dropLocked makes one keep-or-drop decision: drop when the draw is below pct.
func (f *FaultTransport) dropLocked(pct int) bool {
	if pct >= 100 {
		return true
	}
	return f.rnd.Intn(100) < pct
}

func (f *FaultTransport) enqueueLocked(m raft.Message, d time.Duration) {
	q := f.queues[m.To]
	if q == nil {
		q = &peerQueue{}
		f.queues[m.To] = q
	}
	if len(q.items) >= MaxDelayQueue {
		f.stats.QueueOverflow++
		return
	}
	due := f.clock.Now().Add(d)
	if n := len(q.items); n > 0 && due.Before(q.items[n-1].due) {
		due = q.items[n-1].due
	}
	q.items = append(q.items, queued{m: m, due: due})
	if len(q.items) == 1 {
		f.scheduleLocked(m.To, q)
	}
}

// scheduleLocked arms q's timer for its head message, invalidating any earlier
// timer.
func (f *FaultTransport) scheduleLocked(peer raft.NodeID, q *peerQueue) {
	if q.timer != nil {
		q.timer.Stop()
	}
	q.tgen++
	gen := q.tgen
	wait := max(q.items[0].due.Sub(f.clock.Now()), 0)
	q.timer = f.clock.AfterFunc(wait, func() { f.flush(peer, gen) })
}

// flush releases every queued message of peer that is due.
func (f *FaultTransport) flush(peer raft.NodeID, gen uint64) {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()

	f.mu.Lock()
	q := f.queues[peer]
	if q == nil || q.tgen != gen {
		f.mu.Unlock()
		return
	}
	now := f.clock.Now()
	var ready []raft.Message
	for len(q.items) > 0 && !q.items[0].due.After(now) {
		ready = append(ready, q.items[0].m)
		q.items = q.items[1:]
	}
	if len(q.items) > 0 {
		f.scheduleLocked(peer, q)
	} else {
		delete(f.queues, peer)
	}
	f.mu.Unlock()

	if len(ready) > 0 {
		f.cfg.Inner.Send(ready)
	}
}

// discardQueueLocked throws away a peer's queued messages.
func (f *FaultTransport) discardQueueLocked(peer raft.NodeID) {
	q := f.queues[peer]
	if q == nil {
		return
	}
	if q.timer != nil {
		q.timer.Stop()
	}
	q.tgen++
	f.stats.Discarded += uint64(len(q.items))
	delete(f.queues, peer)
}

// pruneQueuesLocked discards the queues of peers that no longer have a delay.
func (f *FaultTransport) pruneQueuesLocked() {
	for peer := range f.queues {
		if f.effectiveDelay(peer) == 0 {
			f.discardQueueLocked(peer)
		}
	}
}

// AddPeer forwards to the inner transport when it is Reconfigurable, because
// node.reconcileTransport silently skips a transport that lacks the methods.
func (f *FaultTransport) AddPeer(id raft.NodeID, addr string) error {
	if r, ok := f.cfg.Inner.(node.Reconfigurable); ok {
		return r.AddPeer(id, addr)
	}
	return nil
}

// RemovePeer is the counterpart of AddPeer.
func (f *FaultTransport) RemovePeer(id raft.NodeID) {
	if r, ok := f.cfg.Inner.(node.Reconfigurable); ok {
		r.RemovePeer(id)
	}
}

// SetPartition replaces the set of peers cut off in both directions. An empty
// set heals. The partition expires FaultExpiry after this call.
func (f *FaultTransport) SetPartition(blocked []raft.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.partTmr != nil {
		f.partTmr.Stop()
		f.partTmr = nil
	}
	f.partGen = 0
	f.blocked = nil
	if len(blocked) == 0 {
		return
	}

	f.blocked = make(map[raft.NodeID]struct{}, len(blocked))
	for _, id := range blocked {
		f.blocked[id] = struct{}{}
	}
	f.gen++
	gen := f.gen
	f.partGen = gen
	f.partTmr = f.clock.AfterFunc(FaultExpiry, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.partGen != gen {
			return
		}
		f.blocked, f.partGen, f.partTmr = nil, 0, nil
	})
}

// setRuleLocked installs, replaces or (for a zero value) removes the rule of
// peer in rules, arming a fresh expiry timer for a non-zero one.
func (f *FaultTransport) setRuleLocked(rules map[raft.NodeID]*rule, peer raft.NodeID, r *rule) {
	if old := rules[peer]; old != nil && old.timer != nil {
		old.timer.Stop()
	}
	delete(rules, peer)
	if r == nil {
		return
	}
	f.gen++
	r.gen = f.gen
	gen := r.gen
	r.timer = f.clock.AfterFunc(FaultExpiry, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if cur := rules[peer]; cur == nil || cur.gen != gen {
			return
		}
		delete(rules, peer)
		f.pruneQueuesLocked()
	})
	rules[peer] = r
}

// SetDelay delays every message sent to peer by d; peer zero means all peers.
// A zero d clears it and discards what was queued. The delay expires
// FaultExpiry after this call.
func (f *FaultTransport) SetDelay(peer raft.NodeID, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var r *rule
	if d > 0 {
		r = &rule{delay: d}
	}
	f.setRuleLocked(f.delays, peer, r)
	f.pruneQueuesLocked()
}

// SetDropPercent drops pct percent of messages sent to peer; peer zero means
// all peers. A zero pct clears it. The loss expires FaultExpiry after this
// call.
func (f *FaultTransport) SetDropPercent(peer raft.NodeID, pct int) error {
	if pct < 0 || pct > 100 {
		return errors.New("transport: drop percentage must be between 0 and 100")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	var r *rule
	if pct > 0 {
		r = &rule{pct: pct}
	}
	f.setRuleLocked(f.drops, peer, r)
	return nil
}

// ClearFaults removes every fault and discards queued messages.
func (f *FaultTransport) ClearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.partTmr != nil {
		f.partTmr.Stop()
	}
	f.partTmr, f.partGen, f.blocked = nil, 0, nil
	for peer := range f.delays {
		f.setRuleLocked(f.delays, peer, nil)
	}
	for peer := range f.drops {
		f.setRuleLocked(f.drops, peer, nil)
	}
	for peer := range f.queues {
		f.discardQueueLocked(peer)
	}
}

// AllowIngress reports whether a message claiming to come from from may enter
// the node. Both Deliver and DeliverSnapshot consult it. It keys on the sender
// the message claims; this is a test fault, not an access control.
func (f *FaultTransport) AllowIngress(from raft.NodeID) bool {
	if from == f.cfg.Self {
		return true
	}
	if f.paused() {
		f.countDrop()
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, cut := f.blocked[from]; cut {
		f.stats.Dropped++
		return false
	}
	return true
}

func (f *FaultTransport) countDrop() {
	f.mu.Lock()
	f.stats.Dropped++
	f.mu.Unlock()
}

// Stats returns the counters.
func (f *FaultTransport) Stats() FaultStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// WrapRaftServer returns a RaftService that filters ingress through f before
// handing messages to inner.
func (f *FaultTransport) WrapRaftServer(inner raftkvv1.RaftServiceServer) raftkvv1.RaftServiceServer {
	return &ingressServer{inner: inner, faults: f}
}

var errIngressBlocked = status.Error(codes.Unavailable, "transport: peer unreachable (injected fault)")

type ingressServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	inner  raftkvv1.RaftServiceServer
	faults *FaultTransport
}

func (s *ingressServer) Deliver(ctx context.Context, req *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	if !s.faults.AllowIngress(raft.NodeID(req.GetMessage().GetFrom())) {
		return nil, errIngressBlocked
	}
	return s.inner.Deliver(ctx, req)
}

func (s *ingressServer) DeliverSnapshot(stream raftkvv1.RaftService_DeliverSnapshotServer) error {
	return s.inner.DeliverSnapshot(&filteredStream{
		RaftService_DeliverSnapshotServer: stream,
		faults:                            s.faults,
	})
}

// filteredStream rejects a snapshot stream whose sender is cut off. The header
// frame names the sender, so a blocked stream fails on its first frame, before
// the handler has buffered any data. Every later frame is checked again, so a
// stream opened before a partition or pause stops at its next chunk.
type filteredStream struct {
	raftkvv1.RaftService_DeliverSnapshotServer

	faults *FaultTransport
	from   raft.NodeID
}

func (s *filteredStream) Recv() (*raftkvv1.SnapshotChunk, error) {
	chunk, err := s.RaftService_DeliverSnapshotServer.Recv()
	if err != nil {
		return nil, err
	}
	if h, ok := chunk.GetFrame().(*raftkvv1.SnapshotChunk_Header); ok {
		s.from = raft.NodeID(h.Header.GetFrom())
	}
	if !s.faults.AllowIngress(s.from) {
		return nil, errIngressBlocked
	}
	return chunk, nil
}

var (
	_ node.Transport      = (*FaultTransport)(nil)
	_ node.Reconfigurable = (*FaultTransport)(nil)
)
