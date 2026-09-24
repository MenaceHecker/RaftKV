package node

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

var (
	ErrNotLeader = raft.ErrNotLeader

	ErrLostLeadership = errors.New("node: leadership changed before the request committed")

	ErrStopped = errors.New("node: stopped")

	ErrTimeout = errors.New("node: request timed out")
)

type Transport interface {
	Send(msgs []raft.Message)
}

type Reconfigurable interface {
	AddPeer(id raft.NodeID, addr string) error

	RemovePeer(id raft.NodeID)
}

type Config struct {
	ID raft.NodeID

	Peers []raft.NodeID

	DataDir string

	Transport Transport

	TickInterval time.Duration

	ElectionTick  int
	HeartbeatTick int

	SnapshotThreshold uint64

	Sync storage.SyncPolicy

	Metrics Recorder

	MaxCommittedEntries int

	MaxProposalBatch int
}

const (
	DefaultTickInterval      = 100 * time.Millisecond
	DefaultElectionTick      = 10
	DefaultHeartbeatTick     = 1
	DefaultSnapshotThreshold = 10000

	DefaultMaxProposalBatch = 64
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
	if c.Metrics == nil {
		c.Metrics = nopRecorder{}
	}
	if c.MaxProposalBatch == 0 {
		c.MaxProposalBatch = DefaultMaxProposalBatch
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

type Status struct {
	ID      raft.NodeID
	Leader  raft.NodeID
	Term    raft.Term
	State   raft.State
	Commit  raft.Index
	Applied raft.Index

	Members raft.ConfState

	SnapshotsReceived uint64
}

type proposal struct {
	term raft.Term
	done chan error
}

type read struct {
	index    raft.Index
	resolved bool
	done     chan error
}

type Node struct {
	cfg Config

	raft    *raft.Node
	storage *storage.DiskStorage
	kv      *statemachine.KV

	recvc    chan raft.Message
	proposec chan proposalRequest
	readc    chan readRequest
	statusc  chan chan Status
	compactc chan chan error
	confc    chan confChangeRequest

	stopc    chan struct{}
	donec    chan struct{}
	stopOnce sync.Once

	pending map[raft.Index]*proposal
	reads   map[string]*read

	deferred []readRequest

	readSeq atomic.Uint64

	confSeq        uint64
	transportPeers map[raft.NodeID]string

	lastSnapshot raft.Index

	snapshotsReceived uint64

	applyc chan struct{}

	proposalBatch []proposalRequest

	lastTerm   raft.Term
	lastLeader raft.NodeID
}

type proposalRequest struct {
	data []byte
	done chan error
}

type readRequest struct {
	context []byte
	done    chan error
}

type confChangeRequest struct {
	change raft.ConfChange
	done   chan error
}

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

	if snap.Meta.Index > 0 {
		if err := kv.Restore(snap.Data); err != nil {
			store.Close()
			return nil, fmt.Errorf("node: restoring snapshot: %w", err)
		}
	}

	var initialConf *raft.ConfState
	if !snap.Conf.IsEmpty() {
		conf := snap.Conf
		initialConf = &conf
	}

	rn, err := raft.NewNode(raft.Config{
		ID:                  cfg.ID,
		Peers:               cfg.Peers,
		InitialConfState:    initialConf,
		ElectionTick:        cfg.ElectionTick,
		HeartbeatTick:       cfg.HeartbeatTick,
		MaxCommittedEntries: cfg.MaxCommittedEntries,
		CheckQuorum:         true,
		PreVote:             true,
		Storage:             meteredStorage{Storage: store, rec: cfg.Metrics},
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
		compactc:     make(chan chan error),
		confc:        make(chan confChangeRequest),
		applyc:       make(chan struct{}, 1),
		stopc:        make(chan struct{}),
		donec:        make(chan struct{}),
		pending:      make(map[raft.Index]*proposal),
		reads:        make(map[string]*read),
		lastSnapshot: snap.Meta.Index,
	}

	go n.run()
	return n, nil
}

func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopc) })
	<-n.donec
	return n.storage.Close()
}

func (n *Node) Step(m raft.Message) {
	select {
	case n.recvc <- m:
	case <-n.stopc:
	default:
	}
}

func (n *Node) Status() Status {
	reply := make(chan Status, 1)
	select {
	case n.statusc <- reply:
		return <-reply
	case <-n.donec:
		return Status{ID: n.cfg.ID}
	}
}

func (n *Node) Done() <-chan struct{} { return n.donec }

func (n *Node) Stopped() bool {
	select {
	case <-n.donec:
		return true
	default:
		return false
	}
}

func (n *Node) Propose(ctx context.Context, cmd statemachine.Command) error {
	start := time.Now()
	var err error
	defer func() { n.cfg.Metrics.ObserveProposal(classify(err), time.Since(start)) }()

	req := proposalRequest{data: cmd.Encode(), done: make(chan error, 1)}

	select {
	case n.proposec <- req:
	case <-ctx.Done():
		err = ctx.Err()
		return err
	case <-n.donec:
		err = ErrStopped
		return err
	}

	select {
	case err = <-req.done:
		return err
	case <-ctx.Done():
		err = ctx.Err()
		return err
	case <-n.donec:
		err = ErrStopped
		return err
	}
}

func (n *Node) Get(ctx context.Context, key string) ([]byte, bool, error) {
	start := time.Now()
	var err error
	defer func() { n.cfg.Metrics.ObserveRead(classify(err), time.Since(start)) }()

	seq := n.readSeq.Add(1)
	rctx := make([]byte, 8)
	for i := range rctx {
		rctx[i] = byte(seq >> (8 * i))
	}

	req := readRequest{context: rctx, done: make(chan error, 1)}

	select {
	case n.readc <- req:
	case <-ctx.Done():
		err = ctx.Err()
		return nil, false, err
	case <-n.donec:
		err = ErrStopped
		return nil, false, err
	}

	select {
	case err = <-req.done:
		if err != nil {
			return nil, false, err
		}
	case <-ctx.Done():
		err = ctx.Err()
		return nil, false, err
	case <-n.donec:
		err = ErrStopped
		return nil, false, err
	}

	value, ok := n.kv.Get(key)
	return value, ok, nil
}

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
				n.failAllPending(fmt.Errorf("node: %w", err))
				return
			}

		case m := <-n.recvc:
			if err := n.raft.Step(m); err != nil {
				if fatal(err) {
					n.failAllPending(fmt.Errorf("node: %w", err))
					return
				}
				continue
			}

		case req := <-n.proposec:
			if err := n.handleProposal(req); err != nil {
				n.failAllPending(fmt.Errorf("node: %w", err))
				return
			}

		case req := <-n.readc:
			n.handleRead(req)

		case reply := <-n.statusc:
			reply <- n.status()

		case reply := <-n.compactc:
			reply <- n.compact()

		case req := <-n.confc:
			if err := n.handleConfChange(req); err != nil {
				n.failAllPending(fmt.Errorf("node: %w", err))
				return
			}

		case <-n.applyc:
		}

		n.processReady()
		n.retryDeferredReads()
	}
}

func (n *Node) status() Status {
	return Status{
		ID:      n.raft.ID(),
		Leader:  n.raft.Leader(),
		Term:    n.raft.Term(),
		State:   n.raft.State(),
		Commit:  n.raft.CommitIndex(),
		Applied: n.kv.Applied(),
		Members: n.raft.ConfState(),

		SnapshotsReceived: n.snapshotsReceived,
	}
}

func (n *Node) handleProposal(req proposalRequest) error {
	batch := append(n.proposalBatch[:0], req)
	yielded := false
collect:
	for len(batch) < n.cfg.MaxProposalBatch {
		select {
		case next := <-n.proposec:
			batch = append(batch, next)
		default:
			if len(batch) == 1 && !yielded {
				yielded = true
				runtime.Gosched()
				continue
			}
			break collect
		}
	}
	n.proposalBatch = batch

	datas := make([][]byte, len(batch))
	for i, r := range batch {
		datas[i] = r.data
	}

	if err := n.raft.ProposeBatch(datas); err != nil {
		for _, r := range batch {
			r.done <- err
		}
		if fatal(err) {
			return err
		}
		return nil
	}

	last := n.raft.LastIndex()
	term := n.raft.Term()
	first := last - raft.Index(len(batch)) + 1
	for i, r := range batch {
		n.pending[first+raft.Index(i)] = &proposal{term: term, done: r.done}
	}
	return nil
}

func (n *Node) handleRead(req readRequest) {
	n.startRead(req)
}

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

func (n *Node) processReady() {
	n.observeLeadership()

	n.reconcileTransport()

	rd := n.raft.Ready()
	if rd.IsEmpty() {
		return
	}

	if len(rd.Messages) > 0 {
		n.cfg.Transport.Send(rd.Messages)
	}

	if rd.Snapshot != nil {
		if err := n.kv.Restore(rd.Snapshot.Data); err != nil {
			n.failAllPending(fmt.Errorf("node: restoring snapshot: %w", err))
			return
		}
		n.lastSnapshot = rd.Snapshot.Index
		n.snapshotsReceived++
		n.cfg.Metrics.SnapshotReceived()
	}

	if len(rd.CommittedEntries) > 0 {
		start := time.Now()
		for _, e := range rd.CommittedEntries {
			n.applyEntry(e)
		}
		n.cfg.Metrics.ObserveApply(len(rd.CommittedEntries), time.Since(start))
	}

	for _, rs := range rd.ReadStates {
		if r, ok := n.reads[string(rs.Context)]; ok {
			r.index = rs.Index
			r.resolved = true
		}
	}
	n.resolveReads()

	n.raft.Advance(rd)
	n.observeLeadership()

	if n.raft.State() != raft.Leader {
		n.failAllPending(ErrNotLeader)
	}

	n.maybeSnapshot()

	if n.raft.HasUnapplied() {
		select {
		case n.applyc <- struct{}{}:
		default:
		}
	}
}

func fatal(err error) bool {
	return errors.Is(err, raft.ErrStorage)
}

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
		p.done <- ErrLostLeadership
	default:
		p.done <- nil
	}
}

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

func (n *Node) AddNode(ctx context.Context, id raft.NodeID, addr string) error {
	if addr == "" {
		return errors.New("node: a new member needs an address")
	}
	return n.proposeConfChange(ctx, raft.ConfChange{
		Type: raft.ConfChangeAddNode, NodeID: id, Addr: addr,
	})
}

func (n *Node) RemoveNode(ctx context.Context, id raft.NodeID) error {
	return n.proposeConfChange(ctx, raft.ConfChange{
		Type: raft.ConfChangeRemoveNode, NodeID: id,
	})
}

func (n *Node) proposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	req := confChangeRequest{change: cc, done: make(chan error, 1)}

	select {
	case n.confc <- req:
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

func (n *Node) handleConfChange(req confChangeRequest) error {
	before := n.raft.LastIndex()

	if err := n.raft.ProposeConfChange(req.change); err != nil {
		req.done <- err
		if fatal(err) {
			return err
		}
		return nil
	}

	index := n.raft.LastIndex()
	if index == before {
		req.done <- nil
		return nil
	}
	n.pending[index] = &proposal{term: n.raft.Term(), done: req.done}
	return nil
}

func (n *Node) Compact(ctx context.Context) error {
	reply := make(chan error, 1)

	select {
	case n.compactc <- reply:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}

	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.donec:
		return ErrStopped
	}
}

func (n *Node) compact() error {
	applied := n.kv.Applied()
	if applied == 0 {
		return errors.New("node: nothing has been applied yet")
	}
	if applied <= n.lastSnapshot {
		return nil
	}

	start := time.Now()
	data, err := n.kv.Snapshot()
	if err != nil {
		return fmt.Errorf("node: snapshotting the state machine: %w", err)
	}
	if err := n.storage.CreateSnapshot(applied, data, n.raft.ConfState()); err != nil {
		return fmt.Errorf("node: compacting: %w", err)
	}
	n.lastSnapshot = applied
	n.cfg.Metrics.SnapshotCreated(uint64(applied), time.Since(start))
	return nil
}

func (n *Node) maybeSnapshot() {
	applied := n.kv.Applied()
	if applied < n.lastSnapshot+raft.Index(n.cfg.SnapshotThreshold) {
		return
	}

	start := time.Now()
	data, err := n.kv.Snapshot()
	if err != nil {
		return
	}
	if err := n.storage.CreateSnapshot(applied, data, n.raft.ConfState()); err != nil {
		return
	}
	n.lastSnapshot = applied
	n.cfg.Metrics.SnapshotCreated(uint64(applied), time.Since(start))
}

func (n *Node) reconcileTransport() {
	tr, ok := n.cfg.Transport.(Reconfigurable)
	if !ok {
		return
	}
	seq := n.raft.ConfSeq()
	if seq == n.confSeq && n.transportPeers != nil {
		return
	}

	conf := n.raft.ConfState()
	wanted := make(map[raft.NodeID]string, len(conf.Addrs))
	for _, id := range conf.Members() {
		if id == n.cfg.ID {
			continue
		}
		if addr := conf.Addrs[id]; addr != "" {
			wanted[id] = addr
		}
	}

	if n.transportPeers == nil {
		n.transportPeers = make(map[raft.NodeID]string, len(wanted))
	}

	complete := true
	for id, addr := range wanted {
		if n.transportPeers[id] == addr {
			continue
		}
		if err := tr.AddPeer(id, addr); err != nil {
			complete = false
			continue
		}
		n.transportPeers[id] = addr
	}
	for id := range n.transportPeers {
		if _, still := wanted[id]; still {
			continue
		}
		tr.RemovePeer(id)
		delete(n.transportPeers, id)
	}

	if complete {
		n.confSeq = seq
	}
}

func (n *Node) observeLeadership() {
	term, leader := n.raft.Term(), n.raft.Leader()
	if term == n.lastTerm && leader == n.lastLeader {
		return
	}
	n.lastTerm, n.lastLeader = term, leader
	n.cfg.Metrics.LeaderChanged(uint64(term), uint64(leader), n.raft.State() == raft.Leader)
}
