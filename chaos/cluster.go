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

type OpKind int

const (
	OpWrite OpKind = iota
	OpRead
)

func (k OpKind) String() string {
	if k == OpWrite {
		return "write"
	}
	return "read"
}

type OpStatus int

const (
	StatusPending OpStatus = iota
	StatusOK
	StatusFailed
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

type Op struct {
	Kind   OpKind
	Client int
	Key    string

	Value string
	Found bool

	Invoked  int64
	Returned int64
	Status   OpStatus

	index raft.Index
	term  raft.Term

	readCtx string

	readIndex raft.Index
	readReady bool

	node raft.NodeID

	clientID uint64
	seq      uint64
}

type nodeStorage interface {
	raft.Storage
	CreateSnapshot(index raft.Index, data []byte, conf raft.ConfState) error
}

type Config struct {
	Nodes         int
	Seed          int64
	Faults        Faults
	ElectionTick  int
	HeartbeatTick int

	DataDir string

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

type Cluster struct {
	cfg Config
	net *Network

	ids      []raft.NodeID
	nodes    map[raft.NodeID]*raft.Node
	machines map[raft.NodeID]*statemachine.KV

	storages map[raft.NodeID]nodeStorage

	down map[raft.NodeID]bool

	pending []*Op
	history []*Op

	snapshotsInstalled map[raft.NodeID]int

	seq int
}

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

func (c *Cluster) openStorage(id raft.NodeID) (nodeStorage, error) {
	if c.cfg.DataDir == "" {
		return raft.NewMemoryStorage(), nil
	}

	dir := filepath.Join(c.cfg.DataDir, fmt.Sprintf("node-%d", id))
	st, _, err := storage.OpenDiskStorage(storage.DiskConfig{
		Dir:         dir,
		Sync:        c.cfg.Sync,
		SegmentSize: 16 << 10,
	})
	if err != nil {
		return nil, fmt.Errorf("chaos: opening storage for node %d: %w", id, err)
	}
	return st, nil
}

func (c *Cluster) closeStorage(id raft.NodeID) {
	if d, ok := c.storages[id].(*storage.DiskStorage); ok {
		d.Close()
	}
}

func (c *Cluster) start(id raft.NodeID) error {
	n, err := raft.NewNode(raft.Config{
		ID:            id,
		Peers:         c.ids,
		ElectionTick:  c.cfg.ElectionTick,
		HeartbeatTick: c.cfg.HeartbeatTick,
		Storage:       c.storages[id],
		CheckQuorum:   true,
		PreVote:       true,
		Rand:          rand.New(rand.NewSource(c.cfg.Seed + int64(id)*7919)),
	})
	if err != nil {
		return fmt.Errorf("chaos: starting node %d: %w", id, err)
	}

	c.nodes[id] = n
	c.machines[id] = statemachine.New()

	if snap, err := c.storages[id].Snapshot(); err == nil && snap.Index > 0 {
		if err := c.machines[id].Restore(snap.Data); err != nil {
			return fmt.Errorf("chaos: node %d restoring its own snapshot: %w", id, err)
		}
	}

	c.down[id] = false
	return nil
}

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

	if err := c.storages[id].CreateSnapshot(applied, data, n.ConfState()); err != nil {
		return nil
	}
	return nil
}

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

func (c *Cluster) SnapshotsInstalled(id raft.NodeID) int { return c.snapshotsInstalled[id] }

func (c *Cluster) TotalSnapshotsInstalled() int {
	var total int
	for _, n := range c.snapshotsInstalled {
		total += n
	}
	return total
}

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

func (c *Cluster) RemoveNode(id raft.NodeID) error {
	leader, ok, err := c.Leader()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("chaos: no leader to propose a membership change to")
	}
	if leader == id {
		_ = leader
	}

	return c.nodes[leader].ProposeConfChange(raft.ConfChange{
		Type:   raft.ConfChangeRemoveNode,
		NodeID: id,
	})
}

func (c *Cluster) Members(id raft.NodeID) []raft.NodeID {
	n, ok := c.nodes[id]
	if !ok || c.down[id] {
		return nil
	}
	return n.Members()
}

func (c *Cluster) InJoint(id raft.NodeID) bool {
	n, ok := c.nodes[id]
	if !ok || c.down[id] {
		return false
	}
	return n.InJointConfiguration()
}

func (c *Cluster) MembershipSettled() bool {
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

func (c *Cluster) Network() *Network { return c.net }

func (c *Cluster) IDs() []raft.NodeID { return c.ids }

func (c *Cluster) Now() int64 { return c.net.Now() }

func (c *Cluster) History() []Op {
	out := make([]Op, 0, len(c.history))
	for _, op := range c.history {
		out = append(out, *op)
	}
	return out
}

func (c *Cluster) Tick() error {
	for _, m := range c.net.Deliver() {
		n, ok := c.nodes[m.To]
		if !ok || c.down[m.To] {
			continue
		}
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

	return c.checkElectionSafety()
}

func (c *Cluster) checkElectionSafety() error {
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

func (c *Cluster) TickN(n int) error {
	for range n {
		if err := c.Tick(); err != nil {
			return err
		}
	}
	return nil
}

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

func (c *Cluster) Crash(id raft.NodeID) {
	if c.down[id] {
		return
	}
	c.down[id] = true
	delete(c.nodes, id)
	delete(c.machines, id)
	c.closeStorage(id)
	c.net.DropAllInFlight(id)

	for _, op := range c.pending {
		if op.node == id && op.Status == StatusPending {
			c.finish(op, StatusUnknown)
		}
	}
}

func (c *Cluster) Restart(id raft.NodeID) error {
	if !c.down[id] {
		return nil
	}

	c.net.DropAllInFlight(id)

	st, err := c.openStorage(id)
	if err != nil {
		return err
	}
	c.storages[id] = st

	return c.start(id)
}

func (c *Cluster) IsDown(id raft.NodeID) bool { return c.down[id] }

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
		return nil
	}
	return nil
}

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
		c.finish(op, StatusFailed)
		return op
	}

	op.node = node
	op.readCtx = ctx
	c.pending = append(c.pending, op)
	return op
}

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
		c.finish(op, StatusFailed)
		return op
	}

	op.node = id
	op.readCtx = ctx
	c.pending = append(c.pending, op)
	return op
}

func (c *Cluster) noteReadIndex(id raft.NodeID, rs raft.ReadState) {
	for _, op := range c.pending {
		if op.Kind == OpRead && op.node == id && op.readCtx == string(rs.Context) {
			op.readIndex = rs.Index
			op.readReady = true
			return
		}
	}
}

func (c *Cluster) resolve() {
	remaining := c.pending[:0]

	for _, op := range c.pending {
		if op.Status != StatusPending {
			continue
		}

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

func (c *Cluster) resolveWrite(op *Op, n *raft.Node) bool {
	kv := c.machines[op.node]
	if kv.Applied() < op.index {
		return false
	}

	term, err := n.TermAt(op.index)
	if err != nil || term != op.term {
		c.finish(op, StatusUnknown)
		return true
	}

	c.finish(op, StatusOK)
	return true
}

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

func (c *Cluster) finish(op *Op, status OpStatus) {
	op.Status = status
	op.Returned = c.net.Now()
}

func (c *Cluster) FailPending() {
	for _, op := range c.pending {
		if op.Status == StatusPending {
			c.finish(op, StatusUnknown)
		}
	}
	c.pending = nil
}

func (c *Cluster) Converged() (bool, error) {
	var reference []byte
	var refID raft.NodeID

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

func (c *Cluster) currentMembers() map[raft.NodeID]bool {
	if leader, ok, err := c.Leader(); err == nil && ok {
		return memberSet(c.Members(leader))
	}

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

func memberSet(ids []raft.NodeID) map[raft.NodeID]bool {
	set := make(map[raft.NodeID]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func writeCommand(key, value string) []byte {
	return statemachine.Command{
		Op: statemachine.OpPut, Key: key, Value: []byte(value),
	}.Encode()
}

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
