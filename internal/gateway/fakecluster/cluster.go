// Package fakecluster is an in-memory gateway.Cluster: a scripted leader,
// crash and restart, partition and pause, a linearizable register and a
// switchable lying-read mode. It exists so the gateway is tested with no AWS
// and no real node.
package fakecluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
)

type node struct {
	info        gateway.NodeInfo
	down        bool
	paused      bool
	blocked     []uint64
	netemDelay  time.Duration
	netemDrop   uint32
	hangStatus  bool
	statusCalls int
}

type entry struct {
	cur     []byte
	prev    []byte
	hasPrev bool
}

// Cluster implements gateway.Cluster.
type Cluster struct {
	mu    sync.Mutex
	cond  *sync.Cond
	nodes map[uint64]*node
	order []uint64

	leader   uint64
	term     uint64
	commit   uint64
	data     map[string]*entry
	nextCli  uint64
	inflight map[uint64]int
	lastSeq  map[uint64]uint64
	bad      []string

	lying    bool
	hangKV   bool
	gate     chan struct{}
	blocked  int
	active   int
	clients  map[uint64]bool
	events   map[uint64][]chan gateway.NodeEvent
	eventSeq uint64
}

// New builds a three node cluster, one node per zone, with node 1 leading.
func New() *Cluster {
	c := &Cluster{
		nodes:    map[uint64]*node{},
		data:     map[string]*entry{},
		inflight: map[uint64]int{},
		lastSeq:  map[uint64]uint64{},
		clients:  map[uint64]bool{},
		events:   map[uint64][]chan gateway.NodeEvent{},
		leader:   1,
		term:     1,
	}
	c.cond = sync.NewCond(&c.mu)
	for i, az := range []string{"us-east-1a", "us-east-1b", "us-east-1c"} {
		id := uint64(i + 1)
		c.nodes[id] = &node{info: gateway.NodeInfo{ID: id, AZ: az}}
		c.order = append(c.order, id)
	}
	return c
}

var _ gateway.Cluster = (*Cluster)(nil)

// Topology implements gateway.Cluster.
func (c *Cluster) Topology() []gateway.NodeInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]gateway.NodeInfo, 0, len(c.order))
	for _, id := range c.order {
		out = append(out, c.nodes[id].info)
	}
	return out
}

// Status implements gateway.Cluster.
func (c *Cluster) Status(ctx context.Context, id uint64) (gateway.NodeStatus, error) {
	c.mu.Lock()
	n, ok := c.nodes[id]
	if !ok {
		c.mu.Unlock()
		return gateway.NodeStatus{}, fmt.Errorf("fakecluster: no node %d", id)
	}
	n.statusCalls++
	hang := n.hangStatus
	c.cond.Broadcast()
	c.mu.Unlock()
	if hang {
		<-ctx.Done()
		return gateway.NodeStatus{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.down {
		return gateway.NodeStatus{}, fmt.Errorf("fakecluster: node %d is down", id)
	}
	role := "follower"
	if id == c.leader {
		role = "leader"
	}
	return gateway.NodeStatus{
		ID: id, LeaderID: c.leader, Term: c.term, Role: role,
		CommitIndex: c.commit, AppliedIndex: c.commit,
		FirstIndex: 1, LastIndex: c.commit + 1, SnapshotIndex: 0,
		Paused: n.paused,
	}, nil
}

// NewKV implements gateway.Cluster.
func (c *Cluster) NewKV() gateway.KV {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextCli++
	c.clients[c.nextCli] = true
	return &session{c: c, id: c.nextCli}
}

// Crash implements gateway.Cluster.
func (c *Cluster) Crash(_ context.Context, id uint64) error {
	return c.with(id, func(n *node) { n.down = true; c.moveLeaderLocked() })
}

// Pause implements gateway.Cluster.
func (c *Cluster) Pause(_ context.Context, id uint64, d time.Duration) error {
	return c.with(id, func(n *node) { n.paused = d > 0 })
}

// Partition implements gateway.Cluster.
func (c *Cluster) Partition(_ context.Context, id uint64, blocked []uint64) error {
	return c.with(id, func(n *node) { n.blocked = append([]uint64(nil), blocked...) })
}

// Netem implements gateway.Cluster.
func (c *Cluster) Netem(_ context.Context, id, _ uint64, delay time.Duration, drop uint32) error {
	return c.with(id, func(n *node) { n.netemDelay, n.netemDrop = delay, drop })
}

// ClearFaults implements gateway.Cluster.
func (c *Cluster) ClearFaults(_ context.Context, id uint64) error {
	return c.with(id, func(n *node) {
		n.blocked, n.paused, n.netemDelay, n.netemDrop = nil, false, 0, 0
	})
}

func (c *Cluster) with(id uint64, f func(*node)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("fakecluster: no node %d", id)
	}
	f(n)
	return nil
}

// moveLeaderLocked elects the lowest-numbered live node when the leader is
// down, as a scripted stand-in for an election.
func (c *Cluster) moveLeaderLocked() {
	if l := c.nodes[c.leader]; l != nil && !l.down {
		return
	}
	for _, id := range c.order {
		if !c.nodes[id].down {
			c.setLeaderLocked(id)
			return
		}
	}
}

func (c *Cluster) setLeaderLocked(id uint64) {
	c.leader = id
	c.term++
	for nid, subs := range c.events {
		role := "follower"
		if nid == id {
			role = "leader"
		}
		c.eventSeq++
		ev := gateway.NodeEvent{Node: nid, Term: c.term, Role: role, LeaderID: id, Seq: c.eventSeq}
		for _, ch := range subs {
			select {
			case ch <- ev:
			default:
			}
		}
	}
}

// Events streams leadership transitions until ctx ends.
func (c *Cluster) Events(ctx context.Context, id uint64) (<-chan gateway.NodeEvent, error) {
	c.mu.Lock()
	if _, ok := c.nodes[id]; !ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("fakecluster: no node %d", id)
	}
	ch := make(chan gateway.NodeEvent, 16)
	c.events[id] = append(c.events[id], ch)
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		defer c.mu.Unlock()
		subs := c.events[id]
		for i, s := range subs {
			if s == ch {
				c.events[id] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		close(ch)
	}()
	return ch, nil
}

// Controls for tests.

// SetLeader scripts a leader change.
func (c *Cluster) SetLeader(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLeaderLocked(id)
}

// Restart brings a crashed node back.
func (c *Cluster) Restart(id uint64) { _ = c.with(id, func(n *node) { n.down = false }) }

// SetLyingReads makes Get return the value before the latest write.
func (c *Cluster) SetLyingReads(on bool) { c.mu.Lock(); c.lying = on; c.mu.Unlock() }

// HangKV makes every KV call block until its context ends, like a request
// sent to an isolated leader.
func (c *Cluster) HangKV(on bool) { c.mu.Lock(); c.hangKV = on; c.mu.Unlock() }

// HangStatus makes Status on one node block until its context ends.
func (c *Cluster) HangStatus(id uint64, on bool) {
	_ = c.with(id, func(n *node) { n.hangStatus = on })
}

// HoldWrites makes every write block, after it has entered, until the
// returned function is called.
func (c *Cluster) HoldWrites() (release func()) {
	c.mu.Lock()
	gate := make(chan struct{})
	c.gate = gate
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			c.gate = nil
			c.mu.Unlock()
			close(gate)
		})
	}
}

// WaitBlocked returns once n writes are held by HoldWrites.
func (c *Cluster) WaitBlocked(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.blocked < n {
		c.cond.Wait()
	}
}

// WaitIdle returns once no KV call is executing.
func (c *Cluster) WaitIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.active > 0 {
		c.cond.Wait()
	}
}

// WaitStatusCalls returns once Status has been called at least n times on
// the node.
func (c *Cluster) WaitStatusCalls(id uint64, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.nodes[id].statusCalls < n {
		c.cond.Wait()
	}
}

// StatusCalls is how many times Status was called on a node.
func (c *Cluster) StatusCalls(id uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id].statusCalls
}

// Value reads the register directly, bypassing sessions.
func (c *Cluster) Value(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), e.cur...), true
}

// Keys lists the stored keys in order.
func (c *Cluster) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.data))
	for k := range c.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Violations reports session misuse the real state machine would turn into a
// silently lost write: two writes in flight for one client id.
func (c *Cluster) Violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.bad...)
}

// Clients is the number of sessions handed out.
func (c *Cluster) Clients() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.clients)
}

// Down reports whether a node is crashed.
func (c *Cluster) Down(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id].down
}

type session struct {
	c   *Cluster
	id  uint64
	seq uint64
}

func (s *session) enter(ctx context.Context, write bool) (seq uint64, err error) {
	c := s.c
	c.mu.Lock()
	c.active++
	if write {
		c.inflight[s.id]++
		if c.inflight[s.id] > 1 {
			c.bad = append(c.bad, fmt.Sprintf("client %d has two writes in flight", s.id))
		}
		s.seq++
		seq = s.seq
		if seq <= c.lastSeq[s.id] {
			c.bad = append(c.bad, fmt.Sprintf("client %d seq %d applied out of order", s.id, seq))
		}
	}
	hang, gate := c.hangKV, c.gate
	if write && gate != nil {
		c.blocked++
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	if hang {
		<-ctx.Done()
		return seq, gateway.ErrIndeterminate
	}
	if write && gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			c.mu.Lock()
			c.blocked--
			c.mu.Unlock()
			return seq, gateway.ErrIndeterminate
		}
		c.mu.Lock()
		c.blocked--
		c.mu.Unlock()
	}
	return seq, nil
}

func (s *session) leave(write bool) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	if write {
		c.inflight[s.id]--
	}
	c.cond.Broadcast()
}

// Get implements gateway.Cluster.
func (s *session) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if _, err := s.enter(ctx, false); err != nil {
		s.leave(false)
		return nil, false, err
	}
	defer s.leave(false)
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[key]
	if !ok {
		return nil, false, nil
	}
	if c.lying && e.hasPrev {
		return append([]byte(nil), e.prev...), true, nil
	}
	return append([]byte(nil), e.cur...), true, nil
}

// Put implements gateway.Cluster.
func (s *session) Put(ctx context.Context, key string, value []byte) error {
	seq, err := s.enter(ctx, true)
	if err != nil {
		s.leave(true)
		return err
	}
	defer s.leave(true)
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if l := c.nodes[c.leader]; l == nil || l.down {
		return gateway.ErrNoLeader
	}
	e := c.data[key]
	if e == nil {
		e = &entry{}
		c.data[key] = e
	} else {
		e.prev, e.hasPrev = e.cur, true
	}
	e.cur = append([]byte(nil), value...)
	c.lastSeq[s.id] = seq
	c.commit++
	return nil
}

// Delete implements gateway.Cluster.
func (s *session) Delete(ctx context.Context, key string) error {
	seq, err := s.enter(ctx, true)
	if err != nil {
		s.leave(true)
		return err
	}
	defer s.leave(true)
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if l := c.nodes[c.leader]; l == nil || l.down {
		return gateway.ErrNoLeader
	}
	delete(c.data, key)
	c.lastSeq[s.id] = seq
	c.commit++
	return nil
}
