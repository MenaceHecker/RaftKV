package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

const (
	testTick = 5 * time.Millisecond

	settleTimeout = 20 * time.Second
)

type testCluster struct {
	t *testing.T

	mu    sync.RWMutex
	nodes map[raft.NodeID]*Node
	dirs  map[raft.NodeID]string
	ids   []raft.NodeID

	blocked map[[2]raft.NodeID]bool

	tune func(*Config)
}

func (c *testCluster) Send(msgs []raft.Message) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, m := range msgs {
		if c.blocked[[2]raft.NodeID{m.From, m.To}] {
			continue
		}
		if dst, ok := c.nodes[m.To]; ok {
			dst.Step(m)
		}
	}
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	return newTunedTestCluster(t, size, nil)
}

func newTunedTestCluster(t *testing.T, size int, tune func(*Config)) *testCluster {
	t.Helper()

	c := &testCluster{
		tune:    tune,
		t:       t,
		nodes:   make(map[raft.NodeID]*Node, size),
		dirs:    make(map[raft.NodeID]string, size),
		blocked: make(map[[2]raft.NodeID]bool),
	}
	for i := range size {
		c.ids = append(c.ids, raft.NodeID(i+1))
	}

	root := t.TempDir()
	for _, id := range c.ids {
		dir := filepath.Join(root, fmt.Sprintf("node-%d", id))
		c.dirs[id] = dir
		c.start(id)
	}

	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()

	cfg := Config{
		ID:            id,
		Peers:         c.ids,
		DataDir:       c.dirs[id],
		Transport:     c,
		TickInterval:  testTick,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	}
	if c.tune != nil {
		c.tune(&cfg)
	}

	n, err := Start(cfg)
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
}

func (c *testCluster) stop(id raft.NodeID) {
	c.t.Helper()

	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()

	if n != nil {
		if err := n.Stop(); err != nil {
			c.t.Fatalf("stopping node %d: %v", id, err)
		}
	}
}

func (c *testCluster) stopAll() {
	c.mu.Lock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.nodes = make(map[raft.NodeID]*Node)
	c.mu.Unlock()

	for _, n := range nodes {
		n.Stop()
	}
}

func (c *testCluster) node(id raft.NodeID) *Node {
	c.t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("node %d is not running", id)
	}
	return n
}

func (c *testCluster) running() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Node, 0, len(c.nodes))
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			out = append(out, n)
		}
	}
	return out
}

func (c *testCluster) isolate(id raft.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, other := range c.ids {
		if other == id {
			continue
		}
		c.blocked[[2]raft.NodeID{id, other}] = true
		c.blocked[[2]raft.NodeID{other, id}] = true
	}
}

func (c *testCluster) heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[[2]raft.NodeID]bool)
}

func (c *testCluster) awaitLeader() *Node {
	c.t.Helper()

	var found *Node
	c.eventually("a leader to be elected", func() bool {
		var leaders []*Node
		for _, n := range c.running() {
			if n.Status().State == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) != 1 {
			return false
		}
		found = leaders[0]
		return true
	})
	return found
}

func (c *testCluster) awaitLeaderOtherThan(excluded raft.NodeID) *Node {
	c.t.Helper()

	var found *Node
	c.eventually(fmt.Sprintf("a leader other than node %d", excluded), func() bool {
		for _, n := range c.running() {
			s := n.Status()
			if s.State == raft.Leader && s.ID != excluded {
				found = n
				return true
			}
		}
		return false
	})
	return found
}

func (c *testCluster) eventually(what string, cond func() bool) {
	c.t.Helper()

	deadline := time.Now().Add(settleTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(testTick)
	}
	c.t.Fatalf("timed out after %v waiting for %s\n%s", settleTimeout, what, c.dump())
}

func (c *testCluster) dump() string {
	out := "cluster state:\n"
	for _, n := range c.running() {
		s := n.Status()
		out += fmt.Sprintf("  node %d: state=%-9s term=%d leader=%d commit=%d applied=%d\n",
			s.ID, s.State, s.Term, s.Leader, s.Commit, s.Applied)
	}
	c.mu.RLock()
	if len(c.blocked) > 0 {
		out += fmt.Sprintf("  %d blocked links\n", len(c.blocked))
	}
	c.mu.RUnlock()
	return out
}

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), settleTimeout)
}

func put(t *testing.T, n *Node, client, seq uint64, key, value string) error {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()
	return n.Propose(ctx, statemachine.Command{
		ClientID: client, Seq: seq, Op: statemachine.OpPut, Key: key, Value: []byte(value),
	})
}

func mustPut(t *testing.T, n *Node, client, seq uint64, key, value string) {
	t.Helper()
	if err := put(t, n, client, seq, key, value); err != nil {
		t.Fatalf("writing %s=%s: %v", key, value, err)
	}
}

func mustGet(t *testing.T, n *Node, key string) string {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()

	value, ok, err := n.Get(ctx, key)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	if !ok {
		t.Fatalf("key %s is absent", key)
	}
	return string(value)
}

func TestClusterElectsALeader(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	c.eventually("all followers to recognize the leader", func() bool {
		want := leader.Status()
		for _, n := range c.running() {
			s := n.Status()
			if s.Leader != want.ID || s.Term != want.Term {
				return false
			}
		}
		return true
	})
}

func TestWriteThenLinearizableRead(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	mustPut(t, leader, 1, 1, "x", "hello")

	if got := mustGet(t, leader, "x"); got != "hello" {
		t.Fatalf("x = %q, want hello", got)
	}

	ctx, cancel := testContext(t)
	defer cancel()
	if _, ok, err := leader.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("Get(missing) = ok %v, err %v; want absent", ok, err)
	}
}

func TestWritesReplicateToEveryNode(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	for i := range 10 {
		mustPut(t, leader, 1, uint64(i+1), fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	want := leader.Status().Applied
	c.eventually("every node to apply the same entries", func() bool {
		for _, n := range c.running() {
			if n.Status().Applied != want {
				return false
			}
		}
		return true
	})
}

func TestFollowersRedirectRatherThanServe(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("followers to learn the leader", func() bool {
		for _, n := range c.running() {
			if n.Status().ID != leaderID && n.Status().Leader != leaderID {
				return false
			}
		}
		return true
	})

	ctx, cancel := testContext(t)
	defer cancel()

	for _, n := range c.running() {
		s := n.Status()
		if s.ID == leaderID {
			continue
		}

		err := n.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
		})
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("writing to follower %d gave %v, want ErrNotLeader", s.ID, err)
		}

		if _, _, err := n.Get(ctx, "k"); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("reading from follower %d gave %v, want ErrNotLeader; a follower "+
				"must never serve a read from its own state", s.ID, err)
		}

		if s.Leader != leaderID {
			t.Fatalf("follower %d names leader %d, want %d; a client could not be "+
				"redirected", s.ID, s.Leader, leaderID)
		}
	}
}

func TestStaleRetryIsDeduplicatedThroughTheFullStack(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	mustPut(t, leader, 7, 1, "x", "first")
	mustPut(t, leader, 7, 2, "x", "second")
	mustPut(t, leader, 7, 1, "x", "first")

	if got := mustGet(t, leader, "x"); got != "second" {
		t.Fatalf("x = %q after a stale retry, want second", got)
	}

	mustPut(t, leader, 7, 3, "y", "mine")
	mustPut(t, leader, 8, 1, "y", "somebody else")
	mustPut(t, leader, 7, 3, "y", "mine")

	if got := mustGet(t, leader, "y"); got != "somebody else" {
		t.Fatalf("y = %q after a client resent its latest write, want somebody else", got)
	}
}

func TestConcurrentWritesAllCommit(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	const writers = 25
	var wg sync.WaitGroup
	errs := make([]error, writers)

	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = put(t, leader, uint64(i+1), 1, fmt.Sprintf("key-%d", i), "value")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent write %d failed: %v", i, err)
		}
	}

	for i := range writers {
		if got := mustGet(t, leader, fmt.Sprintf("key-%d", i)); got != "value" {
			t.Fatalf("key-%d = %q, want value", i, got)
		}
	}
}

func TestDataSurvivesNodeRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	for i := range 5 {
		mustPut(t, leader, 1, uint64(i+1), fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	var victim raft.NodeID
	for _, id := range c.ids {
		if id != leaderID {
			victim = id
			break
		}
	}

	c.stop(victim)

	mustPut(t, leader, 1, 6, "during-outage", "written")

	c.start(victim)

	c.eventually("the restarted node to catch up", func() bool {
		want := leader.Status().Applied
		return c.node(victim).Status().Applied == want
	})
}

func TestLeaderRestartPreservesCommittedData(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	mustPut(t, leader, 1, 1, "durable", "value")

	c.stop(leaderID)

	next := c.awaitLeaderOtherThan(leaderID)
	if got := mustGet(t, next, "durable"); got != "value" {
		t.Fatalf("durable = %q on the new leader, want value; a committed write "+
			"was lost across a leader change", got)
	}

	c.start(leaderID)
	mustPut(t, next, 1, 2, "after", "rejoin")

	c.eventually("the restarted leader to rejoin and catch up", func() bool {
		want := next.Status().Applied
		return c.node(leaderID).Status().Applied == want
	})
}

func TestIsolatedLeaderCannotCommit(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	mustPut(t, leader, 1, 1, "before", "partition")
	c.isolate(leaderID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*testTick)
	defer cancel()

	err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 2, Op: statemachine.OpPut, Key: "during", Value: []byte("partition"),
	})
	if err == nil {
		t.Fatalf("an isolated leader committed a write\n%s", c.dump())
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 20*testTick)
	defer readCancel()
	if _, _, err := leader.Get(readCtx, "before"); err == nil {
		t.Fatalf("an isolated leader served a read; it could be arbitrarily stale\n%s", c.dump())
	}
}

func TestMajoritySideKeepsWorkingDuringPartition(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	mustPut(t, leader, 1, 1, "before", "partition")
	c.isolate(leaderID)

	next := c.awaitLeaderOtherThan(leaderID)

	mustPut(t, next, 2, 1, "during", "partition")
	if got := mustGet(t, next, "before"); got != "partition" {
		t.Fatalf("before = %q on the new leader, want partition", got)
	}
}

func TestPartitionedLeaderRejoinsAndCatchesUp(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	mustPut(t, leader, 1, 1, "before", "partition")
	c.isolate(leaderID)

	next := c.awaitLeaderOtherThan(leaderID)
	mustPut(t, next, 2, 1, "during", "partition")

	c.heal()

	c.eventually("the deposed leader to step down and catch up", func() bool {
		old := c.node(leaderID).Status()
		cur := next.Status()
		return old.State != raft.Leader && old.Applied == cur.Applied
	})
}

func TestStopIsIdempotent(t *testing.T) {
	c := newTestCluster(t, 1)
	n := c.awaitLeader()

	if err := n.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := n.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	ctx, cancel := testContext(t)
	defer cancel()

	if err := n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Propose after Stop gave %v, want ErrStopped", err)
	}
	if _, _, err := n.Get(ctx, "k"); !errors.Is(err, ErrStopped) {
		t.Fatalf("Get after Stop gave %v, want ErrStopped", err)
	}

	c.mu.Lock()
	delete(c.nodes, n.Status().ID)
	c.mu.Unlock()
}

func TestSingleNodeClusterServesImmediately(t *testing.T) {
	c := newTestCluster(t, 1)
	n := c.awaitLeader()

	mustPut(t, n, 1, 1, "solo", "value")
	if got := mustGet(t, n, "solo"); got != "value" {
		t.Fatalf("solo = %q, want value", got)
	}
}

func TestSnapshotAndCompactionAcrossRestart(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "node-1")

	start := func() *Node {
		t.Helper()
		n, err := Start(Config{
			ID:                1,
			Peers:             []raft.NodeID{1},
			DataDir:           dir,
			Transport:         nopTransport{},
			TickInterval:      testTick,
			ElectionTick:      10,
			HeartbeatTick:     1,
			SnapshotThreshold: 10,
			Sync:              storage.SyncNever,
		})
		if err != nil {
			t.Fatalf("starting: %v", err)
		}
		return n
	}

	n := start()

	deadline := time.Now().Add(settleTimeout)
	for n.Status().State != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(testTick)
	}
	if n.Status().State != raft.Leader {
		t.Fatal("single node did not become leader")
	}

	const writes = 50
	for i := range writes {
		mustPut(t, n, 1, uint64(i+1), fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}

	before := n.Status().Applied
	if err := n.Stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}

	restarted := start()
	t.Cleanup(func() { restarted.Stop() })

	deadline = time.Now().Add(settleTimeout)
	for restarted.Status().State != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(testTick)
	}

	for i := range writes {
		want := fmt.Sprintf("value-%d", i)
		if got := mustGet(t, restarted, fmt.Sprintf("key-%d", i)); got != want {
			t.Fatalf("key-%d = %q after restart, want %q", i, got, want)
		}
	}
	if got := restarted.Status().Applied; got < before {
		t.Fatalf("applied index went backwards across a restart: %d then %d", before, got)
	}
}

type nopTransport struct{}

func (nopTransport) Send([]raft.Message) {}
