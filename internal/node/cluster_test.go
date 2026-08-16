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

// Tests for the node driver.
//
// Unlike the Raft core's harness, this one runs real goroutines against real
// time: the driver exists precisely to introduce those, so testing it without
// them would test nothing. That makes these tests inherently less reproducible
// than the deterministic ones below them, which is why they check outcomes
// that must eventually hold rather than exact interleavings, and why the
// deterministic suite remains where the subtle consensus properties are
// pinned down.
//
// Ticks are short so a full election fits in milliseconds, and every wait is
// bounded so a hang fails with a diagnosis instead of a timeout.

const (
	// testTick keeps elections fast. It is well above the scheduling noise a
	// loaded CI machine introduces, so a slow moment does not read as a
	// failure.
	testTick = 5 * time.Millisecond

	// settleTimeout bounds how long a test waits for a condition that should
	// hold within a few election rounds.
	//
	// It is far longer than the handful of ticks these conditions actually
	// need. The generosity is deliberate: these tests share a machine with
	// the storage suite's fsync-heavy runs, and a scheduling stall there must
	// not read as a consensus failure here. A real hang still fails, just
	// later, and with a cluster dump explaining what was true at the time.
	settleTimeout = 20 * time.Second
)

// testCluster is a set of driver nodes wired together in one process.
type testCluster struct {
	t *testing.T

	mu    sync.RWMutex
	nodes map[raft.NodeID]*Node
	dirs  map[raft.NodeID]string
	ids   []raft.NodeID

	// blocked records which ordered pairs cannot exchange messages, which is
	// how partitions are simulated.
	blocked map[[2]raft.NodeID]bool
}

// Send implements Transport by handing messages straight to the destination
// node, subject to whatever partition is in force.
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

// newTestCluster starts a cluster of the given size with IDs 1..size.
func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	c := &testCluster{
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

// start brings up one node against its existing data directory.
func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()

	n, err := Start(Config{
		ID:            id,
		Peers:         c.ids,
		DataDir:       c.dirs[id],
		Transport:     c,
		TickInterval:  testTick,
		ElectionTick:  10,
		HeartbeatTick: 1,
		// Tests do not survive power loss, and fsyncing every append makes
		// them an order of magnitude slower for no additional coverage.
		Sync: storage.SyncNever,
	})
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
}

// stop shuts one node down, leaving its data directory intact so it can be
// restarted.
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

// node returns a running node by ID.
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

// running returns every node currently up.
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

// isolate cuts a node off from every other node, in both directions.
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

// heal restores full connectivity.
func (c *testCluster) heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[[2]raft.NodeID]bool)
}

// awaitLeader waits for exactly one node to consider itself leader and returns
// it.
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

// awaitLeaderOtherThan waits for leadership to move away from a given node.
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

// eventually polls until cond holds, failing with a cluster dump if it never
// does. Real time makes some waiting unavoidable; bounding it means a broken
// invariant reports what was actually true rather than hanging.
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

// dump renders every running node's status, so a failure is diagnosable from
// the test output alone.
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

// testContext returns a context bounded by the settle timeout.
func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), settleTimeout)
}

// put writes a key through a node.
func put(t *testing.T, n *Node, client, seq uint64, key, value string) error {
	t.Helper()
	ctx, cancel := testContext(t)
	defer cancel()
	return n.Propose(ctx, statemachine.Command{
		ClientID: client, Seq: seq, Op: statemachine.OpPut, Key: key, Value: []byte(value),
	})
}

// mustPut writes a key and fails the test if it does not commit.
func mustPut(t *testing.T, n *Node, client, seq uint64, key, value string) {
	t.Helper()
	if err := put(t, n, client, seq, key, value); err != nil {
		t.Fatalf("writing %s=%s: %v", key, value, err)
	}
}

// mustGet performs a linearizable read and fails if it errors or is absent.
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

	// Every follower must converge on the same leader and term, which is what
	// makes redirection possible.
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

	// A key that was never written must read as absent rather than empty.
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

	// Every node must apply the same entries. Followers cannot be read
	// through Get, which requires leadership, so this checks the applied
	// index instead — the state machine is deterministic, so equal applied
	// indexes mean equal state.
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
	// The property the server layer's redirect is built on: a follower must
	// refuse both writes and reads, and must name the leader so the client
	// knows where to go.
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
	// The same hazard the state machine tests cover, but end to end: through
	// the driver, the log, the WAL, and back out through a linearizable read.
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()

	mustPut(t, leader, 7, 1, "x", "first")
	mustPut(t, leader, 7, 2, "x", "second")
	mustPut(t, leader, 7, 1, "x", "first") // the delayed retry

	if got := mustGet(t, leader, "x"); got != "second" {
		t.Fatalf("x = %q after a stale retry, want second", got)
	}
}

func TestConcurrentWritesAllCommit(t *testing.T) {
	// Many clients writing at once must every one of them either commit or
	// report a reason. A proposal that is silently dropped would leave a
	// client waiting forever.
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
	// A follower is stopped and brought back against the same data directory,
	// which is what a crash and restart looks like from the cluster's side.
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

	// The cluster keeps working with two of three.
	mustPut(t, leader, 1, 6, "during-outage", "written")

	c.start(victim)

	// The restarted node must catch up to everything, including what it
	// missed while down.
	c.eventually("the restarted node to catch up", func() bool {
		want := leader.Status().Applied
		return c.node(victim).Status().Applied == want
	})
}

func TestLeaderRestartPreservesCommittedData(t *testing.T) {
	// Losing the leader is the harder case: a new one must be elected and
	// every committed write must still be there.
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

	// And the old leader rejoins without disturbing anything.
	c.start(leaderID)
	mustPut(t, next, 1, 2, "after", "rejoin")

	c.eventually("the restarted leader to rejoin and catch up", func() bool {
		want := next.Status().Applied
		return c.node(leaderID).Status().Applied == want
	})
}

func TestIsolatedLeaderCannotCommit(t *testing.T) {
	// A leader cut off from the majority must not be able to commit or to
	// serve a read. It will still believe it leads for a while, which is
	// exactly the dangerous window.
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
	// The other half of the partition story: the side with a majority must
	// elect a new leader and keep serving.
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
	// Once healed, the deposed leader must step down, discard anything it
	// wrote alone, and converge on the majority's log.
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

	// Requests after Stop must fail rather than block forever.
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
	// A one-node cluster is its own majority. This is the configuration that
	// exposed the leader-never-commits-its-no-op bug, so it is worth
	// exercising end to end.
	c := newTestCluster(t, 1)
	n := c.awaitLeader()

	mustPut(t, n, 1, 1, "solo", "value")
	if got := mustGet(t, n, "solo"); got != "value" {
		t.Fatalf("solo = %q, want value", got)
	}
}

func TestSnapshotAndCompactionAcrossRestart(t *testing.T) {
	// With a low threshold the node compacts repeatedly, so a restart has to
	// rebuild from a snapshot plus the log tail rather than from the log
	// alone.
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

	// Everything written before the restart must still be readable, which
	// means the snapshot and the log tail were spliced together correctly.
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

// nopTransport discards messages, for a single-node cluster that has nobody to
// talk to.
type nopTransport struct{}

func (nopTransport) Send([]raft.Message) {}
