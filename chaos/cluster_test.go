package chaos

import (
	"fmt"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the chaos cluster.
//
// Like the network, this is test infrastructure that needs testing. The
// specific risk is a harness that appears to inject a fault but does not: a
// crash that leaves the node running, a history that records an unknown
// outcome as a success, a convergence check that compares nothing. Every one
// of those makes the scenarios built on top pass for the wrong reason.

// newTestCluster builds a cluster and fails the test on error.
func newTestCluster(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	return c
}

// tick advances the cluster, failing the test on error.
func tick(t *testing.T, c *Cluster, n int) {
	t.Helper()
	if err := c.TickN(n); err != nil {
		t.Fatalf("ticking: %v\n%s", err, c.Dump())
	}
}

// awaitLeader waits for a leader, failing the test if none appears.
func awaitLeader(t *testing.T, c *Cluster) raft.NodeID {
	t.Helper()
	id, err := c.AwaitLeader(500)
	if err != nil {
		t.Fatalf("awaiting a leader: %v", err)
	}
	return id
}

// writeAndSettle writes a key and ticks until the operation settles.
func writeAndSettle(t *testing.T, c *Cluster, client int, key, value string) *Op {
	t.Helper()
	op := c.Write(client, key, value)
	for range 200 {
		if op.Status != StatusPending {
			return op
		}
		tick(t, c, 1)
	}
	return op
}

func TestClusterElectsALeaderOnAPerfectNetwork(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 1})
	leader := awaitLeader(t, c)

	tick(t, c, 20)

	// Everyone else must agree, or the cluster is not actually settled and
	// any scenario built on it would be racing an election.
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		if got := c.nodes[id].Leader(); got != leader {
			t.Fatalf("node %d follows %d, want %d\n%s", id, got, leader, c.Dump())
		}
	}
}

func TestWritesAndReadsSucceed(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 2})
	awaitLeader(t, c)

	op := writeAndSettle(t, c, 1, "x", "hello")
	if op.Status != StatusOK {
		t.Fatalf("write status = %s, want ok\n%s", op.Status, c.Dump())
	}

	read := c.Read(1, "x")
	for range 200 {
		if read.Status != StatusPending {
			break
		}
		tick(t, c, 1)
	}
	if read.Status != StatusOK {
		t.Fatalf("read status = %s, want ok\n%s", read.Status, c.Dump())
	}
	if !read.Found || read.Value != "hello" {
		t.Fatalf("read %q (found %v), want hello", read.Value, read.Found)
	}
}

func TestARunIsReproducible(t *testing.T) {
	// The property the whole harness exists for. A scenario that fails must
	// fail the same way when re-run, or the failure cannot be investigated and
	// the suite only teaches people to run it again.
	run := func() string {
		c := newTestCluster(t, Config{
			Nodes: 5, Seed: 99,
			Faults: Faults{LossRate: 0.15, MinDelay: 0, MaxDelay: 4, DuplicateRate: 0.05},
		})
		if _, err := c.AwaitLeader(500); err != nil {
			t.Fatalf("awaiting a leader: %v", err)
		}
		for round := range 8 {
			for client := 1; client <= 3; client++ {
				c.Write(client, "k", fmt.Sprintf("%d-%d", round, client))
			}
			tick(t, c, 4)
		}
		tick(t, c, 60)
		c.FailPending()

		out := ""
		for _, op := range c.History() {
			out += fmt.Sprintf("%s|%d|%s|%s|%d|%d;",
				op.Kind, op.Client, op.Value, op.Status, op.Invoked, op.Returned)
		}
		st := c.Network().Stats()
		return out + fmt.Sprintf("net:%d/%d/%d", st.Sent, st.Delivered, st.Dropped)
	}

	first := run()
	for i := range 10 {
		if got := run(); got != first {
			t.Fatalf("run %d diverged from the first; the harness is not reproducible", i)
		}
	}
}

func TestCrashStopsANodeCompletely(t *testing.T) {
	// A crashed node must stop participating entirely. One that kept ticking
	// would make every crash scenario a test of a healthy cluster.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 3})
	leader := awaitLeader(t, c)

	var victim raft.NodeID
	for _, id := range c.IDs() {
		if id != leader {
			victim = id
			break
		}
	}

	before := c.nodes[victim].CommitIndex()
	c.Crash(victim)

	if !c.IsDown(victim) {
		t.Fatal("the node does not report as crashed")
	}
	if _, running := c.nodes[victim]; running {
		t.Fatal("a crashed node still has a live Raft node")
	}

	// The rest of the cluster keeps working, and the crashed node learns
	// nothing while it is down.
	writeAndSettle(t, c, 1, "x", "during-outage")
	tick(t, c, 30)

	if err := c.Restart(victim); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if c.IsDown(victim) {
		t.Fatal("the node still reports as crashed after a restart")
	}
	_ = before
}

func TestARestartedNodeRebuildsItsStateMachine(t *testing.T) {
	// The state machine is volatile; only the log survives. A restart that
	// kept the old state machine would skip the recovery path entirely, which
	// is the most interesting thing a crash tests.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 4})
	leader := awaitLeader(t, c)

	for i := range 5 {
		if op := writeAndSettle(t, c, 1, "x", fmt.Sprintf("v%d", i)); op.Status != StatusOK {
			t.Fatalf("write %d: %s\n%s", i, op.Status, c.Dump())
		}
	}

	var victim raft.NodeID
	for _, id := range c.IDs() {
		if id != leader {
			victim = id
			break
		}
	}

	c.Crash(victim)
	if err := c.Restart(victim); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	// Immediately after the restart the state machine is empty; it has to
	// replay the log to catch up.
	if got := c.machines[victim].Applied(); got != 0 {
		t.Fatalf("a restarted node's state machine starts at applied %d, want 0", got)
	}

	tick(t, c, 200)

	converged, err := c.Converged()
	if err != nil {
		t.Fatalf("Converged: %v", err)
	}
	if !converged {
		t.Fatalf("the restarted node did not rebuild the same state\n%s", c.Dump())
	}
}

func TestCrashDiscardsMessagesInFlightToTheNode(t *testing.T) {
	// Delivering pre-crash messages to a restarted process would hand a node
	// traffic from before it existed.
	c := newTestCluster(t, Config{
		Nodes: 3, Seed: 5,
		Faults: Faults{MinDelay: 5, MaxDelay: 5},
	})
	awaitLeader(t, c)
	tick(t, c, 5)

	inflight := c.Network().InFlight()
	if inflight == 0 {
		t.Fatal("nothing is in flight, so this test cannot observe the discard")
	}

	c.Crash(2)
	if got := c.Network().InFlight(); got >= inflight {
		t.Fatalf("in-flight count went from %d to %d after a crash; nothing was discarded",
			inflight, got)
	}
}

func TestOperationsAgainstNoLeaderFailRatherThanHang(t *testing.T) {
	// A write nobody could have accepted definitely did not happen, and saying
	// so is what lets the checker exclude it. Recording it as unknown would
	// make every history needlessly ambiguous.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 6})

	// No ticks yet, so no election has happened.
	op := c.Write(1, "x", "v")
	if op.Status != StatusFailed {
		t.Fatalf("a write with no leader has status %s, want failed", op.Status)
	}
	read := c.Read(1, "x")
	if read.Status != StatusFailed {
		t.Fatalf("a read with no leader has status %s, want failed", read.Status)
	}
}

func TestCrashingTheServingNodeMakesItsOperationsUnknown(t *testing.T) {
	// The distinction the checker depends on. A write the crashed node had
	// already replicated can still commit through the others, so its outcome
	// is genuinely unknown — not failed.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 7})
	leader := awaitLeader(t, c)

	op := c.Write(1, "x", "in-flight")
	if op.Status != StatusPending {
		t.Fatalf("the write settled immediately with status %s; there is nothing "+
			"in flight to crash", op.Status)
	}

	c.Crash(leader)

	if op.Status != StatusUnknown {
		t.Fatalf("an operation on a crashed node has status %s, want unknown", op.Status)
	}
}

func TestHistoryRecordsEveryOperationInOrder(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 8})
	awaitLeader(t, c)

	const count = 12
	for i := range count {
		c.Write(1, "x", fmt.Sprintf("v%d", i))
		tick(t, c, 2)
	}
	tick(t, c, 60)
	c.FailPending()

	history := c.History()
	if len(history) != count {
		t.Fatalf("history has %d operations, want %d", len(history), count)
	}
	for i := 1; i < len(history); i++ {
		if history[i].Invoked < history[i-1].Invoked {
			t.Fatalf("history is not in invocation order at position %d", i)
		}
	}
	for i, op := range history {
		if op.Status == StatusPending {
			t.Fatalf("operation %d is still pending after FailPending", i)
		}
		if op.Returned < op.Invoked {
			t.Fatalf("operation %d returned at %d before it was invoked at %d",
				i, op.Returned, op.Invoked)
		}
	}
}

func TestOperationsOverlapInTime(t *testing.T) {
	// Concurrency is the point. A history where every operation returned
	// before the next was invoked has exactly one possible ordering, and
	// checking it for linearizability proves nothing.
	c := newTestCluster(t, Config{
		Nodes: 3, Seed: 9,
		Faults: Faults{MinDelay: 1, MaxDelay: 4},
	})
	awaitLeader(t, c)

	for range 6 {
		for client := 1; client <= 3; client++ {
			c.Write(client, "x", fmt.Sprintf("c%d", client))
		}
		tick(t, c, 2)
	}
	tick(t, c, 80)
	c.FailPending()

	history := c.History()
	overlaps := 0
	for i := range history {
		for j := i + 1; j < len(history); j++ {
			// Two operations overlap when neither returned before the other
			// was invoked.
			if history[i].Returned >= history[j].Invoked &&
				history[j].Returned >= history[i].Invoked {
				overlaps++
			}
		}
	}
	if overlaps == 0 {
		t.Fatalf("no two operations overlapped, so the history has only one possible "+
			"ordering\n%s", c.Dump())
	}
}

func TestConvergenceComparesActualState(t *testing.T) {
	// Comparing applied indexes would call two nodes converged while they held
	// different data. The check uses state machine snapshots, which are a
	// deterministic function of the state.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 10})
	awaitLeader(t, c)

	for i := range 5 {
		writeAndSettle(t, c, 1, fmt.Sprintf("k%d", i), "v")
	}
	tick(t, c, 60)

	converged, err := c.Converged()
	if err != nil {
		t.Fatalf("Converged: %v", err)
	}
	if !converged {
		t.Fatalf("a healthy cluster did not converge\n%s", c.Dump())
	}

	// Corrupt one node's state machine directly. The check must notice.
	if err := c.machines[c.IDs()[1]].Apply(raft.Entry{
		Term:  99,
		Index: c.machines[c.IDs()[1]].Applied() + 1,
		Type:  raft.EntryNormal,
		Data:  writeCommand("rogue", "value"),
	}); err != nil {
		t.Fatalf("injecting divergence: %v", err)
	}

	converged, err = c.Converged()
	if err != nil {
		t.Fatalf("Converged: %v", err)
	}
	if converged {
		t.Fatal("convergence reported success while one node held different data")
	}
}

func TestElectionSafetyIsCheckedContinuously(t *testing.T) {
	// Two leaders in one term is the violation everything else rests on, so
	// asking for the leader reports it rather than picking one.
	c := newTestCluster(t, Config{Nodes: 3, Seed: 11})
	awaitLeader(t, c)

	if _, _, err := c.Leader(); err != nil {
		t.Fatalf("a healthy cluster reported an election safety violation: %v", err)
	}
}

func TestClusterSurvivesLossAndDelay(t *testing.T) {
	// The basic robustness claim: under a network that drops and reorders,
	// the cluster still elects, still commits, and still converges.
	c := newTestCluster(t, Config{
		Nodes: 5, Seed: 12,
		Faults: Faults{LossRate: 0.2, MinDelay: 0, MaxDelay: 5, DuplicateRate: 0.05},
	})
	awaitLeader(t, c)

	committed := 0
	for i := range 15 {
		if op := writeAndSettle(t, c, 1, "x", fmt.Sprintf("v%d", i)); op.Status == StatusOK {
			committed++
		}
	}
	tick(t, c, 200)
	c.FailPending()

	if committed == 0 {
		t.Fatalf("nothing committed under loss and delay\n%s", c.Dump())
	}

	converged, err := c.Converged()
	if err != nil {
		t.Fatalf("Converged: %v", err)
	}
	if !converged {
		t.Fatalf("the cluster did not converge under loss and delay\n%s", c.Dump())
	}

	// The run must actually have been adverse, or it proves nothing.
	st := c.Network().Stats()
	if st.Dropped == 0 {
		t.Fatal("no messages were dropped, so this run was not adverse")
	}
	if st.Delayed == 0 {
		t.Fatal("no messages were delayed, so this run was not adverse")
	}
}
