package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// T3.16: at most one configuration change may be pending (Raft paper §6,
// dissertation §4.1 and §4.3). A change is pending while its entry sits above
// the commit index. The tests hold messages back with c.filter so that the
// window is observable.

func addNode(id NodeID) ConfChange {
	return ConfChange{Type: ConfChangeAddNode, NodeID: id, Addr: fmt.Sprintf("h:%d", id)}
}

var leaveJoint = ConfChange{Type: ConfChangeLeaveJoint}

// mustBePending fails unless err is ErrConfChangePending (and so also
// ErrConfChangeInFlight) and its message contains every want.
func mustBePending(t *testing.T, err error, want ...string) {
	t.Helper()
	if !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("got %v, want ErrConfChangePending", err)
	}
	if !errors.Is(err, ErrConfChangeInFlight) {
		t.Fatalf("got %v, want it to also match ErrConfChangeInFlight", err)
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Fatalf("error %q does not contain %q", err, w)
		}
	}
}

func isolate(c *cluster, id NodeID) {
	c.t.Helper()
	var others []NodeID
	for _, o := range c.ids {
		if o != id {
			others = append(others, o)
		}
	}
	c.partition([]NodeID{id}, others)
}

// Test 1: a Leave proposed before the joint entry commits is refused.
func TestLeaveBeforeJointCommitsIsRejected(t *testing.T) {
	c, n := leaderWithConfChange(t, 3, 850)
	isolate(c, n.ID())

	if err := n.ProposeConfChange(addNode(4)); err != nil {
		t.Fatalf("Add 4: %v", err)
	}
	joint := n.LastIndex()
	last := n.LastIndex()

	err := n.ProposeConfChange(leaveJoint)
	mustBePending(t, err, fmt.Sprintf("index %d", joint))
	if n.LastIndex() != last {
		t.Fatalf("the refused Leave still reached the log: last %d, was %d", n.LastIndex(), last)
	}
	if !n.InJointConfiguration() {
		t.Fatal("the refused Leave still ended the joint configuration")
	}

	c.heal()
	c.tickN(defaultElectionTick)
	cur := c.node(n.ID())
	if cur.InJointConfiguration() || len(cur.Members()) != 4 {
		t.Fatalf("the automatic leave did not complete after healing\n%s", c.dump())
	}
}

// Test 2: a second change is refused while the automatic leave is uncommitted.
func TestSecondChangeWhileLeaveIsUncommittedIsRejected(t *testing.T) {
	c, n := leaderWithConfChange(t, 3, 851)

	if err := n.ProposeConfChange(addNode(4)); err != nil {
		t.Fatalf("Add 4: %v", err)
	}
	leaveIdx := n.LastIndex() + 1
	c.filter = func(m Message) bool {
		return m.Type != MsgAppendResponse || m.MatchIndex < leaveIdx
	}
	c.deliverAll()

	if got := countLeaveEntries(c.logEntries(n.ID())); got != 1 || n.LastIndex() != leaveIdx {
		t.Fatalf("expected the automatic leave at %d; leaves=%d last=%d\n%s", leaveIdx, got, n.LastIndex(), c.dump())
	}
	if n.CommitIndex() >= leaveIdx {
		t.Fatalf("the leave committed (commit %d); the filter did not hold its acks", n.CommitIndex())
	}

	mustBePending(t, n.ProposeConfChange(addNode(5)), fmt.Sprintf("index %d", leaveIdx))

	c.heal()
	c.tickN(defaultElectionTick)
	if n.CommitIndex() < leaveIdx {
		t.Fatalf("the leave never committed\n%s", c.dump())
	}
	if err := n.ProposeConfChange(leaveJoint); !errors.Is(err, ErrNotInJoint) {
		t.Fatalf("Leave after the transition gave %v, want ErrNotInJoint", err)
	}
	if err := n.ProposeConfChange(addNode(5)); err != nil {
		t.Fatalf("Add 5 after the leave committed: %v", err)
	}
}

// Test 3: the error value for a second change during the joint phase.
func TestSecondChangeWhileJointUncommittedIsRejectedAsPending(t *testing.T) {
	c, n := leaderWithConfChange(t, 3, 852)
	isolate(c, n.ID())

	if err := n.ProposeConfChange(addNode(4)); err != nil {
		t.Fatalf("Add 4: %v", err)
	}
	joint := n.LastIndex()
	mustBePending(t, n.ProposeConfChange(addNode(5)), fmt.Sprintf("index %d", joint))
	mustBePending(t, n.ProposeConfChange(leaveJoint), fmt.Sprintf("index %d", joint))
}

// Test 4(d): a new leader that has not committed in its term refuses a change.
func TestLeaderWithoutACommittedEntryInItsTermRefusesAChange(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 853})
	c.filter = func(m Message) bool { return m.Type != MsgAppendResponse }
	c.campaign(1)
	n := c.node(1)
	if n.State() != Leader {
		t.Fatalf("node 1 did not win\n%s", c.dump())
	}
	if n.hasCommittedInCurrentTerm() {
		t.Fatal("the no-op committed; the fixture did not hold the acks")
	}

	mustBePending(t, n.ProposeConfChange(addNode(4)), "not yet committed an entry in its term")
	if n.LastIndex() != 1 {
		t.Fatalf("the refused change reached the log (last %d)", n.LastIndex())
	}
}

// Test 6: the automatic leave still runs, exactly once.
func TestAutomaticLeaveStillRuns(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 855})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	if err := n.ProposeConfChange(addNode(6)); err != nil {
		t.Fatalf("Add 6: %v", err)
	}
	c.deliverAll()
	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: otherThan(c, leader)}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	c.deliverAll()

	if got := countLeaveEntries(c.logEntries(leader)); got != 2 {
		t.Fatalf("%d leave entries for two changes, want exactly two\n%s", got, c.dump())
	}
	if n.InJointConfiguration() {
		t.Fatal("the transition did not complete")
	}
}

func otherThan(c *cluster, id NodeID) NodeID {
	for _, o := range c.ids {
		if o != id {
			return o
		}
	}
	return None
}

// Test 7: chained uncommitted changes must not let a minority grow its own
// quorum. Leader 1 is cut off from {2,3}. It appends Add 4, Leave, Add 5, Leave
// back to back (direct calls: no client can send these, see the card). If all
// are accepted, node 1's configuration is {1..5}, a majority of three, and acks
// from {1,4,5} commit entries that {2,3}, a majority of the old configuration,
// elect a new leader to overwrite. Two nodes would then commit different
// entries at one index (State Machine Safety, Raft paper §5.4.3).
func TestChainedUncommittedChangesCannotBuildDisjointQuorums(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 856, voters: []NodeID{1, 2, 3}})
	c.campaign(1)
	n1 := c.node(1)
	if n1.State() != Leader {
		t.Fatalf("node 1 did not win\n%s", c.dump())
	}
	c.tickN(2)

	group := map[NodeID]int{1: 0, 4: 0, 5: 0, 2: 1, 3: 1}
	c.filter = func(m Message) bool { return group[m.From] == group[m.To] }

	first := n1.LastIndex() + 1
	var secondErr error
	for i, cc := range []ConfChange{addNode(4), leaveJoint, addNode(5), leaveJoint} {
		err := n1.ProposeConfChange(cc)
		if i == 1 {
			secondErr = err
		}
	}

	c.tickN(defaultElectionTick * 4)

	var other *Node
	for _, id := range []NodeID{2, 3} {
		if c.node(id).State() == Leader {
			other = c.node(id)
		}
	}
	if other == nil {
		t.Fatalf("neither node 2 nor 3 won an election in the old majority\n%s", c.dump())
	}

	top := min(n1.CommitIndex(), other.CommitIndex())
	for i := first; i <= top; i++ {
		a, b := c.logEntries(1)[i-c.storages[1].FirstIndex()], c.logEntries(other.ID())[i-c.storages[other.ID()].FirstIndex()]
		if a.Term != b.Term || a.Type != b.Type || string(a.Data) != string(b.Data) {
			t.Fatalf("State Machine Safety violated: node 1 and node %d committed different entries at index %d "+
				"(term %d vs %d, commit %d vs %d)\n%s", other.ID(), i, a.Term, b.Term,
				n1.CommitIndex(), other.CommitIndex(), c.dump())
		}
	}
	mustBePending(t, secondErr)
}

// confEntriesAbove counts configuration entries in a node's log above index i.
func confEntriesAbove(c *cluster, id NodeID, i Index) int {
	count := 0
	for _, e := range c.logEntries(id) {
		if e.Index > i && e.Type == EntryConfChange {
			count++
		}
	}
	return count
}

// Test 9: across random schedules of changes, partitions, restarts and heals,
// a proposal is never accepted while a configuration entry sits above the
// proposer's commit index, and a leader that has committed in its term never
// has two. The failure message carries the seed.
func TestAtMostOnePendingConfEntry(t *testing.T) {
	seeds := 500
	if testing.Short() {
		seeds = 50
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		runPendingSchedule(t, seed)
	}
}

func runPendingSchedule(t *testing.T, seed int64) {
	t.Helper()
	c := newCluster(t, 5, clusterOpts{seed: seed, voters: []NodeID{1, 2, 3}})
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic simulation

	for step := 0; step < 120; step++ {
		id := c.ids[rng.Intn(len(c.ids))]
		n := c.node(id)

		switch rng.Intn(10) {
		case 0, 1, 2:
			for range 1 + rng.Intn(3) {
				tickMembers(c)
			}
		case 3:
			_ = n.Propose([]byte("x"))
			c.deliverAll()
		case 4, 5, 6:
			var cc ConfChange
			switch rng.Intn(3) {
			case 0:
				cc = addNode(NodeID(4 + rng.Intn(2))) //nolint:gosec // 4 or 5
			case 1:
				cc = ConfChange{Type: ConfChangeRemoveNode, NodeID: NodeID(1 + rng.Intn(5))} //nolint:gosec // 1..5
			default:
				cc = leaveJoint
			}
			above := confEntriesAbove(c, id, n.CommitIndex())
			if err := n.ProposeConfChange(cc); err == nil && above > 0 {
				t.Fatalf("seed %d step %d: node %d accepted %v with %d configuration entries above its commit index %d",
					seed, step, id, cc.Type, above, n.CommitIndex())
			}
			c.deliverAll()
		case 7:
			var a, b []NodeID
			for _, o := range c.ids {
				if rng.Intn(2) == 0 {
					a = append(a, o)
				} else {
					b = append(b, o)
				}
			}
			group := map[NodeID]int{}
			for _, o := range a {
				group[o] = 0
			}
			for _, o := range b {
				group[o] = 1
			}
			c.filter = func(m Message) bool { return group[m.From] == group[m.To] }
		case 8:
			c.heal()
		case 9:
			c.restart(id, clusterOpts{seed: seed})
		}

		for _, o := range c.ids {
			on := c.node(o)
			if on.State() == Leader && on.hasCommittedInCurrentTerm() {
				if got := confEntriesAbove(c, o, on.CommitIndex()); got > 1 {
					t.Fatalf("seed %d step %d: leader %d has %d configuration entries above its commit index %d\n%s",
						seed, step, o, got, on.CommitIndex(), c.dump())
				}
			}
		}
	}
}

// tickMembers ticks every node that is a voter in its own configuration. A node
// outside its own configuration that times out campaigns and, on winning,
// dereferences its missing progress entry in becomeLeader (node.go:508): a
// separate defect found while writing this test, out of T3.16's scope.
func tickMembers(c *cluster) {
	c.t.Helper()
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.conf.hasVoter(id) {
			continue
		}
		if err := n.Tick(); err != nil {
			c.t.Fatalf("node %d tick: %v", id, err)
		}
	}
	c.deliverAll()
}
