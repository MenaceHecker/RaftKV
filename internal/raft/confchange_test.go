package raft

import (
	"errors"
	"testing"
)

// Tests for applying configuration changes from the log (§6).
//
// The behaviour that needs the most defending is that a change takes effect
// when its entry is *appended*, not when it commits. That sounds reckless —
// acting on something that might never be agreed — but the alternative is
// circular: whether the entry is committed is itself decided by a majority,
// and which majority depends on the configuration.
//
// Accepting the rule means accepting that a configuration can be undone, so
// the tests below spend as much effort on a change being correctly reverted as
// on it being applied.

// leaderWithConfChange elects node 1 and returns the cluster and that node.
func leaderWithConfChange(t *testing.T, size int, seed int64) (*cluster, *Node) {
	t.Helper()
	c := newCluster(t, size, clusterOpts{seed: seed})
	leader := c.awaitLeader(defaultElectionTick * 2)
	return c, c.node(leader)
}

func TestConfChangeTakesEffectOnAppendNotCommit(t *testing.T) {
	// §6: a server uses the latest configuration in its log whether or not it
	// has committed. A leader that kept using the old configuration while the
	// new one sat in its log would judge commitment by the wrong majority.
	c, n := leaderWithConfChange(t, 3, 800)

	commitBefore := n.CommitIndex()
	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	if !n.InJointConfiguration() {
		t.Fatal("the configuration did not change until the entry committed")
	}
	if got := n.CommitIndex(); got != commitBefore {
		t.Fatalf("commit index moved from %d to %d; the test no longer shows that the "+
			"change applied before commitment", commitBefore, got)
	}

	members := n.Members()
	if len(members) != 4 || members[3] != 4 {
		t.Fatalf("members = %v, want the new node included immediately", members)
	}
	_ = c
}

func TestFollowersAdoptTheChangeAsItReplicates(t *testing.T) {
	// Every node derives its membership from the same entries, so a change
	// reaches followers by ordinary replication rather than any side channel.
	c, n := leaderWithConfChange(t, 3, 801)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	c.deliverAll()

	for _, id := range c.ids {
		f := c.node(id)
		if !f.InJointConfiguration() {
			t.Fatalf("node %d did not enter the joint configuration\n%s", id, c.dump())
		}
		if len(f.Members()) != 4 {
			t.Fatalf("node %d has members %v, want four", id, f.Members())
		}
	}
}

func TestCompletingATransitionLeavesTheNewConfiguration(t *testing.T) {
	c, n := leaderWithConfChange(t, 3, 802)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("adding node 4: %v", err)
	}
	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leaving joint: %v", err)
	}
	c.deliverAll()

	for _, id := range c.ids {
		f := c.node(id)
		if f.InJointConfiguration() {
			t.Fatalf("node %d is still in a joint configuration\n%s", id, c.dump())
		}
		if len(f.Members()) != 4 {
			t.Fatalf("node %d has members %v, want four", id, f.Members())
		}
	}
}

func TestTruncationRevertsAConfigurationChange(t *testing.T) {
	// The property that justifies rebuilding rather than editing in place.
	//
	// A change that was appended but never committed can be overwritten by a
	// new leader. A node that had already adopted it has no undo record to
	// consult, so the configuration has to be derivable from whatever the log
	// says after the truncation.
	c := newCluster(t, 3, clusterOpts{seed: 803})
	n := c.node(1)

	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	c.deliverAll()

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	if !n.InJointConfiguration() {
		t.Fatal("the change was not adopted on append")
	}
	changeIndex := n.LastIndex()

	// A new leader in a later term overwrites that entry with one of its own.
	// The follower must reject the configuration along with the entry.
	newTerm := n.Term() + 1
	prevTerm, err := n.log.term(changeIndex - 1)
	if err != nil {
		t.Fatalf("reading the term before the change: %v", err)
	}

	err = n.Step(Message{
		Type:         MsgAppendRequest,
		From:         2,
		To:           1,
		Term:         newTerm,
		PrevLogIndex: changeIndex - 1,
		PrevLogTerm:  prevTerm,
		CommitIndex:  changeIndex - 1,
		Entries: []Entry{
			{Term: newTerm, Index: changeIndex, Type: EntryNormal, Data: []byte("replacement")},
		},
	})
	if err != nil {
		t.Fatalf("stepping the overwriting append: %v", err)
	}

	if n.InJointConfiguration() {
		t.Fatalf("the node is still in a joint configuration after the entry that "+
			"created it was overwritten\nmembers: %v", n.Members())
	}
	if got := n.Members(); len(got) != 3 {
		t.Fatalf("members = %v after the change was reverted, want the original three", got)
	}
}

func TestConfigurationIsDerivedFromTheLogOnRestart(t *testing.T) {
	// A node's membership lives in its log, not in the peer list it was
	// started with. One that crashed part-way through a change must come back
	// believing what its log says, or it would use a different majority from
	// everyone else.
	c := newCluster(t, 3, clusterOpts{seed: 804})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	c.deliverAll()

	// Restart a follower that had accepted the change.
	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	if !c.node(follower).InJointConfiguration() {
		t.Fatal("the follower never adopted the change, so the restart proves nothing")
	}

	c.restart(follower, clusterOpts{seed: 804})
	restarted := c.node(follower)

	if !restarted.InJointConfiguration() {
		t.Fatalf("the restarted node forgot the in-flight change; members = %v",
			restarted.Members())
	}
	if len(restarted.Members()) != 4 {
		t.Fatalf("restarted members = %v, want four", restarted.Members())
	}
}

func TestOnlyOneChangeMayBeInFlight(t *testing.T) {
	_, n := leaderWithConfChange(t, 3, 805)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 5})
	if !errors.Is(err, ErrConfChangeInFlight) {
		t.Fatalf("a second change gave %v, want ErrConfChangeInFlight", err)
	}
}

func TestLeaveJointRequiresATransitionToBeOpen(t *testing.T) {
	// Leaving is the one change that requires a transition in progress, where
	// every other requires none. Treating them alike would make it impossible
	// to finish what was started.
	_, n := leaderWithConfChange(t, 3, 806)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); !errors.Is(err, ErrNotInJoint) {
		t.Fatalf("leaving with no transition open gave %v, want ErrNotInJoint", err)
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != nil {
		t.Fatalf("adding a node: %v", err)
	}
	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leaving an open transition: %v", err)
	}
}

func TestOnlyTheLeaderMayChangeMembership(t *testing.T) {
	c, leaderNode := leaderWithConfChange(t, 3, 807)

	for _, id := range c.ids {
		if id == leaderNode.ID() {
			continue
		}
		f := c.node(id)
		before := f.LastIndex()

		err := f.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("a follower's change gave %v, want ErrNotLeader", err)
		}
		if f.LastIndex() != before {
			t.Fatalf("a rejected change still reached follower %d's log", id)
		}
	}
}

func TestInvalidChangesNeverReachTheLog(t *testing.T) {
	// A change that every node would have to independently decide to ignore
	// is worse than one that was never written: they might not all agree on
	// what to ignore.
	_, n := leaderWithConfChange(t, 3, 808)
	before := n.LastIndex()

	for name, cc := range map[string]ConfChange{
		"already a voter": {Type: ConfChangeAddNode, NodeID: 2},
		"not a voter":     {Type: ConfChangeRemoveNode, NodeID: 99},
		"zero node ID":    {Type: ConfChangeAddNode, NodeID: None},
		"unknown type":    {Type: ConfChangeType(99), NodeID: 4},
	} {
		if err := n.ProposeConfChange(cc); err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if got := n.LastIndex(); got != before {
			t.Fatalf("%s reached the log: last index moved from %d to %d", name, before, got)
		}
	}
}

func TestRemovingANodeCompletesATransition(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 809})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	var victim NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: victim}); err != nil {
		t.Fatalf("removing node %d: %v", victim, err)
	}
	if !n.InJointConfiguration() {
		t.Fatal("removal did not enter a joint configuration")
	}
	// During the transition the departing node still counts in the old set.
	if !n.conf.hasVoter(victim) {
		t.Fatal("the departing node stopped being a voter before the transition completed")
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leaving joint: %v", err)
	}
	c.deliverAll()

	if n.conf.hasVoter(victim) {
		t.Fatalf("node %d is still a voter after the removal completed; members = %v",
			victim, n.Members())
	}
	if got := len(n.Members()); got != 4 {
		t.Fatalf("%d members after removing one from five, want 4", got)
	}
}

func TestLeaderTracksProgressForNewMembers(t *testing.T) {
	// A new member needs a progress entry before the leader can send it
	// anything, and it has to exist the moment the configuration changes,
	// because the next commit decision is made against the new membership.
	_, n := leaderWithConfChange(t, 3, 810)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	if n.progress[4] == nil {
		t.Fatal("the leader has no replication state for the node it just added")
	}
	if got := n.progress[4].next; got == 0 {
		t.Fatal("the new member's next index was never initialised")
	}
}

func TestLeaderDropsProgressForRemovedMembers(t *testing.T) {
	// A node no longer in any configuration must stop contributing to
	// majorities. Leaving its progress behind would let a departed member
	// carry a commit decision.
	c := newCluster(t, 5, clusterOpts{seed: 811})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	var victim NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: victim}); err != nil {
		t.Fatalf("removing: %v", err)
	}
	// Still tracked during the transition: it remains a voter in C_old.
	if n.progress[victim] == nil {
		t.Fatal("the departing node lost its progress entry during the transition, " +
			"while it still counts toward the old majority")
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leaving joint: %v", err)
	}

	if n.progress[victim] != nil {
		t.Fatalf("node %d still has replication state after leaving the cluster", victim)
	}
}

func TestRebuildingIsIdempotent(t *testing.T) {
	// Rebuilding runs on every append that touches membership, so doing it
	// twice must land in the same place. A rebuild that accumulated state
	// would drift with the number of appends rather than the log's contents.
	_, n := leaderWithConfChange(t, 3, 812)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	before := n.Members()
	joint := n.InJointConfiguration()

	for range 5 {
		if err := n.rebuildConfig(); err != nil {
			t.Fatalf("rebuildConfig: %v", err)
		}
	}

	after := n.Members()
	if len(before) != len(after) || n.InJointConfiguration() != joint {
		t.Fatalf("repeated rebuilds changed the configuration: %v (joint %v) became %v (joint %v)",
			before, joint, after, n.InJointConfiguration())
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("repeated rebuilds changed membership: %v became %v", before, after)
		}
	}
}

func TestAddressTravelsWithTheChange(t *testing.T) {
	// The transport needs the new member's address to reach it, and the only
	// place it can come from is the change itself, since no node can be told
	// about a member it has never heard of.
	c, n := leaderWithConfChange(t, 3, 813)

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "10.0.0.4:9000"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	c.deliverAll()

	for _, id := range c.ids {
		f := c.node(id)
		if got := f.conf.addrs[4]; got != "10.0.0.4:9000" {
			t.Fatalf("node %d has address %q for the new member, want 10.0.0.4:9000", id, got)
		}
	}
}
