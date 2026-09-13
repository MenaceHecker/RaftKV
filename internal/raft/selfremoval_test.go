package raft

import "testing"

// Tests for a leader removing itself from the configuration.
//
// This case was found by the chaos suite rather than by reasoning, and it
// crashed the node outright: finishing the transition dereferenced the
// leader's own replication progress, which adopting the new configuration had
// just deleted because the leader was no longer a member of it. The tests
// below pin both halves of the answer, the one that stops it crashing and the
// one that decides what it should do instead.

// leaderOf returns a cluster with a settled leader, for tests that need one.
//
// Pre-vote is on, and that matters more than it looks. A removed node keeps
// running and still believes it is a voter, so without pre-vote it campaigns,
// raises the term and deposes the leader within a few election timeouts.
// Every assertion below about a leader standing down, or not standing down,
// would then be observing that churn rather than the behaviour under test, and
// the step-down test in particular would pass with the code removed.
func leaderOf(t *testing.T, size int, seed int64) (*cluster, NodeID) {
	t.Helper()
	c := newCluster(t, size, clusterOpts{seed: seed, preVote: true})
	return c, c.awaitLeader(defaultElectionTick * 3)
}

func TestLeaderRemovingItselfDoesNotPanic(t *testing.T) {
	c, leader := leaderOf(t, 3, 77)

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeRemoveNode, NodeID: leader,
	}); err != nil {
		t.Fatalf("proposing self-removal: %v", err)
	}

	// Running the transition to completion is what crashed: the leave-joint
	// entry is proposed from inside the append path, by which point the
	// leader has already written itself out of the configuration.
	c.tickN(defaultElectionTick * 4)
}

func TestLeaderStandsDownAfterRemovingItself(t *testing.T) {
	// Not crashing is not enough. A node that is no longer a member cannot
	// count itself towards a majority, so it could never commit anything
	// again, and a cluster that has removed a node should not still be taking
	// instructions from it.
	c, leader := leaderOf(t, 3, 78)

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeRemoveNode, NodeID: leader,
	}); err != nil {
		t.Fatalf("proposing self-removal: %v", err)
	}
	c.tickN(defaultElectionTick * 4)

	old := c.node(leader)
	if old.State() == Leader {
		t.Errorf("node %d still leads a cluster it is no longer a member of", leader)
	}
	if old.conf.hasVoter(leader) {
		t.Errorf("node %d is still a voter in its own configuration", leader)
	}
}

func TestClusterElectsAReplacementAfterTheLeaderLeaves(t *testing.T) {
	// The cluster has to keep working, which is the point of allowing the
	// change at all.
	c, leader := leaderOf(t, 3, 79)

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeRemoveNode, NodeID: leader,
	}); err != nil {
		t.Fatalf("proposing self-removal: %v", err)
	}
	c.tickN(defaultElectionTick * 6)

	var replacement NodeID
	for _, id := range c.ids {
		if id != leader && c.node(id).State() == Leader {
			replacement = id
		}
	}
	if replacement == None {
		t.Fatalf("no replacement leader was elected after node %d left\n%s", leader, c.dump())
	}

	// And it can still commit.
	if err := c.node(replacement).Propose([]byte("after")); err != nil {
		t.Fatalf("proposing to the new leader: %v", err)
	}
	c.deliverAll()
	c.tickN(defaultElectionTick)

	idx := c.node(replacement).LastIndex()
	c.assertCommitted(replacement, idx)

	// The departed node must not be counted in that majority.
	if c.node(replacement).conf.hasVoter(leader) {
		t.Errorf("the new configuration still contains the removed node %d", leader)
	}
}

func TestRemovingAFollowerLeavesTheLeaderAlone(t *testing.T) {
	// The ordinary case still has to work: removing somebody else must not
	// make the leader stand down.
	c, leader := leaderOf(t, 3, 80)

	var victim NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}

	term := c.node(leader).Term()

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeRemoveNode, NodeID: victim,
	}); err != nil {
		t.Fatalf("proposing removal: %v", err)
	}
	c.tickN(defaultElectionTick * 4)

	if got := c.node(leader).State(); got != Leader {
		t.Errorf("the leader became %s after removing a follower", got)
	}
	// The term is what makes this test mean anything. A leader that stood
	// down needlessly would usually win the cluster straight back, so
	// checking only that it leads would pass either way; an unchanged term
	// says it never stopped leading in the first place.
	if got := c.node(leader).Term(); got != term {
		t.Errorf("the term moved from %d to %d while removing a follower, so the "+
			"leader stood down and was re-elected rather than carrying on", term, got)
	}
	if c.node(leader).conf.hasVoter(victim) {
		t.Errorf("node %d is still a voter after being removed", victim)
	}
}
