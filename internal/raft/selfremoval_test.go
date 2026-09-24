package raft

import "testing"

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

	c.tickN(defaultElectionTick * 4)
}

func TestLeaderStandsDownAfterRemovingItself(t *testing.T) {
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

	if err := c.node(replacement).Propose([]byte("after")); err != nil {
		t.Fatalf("proposing to the new leader: %v", err)
	}
	c.deliverAll()
	c.tickN(defaultElectionTick)

	idx := c.node(replacement).LastIndex()
	c.assertCommitted(replacement, idx)

	if c.node(replacement).conf.hasVoter(leader) {
		t.Errorf("the new configuration still contains the removed node %d", leader)
	}
}

func TestRemovingAFollowerLeavesTheLeaderAlone(t *testing.T) {
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
	if got := c.node(leader).Term(); got != term {
		t.Errorf("the term moved from %d to %d while removing a follower, so the "+
			"leader stood down and was re-elected rather than carrying on", term, got)
	}
	if c.node(leader).conf.hasVoter(victim) {
		t.Errorf("node %d is still a voter after being removed", victim)
	}
}
