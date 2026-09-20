package raft

import "testing"

// Tests for a leader noticing it has lost contact with the cluster.
//
// Raft does not require this and is safe without it: a leader cut off from a
// majority cannot commit anything and read-index will not let it answer a
// read. What it cannot do is notice. It goes on advertising itself as leader,
// so a readiness probe asking "is there a leader" keeps getting yes, and
// anything routing by that answer keeps sending work to the one node
// guaranteed not to finish it.

func TestALeaderThatLosesItsMajorityStepsDown(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 900, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)

	c.partition([]NodeID{leader}, otherNodes(c.ids, leader))
	c.tickN(defaultElectionTick * 3)

	if got := c.node(leader).State(); got == Leader {
		t.Errorf("node %d still leads after an election timeout with nobody reachable\n%s",
			leader, c.dump())
	}
}

func TestALeaderWithAMajorityKeepsLeading(t *testing.T) {
	// The check must not unseat a healthy leader, which would be a far worse
	// bug than the one it fixes.
	c := newCluster(t, 5, clusterOpts{seed: 901, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	term := c.node(leader).Term()

	// Two of five gone still leaves a majority.
	rest := otherNodes(c.ids, leader)
	c.partition([]NodeID{rest[0], rest[1]}, append([]NodeID{leader}, rest[2:]...))
	c.tickN(defaultElectionTick * 6)

	if got := c.node(leader).State(); got != Leader {
		t.Errorf("node %d stepped down while still reaching a majority, now %s\n%s",
			leader, got, c.dump())
	}
	if got := c.node(leader).Term(); got != term {
		t.Errorf("the term moved from %d to %d while a majority was reachable", term, got)
	}
}

func TestWithoutCheckQuorumALeaderHoldsOnForever(t *testing.T) {
	// The control. Without it the first test would pass against an
	// implementation that unseated leaders for some unrelated reason, and
	// this records what the default behaviour actually is.
	c := newCluster(t, 3, clusterOpts{seed: 900})
	leader := c.awaitLeader(defaultElectionTick * 3)

	c.partition([]NodeID{leader}, otherNodes(c.ids, leader))
	c.tickN(defaultElectionTick * 10)

	if got := c.node(leader).State(); got != Leader {
		t.Fatalf("node %d gave up leadership without the quorum check, so the check is "+
			"not what the other test is measuring", leader)
	}
}

func TestSteppingDownDoesNotRaiseTheTerm(t *testing.T) {
	// A leader standing down is conceding, not campaigning. Raising the term
	// on the way out would disrupt the cluster that replaced it the moment
	// the partition healed, which is what pre-vote exists to prevent.
	c := newCluster(t, 3, clusterOpts{seed: 902, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	term := c.node(leader).Term()

	c.partition([]NodeID{leader}, otherNodes(c.ids, leader))
	c.tickN(defaultElectionTick * 3)

	// The step-down has to have happened, or the term is trivially unchanged
	// and this test asserts nothing. An earlier version ticked for only two
	// election timeouts, which is the range the randomized timeout is drawn
	// from, so it sometimes measured a leader that had not yet noticed.
	if got := c.node(leader).State(); got == Leader {
		t.Fatalf("node %d had not stepped down yet, so the term check proves nothing", leader)
	}
	if got := c.node(leader).Term(); got != term {
		t.Errorf("stepping down moved the term from %d to %d", term, got)
	}
}

func TestAQuorumOfOneNeverStepsDown(t *testing.T) {
	// A single node cluster is its own majority and must not unseat itself.
	c := newCluster(t, 1, clusterOpts{seed: 903, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)

	c.tickN(defaultElectionTick * 8)

	if got := c.node(leader).State(); got != Leader {
		t.Errorf("the only node in the cluster stepped down, now %s", got)
	}
}
