package raft

import "testing"

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
	c := newCluster(t, 5, clusterOpts{seed: 901, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	term := c.node(leader).Term()

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
	c := newCluster(t, 3, clusterOpts{seed: 902, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	term := c.node(leader).Term()

	c.partition([]NodeID{leader}, otherNodes(c.ids, leader))
	c.tickN(defaultElectionTick * 3)

	if got := c.node(leader).State(); got == Leader {
		t.Fatalf("node %d had not stepped down yet, so the term check proves nothing", leader)
	}
	if got := c.node(leader).Term(); got != term {
		t.Errorf("stepping down moved the term from %d to %d", term, got)
	}
}

func TestAQuorumOfOneNeverStepsDown(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{seed: 903, checkQuorum: true})
	leader := c.awaitLeader(defaultElectionTick * 3)

	c.tickN(defaultElectionTick * 8)

	if got := c.node(leader).State(); got != Leader {
		t.Errorf("the only node in the cluster stepped down, now %s", got)
	}
}
