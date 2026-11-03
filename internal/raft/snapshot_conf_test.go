package raft

import (
	"slices"
	"testing"
)

// settleAddNode4 proposes Add 4 on the leader and delivers messages until the
// automatic Leave has been appended and committed everywhere reachable. Node 4
// does not exist: the joint and settled quorums are met by nodes 1, 2 and 3.
func settleAddNode4(t *testing.T, c *cluster, leader NodeID) {
	t.Helper()

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeAddNode, NodeID: 4, Addr: "host:4",
	}); err != nil {
		t.Fatalf("Add 4: %v", err)
	}
	c.deliverAll()
	c.tickN(3)

	if cs := c.node(leader).ConfState(); cs.Joint || !slices.Equal(cs.Voters, []NodeID{1, 2, 3, 4}) {
		t.Fatalf("setup: leader configuration after Add 4 and its Leave = %s, want voters=[1 2 3 4]\n%s",
			cs, c.dump())
	}
	for _, id := range c.ids {
		if c.node(id).CommitIndex() != c.node(leader).LastIndex() {
			t.Fatalf("setup: node %d has not committed the Leave\n%s", id, c.dump())
		}
	}
}

// compactThroughCommitted snapshots a node's storage at its commit index with
// the configuration that is correct at that index (settled {1,2,3,4}).
func compactThroughCommitted(t *testing.T, c *cluster, id NodeID) Index {
	t.Helper()
	at := c.node(id).CommitIndex()
	conf := ConfState{Voters: []NodeID{1, 2, 3, 4}}
	if err := c.storages[id].CreateSnapshot(at, nil, conf); err != nil {
		t.Fatalf("compacting node %d at %d: %v", id, at, err)
	}
	return at
}

func assertSettledWithFour(t *testing.T, c *cluster, id NodeID, when string) {
	t.Helper()
	cs := c.node(id).ConfState()
	if cs.Joint || !slices.Equal(cs.Voters, []NodeID{1, 2, 3, 4}) {
		t.Errorf("node %d %s: configuration = %s, want voters=[1 2 3 4]", id, when, cs)
	}
}

func assertJointWithFourAndFive(t *testing.T, c *cluster, id NodeID, when string) {
	t.Helper()
	cs := c.node(id).ConfState()
	if !cs.Joint ||
		!slices.Equal(cs.Voters, []NodeID{1, 2, 3, 4}) ||
		!slices.Equal(cs.Incoming, []NodeID{1, 2, 3, 4, 5}) {
		t.Errorf("node %d %s: configuration = %s, want joint old=[1 2 3 4] new=[1 2 3 4 5]",
			id, when, cs)
	}
}

func TestConfigAfterLeaderCompactionKeepsEarlierMembers(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	leader := c.mustLeader()

	settleAddNode4(t, c, leader)
	compactThroughCommitted(t, c, leader)

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeAddNode, NodeID: 5, Addr: "host:5",
	}); err != nil {
		t.Fatalf("Add 5: %v", err)
	}

	assertJointWithFourAndFive(t, c, leader, "after compacting through Add 4 and its Leave, then Add 5")
}

func TestConfigAfterFollowerCompactionKeepsEarlierMembers(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	leader := c.mustLeader()

	settleAddNode4(t, c, leader)
	follower := NodeID(2)
	compactThroughCommitted(t, c, follower)

	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeAddNode, NodeID: 5, Addr: "host:5",
	}); err != nil {
		t.Fatalf("Add 5: %v", err)
	}
	c.deliverAll()
	c.tickN(3)

	// Delivery also carries Add 5's automatic Leave, so the follower must
	// end settled on {1,2,3,4,5}; losing node 4 is the defect.
	cs := c.node(follower).ConfState()
	if cs.Joint || !slices.Equal(cs.Voters, []NodeID{1, 2, 3, 4, 5}) {
		t.Errorf("node %d after compacting through Add 4 and its Leave, then receiving Add 5 and its Leave: configuration = %s, want voters=[1 2 3 4 5]",
			follower, cs)
	}
}

func TestConfigSurvivesTruncationAfterCompaction(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	leader := c.mustLeader()

	settleAddNode4(t, c, leader)
	follower := NodeID(3)
	last := compactThroughCommitted(t, c, follower)
	term := c.node(follower).Term()

	add5 := ConfChange{Type: ConfChangeAddNode, NodeID: 5, Addr: "host:5"}
	step := func(m Message) {
		t.Helper()
		m.Type, m.From, m.To = MsgAppendRequest, leader, follower
		m.PrevLogIndex, m.PrevLogTerm = last, term
		if err := c.node(follower).Step(m); err != nil {
			t.Fatalf("stepping append on node %d: %v", follower, err)
		}
		c.collect()
		c.inflight = nil
	}

	step(Message{Term: term, Entries: []Entry{
		{Term: term, Index: last + 1, Type: EntryConfChange, Data: add5.Encode()},
	}})
	assertJointWithFourAndFive(t, c, follower, "holding Add 5 above the snapshot")

	step(Message{Term: term + 1, Entries: []Entry{
		{Term: term + 1, Index: last + 1, Type: EntryNormal, Data: []byte("overwrite")},
	}})

	assertSettledWithFour(t, c, follower, "after Add 5 was truncated by a conflicting entry")
}

func TestConfigAfterCompactionAcrossRestart(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	leader := c.mustLeader()

	settleAddNode4(t, c, leader)
	compactThroughCommitted(t, c, 2)

	c.restart(2, clusterOpts{seed: 1})

	assertSettledWithFour(t, c, 2, "restarted over a snapshot that holds Add 4 and its Leave")
}
