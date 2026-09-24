package raft

import "testing"

func TestSingleNodeElectsItself(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{seed: 1})
	c.campaign(1)

	if got := c.node(1).State(); got != Leader {
		t.Fatalf("single node state = %s, want Leader\n%s", got, c.dump())
	}
	if got := c.node(1).Term(); got != 1 {
		t.Fatalf("term = %d, want 1", got)
	}

	if got := c.node(1).CommitIndex(); got != c.node(1).LastIndex() {
		t.Fatalf("commit index = %d, last index = %d; a single-node leader must commit "+
			"its own entries without waiting for anyone\n%s",
			got, c.node(1).LastIndex(), c.dump())
	}

	if err := c.propose(1, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if got := c.commands(1); len(got) != 1 || got[0] != "set x=1" {
		t.Fatalf("applied %v, want [set x=1]\n%s", got, c.dump())
	}
}

func TestElectionTimeoutProducesLeader(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 2})

	for _, id := range c.ids {
		if got := c.node(id).State(); got != Follower {
			t.Fatalf("node %d starts as %s, want Follower", id, got)
		}
	}

	leader := c.awaitLeader(defaultElectionTick * 2)

	if got := c.node(leader).Term(); got < 1 {
		t.Fatalf("leader %d has term %d, want at least 1", leader, got)
	}

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		n := c.node(id)
		if n.State() != Follower {
			t.Fatalf("node %d is %s, want Follower\n%s", id, n.State(), c.dump())
		}
		if n.Leader() != leader {
			t.Fatalf("node %d follows %d, want %d\n%s", id, n.Leader(), leader, c.dump())
		}
		if n.Term() != c.node(leader).Term() {
			t.Fatalf("node %d in term %d, leader in term %d\n%s",
				id, n.Term(), c.node(leader).Term(), c.dump())
		}
	}
}

func TestCandidateWinsWithMajority(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 3})
	c.partition([]NodeID{1, 2, 3}, []NodeID{4}, []NodeID{5})

	c.campaign(1)

	if got := c.node(1).State(); got != Leader {
		t.Fatalf("node 1 state = %s, want Leader (3 of 5 reachable)\n%s", got, c.dump())
	}
}

func TestCandidateLosesWithoutMajority(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 4})
	c.partition([]NodeID{1, 2}, []NodeID{3, 4, 5})

	c.campaign(1)

	if got := c.node(1).State(); got != Candidate {
		t.Fatalf("node 1 state = %s, want Candidate (only 2 of 5 reachable)\n%s", got, c.dump())
	}
}

func TestOneVotePerTerm(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 5})

	c.partition([]NodeID{1}, []NodeID{2}, []NodeID{3})
	c.campaign(1)
	c.campaign(2)

	n1, n2, n3 := c.node(1), c.node(2), c.node(3)
	if n1.Term() != n2.Term() {
		t.Fatalf("candidates in different terms (%d, %d); test needs them equal",
			n1.Term(), n2.Term())
	}
	term := n1.Term()

	req := Message{
		Type:         MsgVoteRequest,
		To:           3,
		Term:         term,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	first := req
	first.From = 1
	if err := n3.Step(first); err != nil {
		t.Fatalf("stepping first vote request: %v", err)
	}

	second := req
	second.From = 2
	if err := n3.Step(second); err != nil {
		t.Fatalf("stepping second vote request: %v", err)
	}

	rd := n3.Ready()
	if len(rd.Messages) != 2 {
		t.Fatalf("got %d vote responses, want 2", len(rd.Messages))
	}
	if !rd.Messages[0].Granted {
		t.Fatalf("first vote request was refused, want granted")
	}
	if rd.Messages[1].Granted {
		t.Fatalf("second vote request in term %d was granted; a node may vote only once per term\n%s",
			term, c.dump())
	}
}

func TestVoteIsIdempotentForSameCandidate(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 6})
	n := c.node(3)

	req := Message{Type: MsgVoteRequest, From: 1, To: 3, Term: 1}

	if err := n.Step(req); err != nil {
		t.Fatalf("stepping vote request: %v", err)
	}
	if err := n.Step(req); err != nil {
		t.Fatalf("stepping repeated vote request: %v", err)
	}

	rd := n.Ready()
	if len(rd.Messages) != 2 {
		t.Fatalf("got %d responses, want 2", len(rd.Messages))
	}
	for i, m := range rd.Messages {
		if !m.Granted {
			t.Fatalf("response %d was refused; a repeated request from the same candidate must be granted", i)
		}
	}
}

func TestStaleLogCandidateIsRejected(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 7})

	c.campaign(1)
	if err := c.propose(1, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	voter := c.node(1)
	behind := voter.LastIndex() - 1

	stale := Message{
		Type:         MsgVoteRequest,
		From:         3,
		To:           1,
		Term:         voter.Term() + 5,
		LastLogIndex: behind,
		LastLogTerm:  1,
	}
	if err := voter.Step(stale); err != nil {
		t.Fatalf("stepping stale vote request: %v", err)
	}

	rd := voter.Ready()
	var resp *Message
	for i := range rd.Messages {
		if rd.Messages[i].Type == MsgVoteResponse {
			resp = &rd.Messages[i]
		}
	}
	if resp == nil {
		t.Fatalf("no vote response produced\n%s", c.dump())
	}
	if resp.Granted {
		t.Fatalf("granted a vote to a candidate with a stale log (last index %d vs our %d)\n%s",
			behind, voter.LastIndex(), c.dump())
	}

	if voter.Term() != stale.Term {
		t.Fatalf("term = %d, want %d; a higher term must be adopted even when the vote is refused",
			voter.Term(), stale.Term)
	}
	if voter.State() != Follower {
		t.Fatalf("state = %s, want Follower after seeing a higher term", voter.State())
	}
}

func TestHigherTermDeposesLeader(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 8})
	c.campaign(1)

	leader := c.node(1)
	if leader.State() != Leader {
		t.Fatalf("node 1 is %s, want Leader", leader.State())
	}
	oldTerm := leader.Term()

	err := leader.Step(Message{
		Type: MsgAppendRequest,
		From: 2,
		To:   1,
		Term: oldTerm + 1,
	})
	if err != nil {
		t.Fatalf("stepping higher-term append: %v", err)
	}

	if got := leader.State(); got != Follower {
		t.Fatalf("state = %s, want Follower after seeing term %d", got, oldTerm+1)
	}
	if got := leader.Term(); got != oldTerm+1 {
		t.Fatalf("term = %d, want %d", got, oldTerm+1)
	}
	if got := leader.Leader(); got != 2 {
		t.Fatalf("recognized leader = %d, want 2", got)
	}
}

func TestLeaderIsElectedAfterSplitVote(t *testing.T) {
	c := newCluster(t, 4, clusterOpts{seed: 9})

	c.partition([]NodeID{1, 3}, []NodeID{2, 4})
	c.campaign(1)
	c.campaign(2)

	if _, ok := c.leader(); ok {
		t.Fatalf("a leader emerged from a split vote\n%s", c.dump())
	}

	c.heal()
	leader := c.awaitLeader(defaultElectionTick * 10)

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if got := c.node(id).State(); got != Follower {
			t.Fatalf("node %d is %s after the election resolved, want Follower\n%s",
				id, got, c.dump())
		}
	}
}

func TestTermAndVoteSurviveRestart(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 10})

	voter := c.node(3)
	err := voter.Step(Message{
		Type: MsgVoteRequest,
		From: 1,
		To:   3,
		Term: 7,
	})
	if err != nil {
		t.Fatalf("stepping vote request: %v", err)
	}
	voter.Ready()

	if voter.Term() != 7 {
		t.Fatalf("term before restart = %d, want 7", voter.Term())
	}

	c.restart(3, clusterOpts{seed: 10})
	restarted := c.node(3)

	if got := restarted.Term(); got != 7 {
		t.Fatalf("term after restart = %d, want 7", got)
	}
	if got := restarted.State(); got != Follower {
		t.Fatalf("state after restart = %s, want Follower; leadership is not durable", got)
	}

	err = restarted.Step(Message{
		Type: MsgVoteRequest,
		From: 2,
		To:   3,
		Term: 7,
	})
	if err != nil {
		t.Fatalf("stepping second vote request: %v", err)
	}

	rd := restarted.Ready()
	if len(rd.Messages) != 1 {
		t.Fatalf("got %d responses, want 1", len(rd.Messages))
	}
	if rd.Messages[0].Granted {
		t.Fatalf("granted a second vote in term 7 after restart; the persisted vote was lost\n%s", c.dump())
	}
}
