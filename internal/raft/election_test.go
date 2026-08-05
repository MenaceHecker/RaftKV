package raft

import "testing"

// Tests for leader election (§5.2, §5.4.1).
//
// These cover the first two of the paper's five safety properties:
//
//   - Election Safety: at most one leader per term. The harness asserts this on
//     every call to leader(), so every test in the package checks it
//     continuously; the tests here drive the cases where it is most at risk.
//   - Leader Completeness (in part): a candidate whose log is behind cannot win,
//     which is what stops a leader from ever missing a committed entry. The
//     replication tests finish this off by showing committed data survives a
//     leader change.

func TestSingleNodeElectsItself(t *testing.T) {
	// A one-node cluster is its own majority, so campaigning is decided by the
	// self-vote alone with no messages exchanged.
	c := newCluster(t, 1, clusterOpts{seed: 1})
	c.campaign(1)

	if got := c.node(1).State(); got != Leader {
		t.Fatalf("single node state = %s, want Leader\n%s", got, c.dump())
	}
	if got := c.node(1).Term(); got != 1 {
		t.Fatalf("term = %d, want 1", got)
	}
}

func TestElectionTimeoutProducesLeader(t *testing.T) {
	// No node is told to campaign. A leader must emerge purely from election
	// timeouts firing, which is the real startup path.
	c := newCluster(t, 5, clusterOpts{seed: 2})

	for _, id := range c.ids {
		if got := c.node(id).State(); got != Follower {
			t.Fatalf("node %d starts as %s, want Follower", id, got)
		}
	}

	// Two full election timeouts is ample: the randomized timeout is drawn
	// from [ElectionTick, 2*ElectionTick), so every node has campaigned at
	// least once by then.
	leader := c.awaitLeader(defaultElectionTick * 2)

	if got := c.node(leader).Term(); got < 1 {
		t.Fatalf("leader %d has term %d, want at least 1", leader, got)
	}

	// Everyone else must be a follower that recognizes this leader.
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
	// Three of five nodes is a majority, so an election succeeds with two
	// nodes unreachable. This is the availability guarantee: a cluster of
	// 2f+1 tolerates f failures.
	c := newCluster(t, 5, clusterOpts{seed: 3})
	c.partition([]NodeID{1, 2, 3}, []NodeID{4}, []NodeID{5})

	c.campaign(1)

	if got := c.node(1).State(); got != Leader {
		t.Fatalf("node 1 state = %s, want Leader (3 of 5 reachable)\n%s", got, c.dump())
	}
}

func TestCandidateLosesWithoutMajority(t *testing.T) {
	// Two of five is not a majority, so the candidate must not win — even
	// though nothing rejects it and it simply never hears back from three
	// nodes. Silence must never be read as consent.
	c := newCluster(t, 5, clusterOpts{seed: 4})
	c.partition([]NodeID{1, 2}, []NodeID{3, 4, 5})

	c.campaign(1)

	if got := c.node(1).State(); got != Candidate {
		t.Fatalf("node 1 state = %s, want Candidate (only 2 of 5 reachable)\n%s", got, c.dump())
	}
}

func TestOneVotePerTerm(t *testing.T) {
	// Two candidates campaign in the same term. A voter that has already voted
	// must refuse the second, which is what makes it arithmetically impossible
	// for both to reach a majority (Election Safety).
	c := newCluster(t, 3, clusterOpts{seed: 5})

	// Isolate everyone so the campaigns do not interfere, then hand-deliver
	// the vote requests to node 3 and watch how it answers each.
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
	// A repeated request from the candidate already voted for must be granted
	// again. The retry exists because the first response may have been lost,
	// and refusing it would turn a dropped message into a lost election.
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
	// The election restriction (§5.4.1). A candidate whose log is behind must
	// not win, however high its term — this is what guarantees a new leader
	// already holds every committed entry.
	c := newCluster(t, 3, clusterOpts{seed: 7})

	// Give node 1 a log by electing it and committing a command.
	c.campaign(1)
	if err := c.propose(1, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	voter := c.node(1)
	behind := voter.LastIndex() - 1

	// A candidate from a much later term, but with a shorter log at an older
	// term. The high term forces the voter to step down; the stale log must
	// still cost it the vote.
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

	// The higher term must still have been adopted: the vote is refused, but
	// the term information in the message is valid and cannot be ignored.
	if voter.Term() != stale.Term {
		t.Fatalf("term = %d, want %d; a higher term must be adopted even when the vote is refused",
			voter.Term(), stale.Term)
	}
	if voter.State() != Follower {
		t.Fatalf("state = %s, want Follower after seeing a higher term", voter.State())
	}
}

func TestHigherTermDeposesLeader(t *testing.T) {
	// A leader that sees a higher term steps down immediately. Without this a
	// partitioned-then-rejoined leader would keep issuing appends in a dead
	// term and could conflict with the real leader.
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
	// An even-sized cluster where two candidates campaign at once can tie with
	// no winner. Liveness depends on the randomized timeout: the next round
	// must not reproduce the same split.
	c := newCluster(t, 4, clusterOpts{seed: 9})

	c.partition([]NodeID{1, 3}, []NodeID{2, 4})
	c.campaign(1)
	c.campaign(2)

	// Two candidates, two votes each in a four-node cluster: neither reaches
	// the three-node majority.
	if _, ok := c.leader(); ok {
		t.Fatalf("a leader emerged from a split vote\n%s", c.dump())
	}

	// Once the partition heals, a fresh round must resolve. Redrawing the
	// timeout on every state change is what breaks the symmetry.
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
	// Phase 1's persistence requirement. A node that crashes after voting must
	// not forget: on restart it could otherwise vote a second time in the same
	// term and help elect a second leader.
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

	// The recovered vote must still bind. A second candidate in the same term
	// has to be refused.
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
