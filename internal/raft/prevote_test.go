package raft

import "testing"

func disruptionScenario(t *testing.T, preVote bool) (beforeTerm, afterTerm Term, before, after NodeID) {
	t.Helper()

	c := newCluster(t, 3, clusterOpts{seed: 42, preVote: preVote})
	leader := c.awaitLeader(defaultElectionTick * 3)

	var victim NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}

	beforeTerm = c.node(leader).Term()
	before = leader

	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))
	c.tickN(defaultElectionTick * 6)

	c.heal()
	c.tickN(defaultElectionTick * 3)

	after = c.mustLeader()
	afterTerm = c.node(after).Term()
	return beforeTerm, afterTerm, before, after
}

func otherNodes(ids []NodeID, except NodeID) []NodeID {
	var out []NodeID
	for _, id := range ids {
		if id != except {
			out = append(out, id)
		}
	}
	return out
}

func TestPreVoteKeepsARejoiningNodeFromDisruptingTheLeader(t *testing.T) {
	beforeTerm, afterTerm, before, after := disruptionScenario(t, true)

	if afterTerm != beforeTerm {
		t.Errorf("term moved from %d to %d when an isolated node rejoined; "+
			"pre-vote is meant to make that rejoin invisible", beforeTerm, afterTerm)
	}
	if after != before {
		t.Errorf("leadership moved from %d to %d when an isolated node rejoined", before, after)
	}
}

func TestWithoutPreVoteARejoiningNodeDisruptsTheLeader(t *testing.T) {
	beforeTerm, afterTerm, _, _ := disruptionScenario(t, false)

	if afterTerm <= beforeTerm {
		t.Fatalf("term stayed at %d without pre-vote; the scenario is not "+
			"reproducing the disruption pre-vote exists to prevent", beforeTerm)
	}
	t.Logf("without pre-vote the term went %d -> %d", beforeTerm, afterTerm)
}

func TestPreVoteStillAllowsARealElection(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 7, preVote: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	term := c.node(leader).Term()

	c.partition([]NodeID{leader}, otherNodes(c.ids, leader))
	c.tickN(defaultElectionTick * 6)

	var elected NodeID
	for _, id := range otherNodes(c.ids, leader) {
		if c.node(id).State() == Leader {
			elected = id
		}
	}
	if elected == None {
		t.Fatalf("the majority did not elect a leader after losing node %d\n%s", leader, c.dump())
	}
	if got := c.node(elected).Term(); got <= term {
		t.Errorf("new leader %d is at term %d, want higher than %d", elected, got, term)
	}
}

func TestPreVoteIsRefusedWhileTheLeaderIsStillReachable(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 11, preVote: true})
	leader := c.awaitLeader(defaultElectionTick * 3)

	var follower, asker NodeID
	rest := otherNodes(c.ids, leader)
	follower, asker = rest[0], rest[1]

	f := c.node(follower)
	if f.leader == None {
		t.Fatalf("follower %d does not recognize a leader; the setup is wrong", follower)
	}

	err := f.Step(Message{
		Type:         MsgPreVoteRequest,
		From:         asker,
		To:           follower,
		Term:         f.Term() + 1,
		LastLogIndex: f.log.lastIndex(),
		LastLogTerm:  f.log.lastTerm(),
	})
	if err != nil {
		t.Fatalf("stepping pre-vote: %v", err)
	}

	resp := lastMessageOfType(t, f, MsgPreVoteResponse)
	if resp.Granted {
		t.Error("a follower granted a pre-vote while its leader was still reachable")
	}
	if resp.Term != f.Term() {
		t.Errorf("rejection carried term %d, want the responder's real term %d", resp.Term, f.Term())
	}
}

func TestPreVoteRequestChangesNothingAboutTheResponder(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 13, preVote: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	follower := otherNodes(c.ids, leader)[0]

	f := c.node(follower)

	for range 3 {
		if err := f.Tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	if f.electionElapsed == 0 {
		t.Fatal("the follower's election timer did not advance; the test cannot detect a reset")
	}
	if f.State() != Follower {
		t.Fatalf("follower became %s while advancing its timer", f.State())
	}

	termBefore, voteBefore, elapsedBefore := f.Term(), f.vote, f.electionElapsed

	if err := f.Step(Message{
		Type:         MsgPreVoteRequest,
		From:         leader,
		To:           follower,
		Term:         termBefore + 50,
		LastLogIndex: f.log.lastIndex(),
		LastLogTerm:  f.log.lastTerm(),
	}); err != nil {
		t.Fatalf("stepping pre-vote: %v", err)
	}

	if f.Term() != termBefore {
		t.Errorf("term changed from %d to %d on a pre-vote request", termBefore, f.Term())
	}
	if f.vote != voteBefore {
		t.Errorf("vote changed from %d to %d on a pre-vote request", voteBefore, f.vote)
	}
	if f.electionElapsed != elapsedBefore {
		t.Errorf("election timer moved from %d to %d on a pre-vote request",
			elapsedBefore, f.electionElapsed)
	}
	if f.State() != Follower {
		t.Errorf("responder became %s on a pre-vote request", f.State())
	}
}

func TestPreCandidateDoesNotRaiseItsTerm(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 17, preVote: true})
	leader := c.awaitLeader(defaultElectionTick * 3)
	victim := otherNodes(c.ids, leader)[0]

	before := c.node(victim).Term()

	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))
	c.tickN(defaultElectionTick * 8)

	v := c.node(victim)
	if got := v.Term(); got != before {
		t.Errorf("an isolated pre-candidate went from term %d to %d after "+
			"many election timeouts; it should not raise its term at all", before, got)
	}
	if v.State() != PreCandidate {
		t.Errorf("isolated node is %s, want PreCandidate", v.State())
	}
}

func TestPreVoteRejectionTeachesAStaleNodeTheRealTerm(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 19, preVote: true})
	leader := c.awaitLeader(defaultElectionTick * 3)

	n := c.node(otherNodes(c.ids, leader)[0])
	if err := n.becomePreCandidate(); err != nil {
		t.Fatalf("becomePreCandidate: %v", err)
	}

	future := n.Term() + 10
	if err := n.Step(Message{
		Type:    MsgPreVoteResponse,
		From:    leader,
		To:      n.id,
		Term:    future,
		Granted: false,
	}); err != nil {
		t.Fatalf("stepping response: %v", err)
	}

	if n.Term() != future {
		t.Errorf("node is at term %d after a rejection naming term %d", n.Term(), future)
	}
	if n.State() != Follower {
		t.Errorf("node is %s after learning of a newer term, want Follower", n.State())
	}
}

func TestPreCandidateStandsDownWhenItHearsFromALeader(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  MessageType
	}{
		{"heartbeat", MsgHeartbeat},
		{"append", MsgAppendRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, clusterOpts{seed: 23, preVote: true})
			leader := c.awaitLeader(defaultElectionTick * 3)
			victim := otherNodes(c.ids, leader)[0]

			v := c.node(victim)
			if err := v.becomePreCandidate(); err != nil {
				t.Fatalf("becomePreCandidate: %v", err)
			}
			if v.State() != PreCandidate {
				t.Fatalf("node is %s, want PreCandidate", v.State())
			}

			m := Message{
				Type: tc.msg,
				From: leader,
				To:   victim,
				Term: v.Term(),
			}
			if tc.msg == MsgAppendRequest {
				m.PrevLogIndex = v.log.lastIndex()
				m.PrevLogTerm = v.log.lastTerm()
			}
			if err := v.Step(m); err != nil {
				t.Fatalf("stepping %s: %v", tc.msg, err)
			}

			if got := v.State(); got != Follower {
				t.Errorf("node is still %s after a %s from leader %d, want Follower",
					got, tc.msg, leader)
			}
			if v.leader != leader {
				t.Errorf("node recognizes leader %d, want %d", v.leader, leader)
			}
		})
	}
}

func lastMessageOfType(t *testing.T, n *Node, typ MessageType) Message {
	t.Helper()
	for i := len(n.msgs) - 1; i >= 0; i-- {
		if n.msgs[i].Type == typ {
			return n.msgs[i]
		}
	}
	t.Fatalf("node %d sent no %s", n.id, typ)
	return Message{}
}
