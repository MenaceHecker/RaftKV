package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// hardStateSpy counts SetHardState calls so a test can prove a node did not
// persist anything.
type hardStateSpy struct {
	*MemoryStorage
	sets int
}

func (s *hardStateSpy) SetHardState(hs HardState) error {
	s.sets++
	return s.MemoryStorage.SetHardState(hs)
}

// newNodeWithConf builds node id and then replaces its configuration with
// exactly conf. NewNode insists that Peers contain the node's own ID, so a
// node outside its own configuration can only be produced this way, which is
// how one ends up after applying a committed removal of itself.
func newNodeWithConf(t *testing.T, id NodeID, preVote bool, conf config) (*Node, *hardStateSpy) {
	t.Helper()
	spy := &hardStateSpy{MemoryStorage: NewMemoryStorage()}
	n, err := NewNode(Config{
		ID:            id,
		Peers:         []NodeID{id},
		ElectionTick:  defaultElectionTick,
		HeartbeatTick: defaultHeartbeatTick,
		PreVote:       preVote,
		Storage:       spy,
		Rand:          rand.New(rand.NewSource(int64(id) * 7919)), //nolint:gosec // seeded deterministic test RNG; Config.Rand requires *rand.Rand
	})
	if err != nil {
		t.Fatalf("creating node %d: %v", id, err)
	}
	n.conf = conf
	return n, spy
}

func assertInert(t *testing.T, n *Node, spy *hardStateSpy, wantTerm Term) {
	t.Helper()
	if n.State() != Follower {
		t.Errorf("state = %s, want Follower", n.State())
	}
	if n.Term() != wantTerm {
		t.Errorf("term = %d, want %d", n.Term(), wantTerm)
	}
	if spy.sets != 0 {
		t.Errorf("SetHardState called %d times, want 0", spy.sets)
	}
	if hs, _ := spy.InitialState(); hs != (HardState{}) {
		t.Errorf("persisted hard state = %+v, want empty", hs)
	}
	if rd := n.Ready(); len(rd.Messages) != 0 {
		t.Errorf("node sent %d messages, want none: %+v", len(rd.Messages), rd.Messages)
	}
}

func TestRemovedNodeNeverCampaigns(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", preVote), func(t *testing.T) {
			n, spy := newNodeWithConf(t, 3, preVote, newConfig([]NodeID{1, 2}))
			for i := 0; i < 5*2*defaultElectionTick; i++ {
				if err := n.Tick(); err != nil {
					t.Fatalf("tick %d: %v", i, err)
				}
				if rd := n.Ready(); len(rd.Messages) != 0 {
					t.Fatalf("tick %d: removed node sent %+v", i, rd.Messages)
				}
			}
			assertInert(t, n, spy, 0)
		})
	}
}

func TestRemovedNodeIgnoresMsgCampaign(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", preVote), func(t *testing.T) {
			n, spy := newNodeWithConf(t, 3, preVote, newConfig([]NodeID{1, 2}))
			if err := n.Step(Message{Type: MsgCampaign}); err != nil {
				t.Fatalf("Step(MsgCampaign): %v", err)
			}
			assertInert(t, n, spy, 0)
		})
	}
}

func TestNonMemberCannotBecomeLeaderFromVotes(t *testing.T) {
	n, _ := newNodeWithConf(t, 4, false, newConfig([]NodeID{1, 2, 3}))
	n.state = Candidate
	n.term = 1
	n.votes = map[NodeID]bool{n.id: true}

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("becoming leader outside the configuration panicked: %v", r)
			}
		}()
		for _, from := range []NodeID{1, 2, 3} {
			if err = n.Step(Message{Type: MsgVoteResponse, From: from, To: n.id, Term: 1, Granted: true}); err != nil {
				return
			}
		}
	}()

	if err == nil {
		t.Errorf("a non-member became leader or the error was swallowed; state = %s", n.State())
	}
	if n.State() == Leader {
		t.Errorf("a node outside its own configuration is Leader")
	}
}

func TestMemberStillCampaigns(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(fmt.Sprintf("sole-voter/prevote=%v", preVote), func(t *testing.T) {
			n, _ := newNodeWithConf(t, 1, preVote, newConfig([]NodeID{1}))
			for i := 0; i < 3*defaultElectionTick && n.State() != Leader; i++ {
				if err := n.Tick(); err != nil {
					t.Fatalf("tick: %v", err)
				}
			}
			if n.State() != Leader {
				t.Errorf("sole voter state = %s, want Leader", n.State())
			}
		})
		t.Run(fmt.Sprintf("three-node/prevote=%v", preVote), func(t *testing.T) {
			n, _ := newNodeWithConf(t, 1, preVote, newConfig([]NodeID{1, 2, 3}))
			for i := 0; i < 3*defaultElectionTick && n.State() == Follower; i++ {
				if err := n.Tick(); err != nil {
					t.Fatalf("tick: %v", err)
				}
			}
			if n.State() == Follower {
				t.Errorf("member of a three-node cluster never campaigned")
			}
			if rd := n.Ready(); len(rd.Messages) == 0 {
				t.Errorf("campaigning member sent no vote requests")
			}
		})
	}
}

func jointConf(voters, incoming []NodeID) config {
	c := newConfig(voters)
	c.incoming = voterSet(incoming...)
	c.joint = true
	return c
}

func TestJointIncomingOnlyNodeCampaigns(t *testing.T) {
	// Node 3 is being added: only in incoming. It is a voter of the effective
	// configuration (a majority of both halves is required), so it may campaign.
	n, _ := newNodeWithConf(t, 3, false, jointConf([]NodeID{1, 2}, []NodeID{1, 2, 3}))
	for i := 0; i < 3*defaultElectionTick && n.State() == Follower; i++ {
		if err := n.Tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	if n.State() != Candidate {
		t.Errorf("incoming-only node state = %s, want Candidate", n.State())
	}
}

func TestJointOutgoingOnlyNodeStillCampaignsWhileVoter(t *testing.T) {
	// Node 3 is being removed: only in voters. Until the joint configuration
	// is left it still counts toward the old majority, so it may campaign.
	n, _ := newNodeWithConf(t, 3, false, jointConf([]NodeID{1, 2, 3}, []NodeID{1, 2}))
	for i := 0; i < 3*defaultElectionTick && n.State() == Follower; i++ {
		if err := n.Tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	if n.State() != Candidate {
		t.Errorf("outgoing-only node state = %s, want Candidate", n.State())
	}
}

func TestRemovedNodeThenReAddedCampaignsAgain(t *testing.T) {
	n, spy := newNodeWithConf(t, 3, false, newConfig([]NodeID{1, 2}))
	for i := 0; i < 3*defaultElectionTick; i++ {
		if err := n.Tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	assertInert(t, n, spy, 0)

	n.adoptConfig(newConfig([]NodeID{1, 2, 3}))
	for i := 0; i < 3*defaultElectionTick && n.State() == Follower; i++ {
		if err := n.Tick(); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	if n.State() != Candidate {
		t.Fatalf("re-added node state = %s, want Candidate", n.State())
	}

	for _, from := range []NodeID{1, 2} {
		if err := n.Step(Message{Type: MsgVoteResponse, From: from, To: 3, Term: n.Term(), Granted: true}); err != nil {
			t.Fatalf("vote response from %d: %v", from, err)
		}
	}
	if n.State() != Leader {
		t.Errorf("re-added node state = %s after a majority of grants, want Leader", n.State())
	}
}
