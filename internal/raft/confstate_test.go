package raft

import (
	"testing"
)

func TestConfStateRoundTrip(t *testing.T) {
	cases := map[string]config{
		"simple": newConfig([]NodeID{1, 2, 3}),
		"single": newConfig([]NodeID{7}),
	}

	joint, err := newConfig([]NodeID{1, 2, 3}).enterJoint(
		ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"})
	if err != nil {
		t.Fatalf("entering joint: %v", err)
	}
	cases["joint growing"] = joint

	shrinking, err := newConfig([]NodeID{1, 2, 3, 4, 5}).enterJoint(
		ConfChange{Type: ConfChangeRemoveNode, NodeID: 5})
	if err != nil {
		t.Fatalf("entering joint: %v", err)
	}
	cases["joint shrinking"] = shrinking

	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			got := configFromState(want.toState())

			if got.inJoint() != want.inJoint() {
				t.Fatalf("joint = %v, want %v", got.inJoint(), want.inJoint())
			}
			assertVoters(t, got.voters, sortedNodeIDs(want.voters)...)
			if want.inJoint() {
				assertVoters(t, got.incoming, sortedNodeIDs(want.incoming)...)
			}
			for id, addr := range want.addrs {
				if got.addrs[id] != addr {
					t.Fatalf("address for node %d = %q, want %q", id, got.addrs[id], addr)
				}
			}
		})
	}
}

func TestConfStateIsSortedForDeterminism(t *testing.T) {
	c := newConfig([]NodeID{5, 3, 1, 4, 2})

	for range 20 {
		cs := c.toState()
		for i := 1; i < len(cs.Voters); i++ {
			if cs.Voters[i-1] >= cs.Voters[i] {
				t.Fatalf("voters are not sorted: %v", cs.Voters)
			}
		}
	}
}

func TestJointFlagSurvivesAShrinkingTransition(t *testing.T) {
	joint, err := newConfig([]NodeID{1, 2, 3}).enterJoint(
		ConfChange{Type: ConfChangeRemoveNode, NodeID: 3})
	if err != nil {
		t.Fatalf("entering joint: %v", err)
	}

	cs := joint.toState()
	if !cs.Joint {
		t.Fatal("the joint flag was lost in conversion")
	}
	if len(cs.Incoming) >= len(cs.Voters) {
		t.Fatalf("test premise is wrong: incoming (%d) should be smaller than voters (%d)",
			len(cs.Incoming), len(cs.Voters))
	}

	back := configFromState(cs)
	if !back.inJoint() {
		t.Fatal("a shrinking transition read as finished after a round trip")
	}
}

func TestEmptyConfStateIsRecognisable(t *testing.T) {
	var cs ConfState
	if !cs.IsEmpty() {
		t.Fatal("the zero ConfState does not report itself as empty")
	}
	if newConfig([]NodeID{1}).toState().IsEmpty() {
		t.Fatal("a real configuration reports itself as empty")
	}
}

func TestRestoredConfigurationSupersedesThePeerList(t *testing.T) {
	restored := ConfState{Voters: []NodeID{1, 2, 3, 4}, Addrs: map[NodeID]string{4: "h:4"}}

	n, err := NewNode(Config{
		ID:               1,
		Peers:            []NodeID{1, 2, 3},
		InitialConfState: &restored,
		ElectionTick:     defaultElectionTick,
		HeartbeatTick:    defaultHeartbeatTick,
		Storage:          NewMemoryStorage(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	got := n.Members()
	if len(got) != 4 {
		t.Fatalf("members = %v, want the four from the snapshot rather than the "+
			"three from the peer list", got)
	}
	if addr := n.conf.addrs[4]; addr != "h:4" {
		t.Fatalf("address for the restored member = %q, want h:4", addr)
	}
}

func TestRestoredJointConfigurationStaysJoint(t *testing.T) {
	restored := ConfState{
		Voters:   []NodeID{1, 2, 3},
		Incoming: []NodeID{1, 2, 3, 4},
		Joint:    true,
	}

	n, err := NewNode(Config{
		ID:               1,
		Peers:            []NodeID{1, 2, 3},
		InitialConfState: &restored,
		ElectionTick:     defaultElectionTick,
		HeartbeatTick:    defaultHeartbeatTick,
		Storage:          NewMemoryStorage(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	if !n.InJointConfiguration() {
		t.Fatal("a node restored mid-transition came back believing it had finished")
	}
	if len(n.Members()) != 4 {
		t.Fatalf("members = %v, want all four", n.Members())
	}
}

func TestEmptyRestoredStateFallsBackToThePeerList(t *testing.T) {
	empty := ConfState{}

	n, err := NewNode(Config{
		ID:               1,
		Peers:            []NodeID{1, 2, 3},
		InitialConfState: &empty,
		ElectionTick:     defaultElectionTick,
		HeartbeatTick:    defaultHeartbeatTick,
		Storage:          NewMemoryStorage(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if len(n.Members()) != 3 {
		t.Fatalf("members = %v, want the three from the peer list", n.Members())
	}
}

func TestConfStateReflectsLiveMembershipChanges(t *testing.T) {
	c, n := leaderWithConfChange(t, 3, 820)

	before := n.ConfState()
	if before.Joint {
		t.Fatal("a settled cluster reports itself as mid-transition")
	}
	if len(before.Voters) != 3 {
		t.Fatalf("voters = %v, want three", before.Voters)
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "h:4"}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	during := n.ConfState()
	if !during.Joint {
		t.Fatal("an open transition is not reflected in the configuration state")
	}
	if len(during.Incoming) != 4 {
		t.Fatalf("incoming = %v, want four", during.Incoming)
	}
	if during.Addrs[4] != "h:4" {
		t.Fatalf("the new member's address was not carried: %v", during.Addrs)
	}

	if err := n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint}); err != nil {
		t.Fatalf("leaving joint: %v", err)
	}
	c.deliverAll()

	after := n.ConfState()
	if after.Joint {
		t.Fatal("a completed transition still reports as joint")
	}
	if len(after.Voters) != 4 {
		t.Fatalf("voters = %v, want four", after.Voters)
	}
}

func TestExternallySuppliedJointStateIsTrusted(t *testing.T) {
	damaged := ConfState{
		Voters:   []NodeID{1, 2, 3},
		Incoming: nil,
		Joint:    true,
	}

	c := configFromState(damaged)
	if !c.inJoint() {
		t.Fatal("a configuration that says it is mid-transition reported otherwise; " +
			"a node would decide on a single majority")
	}

	if c.commitReady(1, func(NodeID) Index { return 100 }) {
		t.Fatal("a transition with an unreachable incoming majority committed an entry")
	}
	if c.voteGranted(grants(1, 2, 3)) {
		t.Fatal("a transition with an unreachable incoming majority elected a leader")
	}
}
