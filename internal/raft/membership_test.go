package raft

import (
	"errors"
	"fmt"
	"sort"
	"testing"
)

func voterSet(ids ...NodeID) map[NodeID]struct{} {
	out := make(map[NodeID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func sortedIDs(s map[NodeID]struct{}) []NodeID {
	out := make([]NodeID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func assertVoters(t *testing.T, got map[NodeID]struct{}, want ...NodeID) {
	t.Helper()
	expected := voterSet(want...)
	if len(got) != len(expected) {
		t.Fatalf("voters = %v, want %v", sortedIDs(got), want)
	}
	for id := range expected {
		if _, ok := got[id]; !ok {
			t.Fatalf("voters = %v, want %v", sortedIDs(got), want)
		}
	}
}

func matchAll(idx Index) func(NodeID) Index {
	return func(NodeID) Index { return idx }
}

func matchOnly(idx Index, ids ...NodeID) func(NodeID) Index {
	have := voterSet(ids...)
	return func(id NodeID) Index {
		if _, ok := have[id]; ok {
			return idx
		}
		return 0
	}
}

func grants(ids ...NodeID) map[NodeID]bool {
	out := make(map[NodeID]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

func TestNewConfigHasNoJointPhase(t *testing.T) {
	c := newConfig([]NodeID{1, 2, 3})

	if c.inJoint() {
		t.Fatal("a freshly configured cluster is in a joint transition")
	}
	assertVoters(t, c.voters, 1, 2, 3)
	for _, id := range []NodeID{1, 2, 3} {
		if !c.hasVoter(id) {
			t.Fatalf("node %d is not a voter", id)
		}
	}
	if c.hasVoter(99) {
		t.Fatal("an unknown node reports as a voter")
	}
}

func TestAddNodeEntersJoint(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "host:4"})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	if !joint.inJoint() {
		t.Fatal("adding a node did not enter a joint transition")
	}
	assertVoters(t, joint.voters, 1, 2, 3)
	assertVoters(t, joint.incoming, 1, 2, 3, 4)
	if got := joint.addrs[4]; got != "host:4" {
		t.Fatalf("address for node 4 = %q, want host:4", got)
	}

	if base.inJoint() {
		t.Fatal("enterJoint mutated the configuration it was called on")
	}
	assertVoters(t, base.voters, 1, 2, 3)
}

func TestRemoveNodeEntersJoint(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	joint, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 3})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	assertVoters(t, joint.voters, 1, 2, 3)
	assertVoters(t, joint.incoming, 1, 2)
	if !joint.hasVoter(3) {
		t.Fatal("a node being removed stopped counting as a voter during the transition; " +
			"it still participates in the old majority until the change completes")
	}
}

func TestLeaveJointAdoptsTheNewConfiguration(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	final, err := joint.leaveJoint()
	if err != nil {
		t.Fatalf("leaveJoint: %v", err)
	}

	if final.inJoint() {
		t.Fatal("the transition did not end")
	}
	assertVoters(t, final.voters, 1, 2, 3, 4)
	if len(final.incoming) != 0 {
		t.Fatalf("incoming = %v after leaving joint, want empty", sortedIDs(final.incoming))
	}
}

func TestSecondChangeWhileOneIsInFlightIsRejected(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("first enterJoint: %v", err)
	}

	_, err = joint.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 5})
	if !errors.Is(err, ErrConfChangeInFlight) {
		t.Fatalf("a second change during a transition gave %v, want ErrConfChangeInFlight", err)
	}

	assertVoters(t, joint.incoming, 1, 2, 3, 4)
}

func TestLeaveJointRequiresAnOpenTransition(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.leaveJoint(); !errors.Is(err, ErrNotInJoint) {
		t.Fatalf("leaving a transition that was never entered gave %v, want ErrNotInJoint", err)
	}
}

func TestLeaveJointIsNotAMembershipChange(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeLeaveJoint}); err == nil {
		t.Fatal("a leave-joint was accepted as a membership change")
	}
}

func TestChangesWithNoEffectAreRejected(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 2}); !errors.Is(err, ErrNoChange) {
		t.Fatalf("adding an existing voter gave %v, want ErrNoChange", err)
	}
	if _, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 99}); !errors.Is(err, ErrNoChange) {
		t.Fatalf("removing a non-voter gave %v, want ErrNoChange", err)
	}
}

func TestRemovingTheLastVoterIsRejected(t *testing.T) {
	solo := newConfig([]NodeID{1})

	if _, err := solo.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); !errors.Is(err, ErrEmptyConfiguration) {
		t.Fatalf("removing the last voter gave %v, want ErrEmptyConfiguration", err)
	}
}

func TestZeroNodeIDCannotBeAdded(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: None}); err == nil {
		t.Fatal("the zero node ID was accepted as a cluster member")
	}
}

func TestUnknownChangeTypeIsRejected(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeType(99), NodeID: 4}); err == nil {
		t.Fatal("an unknown configuration change type was accepted")
	}
}

func TestMembersCoversBothConfigurations(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	got := joint.members()
	want := []NodeID{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("members = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("members = %v, want %v (sorted)", got, want)
		}
	}
}

func TestCommitOutsideJointNeedsOneMajority(t *testing.T) {
	c := newConfig([]NodeID{1, 2, 3})

	if c.commitReady(5, matchOnly(5, 1, 2)) != true {
		t.Fatal("two of three did not commit")
	}
	if c.commitReady(5, matchOnly(5, 1)) != false {
		t.Fatal("one of three committed without a majority")
	}
}

func TestCommitDuringJointNeedsBothMajorities(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "a"})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	if joint.commitReady(5, matchOnly(5, 2, 3)) {
		t.Fatal("committed on a majority of the old configuration alone; the new " +
			"configuration's majority could have agreed something else")
	}
	if !joint.commitReady(5, matchOnly(5, 1, 2, 3)) {
		t.Fatal("a majority of both configurations failed to commit")
	}
	if joint.commitReady(5, matchOnly(5, 1)) {
		t.Fatal("committed on a single node")
	}
}

func TestCommitDuringShrinkNeedsBothMajorities(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3, 4, 5})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 5})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	if joint.commitReady(7, matchOnly(7, 1, 2)) {
		t.Fatal("two of five committed")
	}
	if !joint.commitReady(7, matchOnly(7, 1, 2, 3)) {
		t.Fatal("a majority of both configurations failed to commit")
	}
	if joint.commitReady(7, matchOnly(7, 4, 5)) {
		t.Fatal("a minority of both configurations committed")
	}
}

func TestVoteOutsideJointNeedsOneMajority(t *testing.T) {
	c := newConfig([]NodeID{1, 2, 3})

	if !c.voteGranted(grants(1, 2)) {
		t.Fatal("two of three did not win the election")
	}
	if c.voteGranted(grants(1)) {
		t.Fatal("one of three won the election")
	}
}

func TestVoteDuringJointNeedsBothMajorities(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	if joint.voteGranted(grants(2, 3)) {
		t.Fatal("a candidate won on the old configuration's majority alone")
	}
	if !joint.voteGranted(grants(1, 2, 3)) {
		t.Fatal("a candidate with a majority of both configurations lost")
	}
}

func TestVoteIsLostWhenEitherMajorityBecomesUnreachable(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	refused := map[NodeID]bool{2: false, 3: false}
	if !joint.voteLost(refused) {
		t.Fatal("an election with an unreachable majority in one configuration " +
			"was not reported as lost")
	}

	if joint.voteLost(map[NodeID]bool{2: false}) {
		t.Fatal("an election was abandoned while both majorities were still reachable")
	}
}

func TestNoQuorumIsReachableInAnEmptyConfiguration(t *testing.T) {
	empty := config{voters: map[NodeID]struct{}{}}

	if empty.commitReady(1, matchAll(100)) {
		t.Fatal("an empty configuration committed an entry")
	}
	if empty.voteGranted(grants(1, 2, 3)) {
		t.Fatal("an empty configuration elected a leader")
	}
}

func TestTransitionSequenceKeepsQuorumsIntersecting(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}
	joint2, err := joint.leaveJoint()
	if err != nil {
		t.Fatalf("leaveJoint: %v", err)
	}
	joint3, err := joint2.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 5})
	if err != nil {
		t.Fatalf("second enterJoint: %v", err)
	}

	oldMajority := []NodeID{1, 2, 3}
	newMajority := []NodeID{3, 4, 5}

	if !joint3.commitReady(9, matchOnly(9, oldMajority...)) {
		t.Fatalf("a majority of both configurations could not commit")
	}
	if joint3.commitReady(9, matchOnly(9, 4, 5)) {
		t.Fatal("a set that is a majority of neither configuration committed")
	}

	if !overlaps(oldMajority, newMajority) {
		t.Fatal("test premise is wrong: the two sets should overlap at node 3")
	}
}

func overlaps(a, b []NodeID) bool {
	set := voterSet(a...)
	for _, id := range b {
		if _, ok := set[id]; ok {
			return true
		}
	}
	return false
}

func TestConfChangeRoundTrip(t *testing.T) {
	cases := []ConfChange{
		{Type: ConfChangeAddNode, NodeID: 4, Addr: "127.0.0.1:9000"},
		{Type: ConfChangeRemoveNode, NodeID: 2},
		{Type: ConfChangeLeaveJoint},
		{Type: ConfChangeAddNode, NodeID: ^NodeID(0), Addr: ""},
	}

	for _, want := range cases {
		got, err := DecodeConfChange(want.Encode())
		if err != nil {
			t.Fatalf("decoding %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
	}
}

func TestTruncatedConfChangeIsRejected(t *testing.T) {
	full := ConfChange{Type: ConfChangeAddNode, NodeID: 7, Addr: "host:1234"}.Encode()

	for cut := range len(full) {
		if _, err := DecodeConfChange(full[:cut]); err == nil {
			t.Fatalf("a payload truncated to %d of %d bytes was accepted", cut, len(full))
		}
	}
}

func TestConfChangeWithOversizedAddrLengthIsRejected(t *testing.T) {
	cc := ConfChange{Type: ConfChangeAddNode, NodeID: 1, Addr: "x"}
	b := cc.Encode()
	b[9], b[10], b[11], b[12] = 0xff, 0xff, 0xff, 0xff

	if _, err := DecodeConfChange(b); err == nil {
		t.Fatal("a conf change claiming a 4GB address was accepted")
	}
}

func TestAddressesSurviveTransitions(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "host:4"})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}
	final, err := joint.leaveJoint()
	if err != nil {
		t.Fatalf("leaveJoint: %v", err)
	}

	if got := final.addrs[4]; got != "host:4" {
		t.Fatalf("address for node 4 = %q after the transition, want host:4", got)
	}
}

func TestConfigCopiesAreIndependent(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "host:4"})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	joint.voters[99] = struct{}{}
	joint.addrs[99] = "rogue"

	if base.hasVoter(99) {
		t.Fatal("mutating a derived configuration changed the original")
	}
	if _, ok := base.addrs[99]; ok {
		t.Fatal("mutating a derived configuration's addresses changed the original")
	}

	final, err := joint.leaveJoint()
	if err != nil {
		t.Fatalf("leaveJoint: %v", err)
	}
	final.voters[100] = struct{}{}
	if _, ok := joint.incoming[100]; ok {
		t.Fatal("the final configuration shares state with the joint one")
	}
}

func TestJointPhaseIsExplicitNotInferred(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3, 4, 5})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 5})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	if !joint.inJoint() {
		t.Fatal("a shrinking transition did not report itself as joint")
	}
	if len(joint.incoming) >= len(joint.voters) {
		t.Fatalf("test premise is wrong: incoming (%d) should be smaller than voters (%d)",
			len(joint.incoming), len(joint.voters))
	}

	final, err := joint.leaveJoint()
	if err != nil {
		t.Fatalf("leaveJoint: %v", err)
	}
	if final.inJoint() {
		t.Fatal("the configuration still reports as joint after leaving")
	}
}

func TestSequentialChangesGrowAndShrinkACluster(t *testing.T) {
	c := newConfig([]NodeID{1, 2, 3})

	apply := func(cc ConfChange) {
		t.Helper()
		joint, err := c.enterJoint(cc)
		if err != nil {
			t.Fatalf("enterJoint(%+v): %v", cc, err)
		}
		if !joint.inJoint() {
			t.Fatalf("enterJoint(%+v) produced a non-joint configuration", cc)
		}
		final, err := joint.leaveJoint()
		if err != nil {
			t.Fatalf("leaveJoint after %+v: %v", cc, err)
		}
		c = final
	}

	apply(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "host:4"})
	apply(ConfChange{Type: ConfChangeAddNode, NodeID: 5, Addr: "host:5"})
	assertVoters(t, c.voters, 1, 2, 3, 4, 5)

	apply(ConfChange{Type: ConfChangeRemoveNode, NodeID: 2})
	assertVoters(t, c.voters, 1, 3, 4, 5)

	if !c.commitReady(1, matchOnly(1, 1, 3, 4)) {
		t.Fatal("three of four could not commit after the transitions")
	}
	if c.commitReady(1, matchOnly(1, 1, 3)) {
		t.Fatalf("two of four committed\n%v", sortedIDs(c.voters))
	}

	if got := fmt.Sprint(c.members()); got != "[1 3 4 5]" {
		t.Fatalf("members = %s, want [1 3 4 5]", got)
	}
}

func enterJointOn(t *testing.T, n *Node, add ...NodeID) {
	t.Helper()

	c := n.conf
	for _, id := range add[:len(add)-1] {
		joint, err := c.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: id})
		if err != nil {
			t.Fatalf("entering joint for node %d: %v", id, err)
		}
		if c, err = joint.leaveJoint(); err != nil {
			t.Fatalf("leaving joint for node %d: %v", id, err)
		}
	}

	last := add[len(add)-1]
	joint, err := c.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: last})
	if err != nil {
		t.Fatalf("entering joint for node %d: %v", last, err)
	}
	n.conf = joint

	for _, id := range n.conf.members() {
		if n.progress[id] == nil {
			n.progress[id] = &progress{next: n.log.lastIndex() + 1}
		}
	}
}

func TestLeaderCommitsByConfigurationNotPeerCount(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 700})
	n := c.node(1)

	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	c.deliverAll()
	if n.State() != Leader {
		t.Fatalf("node 1 is %s, want Leader", n.State())
	}

	enterJointOn(t, n, 4, 5)
	if !n.InJointConfiguration() {
		t.Fatal("the node is not in a joint configuration")
	}

	if err := n.propose([]Entry{{Type: EntryNormal, Data: []byte("x")}}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	idx := n.log.lastIndex()
	before := n.CommitIndex()

	n.progress[1].match = idx
	n.progress[2].match = idx
	if n.maybeCommit() {
		t.Fatalf("commit advanced from %d to %d on a majority of neither configuration",
			before, n.CommitIndex())
	}

	n.progress[3].match = idx
	if !n.maybeCommit() {
		t.Fatalf("a majority of both configurations failed to commit; commit is still %d, entry is at %d",
			n.CommitIndex(), idx)
	}
	if n.CommitIndex() != idx {
		t.Fatalf("commit index = %d, want %d", n.CommitIndex(), idx)
	}
}

func TestCandidateWinsByConfigurationNotVoteCount(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 701})
	n := c.node(1)

	enterJointOn(t, n, 4, 5)

	if err := n.becomeCandidate(); err != nil {
		t.Fatalf("becomeCandidate: %v", err)
	}

	n.votes = grants(1, 2)
	if n.conf.voteGranted(n.votes) {
		t.Fatal("two votes carried a four- and five-node joint configuration")
	}

	n.votes = grants(1, 2, 3)
	if !n.conf.voteGranted(n.votes) {
		t.Fatal("three votes did not carry a majority of both configurations")
	}
}

func TestBroadcastReachesNodesOnlyInTheIncomingConfiguration(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 702})
	n := c.node(1)

	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	c.deliverAll()
	n.Ready()

	enterJointOn(t, n, 4)

	n.broadcastAppend()

	rd := n.Ready()
	sawNewMember := false
	for _, m := range rd.Messages {
		if m.To == 4 {
			sawNewMember = true
		}
	}
	if !sawNewMember {
		t.Fatalf("the leader did not replicate to a node present only in the incoming "+
			"configuration; it could never catch up\nmessages: %d", len(rd.Messages))
	}
}

func TestSoleVoterCommitsWithoutAcknowledgement(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{seed: 703})
	n := c.node(1)

	if !n.isSoleVoter() {
		t.Fatal("a one-node cluster does not report itself as the sole voter")
	}

	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if n.State() != Leader {
		t.Fatalf("state = %s, want Leader", n.State())
	}
	if n.CommitIndex() != n.LastIndex() {
		t.Fatalf("commit = %d, last = %d; a sole voter must commit its own entries",
			n.CommitIndex(), n.LastIndex())
	}
}

func TestSoleVoterIsNotJustAOneElementPeerList(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{seed: 704})
	n := c.node(1)

	joint, err := n.conf.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 2})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}
	n.conf = joint

	if n.isSoleVoter() {
		t.Fatal("a node mid-transition to a two-node cluster still reports as the " +
			"sole voter; it would commit without the incoming member")
	}
}
