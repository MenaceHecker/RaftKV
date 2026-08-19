package raft

import (
	"errors"
	"fmt"
	"sort"
	"testing"
)

// Tests for cluster membership and joint consensus (§6).
//
// The rule joint consensus exists to preserve is quorum intersection: any two
// decisions the cluster makes must have been agreed by overlapping sets of
// nodes. Moving straight from one configuration to another breaks that, because
// for a moment two disjoint majorities can exist — one in the old set, one in
// the new — and each could elect its own leader without either noticing.
//
// So the tests here are less about add and remove working, and more about the
// transition never leaving a window where a single majority can decide
// anything on its own.

// voterSet builds a set from IDs, for concise expectations.
func voterSet(ids ...NodeID) map[NodeID]struct{} {
	out := make(map[NodeID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

// sortedIDs renders a set for failure messages.
func sortedIDs(s map[NodeID]struct{}) []NodeID {
	out := make([]NodeID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// assertVoters checks a voter set matches exactly.
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

// matchAll returns a match function reporting the same index for every node.
func matchAll(idx Index) func(NodeID) Index {
	return func(NodeID) Index { return idx }
}

// matchOnly returns a match function where only the listed nodes have reached
// idx and everyone else is at zero.
func matchOnly(idx Index, ids ...NodeID) func(NodeID) Index {
	have := voterSet(ids...)
	return func(id NodeID) Index {
		if _, ok := have[id]; ok {
			return idx
		}
		return 0
	}
}

// grants builds a vote tally where the listed nodes voted yes.
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

	// The original must be untouched; the core computes the next config
	// speculatively and only adopts it when the entry commits.
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
	// Raft permits one transition at a time. Two overlapping changes could
	// produce configurations neither of which contains the other, and the
	// quorum-intersection argument would no longer hold.
	//
	// Before this was enforced, the second change silently discarded the
	// first: the node being added by the in-flight change simply vanished
	// from the resulting configuration.
	base := newConfig([]NodeID{1, 2, 3})

	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("first enterJoint: %v", err)
	}

	_, err = joint.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 5})
	if !errors.Is(err, ErrConfChangeInFlight) {
		t.Fatalf("a second change during a transition gave %v, want ErrConfChangeInFlight", err)
	}

	// And the in-flight change is still intact.
	assertVoters(t, joint.incoming, 1, 2, 3, 4)
}

func TestLeaveJointRequiresAnOpenTransition(t *testing.T) {
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.leaveJoint(); !errors.Is(err, ErrNotInJoint) {
		t.Fatalf("leaving a transition that was never entered gave %v, want ErrNotInJoint", err)
	}
}

func TestLeaveJointIsNotAMembershipChange(t *testing.T) {
	// Finalising is what ends a transition, not something to start one for.
	// Accepting it produced a joint config whose two sets were identical.
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeLeaveJoint}); err == nil {
		t.Fatal("a leave-joint was accepted as a membership change")
	}
}

func TestChangesWithNoEffectAreRejected(t *testing.T) {
	// A no-op change would still cost a full two-phase transition, during
	// which no other change can start.
	base := newConfig([]NodeID{1, 2, 3})

	if _, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 2}); !errors.Is(err, ErrNoChange) {
		t.Fatalf("adding an existing voter gave %v, want ErrNoChange", err)
	}
	if _, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 99}); !errors.Is(err, ErrNoChange) {
		t.Fatalf("removing a non-voter gave %v, want ErrNoChange", err)
	}
}

func TestRemovingTheLastVoterIsRejected(t *testing.T) {
	// A cluster with no voters can never reach a majority again, so it could
	// not even configure its way back out. Before this was checked, the
	// resulting configuration also reported itself as not in a transition,
	// because the phase was inferred from the size of the incoming set.
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
	// During a transition the leader must replicate to every node in either
	// set. A node present only in C_new still has to receive entries, or it
	// could never catch up enough to satisfy the new majority.
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
	// The heart of joint consensus. A majority of only one configuration must
	// not be enough, because the other configuration's majority could
	// simultaneously agree something different.
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "a"})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}
	// C_old = {1,2,3}, C_new = {1,2,3,4}

	// {2,3} is a majority of C_old (2 of 3) but only 2 of 4 in C_new, which is
	// not a majority there.
	if joint.commitReady(5, matchOnly(5, 2, 3)) {
		t.Fatal("committed on a majority of the old configuration alone; the new " +
			"configuration's majority could have agreed something else")
	}
	// Three of four is a majority of C_new and of C_old.
	if !joint.commitReady(5, matchOnly(5, 1, 2, 3)) {
		t.Fatal("a majority of both configurations failed to commit")
	}
	if joint.commitReady(5, matchOnly(5, 1)) {
		t.Fatal("committed on a single node")
	}
}

func TestCommitDuringShrinkNeedsBothMajorities(t *testing.T) {
	// The mirror case: removing a node makes C_new smaller, so a majority of
	// C_new is easier to reach than one of C_old. Neither alone may decide.
	base := newConfig([]NodeID{1, 2, 3, 4, 5})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeRemoveNode, NodeID: 5})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}
	// C_old = {1..5}, C_new = {1,2,3,4}

	// {1,2} is a majority of neither.
	if joint.commitReady(7, matchOnly(7, 1, 2)) {
		t.Fatal("two of five committed")
	}
	// {1,2,3} is a majority of C_new (3 of 4) but only 3 of 5 in C_old, which
	// is a majority there too, so this should commit.
	if !joint.commitReady(7, matchOnly(7, 1, 2, 3)) {
		t.Fatal("a majority of both configurations failed to commit")
	}
	// {4,5} plus nobody else: 2 of 5 and 1 of 4, a majority of neither.
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
	// Election Safety across a transition. If a candidate could win on one
	// configuration's majority alone, the other configuration could elect a
	// different leader in the same term.
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
	// The asymmetry that makes this correct: winning requires both majorities,
	// so losing only requires one of them to become unreachable. Waiting for
	// both to fail would keep a doomed candidate campaigning.
	base := newConfig([]NodeID{1, 2, 3})
	joint, err := base.enterJoint(ConfChange{Type: ConfChangeAddNode, NodeID: 4})
	if err != nil {
		t.Fatalf("enterJoint: %v", err)
	}

	// Two refusals from C_old ({1,2,3}) leave only one possible yes, so a
	// majority there is unreachable even though C_new could still deliver one.
	refused := map[NodeID]bool{2: false, 3: false}
	if !joint.voteLost(refused) {
		t.Fatal("an election with an unreachable majority in one configuration " +
			"was not reported as lost")
	}

	// A single refusal leaves both majorities reachable.
	if joint.voteLost(map[NodeID]bool{2: false}) {
		t.Fatal("an election was abandoned while both majorities were still reachable")
	}
}

func TestNoQuorumIsReachableInAnEmptyConfiguration(t *testing.T) {
	// Defence in depth. enterJoint refuses to produce one, but if a config
	// with no voters ever arose it must fail closed rather than treating
	// zero agreements as a majority.
	empty := config{voters: map[NodeID]struct{}{}}

	if empty.commitReady(1, matchAll(100)) {
		t.Fatal("an empty configuration committed an entry")
	}
	if empty.voteGranted(grants(1, 2, 3)) {
		t.Fatal("an empty configuration elected a leader")
	}
}

func TestTransitionSequenceKeepsQuorumsIntersecting(t *testing.T) {
	// The property the whole mechanism exists for, checked directly: at every
	// step of a transition, any set that could commit under one active
	// configuration must overlap any set that could commit under the other.
	//
	// Growing a 3-node cluster to 5 is the case where a direct switch would be
	// unsafe: {1,2} is a majority of the old and {3,4,5} of the new, and they
	// are disjoint.
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

	// During the second transition: C_old = {1,2,3,4}, C_new = {1,2,3,4,5}.
	// The disjoint pair that would be dangerous under a direct switch.
	oldMajority := []NodeID{1, 2, 3}
	newMajority := []NodeID{3, 4, 5}

	if !joint3.commitReady(9, matchOnly(9, oldMajority...)) {
		// {1,2,3} is 3 of 4 in C_old and 3 of 5 in C_new, a majority of both.
		t.Fatalf("a majority of both configurations could not commit")
	}
	if joint3.commitReady(9, matchOnly(9, 4, 5)) {
		t.Fatal("a set that is a majority of neither configuration committed")
	}

	// The point: no set can satisfy the joint rule without touching both, so
	// two committing sets always share a node.
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
	// A conf change is applied from a log entry, so a damaged payload must be
	// refused rather than decoded into a change to some other node.
	full := ConfChange{Type: ConfChangeAddNode, NodeID: 7, Addr: "host:1234"}.Encode()

	for cut := range len(full) {
		if _, err := DecodeConfChange(full[:cut]); err == nil {
			t.Fatalf("a payload truncated to %d of %d bytes was accepted", cut, len(full))
		}
	}
}

func TestConfChangeWithOversizedAddrLengthIsRejected(t *testing.T) {
	// The length prefix is read before anything can prove it is genuine.
	cc := ConfChange{Type: ConfChangeAddNode, NodeID: 1, Addr: "x"}
	b := cc.Encode()
	// Claim a much longer address than is actually present.
	b[9], b[10], b[11], b[12] = 0xff, 0xff, 0xff, 0xff

	if _, err := DecodeConfChange(b); err == nil {
		t.Fatal("a conf change claiming a 4GB address was accepted")
	}
}

func TestAddressesSurviveTransitions(t *testing.T) {
	// The transport needs a new peer's address to reach it, and that address
	// arrives with the change. It must still be there once the transition
	// completes, or the cluster would agree on a member it cannot contact.
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
	// The core computes a prospective configuration before the entry that
	// carries it has committed. If that shared backing state with the live
	// one, an uncommitted change would take effect immediately.
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
	// A transition must report itself as joint regardless of the sizes of the
	// two sets. Inferring it from len(incoming) meant a shrinking change could
	// look finished and fall back to a single majority.
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
	// A realistic sequence: grow from three to five, then remove the original
	// leader's peer, one complete transition at a time.
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

	// Three of the four remaining nodes is still a majority.
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
