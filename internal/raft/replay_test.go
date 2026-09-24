package raft

import "testing"

func drained(t *testing.T) *Node {
	t.Helper()

	n, _ := newLeader(t)
	n.Advance(n.Ready())
	return n
}

func indexesOf(entries []Entry) []Index {
	out := make([]Index, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Index)
	}
	return out
}

func TestCommittedEntriesComeBackUntilTheyAreAdvanced(t *testing.T) {
	n := drained(t)

	if err := n.Propose([]byte("one")); err != nil {
		t.Fatalf("proposing: %v", err)
	}

	first := n.Ready()
	if len(first.CommittedEntries) == 0 {
		t.Fatal("nothing was committed, so there is nothing to hand back")
	}

	second := n.Ready()

	got, want := indexesOf(second.CommittedEntries), indexesOf(first.CommittedEntries)
	if len(got) != len(want) {
		t.Fatalf("a second Ready returned %v, want the same %v; entries nobody "+
			"applied would be lost", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("a second Ready returned %v, want the same %v", got, want)
		}
	}
}

func TestAdvanceStopsThemComingBack(t *testing.T) {
	n := drained(t)

	if err := n.Propose([]byte("one")); err != nil {
		t.Fatalf("proposing: %v", err)
	}

	rd := n.Ready()
	if len(rd.CommittedEntries) == 0 {
		t.Fatal("nothing was committed")
	}
	n.Advance(rd)

	if after := n.Ready(); len(after.CommittedEntries) != 0 {
		t.Fatalf("entries came back after being advanced: %v",
			indexesOf(after.CommittedEntries))
	}
}

func TestAdvancingPartOfTheWayKeepsTheRest(t *testing.T) {
	n := drained(t)

	if err := n.Propose([]byte("one")); err != nil {
		t.Fatalf("proposing: %v", err)
	}
	first := n.Ready()

	if err := n.Propose([]byte("two")); err != nil {
		t.Fatalf("proposing: %v", err)
	}
	n.Advance(first)

	second := n.Ready()
	if len(second.CommittedEntries) == 0 {
		t.Fatal("the entry committed during the apply was never handed over")
	}

	last := first.CommittedEntries[len(first.CommittedEntries)-1].Index
	if got := second.CommittedEntries[0].Index; got != last+1 {
		t.Errorf("the next batch starts at %d, want %d; %d entries were skipped",
			got, last+1, got-last-1)
	}
}

func TestAdvancingASnapshotMarksItApplied(t *testing.T) {
	n, _ := newFragileNode(t)
	atTerm(t, n, 1)

	snap := anImage()
	if err := n.Step(Message{
		Type: MsgInstallSnapshot, From: 2, To: 1, Term: 1, Snapshot: &snap,
	}); err != nil {
		t.Fatalf("installing a snapshot: %v", err)
	}

	rd := n.Ready()
	if rd.Snapshot == nil {
		t.Fatal("the snapshot was not handed to the caller")
	}
	n.Advance(rd)

	if n.log.applied != snap.Index {
		t.Errorf("applied is %d after advancing a snapshot at %d",
			n.log.applied, snap.Index)
	}
}
