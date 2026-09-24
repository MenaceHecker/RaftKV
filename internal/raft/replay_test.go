package raft

import "testing"

// Committed entries are handed out repeatedly until the caller says it has
// applied them.
//
// This is the whole reason Ready and Advance are separate calls. A driver
// takes a batch, applies it to the state machine, and only then advances. If
// the process dies in between, the entries have not been applied anywhere
// durable and must come back, because nothing else would ever mention them
// again. Marking them applied at the moment they were handed over would make
// a crash mid-apply skip them silently: the log would say they were applied,
// the state machine would not hold them, and no check anywhere compares the
// two.
//
// At-least-once is what the design chooses, and it is safe because applying
// the same entry twice is already handled: commands carry a client ID and a
// sequence number, and the state machine ignores one it has seen.

// drained returns a leader with its election no-op already applied, so that
// what a test proposes is the only thing outstanding.
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

	// No Advance. A driver that died here applied nothing.
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
	// The other half. If Advance did not take effect, a driver would apply
	// the same entries forever and never make progress.
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
	// A driver that applies a batch and advances is told about everything
	// that committed while it was busy. Nothing may be skipped between the
	// two calls.
	n := drained(t)

	if err := n.Propose([]byte("one")); err != nil {
		t.Fatalf("proposing: %v", err)
	}
	first := n.Ready()

	// More commits while the caller is applying the batch it already holds.
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
	// A snapshot replaces the state machine wholesale, so advancing one has
	// to move the applied cursor to its index even though no entries came
	// with it. Otherwise the log would go looking for entries the snapshot
	// just replaced.
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
