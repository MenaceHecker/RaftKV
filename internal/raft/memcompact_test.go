package raft

import (
	"bytes"
	"testing"
)

func filledStorage(t *testing.T, n int) *MemoryStorage {
	t.Helper()
	st := NewMemoryStorage()
	entries := make([]Entry, n)
	for i := range entries {
		entries[i] = Entry{Term: Term(i/3 + 1), Index: Index(i + 1), Data: []byte{byte(i)}}
	}
	if err := st.Append(entries); err != nil {
		t.Fatalf("append: %v", err)
	}
	return st
}

func TestCreateSnapshotKeepsLaterEntries(t *testing.T) {
	st := filledStorage(t, 10)

	if err := st.CreateSnapshot(4, []byte("state"), ConfState{Voters: []NodeID{1, 2, 3}}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if got := st.FirstIndex(); got != 5 {
		t.Errorf("FirstIndex = %d, want 5", got)
	}
	if got := st.LastIndex(); got != 10 {
		t.Errorf("LastIndex = %d, want 10", got)
	}

	for i := Index(5); i <= 10; i++ {
		got, err := st.Entries(i, i+1)
		if err != nil {
			t.Fatalf("Entries(%d): %v", i, err)
		}
		if got[0].Index != i {
			t.Errorf("index %d returned the entry for %d", i, got[0].Index)
		}
		if want := byte(i - 1); got[0].Data[0] != want {
			t.Errorf("index %d holds data %d, want %d", i, got[0].Data[0], want)
		}
	}
}

func TestCreateSnapshotAnswersForItsOwnIndex(t *testing.T) {
	st := filledStorage(t, 10)
	want, err := st.Term(4)
	if err != nil {
		t.Fatalf("Term(4) before compaction: %v", err)
	}

	if err := st.CreateSnapshot(4, []byte("state"), ConfState{}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got, err := st.Term(4)
	if err != nil {
		t.Fatalf("Term(4) after compaction: %v", err)
	}
	if got != want {
		t.Errorf("Term(4) = %d after compaction, want %d", got, want)
	}
	if _, err := st.Term(3); err != ErrCompacted {
		t.Errorf("Term(3) = %v, want ErrCompacted", err)
	}
}

func TestSnapshotCarriesWhatWasGiven(t *testing.T) {
	st := filledStorage(t, 10)
	conf := ConfState{Voters: []NodeID{1, 2, 3}}

	wantTerm, err := st.Term(6)
	if err != nil {
		t.Fatalf("Term(6): %v", err)
	}

	if err := st.CreateSnapshot(6, []byte("payload"), conf); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Index != 6 {
		t.Errorf("snapshot index = %d, want 6", snap.Index)
	}
	if !bytes.Equal(snap.Data, []byte("payload")) {
		t.Errorf("snapshot data = %q, want payload", snap.Data)
	}
	if len(snap.Conf.Voters) != 3 {
		t.Errorf("snapshot configuration = %v, want three voters", snap.Conf.Voters)
	}
	if snap.Term != wantTerm {
		t.Errorf("snapshot term = %d, want %d", snap.Term, wantTerm)
	}
}

func TestCompactingTwiceMovesForwardOnly(t *testing.T) {
	st := filledStorage(t, 10)

	if err := st.CreateSnapshot(4, []byte("a"), ConfState{}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := st.CreateSnapshot(4, []byte("b"), ConfState{}); err == nil {
		t.Error("compacting to the same index twice was allowed")
	}
	if err := st.CreateSnapshot(3, []byte("c"), ConfState{}); err == nil {
		t.Error("compacting backwards was allowed")
	}
	if err := st.CreateSnapshot(8, []byte("d"), ConfState{}); err != nil {
		t.Fatalf("compacting forwards: %v", err)
	}
	if got := st.FirstIndex(); got != 9 {
		t.Errorf("FirstIndex = %d after the second compaction, want 9", got)
	}
}

func TestCannotCompactPastTheEnd(t *testing.T) {
	st := filledStorage(t, 10)
	if err := st.CreateSnapshot(11, []byte("x"), ConfState{}); err == nil {
		t.Error("compacting past the last entry was allowed")
	}
}

func TestCompactingEverythingLeavesAnEmptyLog(t *testing.T) {
	st := filledStorage(t, 10)
	if err := st.CreateSnapshot(10, []byte("x"), ConfState{}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if got := st.FirstIndex(); got != 11 {
		t.Errorf("FirstIndex = %d, want 11", got)
	}
	if got := st.LastIndex(); got != 10 {
		t.Errorf("LastIndex = %d, want 10", got)
	}
}
