package node

import (
	"context"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Status reports the first and last log index and the snapshot index
// (T4.7 decision 3). Both log indexes are zero when the log holds no entries.
func TestStatusReportsLogAndSnapshotIndexes(t *testing.T) {
	h := newHarness(t, nil)

	st := h.n.Status()
	if st.FirstIndex != 0 || st.LastIndex != 0 || st.SnapshotIndex != 0 {
		t.Fatalf("an empty node reports first %d last %d snapshot %d, want all zero",
			st.FirstIndex, st.LastIndex, st.SnapshotIndex)
	}

	var entries []raft.Entry
	for i := 1; i <= 5; i++ {
		entries = append(entries, raft.Entry{Term: 1, Index: raft.Index(i), Type: raft.EntryNormal,
			Data: putCmd(uint64(i), "k"+string(rune('0'+i)), byte(i))})
	}
	h.n.Step(raft.Message{Type: raft.MsgAppendRequest, From: 2, To: 1, Term: 1, CommitIndex: 5, Entries: entries})
	st = h.await("five entries applied", func(s Status) bool { return s.Applied == 5 })
	if st.FirstIndex != 1 || st.LastIndex != 5 || st.SnapshotIndex != 0 {
		t.Fatalf("after five entries: first %d last %d snapshot %d, want 1, 5, 0",
			st.FirstIndex, st.LastIndex, st.SnapshotIndex)
	}
	if st.Members.Joint {
		t.Error("a plain configuration reports joint")
	}

	if err := h.n.Compact(context.Background()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	st = h.n.Status()
	if st.SnapshotIndex != 5 {
		t.Fatalf("after Compact: snapshot index %d, want 5", st.SnapshotIndex)
	}
	if st.FirstIndex != 0 && st.FirstIndex <= st.SnapshotIndex {
		t.Errorf("first index %d is not past the snapshot index %d", st.FirstIndex, st.SnapshotIndex)
	}
	if st.LastIndex != 0 && st.LastIndex < st.FirstIndex {
		t.Errorf("last index %d is before first index %d", st.LastIndex, st.FirstIndex)
	}
}
