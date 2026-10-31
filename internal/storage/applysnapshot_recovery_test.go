package storage_test

// T3.11 tests 1 and 2: what a DiskStorage recovers after ApplySnapshot. They
// state today's behavior (characterisation), not the behavior RAFT requires:
// ApplySnapshot drops every in-memory entry (disk.go), the WAL keeps the entries
// above the snapshot, and Open puts them back.

import (
	"fmt"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// logView is the log a DiskStorage presents: its boundary and the entries above it.
type logView struct {
	boundary raft.Index
	term     raft.Term
	tail     []raft.Entry
}

func viewOf(t *testing.T, s *storage.DiskStorage) logView {
	t.Helper()
	meta := s.SnapshotMeta()
	v := logView{boundary: meta.Index, term: meta.Term}
	if first, last := s.FirstIndex(), s.LastIndex(); last >= first {
		tail, err := s.Entries(first, last+1)
		if err != nil {
			t.Fatalf("reading entries %d..%d: %v", first, last, err)
		}
		v.tail = tail
	}
	return v
}

func (v logView) String() string {
	return fmt.Sprintf("snapshot (%d,%d) + tail [%s]", v.boundary, v.term, describe(v.tail))
}

func (v logView) lastIndex() raft.Index {
	if n := len(v.tail); n > 0 {
		return v.tail[n-1].Index
	}
	return v.boundary
}

func openOnDisk(t *testing.T, dir string) (*storage.DiskStorage, storage.Snapshot) {
	t.Helper()
	s, snap, err := storage.OpenDiskStorage(storage.DiskConfig{Dir: dir, Sync: storage.SyncAlways})
	if err != nil {
		t.Fatalf("opening %s: %v", dir, err)
	}
	return s, snap
}

// TestApplySnapshotThenRestartRecoveredLog (T3.11 test 1): log 1..10 at term 1,
// then a snapshot at 5. A mismatching one (term 2) must discard 6..10 (paper
// section 7); a matching one (term 1) keeps them. Today both come back after a
// restart although the in-memory view held nothing above 5.
func TestApplySnapshotThenRestartRecoveredLog(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapTerm raft.Term
		mismatch bool
	}{
		{"mismatching snapshot (5,2)", 2, true},
		{"matching snapshot (5,1)", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, _ := openOnDisk(t, dir)
			requireOK(t, "append", s.Append(logEntries(1, 1, 10)))

			before, err := s.Term(5)
			requireOK(t, "Term(5) before the install", err)
			if got := before != tc.snapTerm; got != tc.mismatch {
				t.Fatalf("precondition: local Term(5) = %d, snapshot term = %d; mismatch = %v, the case wants %v",
					before, tc.snapTerm, got, tc.mismatch)
			}

			requireOK(t, "ApplySnapshot", s.ApplySnapshot(raft.Snapshot{
				Index: 5, Term: tc.snapTerm, Conf: testConf, Data: []byte("state@5"),
			}))
			inMemory := viewOf(t, s)
			requireOK(t, "close", s.Close())

			s2, snap := openOnDisk(t, dir)
			defer func() { _ = s2.Close() }()
			reopened := viewOf(t, s2)

			t.Logf("in memory after the install: %v", inMemory)
			t.Logf("after close and reopen:      %v", reopened)

			if snap.Meta.Index != 5 || snap.Meta.Term != tc.snapTerm {
				t.Fatalf("reopened snapshot is (%d,%d), want (5,%d)", snap.Meta.Index, snap.Meta.Term, tc.snapTerm)
			}
			if inMemory.lastIndex() != 5 || len(inMemory.tail) != 0 {
				t.Fatalf("in-memory view after the install is %v, want nothing above 5", inMemory)
			}
			// Characterisation of today's behavior: the old tail is resurrected.
			want := logEntries(1, 6, 5)
			if !sameEntries(reopened.tail, want) {
				t.Fatalf("reopened tail is [%s], today's code is expected to resurrect [%s]",
					describe(reopened.tail), describe(want))
			}
		})
	}
}

// TestApplySnapshotCrashPointsRecoveredLog (T3.11 test 2): a node holding 1..10
// at term 1 installs the mismatching snapshot (5,2) with a crash at every
// filesystem operation. The recovered store is the pre-call log, or the
// snapshot with the stale tail, or the snapshot with an empty tail; never a hole
// (Open runs validateContiguous). SyncAlways only.
func TestApplySnapshotCrashPointsRecoveredLog(t *testing.T) {
	leader := logEntries(2, 1, 12)
	stale := logEntries(1, 6, 5)
	preCall := logEntries(1, 1, 10)

	counts := map[string]int{}
	crashEverywhere(t, scenario{
		policy: storage.SyncAlways,
		prepare: func(r *rig) {
			requireOK(t, "open", r.open())
			requireOK(t, "append", r.append(logEntries(1, 1, 10)))
		},
		work: func(r *rig) error { return r.applySnapshot(leader, 5) },
		then: func(t *testing.T, r *rig, ctx string, _ crashMode) {
			v := viewOf(t, r.s)
			switch {
			case r.snap.Meta.Index == 0:
				if v.boundary != 0 || !sameEntries(v.tail, preCall) {
					t.Fatalf("%s: no snapshot loaded but the log is %v, want the pre-call log 1..10", ctx, v)
				}
				counts["pre-call log"]++
			case r.snap.Meta.Index == 5 && r.snap.Meta.Term == 2:
				if v.boundary != 5 || v.term != 2 {
					t.Fatalf("%s: snapshot (5,2) loaded but the log boundary is (%d,%d)", ctx, v.boundary, v.term)
				}
				switch {
				case len(v.tail) == 0:
					counts["snapshot, empty tail"]++
				case sameEntries(v.tail, stale):
					counts["snapshot, stale tail"]++
				default:
					t.Fatalf("%s: snapshot (5,2) with tail [%s]; want empty or the stale 6..10", ctx, describe(v.tail))
				}
			default:
				t.Fatalf("%s: recovered snapshot (%d,%d), want none or (5,2)", ctx, r.snap.Meta.Index, r.snap.Meta.Term)
			}
		},
	}, []crashMode{
		{"drop", 1, faultfs.CrashOptions{}},
		{"torn/seed1", 1, faultfs.CrashOptions{Tail: faultfs.TornTail}},
		{"torn/seed2", 2, faultfs.CrashOptions{Tail: faultfs.TornTail}},
	})
	t.Logf("recovered states over all crash points: %v", counts)
}
