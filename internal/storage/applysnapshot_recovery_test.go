package storage_test

// T3.11 tests 1 and 2: what a DiskStorage recovers after ApplySnapshot. A
// mismatching snapshot writes a durable DiscardSuffix, so a restart never brings
// the stale tail back; a matching one keeps its suffix on disk while memory drops
// it (disk.go), which is expected and benign.

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
// then a snapshot at 5. A mismatching one (term 2) discards 6..10 (paper
// section 7) durably; a matching one (term 1) keeps them on disk although the
// in-memory view holds nothing above 5.
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
			if tc.mismatch {
				// Intended spec change (T3.11): a mismatching snapshot writes a durable
				// DiscardSuffix, so the stale tail 6..10 is gone after a restart.
				if len(reopened.tail) != 0 {
					t.Fatalf("reopened tail is [%s], want it discarded", describe(reopened.tail))
				}
				return
			}
			// A matching snapshot keeps its suffix in the WAL (paper section 7) while the
			// in-memory view drops it; the difference is expected and benign, see
			// TestMatchingSnapshotMemoryDiskDifferenceIsBenign.
			want := logEntries(1, 6, 5)
			if !sameEntries(reopened.tail, want) {
				t.Fatalf("reopened tail is [%s], want the matching suffix [%s]",
					describe(reopened.tail), describe(want))
			}
		})
	}
}

// TestApplySnapshotCrashPointsRecoveredLog (T3.11 test 2): a node holding 1..10
// at term 1 installs the mismatching snapshot (5,2) with a crash at every
// filesystem operation. The recovered store is the pre-call log, or the
// snapshot with the stale tail, or the snapshot with an empty tail; never a hole
// (Open runs validateContiguous). A crash point is inside a call, so none of these
// is an acknowledged install: TestAcknowledgedInstallSurvivesCrash covers that.
// SyncAlways only.
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

// TestAcknowledgedInstallSurvivesCrash (T3.11 guarantee): once ApplySnapshot has
// returned success for a mismatching snapshot, no crash and no restart brings the
// discarded suffix back, and entries appended afterwards survive every restart.
// Every crash mode, SyncAlways.
func TestAcknowledgedInstallSurvivesCrash(t *testing.T) {
	quietLogs(t)
	leader := logEntries(2, 1, 12)
	for _, mode := range append([]crashMode{{"drop", 1, faultfs.CrashOptions{}}}, torn(1, 2, 3, 4, 5)...) {
		t.Run(mode.name, func(t *testing.T) {
			f := newFS(t, mode.seed, faultfs.Options{})
			r := newRig(t, f, storage.SyncAlways, 0)
			requireOK(t, "open", r.open())
			requireOK(t, "append", r.append(logEntries(1, 1, 10)))
			requireOK(t, "ApplySnapshot", r.applySnapshot(leader, 5))

			f.Crash(mode.opts)
			r.reopen(mode.name)
			r.check(mode.name)
			requireDiscardedAcrossRestarts(t, r, mode.name)
			r.check(mode.name + " (after the restarts)")
		})
	}
}

func torn(seeds ...int64) []crashMode {
	var out []crashMode
	for _, seed := range seeds {
		out = append(out, crashMode{fmt.Sprintf("torn/seed%d", seed), seed, faultfs.CrashOptions{Tail: faultfs.TornTail}})
	}
	return out
}

// requireDiscardedAcrossRestarts holds an acknowledged mismatching install to its
// guarantee: the log is the snapshot with an empty tail, a second restart with
// no writes in between changes nothing, and entries appended after the install
// (made durable first: SyncAlways) survive every further restart.
func requireDiscardedAcrossRestarts(t *testing.T, r *rig, ctx string) {
	t.Helper()
	if v := viewOf(t, r.s); v.boundary != 5 || v.term != 2 || len(v.tail) != 0 {
		t.Fatalf("%s: the install returned but the recovered log is %v, want snapshot (5,2) and an empty tail", ctx, v)
	}

	requireOK(t, ctx+": close", r.s.Close())
	r.reopen(ctx + " (second restart)")
	if v := viewOf(t, r.s); v.boundary != 5 || len(v.tail) != 0 {
		t.Fatalf("%s: after a second restart the log is %v, want an empty tail", ctx, v)
	}

	requireOK(t, ctx+": append after the install", r.append(logEntries(2, 6, 2)))
	for restart := 1; restart <= 2; restart++ {
		requireOK(t, ctx+": close", r.s.Close())
		r.reopen(fmt.Sprintf("%s (restart %d after appends)", ctx, restart))
		v := viewOf(t, r.s)
		if v.boundary != 5 || !sameEntries(v.tail, logEntries(2, 6, 2)) {
			t.Fatalf("%s: restart %d after the appends gives %v, want snapshot (5,2) + 6/2 7/2", ctx, restart, v)
		}
	}
}

// TestApplySnapshotAtOrBelowTheBoundaryIsRejected (T3.11 condition C): a snapshot
// whose index is at or below the existing snapshot, hence at or below the first
// retained WAL index minus one, has no local term to compare with. It is refused
// before anything is written (disk.go: "not newer than the local state"), never
// counted as a mismatch, so it can never write a DiscardSuffix over entries the
// node still needs.
func TestApplySnapshotAtOrBelowTheBoundaryIsRejected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, s *storage.DiskStorage)
	}{
		{"boundary from a mismatching install", func(t *testing.T, s *storage.DiskStorage) {
			requireOK(t, "append", s.Append(logEntries(1, 1, 10)))
			requireOK(t, "ApplySnapshot", s.ApplySnapshot(raft.Snapshot{Index: 5, Term: 2, Conf: testConf, Data: []byte("state@5")}))
		}},
		{"boundary from a local CreateSnapshot", func(t *testing.T, s *storage.DiskStorage) {
			requireOK(t, "append", s.Append(logEntries(1, 1, 10)))
			requireOK(t, "CreateSnapshot", s.CreateSnapshot(5, []byte("state@5"), testConf))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, _ := openOnDisk(t, dir)
			tc.setup(t, s)
			boundary := s.SnapshotMeta()
			if boundary.Index != 5 {
				t.Fatalf("precondition: boundary is %+v, want index 5", boundary)
			}

			// Entries above the boundary that a wrongly written discard would destroy.
			above := logEntries(boundary.Term, 6, 3)
			if s.LastIndex() < 8 {
				requireOK(t, "append above the boundary", s.Append(above))
			}
			before := viewOf(t, s)

			for _, snap := range []raft.Snapshot{
				{Index: 3, Term: 1, Conf: testConf, Data: []byte("older")},               // below, any term
				{Index: 3, Term: boundary.Term, Conf: testConf, Data: []byte("older")},   // below, same term
				{Index: 5, Term: boundary.Term, Conf: testConf, Data: []byte("same")},    // at the boundary
				{Index: 5, Term: boundary.Term + 7, Conf: testConf, Data: []byte("odd")}, // at it, another term
				{Index: 1, Term: 9, Conf: testConf, Data: []byte("oldest")},
			} {
				if err := s.ApplySnapshot(snap); err == nil {
					t.Errorf("ApplySnapshot(%d,%d) succeeded, want it refused", snap.Index, snap.Term)
				}
			}

			if after := viewOf(t, s); after.String() != before.String() {
				t.Fatalf("a refused ApplySnapshot changed the log: %v -> %v", before, after)
			}
			requireOK(t, "close", s.Close())

			s2, snap := openOnDisk(t, dir)
			defer func() { _ = s2.Close() }()
			if snap.Meta != boundary {
				t.Errorf("snapshot after reopen is %+v, want %+v", snap.Meta, boundary)
			}
			if got := viewOf(t, s2); got.String() != before.String() {
				t.Fatalf("after reopen the log is %v, want %v: a refused call must not leave a discard behind", got, before)
			}
		})
	}
}
