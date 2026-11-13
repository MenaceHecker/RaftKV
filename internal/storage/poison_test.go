package storage_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// T3.9: after the first failed write, sync, roll or cleanup the WAL refuses
// every later write. All faults here are one-shot (Rule.Nth): the next call
// would succeed, so only the code under test can refuse it. A CrashAt FS fails
// every later call by itself and could not tell the difference.

var errDiskFault = errors.New("test: injected disk fault")

// openWAL opens a bare WAL on f.
func openWAL(t *testing.T, f *faultfs.FS, segmentSize int64) (*storage.WAL, storage.Replay) {
	t.Helper()
	w, rep, err := storage.Open(storage.Options{Dir: walDir, Sync: storage.SyncAlways, SegmentSize: segmentSize, FS: f})
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	return w, rep
}

func mustAppend(t *testing.T, w *storage.WAL, es []raft.Entry) {
	t.Helper()
	if err := w.AppendEntries(es); err != nil {
		t.Fatalf("appending %d..%d: %v", es[0].Index, es[len(es)-1].Index, err)
	}
}

// requireRefused fails the test unless every write operation of w now returns
// ErrWALFailed wrapping cause.
func requireRefused(t *testing.T, ctx string, w *storage.WAL, cause error, next raft.Index) {
	t.Helper()
	ops := map[string]func() error{
		"AppendEntries":    func() error { return w.AppendEntries(logEntries(1, next, 1)) },
		"SaveHardState":    func() error { return w.SaveHardState(raft.HardState{Term: 9, VotedFor: 1}) },
		"SaveSnapshotMeta": func() error { return w.SaveSnapshotMeta(storage.SnapshotMeta{Index: 1, Term: 1}) },
		"SaveSnapshotMetaAndDiscardSuffix": func() error {
			return w.SaveSnapshotMetaAndDiscardSuffix(storage.SnapshotMeta{Index: 1, Term: 1}, 2)
		},
		"TruncateBefore": func() error { return w.TruncateBefore(1) },
		"Sync":           func() error { return w.Sync() },
	}
	for name, op := range ops {
		err := op()
		if !errors.Is(err, storage.ErrWALFailed) {
			t.Errorf("%s: %s after the failure returned %v, want ErrWALFailed", ctx, name, err)
			continue
		}
		if cause != nil && !errors.Is(err, cause) {
			t.Errorf("%s: %s returned %v, which does not wrap the first failure %v", ctx, name, err, cause)
		}
	}
}

func TestWriteFailurePoisonsTheWAL(t *testing.T) {
	quietLogs(t)
	f := newFS(t, 1, faultfs.Options{})
	w, _ := openWAL(t, f, 0)
	mustAppend(t, w, logEntries(1, 1, 3))
	if err := w.SaveHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}

	f.Inject(faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync})
	err := w.AppendEntries(logEntries(1, 4, 2))
	if !errors.Is(err, faultfs.ErrSync) {
		t.Fatalf("the append returned %v, want the injected sync failure", err)
	}

	requireRefused(t, "after a failed fsync", w, faultfs.ErrSync, 4)
}

func TestSyncFailurePoisonsTheWAL(t *testing.T) {
	quietLogs(t)
	f := newFS(t, 1, faultfs.Options{})
	w, _ := openWAL(t, f, 0)
	mustAppend(t, w, logEntries(1, 1, 2))

	f.Inject(faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync})
	if err := w.Sync(); !errors.Is(err, faultfs.ErrSync) {
		t.Fatalf("Sync returned %v, want the injected failure", err)
	}
	requireRefused(t, "after a failed Sync", w, faultfs.ErrSync, 3)
}

func TestCloseAfterFailureReturnsTheCauseWithoutSyncing(t *testing.T) {
	quietLogs(t)
	f := newFS(t, 1, faultfs.Options{})
	w, _ := openWAL(t, f, 0)
	mustAppend(t, w, logEntries(1, 1, 2))

	f.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace})
	if err := w.AppendEntries(logEntries(1, 3, 1)); err == nil {
		t.Fatal("the injected write failure did not fail the append")
	}

	// A sync after the failure would be the retry fsyncgate warns about.
	f.Inject(faultfs.Rule{Op: faultfs.OpSync, Sticky: true, Err: faultfs.ErrSync})
	err := w.Close()
	if !errors.Is(err, faultfs.ErrNoSpace) {
		t.Fatalf("Close returned %v, want the first failure (ENOSPC)", err)
	}
	if errors.Is(err, faultfs.ErrSync) {
		t.Fatalf("Close synced a failed WAL: %v", err)
	}
}

func TestShortWriteThenRetryDoesNotBrickTheDirectory(t *testing.T) {
	quietLogs(t)
	f := newFS(t, 1, faultfs.Options{})
	w, _ := openWAL(t, f, 0)
	mustAppend(t, w, logEntries(1, 1, 3))

	f.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ShortWrite(5)})
	if err := w.AppendEntries(logEntries(1, 4, 1)); err == nil {
		t.Fatal("the short write did not fail the append")
	}

	// The caller retries the same entry. Written after 5 bytes of garbage it
	// would turn a torn tail into mid-log damage.
	err := w.AppendEntries(logEntries(1, 4, 1))
	if !errors.Is(err, storage.ErrWALFailed) {
		t.Fatalf("the retry returned %v, want ErrWALFailed", err)
	}

	f.ProcessCrash() // the process dies; the 5 garbage bytes stay on disk
	f.ClearRules()
	w2, rep, err := storage.Open(storage.Options{Dir: walDir, Sync: storage.SyncAlways, FS: f})
	if err != nil {
		t.Fatalf("reopening after the retry: %v", err)
	}
	defer func() { _ = w2.Close() }()
	if n := len(rep.Entries); n != 3 {
		t.Fatalf("recovered %d entries, want the 3 from before the failure", n)
	}
}

func TestSyncFailureOnRollPoisons(t *testing.T) {
	quietLogs(t)
	cases := []struct {
		name string
		rule faultfs.Rule
	}{
		{"ENOSPC creating the new segment", faultfs.Rule{Op: faultfs.OpOpenFile, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace}},
		{"directory fsync of the new segment", faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "wal", Nth: 1, Err: faultfs.ErrSyncDir}},
		{"closing the previous segment", faultfs.Rule{Op: faultfs.OpClose, PathGlob: "*.wal", Nth: 1, Err: errDiskFault}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFS(t, 1, faultfs.Options{})
			w, _ := openWAL(t, f, 1) // every write fills the segment, so every append rolls
			mustAppend(t, w, logEntries(1, 1, 1))

			f.Inject(tc.rule)
			if err := w.AppendEntries(logEntries(1, 2, 1)); !errors.Is(err, tc.rule.Err) {
				t.Fatalf("the append returned %v, want the injected %v", err, tc.rule.Err)
			}
			requireRefused(t, tc.name, w, tc.rule.Err, 3)
		})
	}
}

// walWithSegments returns a WAL that holds a hard state and entries 1..6 in
// separate segments, so TruncateBefore has segments to remove.
func walWithSegments(t *testing.T, f *faultfs.FS) *storage.WAL {
	t.Helper()
	w, _ := openWAL(t, f, 1)
	if err := w.SaveHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	for i := raft.Index(1); i <= 6; i++ {
		mustAppend(t, w, logEntries(1, i, 1))
	}
	if w.SegmentCount() < 4 {
		t.Fatalf("only %d segments, the test needs several to remove", w.SegmentCount())
	}
	return w
}

func TestSyncFailureInTruncateBeforePoisons(t *testing.T) {
	quietLogs(t)
	cases := []struct {
		name string
		rule faultfs.Rule
	}{
		{"hard state write", faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace}},
		{"hard state fsync", faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync}},
		{"removing a segment", faultfs.Rule{Op: faultfs.OpRemove, PathGlob: "*.wal", Nth: 1, Err: errDiskFault}},
		{"directory fsync after the removals", faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "wal", Nth: 1, Err: faultfs.ErrSyncDir}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFS(t, 1, faultfs.Options{})
			w := walWithSegments(t, f)

			f.Inject(tc.rule)
			if err := w.TruncateBefore(5); !errors.Is(err, tc.rule.Err) {
				t.Fatalf("TruncateBefore returned %v, want the injected %v", err, tc.rule.Err)
			}
			requireRefused(t, tc.name, w, tc.rule.Err, 7)
		})
	}
}

// TestEveryFailureAfterPoisonKeepsDurableStateIntact injects a one-shot error
// at each filesystem operation of a workload (not a crash: the next call would
// succeed), has the caller retry, then loses power and holds the recovered
// store to the checker. The FS keeps failed-fsync bytes non-durable even if a
// later fsync succeeds (fsyncgate), so a WAL that retries acks lost data.
func TestEveryFailureAfterPoisonKeepsDurableStateIntact(t *testing.T) {
	quietLogs(t)

	work := func(r *rig) error {
		if err := r.setHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
			return err
		}
		for i := raft.Index(1); i <= 12; i++ {
			if err := r.append(logEntries(1, i, 1)); err != nil {
				return err
			}
			if i == 3 {
				if err := r.setHardState(raft.HardState{Term: 2, VotedFor: 3}); err != nil {
					return err
				}
			}
		}
		if err := r.createSnapshot(9); err != nil {
			return err
		}
		return r.append(logEntries(2, 13, 2))
	}
	fresh := func() *rig {
		r := newRig(t, newFS(t, 1, faultfs.Options{Fsyncgate: true}), storage.SyncAlways, 150)
		requireOK(t, "open", r.open())
		return r
	}

	dry := fresh()
	base := dry.fs.Ops()
	requireOK(t, "dry run", work(dry))
	total := dry.fs.Ops() - base

	modes := []crashMode{
		{"drop", 1, faultfs.CrashOptions{}},
		{"torn/seed1", 1, faultfs.CrashOptions{Tail: faultfs.TornTail}},
		{"process", 1, faultfs.CrashOptions{}},
	}
	for n := 1; n <= total; n++ {
		for _, mode := range modes {
			r := fresh()
			opBase := r.fs.Ops()
			r.fs.Inject(faultfs.Rule{Op: faultfs.OpAny, Nth: n, Err: faultfs.ErrSync})
			ctx := fmt.Sprintf("fault at op %d, mode %s", n, mode.name)

			err := work(r)
			if err == nil {
				t.Fatalf("%s: the workload ignored the injected fault", ctx)
			}
			log := r.fs.Log()
			failedOp := log[opBase+n-1]
			walOp := strings.Contains(failedOp, "/data/wal")

			// The caller retries the write that failed, as a loop that ignores
			// the error would. A WAL fault must not be survivable.
			retry := r.append(logEntries(1, r.lastIndex()+1, 1))
			if walOp && !errors.Is(retry, storage.ErrWALFailed) {
				t.Fatalf("%s (%q): the write after a WAL failure returned %v, want ErrWALFailed", ctx, failedOp, retry)
			}

			if mode.name == "process" {
				r.fs.ProcessCrash()
			} else {
				r.fs.Crash(mode.opts)
			}
			r.fs.ClearRules()
			r.reopen(ctx)
			r.check(ctx)
		}
	}
}

// TestSnapshotFileFailureDoesNotPoisonTheWAL is a control: a failed snapshot
// file write aborts that snapshot and leaves the WAL usable.
func TestSnapshotFileFailureDoesNotPoisonTheWAL(t *testing.T) {
	quietLogs(t)
	r := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncAlways, 0)
	requireOK(t, "open", r.open())
	requireOK(t, "append", r.append(logEntries(1, 1, 5)))

	r.fs.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ErrNoSpace})
	if err := r.createSnapshot(3); !errors.Is(err, faultfs.ErrNoSpace) {
		t.Fatalf("createSnapshot returned %v, want the injected ENOSPC", err)
	}
	if got := r.s.SnapshotMeta().Index; got != 0 {
		t.Fatalf("a failed snapshot moved the boundary to %d", got)
	}

	requireOK(t, "append after the failed snapshot", r.append(logEntries(1, 6, 1)))
	requireOK(t, "hard state after the failed snapshot", r.setHardState(raft.HardState{Term: 2, VotedFor: 1}))
	requireOK(t, "snapshot retry", r.createSnapshot(3))

	r.fs.Crash(faultfs.CrashOptions{})
	r.reopen("after the control")
	r.check("after the control")
	if got := r.s.LastIndex(); got != 6 {
		t.Fatalf("LastIndex = %d after reopen, want 6", got)
	}
}

// cleanupCase is one failure in the cleanup that follows a snapshot has been
// published: TruncateBefore's work in the WAL, or purging old snapshot files.
type cleanupCase struct {
	name string
	rule faultfs.Rule
	// op and path pick which counted operation of the call the rule hits, by
	// reading a dry run's log: the last one when last is set, else the first.
	op   faultfs.Op
	path string
	last bool
	// wal says the failure is in the WAL, which must then refuse writes.
	wal bool
}

func cleanupCases() []cleanupCase {
	return []cleanupCase{
		{name: "TruncateBefore, removing a segment", op: faultfs.OpRemove, path: "/data/wal/", wal: true,
			rule: faultfs.Rule{Op: faultfs.OpRemove, PathGlob: "*.wal", Err: errDiskFault}},
		{name: "TruncateBefore, directory fsync", op: faultfs.OpSyncDir, path: "/data/wal", last: true, wal: true,
			rule: faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "wal", Err: faultfs.ErrSyncDir}},
		{name: "purge, removing an old snapshot", op: faultfs.OpRemove, path: "/data/snap/",
			rule: faultfs.Rule{Op: faultfs.OpRemove, PathGlob: "*.snap", Err: errDiskFault}},
		{name: "purge, directory fsync", op: faultfs.OpSyncDir, path: "/data/snap", last: true,
			rule: faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "snap", Err: faultfs.ErrSyncDir}},
	}
}

// nthInLog returns which matching operation of lines is the one to fail.
func nthInLog(t *testing.T, lines []string, op faultfs.Op, path string, last bool) int {
	t.Helper()
	count := 0
	for _, l := range lines {
		fields := strings.SplitN(l, " ", 3)
		if len(fields) == 3 && fields[1] == op.String() && strings.HasPrefix(fields[2], path) {
			count++
			if !last {
				return count
			}
		}
	}
	if count == 0 {
		t.Fatalf("the dry run made no %s on %s, so the case would test nothing; log:\n%s", op, path, strings.Join(lines, "\n"))
	}
	return count
}

// snapshotScenario prepares a store with a hard state, entries 1..8 each in
// its own segment, and snapshots at 1, 2 and 3, so that the next snapshot has
// segments to remove and an old snapshot to purge.
func snapshotScenario(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncAlways, 1)
	requireOK(t, "open", r.open())
	requireOK(t, "hard state", r.setHardState(raft.HardState{Term: 1, VotedFor: 2}))
	for i := raft.Index(1); i <= 8; i++ {
		requireOK(t, "append", r.append(logEntries(1, i, 1)))
	}
	for _, idx := range []raft.Index{1, 2, 3} {
		requireOK(t, "snapshot", r.createSnapshot(idx))
	}
	return r
}

func TestSnapshotCleanupFailuresAreReportedAndStopTheNode(t *testing.T) {
	quietLogs(t)

	leader := logEntries(2, 1, 12)
	calls := []struct {
		name  string
		call  func(r *rig) error
		index raft.Index
	}{
		{"CreateSnapshot", func(r *rig) error { return r.createSnapshot(6) }, 6},
		{"ApplySnapshot", func(r *rig) error { return r.applySnapshot(leader, 12) }, 12},
	}

	for _, c := range calls {
		for _, tc := range cleanupCases() {
			t.Run(c.name+"/"+tc.name, func(t *testing.T) {
				dry := snapshotScenario(t)
				before := len(dry.fs.Log())
				requireOK(t, "dry run", c.call(dry))
				rule := tc.rule
				rule.Nth = nthInLog(t, dry.fs.Log()[before:], tc.op, tc.path, tc.last)

				r := snapshotScenario(t)
				lastBefore := r.s.LastIndex()
				r.fs.Inject(rule)
				err := c.call(r)
				if !errors.Is(err, rule.Err) {
					t.Fatalf("%s returned %v, want the injected %v", c.name, err, rule.Err)
				}

				// The snapshot was published before the cleanup failed, so memory
				// and disk both move to it.
				if got := r.s.SnapshotMeta().Index; got != c.index {
					t.Errorf("memory is at snapshot %d, want the published %d", got, c.index)
				}
				wantLast := lastBefore
				if c.index > lastBefore {
					wantLast = c.index
				}
				if got := r.s.LastIndex(); got != wantLast {
					t.Errorf("memory LastIndex = %d, want %d", got, wantLast)
				}

				// The WAL refuses what follows a WAL failure; a failed purge of
				// snapshot files does not touch the WAL.
				next := r.s.LastIndex() + 1
				werr := r.s.Append(logEntries(2, next, 1))
				if tc.wal && !errors.Is(werr, storage.ErrWALFailed) {
					t.Errorf("an append after the WAL failure returned %v, want ErrWALFailed", werr)
				}
				if !tc.wal && werr != nil {
					t.Errorf("a failed purge poisoned the WAL: %v", werr)
				}

				r.fs.ProcessCrash()
				r.fs.ClearRules()
				disk := newRig(t, r.fs, storage.SyncAlways, 1)
				requireOK(t, "reopen", disk.open())
				if got := disk.s.SnapshotMeta().Index; got != c.index {
					t.Errorf("the reopened store is at snapshot %d, want %d (memory agreed)", got, c.index)
				}
				wantDisk := wantLast
				if werr == nil {
					wantDisk++ // the append above was accepted, so it is durable
				}
				if got := disk.s.LastIndex(); got != wantDisk {
					t.Errorf("the reopened LastIndex = %d, want %d", got, wantDisk)
				}
			})
		}
	}
}

func TestSnapshotFileFailureLeavesMemoryUnchanged(t *testing.T) {
	quietLogs(t)
	r := snapshotScenario(t)
	lastBefore := r.s.LastIndex()
	r.fs.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ErrNoSpace})
	if err := r.createSnapshot(6); err == nil {
		t.Fatal("the injected failure did not fail CreateSnapshot")
	}
	if got := r.s.SnapshotMeta().Index; got != 3 {
		t.Errorf("a snapshot that was never published moved memory to %d, want 3", got)
	}
	if got := r.s.LastIndex(); got != lastBefore {
		t.Errorf("LastIndex = %d, want %d", got, lastBefore)
	}
}
