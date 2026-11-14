package storage_test

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// Compaction (CreateSnapshot, which ends in WAL.TruncateBefore) re-records the
// HardState into the active segment and then deletes older segments, which may
// hold the only durable copy of the term and vote (Raft paper Figure 2,
// persistent state; section 5.2). The replacement record must be durable before
// the first delete in every fsync mode (RUNBOOK 7.2 item 1, 7.3 "persist the
// intent before deleting data"). T3.10.

const (
	compactionSegmentSize = 150
	compactionEntries     = 12
)

var (
	oldHardState   = raft.HardState{Term: 5, VotedFor: 2}
	newerHardState = raft.HardState{Term: 6, VotedFor: 3}
)

// walFiles lists the WAL directory.
func walFiles(t *testing.T, f *faultfs.FS) []string {
	t.Helper()
	des, err := f.ReadDir(walDir)
	if err != nil {
		t.Fatalf("listing %s: %v", walDir, err)
	}
	var names []string
	for _, d := range des {
		names = append(names, d.Name())
	}
	return names
}

// buildCompactionSetup stores {5,2} and enough entries to roll twice, so {5,2}
// sits in a sealed segment, then makes everything durable.
func buildCompactionSetup(t *testing.T, policy storage.SyncPolicy, seed int64) *rig {
	t.Helper()
	r := newRig(t, newFS(t, seed, faultfs.Options{}), policy, compactionSegmentSize)
	requireOK(t, "open", r.open())
	requireOK(t, "hard state", r.setHardState(oldHardState))
	for i := raft.Index(1); i <= compactionEntries; i++ {
		requireOK(t, "append", r.append(logEntries(5, i, 1)))
	}
	if n := len(walFiles(t, r.fs)); n < 3 {
		t.Fatalf("setup produced %d segments, want at least 3 so {5,2} sits in a sealed one", n)
	}
	// The WAL's own Sync covers the active segment only (T3.12), so make
	// everything, including directory entries, durable through the test hook.
	r.fs.SyncAll()
	return r
}

// compactionSetup is buildCompactionSetup plus the precondition that makes the
// tests below non-vacuous: an identical twin, crashed with drop, recovers {5,2}
// with nothing torn, and no file of this store has unsynced bytes.
func compactionSetup(t *testing.T, policy storage.SyncPolicy, seed int64) *rig {
	t.Helper()
	twin := buildCompactionSetup(t, policy, seed)
	twin.fs.Crash(faultfs.CrashOptions{})
	twin.reopen("precondition")
	if hs, _ := twin.s.InitialState(); hs != oldHardState {
		t.Fatalf("precondition: a crash right after setup recovers hard state %+v, want %+v", hs, oldHardState)
	}
	if twin.repairs != 0 {
		t.Fatalf("precondition: %d segments were torn after setup, want none", twin.repairs)
	}

	r := buildCompactionSetup(t, policy, seed)
	for _, dir := range []string{walDir, snapDir, dataDir} {
		des, err := r.fs.ReadDir(dir)
		if err != nil {
			t.Fatalf("listing %s: %v", dir, err)
		}
		for _, d := range des {
			if d.IsDir() {
				continue
			}
			if u := r.fs.Unsynced(dir + "/" + d.Name()); u != 0 {
				t.Fatalf("precondition: %s/%s has %d unsynced bytes after setup", dir, d.Name(), u)
			}
		}
	}
	return r
}

// crashPoint is one place to cut power: before or after filesystem operation n.
type crashPoint struct {
	n     int
	after bool
	// firstRemove is the operation number of the first segment delete, and
	// removed says whether that delete had taken effect at this point.
	firstRemove int
	removed     bool
}

func (p crashPoint) String() string {
	when := "before"
	if p.after {
		when = "after"
	}
	return fmt.Sprintf("%s op %d (first Remove is op %d)", when, p.n, p.firstRemove)
}

// compactionCrashPoints runs the compaction of the whole log under policy and
// cuts power at every operation inside WAL.TruncateBefore (from the write of the
// replacement HardState to the end), under every mode, then calls verify on the
// reopened store. newer, when set, is saved unsynced after setup.
func compactionCrashPoints(t *testing.T, policy storage.SyncPolicy, newer *raft.HardState, modes []crashMode,
	verify func(t *testing.T, ctx string, r *rig, p crashPoint)) {
	t.Helper()
	quietLogs(t)

	prepare := func(seed int64) *rig {
		r := compactionSetup(t, policy, seed)
		if newer != nil {
			requireOK(t, "newer hard state", r.setHardState(*newer))
		}
		return r
	}

	dry := prepare(1)
	base := dry.fs.Ops()
	requireOK(t, "dry run", dry.createSnapshot(compactionEntries))
	end := dry.fs.Ops()
	log := dry.fs.Log()

	firstRemove, start := 0, 0
	for _, line := range log {
		if f := strings.Fields(line); len(f) == 3 && f[1] == "Remove" && strings.Contains(f[2], ".wal") {
			firstRemove, _ = strconv.Atoi(f[0])
			break
		}
	}
	if firstRemove == 0 {
		t.Fatalf("the compaction removed no segment; log: %v", log[base:])
	}
	for _, line := range log[:firstRemove-1] {
		if f := strings.Fields(line); len(f) == 3 && f[1] == "Write" && strings.Contains(f[2], ".wal") {
			start, _ = strconv.Atoi(f[0])
		}
	}
	if start <= base {
		t.Fatalf("found no WAL write between the setup and the first Remove; log: %v", log[base:])
	}

	ran := 0
	for at := start; at <= end; at++ {
		for _, after := range []bool{false, true} {
			for _, mode := range modes {
				ran++
				r := prepare(mode.seed)
				if after {
					r.fs.CrashAfter(at)
				} else {
					r.fs.CrashAt(at)
				}
				p := crashPoint{n: at, after: after, firstRemove: firstRemove}
				p.removed = (after && at >= firstRemove) || (!after && at > firstRemove)
				ctx := fmt.Sprintf("%s, mode %s", p, mode.name)
				err := r.createSnapshot(compactionEntries)
				if !r.fs.Crashed() {
					t.Fatalf("%s: the crash point was never reached (CreateSnapshot returned %v)", ctx, err)
				}
				r.fs.Crash(mode.opts)
				r.reopen(ctx)
				r.check(ctx)
				verify(t, ctx, r, p)
			}
		}
	}
	if ran == 0 {
		t.Fatal("no crash point ran")
	}
	t.Logf("crash points %d..%d of %d, %d runs", start, end, end, ran)
}

func dropMode() []crashMode { return standardModes()[:1] }

func dropAndTornModes() []crashMode {
	var out []crashMode
	for _, m := range standardModes() {
		if strings.HasPrefix(m.name, "drop") || strings.HasPrefix(m.name, "torn") {
			out = append(out, m)
		}
	}
	return out
}

// requireNotRolledBack fails when the recovered term is below the durable {5,2}
// or the vote is inconsistent with the term.
func requireNotRolledBack(t *testing.T, ctx string, r *rig) {
	t.Helper()
	hs, _ := r.s.InitialState()
	switch {
	case hs.Term < oldHardState.Term:
		t.Fatalf("%s: the term rolled back to %+v although %+v was durable before compaction", ctx, hs, oldHardState)
	case hs.Term == oldHardState.Term && hs.VotedFor != oldHardState.VotedFor:
		t.Fatalf("%s: recovered %+v: the vote in term %d is not the durable one (%+v)", ctx, hs, hs.Term, oldHardState)
	}
}

// TestCompactionDoesNotRollBackHardStateUnderSyncNever: {5,2} was durable in a
// sealed segment before compaction; whatever the crash point inside
// TruncateBefore, power loss must not bring back an older term or vote.
func TestCompactionDoesNotRollBackHardStateUnderSyncNever(t *testing.T) {
	compactionCrashPoints(t, storage.SyncNever, nil, dropMode(),
		func(t *testing.T, ctx string, r *rig, _ crashPoint) {
			t.Helper()
			if hs, _ := r.s.InitialState(); hs != oldHardState {
				t.Fatalf("%s: recovered hard state %+v, want %+v (durable before compaction)", ctx, hs, oldHardState)
			}
		})
}

// TestCompactionKeepsNewerUnsyncedHardState: a newer {6,3} saved unsynced is
// recovered as {5,2} or {6,3} and, once the first segment delete has taken
// effect, must be {6,3}: the replacement record is synced before any delete.
func TestCompactionKeepsNewerUnsyncedHardState(t *testing.T) {
	newer := newerHardState
	compactionCrashPoints(t, storage.SyncNever, &newer, dropAndTornModes(),
		func(t *testing.T, ctx string, r *rig, p crashPoint) {
			t.Helper()
			hs, _ := r.s.InitialState()
			if hs != oldHardState && hs != newerHardState {
				t.Fatalf("%s: recovered hard state %+v, want %+v or %+v", ctx, hs, oldHardState, newerHardState)
			}
			if p.removed && hs != newerHardState {
				t.Fatalf("%s: recovered %+v after a segment was deleted, want %+v: the replacement record was not synced before the delete",
					ctx, hs, newerHardState)
			}
		})
}

// TestCompactionCrashPointsUnderSyncNever: every crash point inside
// TruncateBefore, drop and torn tails, five seeds: the term never goes below
// the durable one; r.check covers log and snapshot consistency and a log with
// no hole.
func TestCompactionCrashPointsUnderSyncNever(t *testing.T) {
	compactionCrashPoints(t, storage.SyncNever, nil, dropAndTornModes(),
		func(t *testing.T, ctx string, r *rig, _ crashPoint) {
			t.Helper()
			requireNotRolledBack(t, ctx, r)
		})
}

// TestCompactionCrashPointsUnderSyncAlways is the control: green before and
// after the fix.
func TestCompactionCrashPointsUnderSyncAlways(t *testing.T) {
	compactionCrashPoints(t, storage.SyncAlways, nil, dropAndTornModes(),
		func(t *testing.T, ctx string, r *rig, _ crashPoint) {
			t.Helper()
			requireNotRolledBack(t, ctx, r)
		})
}

// TestNoSyncWhenNothingIsDeleted pins the decision that the extra fsync is paid
// only when a delete follows: a compaction that removes no segment makes no WAL
// fsync under SyncNever.
func TestNoSyncWhenNothingIsDeleted(t *testing.T) {
	quietLogs(t)
	r := compactionSetup(t, storage.SyncNever, 1)
	base := r.fs.Ops()
	// Entry 1 is in the first segment; the second starts above 2, so nothing
	// is superseded.
	requireOK(t, "CreateSnapshot", r.createSnapshot(1))
	for _, line := range r.fs.Log()[base:] {
		if f := strings.Fields(line); len(f) == 3 && f[1] == "Remove" && strings.Contains(f[2], ".wal") {
			t.Fatalf("the compaction removed a segment (%s), so this does not test the no-delete path", line)
		}
		if f := strings.Fields(line); len(f) == 3 && f[1] == "Sync" && strings.HasSuffix(f[2], ".wal") {
			t.Fatalf("a WAL fsync happened although nothing was deleted: %s", line)
		}
	}
}

// TestSyncFailureBeforeDeleteDeletesNothing: the fsync of the replacement
// record fails once; the compaction returns that error and removes no segment.
func TestSyncFailureBeforeDeleteDeletesNothing(t *testing.T) {
	quietLogs(t)
	r := compactionSetup(t, storage.SyncNever, 1)
	before := walFiles(t, r.fs)

	r.fs.Inject(faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync})
	err := r.createSnapshot(compactionEntries)
	if err == nil {
		t.Fatal("CreateSnapshot succeeded although the fsync of the replacement hard state record was armed to fail")
	}
	if !errors.Is(err, faultfs.ErrSync) {
		t.Fatalf("CreateSnapshot error = %v, want the injected fsync error", err)
	}
	after := map[string]bool{}
	for _, n := range walFiles(t, r.fs) {
		after[n] = true
	}
	for _, n := range before {
		if !after[n] {
			t.Fatalf("segment %s was removed although the fsync of the replacement record failed", n)
		}
	}
}
