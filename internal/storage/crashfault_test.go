package storage_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// faultCase is one injected failure followed by an immediate crash: no retry,
// because a retry after a failed write corrupts the log (shown red in T3.9).
type faultCase struct {
	name        string
	segmentSize int64
	rule        faultfs.Rule
	// call is the operation the fault hits; it must return an error.
	call func(r *rig) error
	// repaired says a process crash must leave a torn tail that Open repairs.
	repaired bool
}

// runFault builds a store holding entries 1..3 and a hard state, arms the rule,
// runs the call, and crashes: under every power-loss mode, then as a process
// crash that keeps the live bytes. After each, Open succeeds and the checker
// passes with the failed call counted as in flight.
func runFault(t *testing.T, tc faultCase) {
	t.Helper()
	flavors := append(standardModes(), crashMode{name: "process"})
	for _, mode := range flavors {
		seed := mode.seed
		if seed == 0 {
			seed = 1
		}
		f := newFS(t, seed, faultfs.Options{})
		r := newRig(t, f, storage.SyncAlways, tc.segmentSize)
		requireOK(t, "open", r.open())
		requireOK(t, "hard state", r.setHardState(raft.HardState{Term: 1, VotedFor: 2}))
		requireOK(t, "append", r.append(logEntries(1, 1, 3)))

		f.Inject(tc.rule)
		ctx := fmt.Sprintf("%s, mode %s", tc.name, mode.name)
		if err := tc.call(r); err == nil {
			t.Fatalf("%s: the injected fault did not fail the call", ctx)
		}

		if mode.name == "process" {
			f.ProcessCrash()
		} else {
			f.Crash(mode.opts)
		}
		f.ClearRules()
		before := r.repairs
		r.reopen(ctx)
		r.check(ctx)
		if tc.repaired && mode.name == "process" && r.repairs == before {
			t.Fatalf("%s: a process crash kept the partial write, so Open should have repaired it", ctx)
		}
	}
}

// TestFsyncFailureIsNotAcked: an fsync error on Append, SetHardState and
// CreateSnapshot (and on the directory fsync and rename around a snapshot)
// fails the call; the state after a crash satisfies the checker with the failed
// write counted as in flight. Whether later writes are refused is T3.9.
func TestFsyncFailureIsNotAcked(t *testing.T) {
	quietLogs(t)
	appendCall := func(r *rig) error { return r.append(logEntries(1, 4, 2)) }
	cases := []faultCase{
		{name: "Append, WAL fsync", rule: faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync}, call: appendCall},
		{name: "SetHardState, WAL fsync", rule: faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync},
			call: func(r *rig) error { return r.setHardState(raft.HardState{Term: 2, VotedFor: 3}) }},
		{name: "CreateSnapshot, snapshot fsync", rule: faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ErrSync},
			call: func(r *rig) error { return r.createSnapshot(2) }},
		{name: "CreateSnapshot, WAL fsync of the snapshot record", rule: faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync},
			call: func(r *rig) error { return r.createSnapshot(2) }},
		{name: "CreateSnapshot, rename fails", rule: faultfs.Rule{Op: faultfs.OpRename, Nth: 1, Err: faultfs.ErrRename},
			call: func(r *rig) error { return r.createSnapshot(2) }},
		{name: "CreateSnapshot, directory fsync fails", rule: faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "snap", Nth: 1, Err: faultfs.ErrSyncDir},
			call: func(r *rig) error { return r.createSnapshot(2) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runFault(t, tc) })
	}
}

// TestShortWriteAndENOSPC: one failing write, then an immediate crash with no
// further writes. Reopen is clean or repaired and the call was not acknowledged.
func TestShortWriteAndENOSPC(t *testing.T) {
	quietLogs(t)
	appendCall := func(r *rig) error { return r.append(logEntries(1, 4, 1)) }
	hsCall := func(r *rig) error { return r.setHardState(raft.HardState{Term: 2, VotedFor: 3}) }
	snapCall := func(r *rig) error { return r.createSnapshot(2) }

	var cases []faultCase
	for _, k := range []int{0, 1, 4, 9, 30, 60} {
		cases = append(cases, faultCase{
			name: fmt.Sprintf("Append, short write keeping %d bytes", k),
			rule: faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ShortWrite(k)},
			call: appendCall, repaired: k > 0,
		})
	}
	cases = append(cases,
		faultCase{name: "SetHardState, short write keeping 5 bytes", repaired: true,
			rule: faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ShortWrite(5)}, call: hsCall},
		faultCase{name: "CreateSnapshot, short write of the snapshot file",
			rule: faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ShortWrite(11)}, call: snapCall},
		faultCase{name: "Append, ENOSPC on the write",
			rule: faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace}, call: appendCall},
		faultCase{name: "Append, ENOSPC creating the rolled segment", segmentSize: 1,
			rule: faultfs.Rule{Op: faultfs.OpOpenFile, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace}, call: appendCall},
		faultCase{name: "CreateSnapshot, ENOSPC creating the temporary file",
			rule: faultfs.Rule{Op: faultfs.OpOpenFile, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ErrNoSpace}, call: snapCall},
		faultCase{name: "CreateSnapshot, ENOSPC on the temporary file write",
			rule: faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.snap.tmp", Nth: 1, Err: faultfs.ErrNoSpace}, call: snapCall},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runFault(t, tc) })
	}

	// ENOSPC on the truncate inside a tail repair makes Open fail; after the
	// crash the next Open repairs the tail.
	t.Run("repair, ENOSPC on Truncate", func(t *testing.T) {
		for name, tail := range badTails(t) {
			sc := repairScenario(tail, nil)
			for _, mode := range standardModes() {
				r := newRig(t, newFS(t, mode.seed, faultfs.Options{}), sc.policy, sc.segmentSize)
				sc.prepare(r)
				r.fs.Inject(faultfs.Rule{Op: faultfs.OpTruncate, Nth: 1, Err: faultfs.ErrNoSpace})
				ctx := fmt.Sprintf("%s, mode %s", name, mode.name)
				if err := r.open(); err == nil {
					t.Fatalf("%s: Open succeeded although the repair could not truncate", ctx)
				}
				r.fs.Crash(mode.opts)
				r.fs.ClearRules()
				r.reopen(ctx)
				r.check(ctx)
				if r.repairs != 1 {
					t.Fatalf("%s: the reopen made %d repairs, want 1", ctx, r.repairs)
				}
			}
		}
	})
}

// syncNeverWork appends 20 entries one by one with a hard state every fourth
// and a snapshot at 10. One segment holds everything (no roll, no segment
// removal); an unsynced roll is T3.12.
func syncNeverWork(r *rig) error {
	if err := r.open(); err != nil {
		return err
	}
	for i := raft.Index(1); i <= 20; i++ {
		if err := r.append(logEntries(1, i, 1)); err != nil {
			return err
		}
		if i%4 == 0 {
			if err := r.setHardState(raft.HardState{Term: raft.Term(i / 4), VotedFor: raft.NodeID(i%3 + 1)}); err != nil {
				return err
			}
		}
		if i == 10 {
			if err := r.createSnapshot(10); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestSyncNeverContract: after a power loss any suffix of entries and of
// unsynced hard-state updates may be lost, but there is no hole, no reorder,
// Open succeeds and snapshots (always fsynced) are never lost. A process crash
// loses nothing.
func TestSyncNeverContract(t *testing.T) {
	quietLogs(t)

	t.Run("a crash at every operation", func(t *testing.T) {
		crashEverywhere(t, scenario{policy: storage.SyncNever, work: syncNeverWork}, standardModes())
	})

	t.Run("power loss at the end really does lose the unsynced suffix", func(t *testing.T) {
		r := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncNever, 0)
		requireOK(t, "workload", syncNeverWork(r))
		r.fs.Crash(faultfs.CrashOptions{})
		r.reopen("drop")
		r.check("drop")
		if r.snap.Meta.Index != 10 {
			t.Fatalf("snapshot %d survived, want 10: snapshots are fsynced", r.snap.Meta.Index)
		}
		if got := r.s.LastIndex(); got >= 20 {
			t.Fatalf("LastIndex = %d after losing every unsynced byte; the test would pass under SyncAlways too", got)
		}
	})

	t.Run("a process crash loses nothing", func(t *testing.T) {
		r := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncNever, 0)
		requireOK(t, "workload", syncNeverWork(r))
		r.fs.ProcessCrash()
		r.reopen("process crash")
		// Strictly the SyncAlways check: every acknowledged write must be there.
		checkRecovered(t, "process crash", r.fs, r.s, r.snap, r.m, storage.SyncAlways)
		if r.s.LastIndex() != 20 || r.snap.Meta.Index != 10 {
			t.Fatalf("last index %d, snapshot %d; want 20 and 10", r.s.LastIndex(), r.snap.Meta.Index)
		}
		if hs, _ := r.s.InitialState(); hs != (raft.HardState{Term: 5, VotedFor: 3}) {
			t.Fatalf("hard state = %+v, want the last one set {5 3}", hs)
		}
	})
}

// randomWork runs a few random writes: extending appends, conflicting appends
// above the snapshot, hard states and snapshots. A conflicting append starts a
// new term, as a new leader would.
func randomWork(r *rig, rng *rand.Rand, term *raft.Term, hsTerm *raft.Term, steps int) error {
	for range steps {
		snapIdx := r.m.snapAcked
		last := r.lastIndex()
		switch p := rng.Intn(100); {
		case p < 50 || last == snapIdx:
			n := 1 + rng.Intn(3)
			if err := r.append(logEntries(*term, last+1, n)); err != nil {
				return err
			}
		case p < 70:
			*term++
			from := snapIdx + 1 + raft.Index(rng.Uint64()%uint64(last-snapIdx))
			if err := r.append(logEntries(*term, from, 1+rng.Intn(3))); err != nil {
				return err
			}
		case p < 85:
			*hsTerm++
			if err := r.setHardState(raft.HardState{Term: *hsTerm, VotedFor: raft.NodeID(1 + rng.Uint64()%3)}); err != nil {
				return err
			}
		default:
			idx := snapIdx + 1 + raft.Index(rng.Uint64()%uint64(last-snapIdx))
			if err := r.createSnapshot(idx); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestRandomWorkloadWithCrashes: for each seed, two generations of random
// writes under SyncAlways, each ending in a power loss at a random operation.
// Handles and locks from generation 1 are stale and released by the crash. No
// injected sync failures and no ApplySnapshot.
func TestRandomWorkloadWithCrashes(t *testing.T) {
	quietLogs(t)
	seeds := 200
	if testing.Short() {
		seeds = 20
	}
	tails := []faultfs.TailMode{faultfs.DropTail, faultfs.TornTail, faultfs.GarbageTail, faultfs.ZeroTail}
	var generations, cutShort, withSnapshot, withConflict int

	for seed := int64(1); seed <= int64(seeds); seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic simulation, not security
		f := newFS(t, seed, faultfs.Options{})
		r := newRig(t, f, storage.SyncAlways, 120)
		term, hsTerm := raft.Term(1), raft.Term(0)

		for gen := 1; gen <= 2; gen++ {
			point, after := 1+rng.Intn(150), rng.Intn(2) == 0
			ctx := fmt.Sprintf("seed %d generation %d (crash point %d, after=%v)", seed, gen, point, after)
			arm(f, point, after)

			var err error
			if gen == 1 {
				if err = r.open(); err == nil {
					err = randomWork(r, rng, &term, &hsTerm, 14)
				}
			} else {
				err = randomWork(r, rng, &term, &hsTerm, 14)
			}
			if err != nil && !f.Crashed() {
				t.Fatalf("%s: the workload failed with no crash point: %v", ctx, err)
			}
			generations++
			if f.Crashed() {
				cutShort++
			}
			if r.m.snapAcked > 0 {
				withSnapshot++
			}
			for _, batch := range r.m.appends {
				if len(batch) > 0 && batch[0].Term > 1 && batch[0].Index <= r.lastIndex() {
					withConflict++
					break
				}
			}

			opts := faultfs.CrashOptions{Tail: tails[rng.Intn(len(tails))]}
			f.Crash(opts)
			r.reopen(ctx)
			r.check(ctx)
			r.rebase()
		}
	}
	t.Logf("%d generations: %d cut short by the crash point, %d with an acknowledged snapshot, %d with a later-term append",
		generations, cutShort, withSnapshot, withConflict)
	if cutShort < generations/4 || withSnapshot == 0 || withConflict == 0 {
		t.Fatalf("the random workload barely exercises crashes, snapshots or conflicting appends")
	}
}
