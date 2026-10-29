package storage_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

func segmentNames(t *testing.T, f *faultfs.FS) []string {
	t.Helper()
	des, err := f.ReadDir(walDir)
	if err != nil {
		t.Fatalf("listing %s: %v", walDir, err)
	}
	var names []string
	for _, d := range des {
		if strings.HasSuffix(d.Name(), ".wal") {
			names = append(names, d.Name())
		}
	}
	return names
}

func requireOK(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// TestRenameBeforeDataIsNotDurable: a snapshot renamed into place before its
// data was synced exists after power loss with no or torn content; the loader
// rejects it and the store falls back to the older snapshot. A file synced and
// renamed but whose directory was never synced leaves the old name behind.
func TestRenameBeforeDataIsNotDurable(t *testing.T) {
	quietLogs(t)
	// Learn the bytes and file name of a valid snapshot at index 6.
	scratch := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncAlways, 0)
	requireOK(t, "open", scratch.open())
	requireOK(t, "append", scratch.append(logEntries(1, 1, 6)))
	requireOK(t, "snapshot", scratch.createSnapshot(6))
	des, err := scratch.fs.ReadDir(snapDir)
	requireOK(t, "list snapshots", err)
	if len(des) != 1 {
		t.Fatalf("scratch store has %d snapshot files, want 1", len(des))
	}
	name := des[0].Name()
	valid, err := scratch.fs.ReadFile(snapDir + "/" + name)
	requireOK(t, "read snapshot", err)
	final, temp := snapDir+"/"+name, snapDir+"/"+name+".tmp"

	// prelude: entries 1..6 and a good snapshot at 3, all durable.
	prelude := func(seed int64) *rig {
		r := newRig(t, newFS(t, seed, faultfs.Options{}), storage.SyncAlways, 0)
		requireOK(t, "open", r.open())
		requireOK(t, "append", r.append(logEntries(1, 1, 6)))
		requireOK(t, "snapshot 3", r.createSnapshot(3))
		requireOK(t, "close", r.s.Close())
		r.fs.SyncAll()
		return r
	}

	t.Run("rename without a file sync, then SyncDir", func(t *testing.T) {
		cases := []struct {
			name string
			seed int64
			opts faultfs.CrashOptions
		}{
			{"drop", 1, faultfs.CrashOptions{}},
			{"zero", 1, faultfs.CrashOptions{Tail: faultfs.ZeroTail}},
		}
		for _, keep := range []int{0, 1, 7, len(valid) / 2, len(valid) - 1} {
			cases = append(cases, struct {
				name string
				seed int64
				opts faultfs.CrashOptions
			}{fmt.Sprintf("torn/keep%d", keep), 1, faultfs.CrashOptions{TornTailAt: map[string]int{final: keep}}})
		}
		for seed := int64(1); seed <= 5; seed++ {
			cases = append(cases, struct {
				name string
				seed int64
				opts faultfs.CrashOptions
			}{fmt.Sprintf("garbage/seed%d", seed), seed, faultfs.CrashOptions{Tail: faultfs.GarbageTail}})
		}

		for _, tc := range cases {
			r := prelude(tc.seed)
			h, err := r.fs.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			requireOK(t, tc.name+": create temp", err)
			_, err = h.Write(valid)
			requireOK(t, tc.name+": write", err)
			requireOK(t, tc.name+": close", h.Close()) // no Sync
			requireOK(t, tc.name+": rename", r.fs.Rename(temp, final))
			requireOK(t, tc.name+": SyncDir", r.fs.SyncDir(snapDir))
			r.fs.Crash(tc.opts)

			got, err := r.fs.ReadFile(final)
			if err != nil {
				t.Fatalf("%s: the final name must exist after the directory sync: %v", tc.name, err)
			}
			if bytes.Equal(got, valid) {
				t.Fatalf("%s: unsynced snapshot data came back whole; the case tests nothing", tc.name)
			}
			if len(got) > len(valid) {
				t.Fatalf("%s: %d bytes came back from a %d-byte write", tc.name, len(got), len(valid))
			}

			r.reopen(tc.name)
			if r.snap.Meta.Index != 3 {
				t.Fatalf("%s: loaded snapshot %d, want the older snapshot 3 (the loader must reject %d bytes of %d)",
					tc.name, r.snap.Meta.Index, len(got), len(valid))
			}
			r.check(tc.name)
		}
	})

	t.Run("sync then rename without SyncDir leaves the old name", func(t *testing.T) {
		r := prelude(1)
		requireOK(t, "create temp's entry", func() error {
			h, err := r.fs.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			if _, err := h.Write(valid); err != nil {
				return err
			}
			if err := h.Sync(); err != nil {
				return err
			}
			if err := r.fs.SyncDir(snapDir); err != nil { // the temp name is durable
				return err
			}
			return h.Close()
		}())
		requireOK(t, "rename", r.fs.Rename(temp, final)) // no SyncDir
		r.fs.Crash(faultfs.CrashOptions{})

		if got, err := r.fs.ReadFile(temp); err != nil || !bytes.Equal(got, valid) {
			t.Fatalf("the old name must survive with its synced data: %d bytes, %v", len(got), err)
		}
		if _, err := r.fs.Stat(final); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("the new name must not exist, Stat = %v", err)
		}
		r.reopen("sync-then-rename")
		if r.snap.Meta.Index != 3 {
			t.Fatalf("loaded snapshot %d, want 3", r.snap.Meta.Index)
		}
		r.check("sync-then-rename")
	})
}

// TestCrashAtEveryOpDuringAppend (SyncAlways): batches of one and three
// entries with a segment size that forces exactly one roll.
func TestCrashAtEveryOpDuringAppend(t *testing.T) {
	crashEverywhere(t, scenario{
		policy:      storage.SyncAlways,
		segmentSize: 200,
		work: func(r *rig) error {
			if err := r.open(); err != nil {
				return err
			}
			if err := r.setHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
				return err
			}
			for _, batch := range [][]raft.Entry{logEntries(1, 1, 1), logEntries(1, 2, 3), logEntries(1, 5, 1)} {
				if err := r.append(batch); err != nil {
					return err
				}
			}
			return nil
		},
		inspect: func(t *testing.T, r *rig) {
			if got := segmentNames(t, r.fs); len(got) != 2 {
				t.Fatalf("workload made %d segments %v, want exactly one roll (2 segments)", len(got), got)
			}
		},
	}, standardModes())
}

// TestCrashAtEveryOpDuringSuffixTruncate: a conflicting Append replaces the
// suffix from index 6. Before it is acknowledged the log is the old 1..10 or
// 1..5 plus a prefix of the new entries, never the old 7..10 after the new 6.
func TestCrashAtEveryOpDuringSuffixTruncate(t *testing.T) {
	crashEverywhere(t, scenario{
		policy: storage.SyncAlways,
		work: func(r *rig) error {
			if err := r.open(); err != nil {
				return err
			}
			if err := r.append(logEntries(1, 1, 10)); err != nil {
				return err
			}
			return r.append(logEntries(2, 6, 3))
		},
	}, standardModes())
}

// TestCrashAtEveryOpDuringSnapshotWrite: CreateSnapshot under SyncAlways. A
// temporary file is never loaded, the old or the new snapshot loads, and
// nothing below the loaded snapshot is needed.
func TestCrashAtEveryOpDuringSnapshotWrite(t *testing.T) {
	crashEverywhere(t, scenario{
		policy: storage.SyncAlways,
		work: func(r *rig) error {
			if err := r.open(); err != nil {
				return err
			}
			if err := r.append(logEntries(1, 1, 8)); err != nil {
				return err
			}
			if err := r.createSnapshot(4); err != nil {
				return err
			}
			if err := r.append(logEntries(1, 9, 4)); err != nil {
				return err
			}
			return r.createSnapshot(10)
		},
	}, standardModes())
}

// TestCrashAtEveryOpDuringCompaction (SyncAlways only): TruncateBefore removes
// whole segments after a snapshot. The hard state never rolls back, every entry
// above the snapshot is present, the log is contiguous and the rebuilt state
// machine matches.
func TestCrashAtEveryOpDuringCompaction(t *testing.T) {
	crashEverywhere(t, scenario{
		policy:      storage.SyncAlways,
		segmentSize: 150,
		work: func(r *rig) error {
			if err := r.open(); err != nil {
				return err
			}
			if err := r.setHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
				return err
			}
			for i := raft.Index(1); i <= 12; i++ {
				if err := r.append(logEntries(1, i, 1)); err != nil {
					return err
				}
				// The newest hard state sits in a segment the snapshot removes, so
				// compaction must carry it forward.
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
		},
		inspect: func(t *testing.T, r *rig) {
			names := segmentNames(t, r.fs)
			if len(names) == 0 || strings.HasPrefix(names[0], "0000000000000001-") {
				t.Fatalf("compaction removed no segment, so this loop does not reach TruncateBefore's deletes: %v", names)
			}
		},
	}, standardModes())
}

// TestApplySnapshotCrashPoints (SyncAlways): an acknowledged snapshot is
// durable and loads; the WAL's snapshot record is never ahead of the snapshot
// file (Open would refuse with ErrCorruptWAL). Entries above the snapshot are
// not asserted here (T3.11).
func TestApplySnapshotCrashPoints(t *testing.T) {
	leader := logEntries(2, 1, 12)
	crashEverywhere(t, scenario{
		policy: storage.SyncAlways,
		work: func(r *rig) error {
			if err := r.open(); err != nil {
				return err
			}
			if err := r.append(logEntries(1, 1, 5)); err != nil {
				return err
			}
			return r.applySnapshot(leader, 12)
		},
	}, standardModes())
}

// badTails are the damaged endings a crash can leave on the last segment: a
// partial header, a record without its last bytes, a record with a flipped
// byte, garbage and zeros. record7 is a valid record for entry 7.
func badTails(t *testing.T) map[string][]byte {
	t.Helper()
	build := func(n int) []byte {
		r := newRig(t, newFS(t, 1, faultfs.Options{}), storage.SyncAlways, 0)
		requireOK(t, "open", r.open())
		requireOK(t, "append", r.append(logEntries(1, 1, n)))
		names := segmentNames(t, r.fs)
		data, err := r.fs.ReadFile(walDir + "/" + names[len(names)-1])
		requireOK(t, "read segment", err)
		return data
	}
	six, seven := build(6), build(7)
	record7 := seven[len(six):]
	flipped := append([]byte(nil), record7...)
	flipped[len(flipped)-1] ^= 0xff
	return map[string][]byte{
		"partial header": record7[:5],
		"short record":   record7[:len(record7)-3],
		"bad checksum":   flipped,
		"garbage":        bytes.Repeat([]byte{0xA5, 0x3C, 0x99}, 12),
		"zeros":          make([]byte, 40),
	}
}

// repairScenario builds a durable store holding entries 1..6 with tail glued
// to the end of its last segment, and opens it under crashes.
func repairScenario(tail []byte, then func(t *testing.T, r *rig, ctx string, mode crashMode)) scenario {
	return scenario{
		policy: storage.SyncAlways,
		prepare: func(r *rig) {
			t := r.t
			requireOK(t, "open", r.open())
			requireOK(t, "append", r.append(logEntries(1, 1, 6)))
			requireOK(t, "close", r.s.Close())
			names := segmentNames(t, r.fs)
			h, err := r.fs.OpenFile(walDir+"/"+names[len(names)-1], os.O_WRONLY|os.O_APPEND, 0o644)
			requireOK(t, "open segment", err)
			_, err = h.Write(tail)
			requireOK(t, "write tail", err)
			requireOK(t, "sync tail", h.Sync())
			requireOK(t, "close", h.Close())
			r.fs.SyncAll()
			r.s, r.repairs = nil, 0
		},
		work: func(r *rig) error { return r.open() },
		then: then,
	}
}

// TestRepairIsDurable: a segment with a bad tail; crash at and after each
// operation of the repair (Truncate, file Sync, SyncDir) under drop, torn,
// garbage and zero; reopen, crash again and reopen. The second open never
// errors, entries 1..6 stay intact, and once the repair has been reported
// durable the tail does not return and is not repaired a second time.
func TestRepairIsDurable(t *testing.T) {
	for name, tail := range badTails(t) {
		t.Run(name, func(t *testing.T) {
			crashEverywhere(t, repairScenario(tail, func(t *testing.T, r *rig, ctx string, mode crashMode) {
				reported := r.repairs
				r.fs.Crash(mode.opts)
				r.reopen(ctx + ", second crash")
				r.check(ctx + ", second crash")
				if r.repairs != reported {
					t.Fatalf("%s: the tail came back, or was repaired again, after a completed open", ctx)
				}
			}), standardModes())
		})
	}
}

// TestCrashDuringRepairThenSecondCrash: crash inside the repair, reopen with a
// crash point inside the repair that runs again, crash once more, reopen.
func TestCrashDuringRepairThenSecondCrash(t *testing.T) {
	quietLogs(t)
	modes := []crashMode{
		{"drop", 1, faultfs.CrashOptions{}},
		{"torn/seed1", 1, faultfs.CrashOptions{Tail: faultfs.TornTail}},
		{"garbage/seed1", 1, faultfs.CrashOptions{Tail: faultfs.GarbageTail}},
	}
	tails := badTails(t)
	for _, name := range []string{"short record", "garbage", "zeros"} {
		tail := tails[name]
		t.Run(name, func(t *testing.T) {
			sc := repairScenario(tail, nil)

			dry := newRig(t, newFS(t, 1, faultfs.Options{}), sc.policy, sc.segmentSize)
			sc.prepare(dry)
			base := dry.fs.Ops()
			requireOK(t, "dry run", sc.work(dry))
			n := dry.fs.Ops() - base
			if n <= 0 || dry.repairs != 1 {
				t.Fatalf("dry run: %d ops, %d repairs; want some ops and exactly one repair", n, dry.repairs)
			}

			ran := 0
			for first := 1; first <= n; first++ {
				for _, firstAfter := range []bool{false, true} {
					for second := 1; second <= n; second++ {
						for _, secondAfter := range []bool{false, true} {
							for _, mode := range modes {
								ran++
								r := newRig(t, newFS(t, mode.seed, faultfs.Options{}), sc.policy, sc.segmentSize)
								sc.prepare(r)
								ctx := fmt.Sprintf("first crash op %d (after=%v), second crash op %d (after=%v), mode %s",
									first, firstAfter, second, secondAfter, mode.name)

								arm(r.fs, first, firstAfter)
								_ = r.open()
								r.fs.Crash(mode.opts)

								arm(r.fs, second, secondAfter)
								_ = r.open() // repairs again; may be cut short
								r.fs.Crash(mode.opts)

								r.reopen(ctx)
								r.check(ctx)
								reported := r.repairs
								r.fs.Crash(mode.opts)
								r.reopen(ctx + ", last crash")
								r.check(ctx + ", last crash")
								if r.repairs != reported {
									t.Fatalf("%s: the tail came back after a completed open", ctx)
								}
							}
						}
					}
				}
			}
			if want := n * n * 4 * len(modes); ran != want {
				t.Fatalf("ran %d, want %d", ran, want)
			}
			t.Logf("%d operations in the repair, %d double-crash points checked", n, ran)
		})
	}
}

// arm sets a crash point n operations from now.
func arm(f *faultfs.FS, n int, after bool) {
	if after {
		f.CrashAfter(f.Ops() + n)
		return
	}
	f.CrashAt(f.Ops() + n)
}
