package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
)

// Tests for the disk-backed raft.Storage.
//
// This is where Phase 2's exit criterion is checked: kill a node mid-operation,
// restart it, and it comes back with correct state. A crash is modelled by
// abandoning a DiskStorage without closing it and opening the same directory
// again — no flush, no cleanup, exactly what a process that stopped existing
// leaves behind.
//
// Several tests run DiskStorage and MemoryStorage side by side. MemoryStorage
// is the reference implementation of the raft.Storage contract, so any place
// the two disagree is a place the consensus core would behave differently on
// disk than it does in the tests from Phase 1.

// openDisk opens a DiskStorage, failing the test on error.
func openDisk(t *testing.T, dir string) (*DiskStorage, Snapshot) {
	t.Helper()
	s, snap, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening disk storage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, snap
}

// putCommand encodes a state machine Put, so the entries under test carry
// realistic payloads rather than arbitrary bytes.
func putCommand(key, value string) []byte {
	return statemachine.Command{Op: statemachine.OpPut, Key: key, Value: []byte(value)}.Encode()
}

// logEntries builds a contiguous run of command entries.
func logEntries(term raft.Term, from raft.Index, count int) []raft.Entry {
	out := make([]raft.Entry, count)
	for i := range out {
		idx := from + raft.Index(i)
		out[i] = raft.Entry{
			Term:  term,
			Index: idx,
			Type:  raft.EntryNormal,
			Data:  putCommand(fmt.Sprintf("key-%d", idx), fmt.Sprintf("value-%d", idx)),
		}
	}
	return out
}

func TestDiskStorageStartsEmpty(t *testing.T) {
	s, snap := openDisk(t, t.TempDir())

	if snap.Meta.Index != 0 {
		t.Fatalf("a fresh node loaded snapshot %+v, want none", snap.Meta)
	}
	if got := s.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1", got)
	}
	if got := s.LastIndex(); got != 0 {
		t.Fatalf("LastIndex = %d, want 0", got)
	}

	hs, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if !hs.IsEmpty() {
		t.Fatalf("InitialState = %+v, want empty", hs)
	}
}

func TestDiskStorageMatchesMemoryStorage(t *testing.T) {
	// The two implementations must be indistinguishable to the Raft core.
	// Phase 1's whole test suite runs against MemoryStorage, so any divergence
	// here is behaviour that was never actually tested.
	dir := t.TempDir()
	disk, _ := openDisk(t, dir)
	mem := raft.NewMemoryStorage()

	entries := logEntries(1, 1, 10)
	if err := disk.Append(entries); err != nil {
		t.Fatalf("disk Append: %v", err)
	}
	if err := mem.Append(entries); err != nil {
		t.Fatalf("memory Append: %v", err)
	}

	if disk.FirstIndex() != mem.FirstIndex() {
		t.Fatalf("FirstIndex: disk %d, memory %d", disk.FirstIndex(), mem.FirstIndex())
	}
	if disk.LastIndex() != mem.LastIndex() {
		t.Fatalf("LastIndex: disk %d, memory %d", disk.LastIndex(), mem.LastIndex())
	}

	// Probe every index, including the sentinels either side of the log.
	for i := raft.Index(0); i <= 12; i++ {
		dt, derr := disk.Term(i)
		mt, merr := mem.Term(i)
		if dt != mt || !errors.Is(derr, merr) && !(derr == nil && merr == nil) {
			t.Fatalf("Term(%d): disk (%d, %v), memory (%d, %v)", i, dt, derr, mt, merr)
		}
	}

	// And every range, valid or not.
	for lo := raft.Index(0); lo <= 12; lo++ {
		for hi := raft.Index(0); hi <= 12; hi++ {
			de, derr := disk.Entries(lo, hi)
			me, merr := mem.Entries(lo, hi)

			if (derr == nil) != (merr == nil) {
				t.Fatalf("Entries(%d, %d): disk error %v, memory error %v", lo, hi, derr, merr)
			}
			if derr != nil {
				if !errors.Is(derr, merr) {
					t.Fatalf("Entries(%d, %d): disk %v, memory %v", lo, hi, derr, merr)
				}
				continue
			}
			if len(de) != len(me) {
				t.Fatalf("Entries(%d, %d): disk %d entries, memory %d", lo, hi, len(de), len(me))
			}
			for k := range de {
				if de[k].Index != me[k].Index || de[k].Term != me[k].Term {
					t.Fatalf("Entries(%d, %d)[%d]: disk %+v, memory %+v", lo, hi, k, de[k], me[k])
				}
			}
		}
	}
}

func TestDiskStorageSurvivesCrash(t *testing.T) {
	// The core promise. Everything written is still there after the process
	// vanishes without closing anything.
	dir := t.TempDir()

	first, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := first.SetHardState(raft.HardState{Term: 5, VotedFor: 3}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	want := logEntries(5, 1, 20)
	if err := first.Append(want); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Deliberately not closed.

	second, _ := openDisk(t, dir)

	hs, err := second.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs != (raft.HardState{Term: 5, VotedFor: 3}) {
		t.Fatalf("recovered hard state %+v, want {5 3}", hs)
	}
	if got := second.LastIndex(); got != 20 {
		t.Fatalf("recovered LastIndex = %d, want 20", got)
	}

	got, err := second.Entries(1, 21)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term ||
			!bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestConflictingAppendSurvivesCrash(t *testing.T) {
	// A follower resolving a log conflict overwrites a suffix. On disk that is
	// only more appends, so recovery has to reconstruct the intended result
	// rather than the raw sequence of writes.
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := s.Append(logEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// The leader's version of index 3 onward, in a later term.
	replacement := logEntries(2, 3, 2)
	if err := s.Append(replacement); err != nil {
		t.Fatalf("conflicting Append: %v", err)
	}

	recovered, _ := openDisk(t, dir)

	if got := recovered.LastIndex(); got != 4 {
		t.Fatalf("recovered LastIndex = %d, want 4; the superseded suffix was not dropped", got)
	}
	for _, idx := range []raft.Index{3, 4} {
		term, err := recovered.Term(idx)
		if err != nil {
			t.Fatalf("Term(%d): %v", idx, err)
		}
		if term != 2 {
			t.Fatalf("index %d has term %d, want 2 (the replacement)", idx, term)
		}
	}
}

func TestCompactionRemovesEntriesAndKeepsBoundary(t *testing.T) {
	// After compaction the entries below the snapshot are gone, but the
	// boundary itself must still answer: a leader replicating the first entry
	// after a snapshot asks for the term at exactly that index.
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	if err := s.Append(logEntries(3, 1, 30)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.CreateSnapshot(20, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	if got := s.FirstIndex(); got != 21 {
		t.Fatalf("FirstIndex = %d, want 21", got)
	}
	if got := s.LastIndex(); got != 30 {
		t.Fatalf("LastIndex = %d, want 30", got)
	}

	// The boundary answers.
	term, err := s.Term(20)
	if err != nil {
		t.Fatalf("Term at the compaction boundary: %v; a follower could never be caught up", err)
	}
	if term != 3 {
		t.Fatalf("Term(20) = %d, want 3", term)
	}

	// Below it does not.
	if _, err := s.Term(19); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Term(19) gave %v, want ErrCompacted", err)
	}
	if _, err := s.Entries(19, 25); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Entries(19, 25) gave %v, want ErrCompacted", err)
	}

	// And the surviving range is intact.
	got, err := s.Entries(21, 31)
	if err != nil {
		t.Fatalf("Entries(21, 31): %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("got %d entries after compaction, want 10", len(got))
	}
}

func TestCompactionSurvivesCrash(t *testing.T) {
	// The full recovery path: restore the snapshot, then replay only the
	// entries after it. This is the Phase 2 exit criterion in one test.
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}

	// Build state through a real state machine so the snapshot is meaningful.
	kv := statemachine.New()
	entries := logEntries(2, 1, 30)
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for _, e := range entries {
		if err := kv.Apply(e); err != nil {
			t.Fatalf("applying %d: %v", e.Index, err)
		}
	}

	// Snapshot at 20, leaving 10 entries in the log.
	appliedThrough := raft.Index(20)
	partial := statemachine.New()
	for _, e := range entries[:appliedThrough] {
		if err := partial.Apply(e); err != nil {
			t.Fatalf("applying %d: %v", e.Index, err)
		}
	}
	data, err := partial.Snapshot()
	if err != nil {
		t.Fatalf("snapshotting: %v", err)
	}
	if err := s.CreateSnapshot(appliedThrough, data); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	// Crash here.

	recovered, snap := openDisk(t, dir)

	if snap.Meta.Index != appliedThrough {
		t.Fatalf("recovered snapshot at index %d, want %d", snap.Meta.Index, appliedThrough)
	}
	if got := recovered.LastIndex(); got != 30 {
		t.Fatalf("recovered LastIndex = %d, want 30", got)
	}

	// Rebuild: snapshot first, then the tail of the log on top.
	rebuilt := statemachine.New()
	if err := rebuilt.Restore(snap.Data); err != nil {
		t.Fatalf("restoring snapshot: %v", err)
	}
	tail, err := recovered.Entries(recovered.FirstIndex(), recovered.LastIndex()+1)
	if err != nil {
		t.Fatalf("reading the tail: %v", err)
	}
	for _, e := range tail {
		if err := rebuilt.Apply(e); err != nil {
			t.Fatalf("replaying %d: %v", e.Index, err)
		}
	}

	if got := rebuilt.Applied(); got != 30 {
		t.Fatalf("rebuilt state machine applied through %d, want 30", got)
	}

	original, _ := kv.Snapshot()
	recreated, _ := rebuilt.Snapshot()
	if !bytes.Equal(original, recreated) {
		t.Fatalf("the rebuilt state machine differs from the original\n"+
			"original keys: %v\nrebuilt keys:  %v", kv.Keys(), rebuilt.Keys())
	}
}

func TestRepeatedCompaction(t *testing.T) {
	// Compaction happens over and over in a long-running node, so each round
	// has to leave the storage in a state the next one can work from.
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	for round := range 5 {
		from := raft.Index(round*20 + 1)
		if err := s.Append(logEntries(1, from, 20)); err != nil {
			t.Fatalf("round %d Append: %v", round, err)
		}

		at := from + 15
		if err := s.CreateSnapshot(at, []byte(fmt.Sprintf("state-%d", round))); err != nil {
			t.Fatalf("round %d CreateSnapshot: %v", round, err)
		}

		if got := s.FirstIndex(); got != at+1 {
			t.Fatalf("round %d FirstIndex = %d, want %d", round, got, at+1)
		}
		if got := s.SnapshotMeta().Index; got != at {
			t.Fatalf("round %d snapshot index = %d, want %d", round, got, at)
		}
	}

	// It must still recover after all that.
	recovered, snap := openDisk(t, dir)
	if snap.Meta.Index != 96 {
		t.Fatalf("recovered snapshot index %d, want 96", snap.Meta.Index)
	}
	if got := recovered.LastIndex(); got != 100 {
		t.Fatalf("recovered LastIndex = %d, want 100", got)
	}
}

func TestCompactionRejectsInvalidIndexes(t *testing.T) {
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	if err := s.Append(logEntries(1, 1, 10)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := s.CreateSnapshot(20, nil); err == nil {
		t.Fatal("snapshotting past the end of the log succeeded")
	}
	if err := s.CreateSnapshot(5, []byte("first")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := s.CreateSnapshot(5, []byte("again")); err == nil {
		t.Fatal("snapshotting at the existing snapshot index succeeded")
	}
	if err := s.CreateSnapshot(3, []byte("backwards")); err == nil {
		t.Fatal("snapshotting before the existing snapshot index succeeded")
	}
}

func TestAppendBelowSnapshotIsIgnored(t *testing.T) {
	// A stale retransmission can carry entries the snapshot already covers.
	// Re-adding them would put the cache behind the snapshot it follows.
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	if err := s.Append(logEntries(1, 1, 20)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.CreateSnapshot(15, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Entirely below the snapshot: nothing to do.
	if err := s.Append(logEntries(1, 5, 5)); err != nil {
		t.Fatalf("appending fully superseded entries: %v", err)
	}
	if got := s.FirstIndex(); got != 16 {
		t.Fatalf("FirstIndex = %d after a superseded append, want 16", got)
	}
	if got := s.LastIndex(); got != 20 {
		t.Fatalf("LastIndex = %d after a superseded append, want 20", got)
	}

	// Straddling the boundary: only the part above it applies.
	straddle := logEntries(9, 10, 12) // indexes 10..21
	if err := s.Append(straddle); err != nil {
		t.Fatalf("appending straddling entries: %v", err)
	}
	if got := s.LastIndex(); got != 21 {
		t.Fatalf("LastIndex = %d, want 21", got)
	}
	term, err := s.Term(16)
	if err != nil {
		t.Fatalf("Term(16): %v", err)
	}
	if term != 9 {
		t.Fatalf("Term(16) = %d, want 9; the part above the boundary was not applied", term)
	}
}

func TestMissingSnapshotIsRefused(t *testing.T) {
	// If the log records a snapshot the disk cannot produce, the entries it
	// covered are gone too. Starting from an empty state machine would
	// silently lose committed data, so recovery has to refuse.
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := s.Append(logEntries(1, 1, 20)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.CreateSnapshot(15, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	// Delete every snapshot, leaving the log's record of them behind.
	snaps, err := filepath.Glob(filepath.Join(dir, snapshotSubdir, "*"+snapshotSuffix))
	if err != nil {
		t.Fatalf("listing snapshots: %v", err)
	}
	if len(snaps) == 0 {
		t.Fatal("no snapshots were written")
	}
	for _, path := range snaps {
		if err := os.Remove(path); err != nil {
			t.Fatalf("removing snapshot: %v", err)
		}
	}

	if _, _, err := OpenDiskStorage(DiskConfig{Dir: dir}); err == nil {
		t.Fatal("recovery succeeded with the snapshot missing, want an error")
	}
}

func TestTornTailIsRepairedThroughDiskStorage(t *testing.T) {
	// The WAL repairs a torn record; this checks the repair is visible as a
	// consistent log through the storage layer rather than only inside the WAL.
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := s.Append(logEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append(logEntries(1, 6, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	truncateTail(t, filepath.Join(dir, walSubdir), 5)

	recovered, _ := openDisk(t, dir)

	if got := recovered.LastIndex(); got != 5 {
		t.Fatalf("recovered LastIndex = %d, want 5 (the torn entry dropped)", got)
	}
	// Whatever survived must be a contiguous, readable log.
	got, err := recovered.Entries(1, 6)
	if err != nil {
		t.Fatalf("Entries after repair: %v", err)
	}
	for i, e := range got {
		if e.Index != raft.Index(i+1) {
			t.Fatalf("entry %d has index %d; the recovered log is not contiguous", i, e.Index)
		}
	}
}

func TestReturnedEntriesAreCopies(t *testing.T) {
	// A caller must not be able to reach into the cache and mutate the log.
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	if err := s.Append(logEntries(1, 1, 5)); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.Entries(1, 6)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	got[0].Term = 999
	got[0].Index = 999

	again, err := s.Entries(1, 6)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if again[0].Term == 999 || again[0].Index == 999 {
		t.Fatal("mutating a returned slice changed the stored log")
	}
}

func TestDiskOperationsAfterCloseAreRejected(t *testing.T) {
	dir := t.TempDir()
	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	if err := s.Append(logEntries(1, 1, 1)); err == nil {
		t.Error("Append succeeded after Close")
	}
	if err := s.SetHardState(raft.HardState{Term: 1}); err == nil {
		t.Error("SetHardState succeeded after Close")
	}
	if err := s.CreateSnapshot(1, nil); err == nil {
		t.Error("CreateSnapshot succeeded after Close")
	}
	if err := s.Close(); err != nil {
		t.Errorf("a second Close returned %v, want nil", err)
	}
}

func TestHardStateSurvivesCompaction(t *testing.T) {
	// Compaction deletes WAL segments, and the vote lives in one of them.
	// Checked here as well as in the WAL tests because this is the path a
	// real node actually takes.
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir, SegmentSize: 256})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}

	want := raft.HardState{Term: 11, VotedFor: 2}
	if err := s.SetHardState(want); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	for _, e := range logEntries(11, 1, 100) {
		if err := s.Append([]raft.Entry{e}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := s.CreateSnapshot(90, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	recovered, _, err := OpenDiskStorage(DiskConfig{Dir: dir, SegmentSize: 256})
	if err != nil {
		t.Fatalf("recovering: %v", err)
	}
	defer recovered.Close()

	hs, err := recovered.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if hs != want {
		t.Fatalf("hard state after compaction = %+v, want %+v; the vote was lost", hs, want)
	}
}

func TestDataDirectoryLayout(t *testing.T) {
	// A node's durable state must be one directory, so it can be copied,
	// archived, or wiped as a unit.
	dir := t.TempDir()
	s, _ := openDisk(t, dir)

	if err := s.Append(logEntries(1, 1, 3)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.CreateSnapshot(2, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	for _, sub := range []string{walSubdir, snapshotSubdir} {
		info, err := os.Stat(filepath.Join(dir, sub))
		if err != nil {
			t.Fatalf("expected subdirectory %q: %v", sub, err)
		}
		if !info.IsDir() {
			t.Fatalf("%q is not a directory", sub)
		}
	}
}
