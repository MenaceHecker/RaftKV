package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the write-ahead log.
//
// The WAL exists to make one promise: anything it acknowledged is still there
// after the process dies. So most of these tests are shaped the same way —
// write something, simulate a crash, reopen, and check what survived. A crash
// is simulated by abandoning the WAL without closing it and, where a torn
// write is being modelled, by chopping bytes off the tail of the newest
// segment, which is what a process killed mid-write leaves behind.

// openWAL opens a WAL in a temporary directory, failing the test on error.
func openWAL(t *testing.T, dir string, opts Options) (*WAL, Replay) {
	t.Helper()
	opts.Dir = dir
	w, rep, err := Open(opts)
	if err != nil {
		t.Fatalf("opening WAL: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, rep
}

// entries builds a contiguous run of entries for terseness in tests.
func entries(term raft.Term, from raft.Index, count int) []raft.Entry {
	out := make([]raft.Entry, count)
	for i := range out {
		idx := from + raft.Index(i)
		out[i] = raft.Entry{
			Term:  term,
			Index: idx,
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf("cmd-%d", idx)),
		}
	}
	return out
}

// appendEach appends entries one call at a time.
//
// Rollover is only considered after a write completes, so a single batched
// append lands wholly in one segment however large it is. Tests that need
// several segments must therefore append separately — batching them would
// quietly produce a one-segment WAL and leave compaction untested.
func appendEach(t *testing.T, w *WAL, es []raft.Entry) {
	t.Helper()
	for _, e := range es {
		if err := w.AppendEntries([]raft.Entry{e}); err != nil {
			t.Fatalf("appending entry %d: %v", e.Index, err)
		}
	}
}

// segmentPaths returns the WAL's segment files, sorted by name.
func segmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		t.Fatalf("listing segments: %v", err)
	}
	sort.Strings(paths)
	return paths
}

// truncateTail chops n bytes off the end of the newest segment, modelling a
// process killed part-way through a write.
func truncateTail(t *testing.T, dir string, n int64) {
	t.Helper()
	paths := segmentPaths(t, dir)
	if len(paths) == 0 {
		t.Fatal("no segments to truncate")
	}
	last := paths[len(paths)-1]

	info, err := os.Stat(last)
	if err != nil {
		t.Fatalf("stat %s: %v", last, err)
	}
	if info.Size() < n {
		t.Fatalf("segment %s is only %d bytes, cannot chop %d", last, info.Size(), n)
	}
	if err := os.Truncate(last, info.Size()-n); err != nil {
		t.Fatalf("truncating %s: %v", last, err)
	}
}

// assertEntries checks a replayed log against what was expected.
func assertEntries(t *testing.T, got []raft.Entry, want []raft.Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("replayed %d entries, want %d\ngot:  %s\nwant: %s",
			len(got), len(want), formatEntries(got), formatEntries(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term ||
			!bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("entry %d = %+v, want %+v\ngot:  %s\nwant: %s",
				i, got[i], want[i], formatEntries(got), formatEntries(want))
		}
	}
}

func formatEntries(es []raft.Entry) string {
	var b []byte
	for _, e := range es {
		b = append(b, fmt.Sprintf("[%d/%d %q]", e.Index, e.Term, e.Data)...)
	}
	if len(b) == 0 {
		return "(empty)"
	}
	return string(b)
}

func TestOpenEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	w, rep := openWAL(t, dir, Options{})

	if len(rep.Entries) != 0 {
		t.Fatalf("fresh WAL replayed %d entries, want 0", len(rep.Entries))
	}
	if !rep.HardState.IsEmpty() {
		t.Fatalf("fresh WAL replayed hard state %+v, want empty", rep.HardState)
	}
	if rep.Repaired {
		t.Fatal("fresh WAL reported a repair")
	}
	if got := w.SegmentCount(); got != 1 {
		t.Fatalf("fresh WAL has %d segments, want 1", got)
	}
}

func TestOpenCreatesMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "wal")
	openWAL(t, dir, Options{})

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("WAL directory was not created: %v", err)
	}
}

func TestEntriesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	want := entries(1, 1, 5)

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(want); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{})
	assertEntries(t, rep.Entries, want)
}

func TestHardStateSurvivesReopen(t *testing.T) {
	// The durability that Election Safety rests on: a node that voted must
	// not forget across a restart.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	for _, hs := range []raft.HardState{
		{Term: 1, VotedFor: 0},
		{Term: 1, VotedFor: 3},
		{Term: 2, VotedFor: 0},
		{Term: 7, VotedFor: 5},
	} {
		if err := w.SaveHardState(hs); err != nil {
			t.Fatalf("saving hard state %+v: %v", hs, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{})
	want := raft.HardState{Term: 7, VotedFor: 5}
	if rep.HardState != want {
		t.Fatalf("replayed hard state %+v, want %+v (the last one written)", rep.HardState, want)
	}
}

func TestConflictingEntriesAreResolvedOnReplay(t *testing.T) {
	// A follower whose log diverges re-appends from an earlier index. Nothing
	// on disk is rewritten, so replay has to resolve the conflict by letting
	// the later record for an index win.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 5)); err != nil {
		t.Fatalf("appending original: %v", err)
	}

	// The leader's version of index 3 onward, in a later term.
	replacement := entries(2, 3, 2)
	if err := w.AppendEntries(replacement); err != nil {
		t.Fatalf("appending replacement: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{})

	want := append(entries(1, 1, 2), replacement...)
	assertEntries(t, rep.Entries, want)

	// The superseded entries must be gone, not merely reordered.
	if n := len(rep.Entries); n != 4 {
		t.Fatalf("replayed %d entries, want 4; the truncated suffix was not dropped", n)
	}
}

func TestTornTailIsRepaired(t *testing.T) {
	// The kill -9 case. A partial record at the tail must be discarded and
	// everything before it kept.
	dir := t.TempDir()
	survives := entries(1, 1, 3)

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(survives); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.AppendEntries(entries(1, 4, 1)); err != nil {
		t.Fatalf("appending doomed entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	truncateTail(t, dir, 5)

	_, rep := openWAL(t, dir, Options{})
	if !rep.Repaired {
		t.Fatal("a torn tail was not reported as repaired")
	}
	assertEntries(t, rep.Entries, survives)
}

func TestTornTailIsRepairedAtEveryCutPoint(t *testing.T) {
	// A crash can interrupt a write at any byte, so repair must work for
	// every possible tail length, not just a convenient one.
	base := entries(1, 1, 3)

	for cut := int64(1); cut <= 20; cut++ {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()

			w, _ := openWAL(t, dir, Options{})
			if err := w.AppendEntries(base); err != nil {
				t.Fatalf("appending: %v", err)
			}
			if err := w.AppendEntries(entries(1, 4, 1)); err != nil {
				t.Fatalf("appending doomed entry: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("closing: %v", err)
			}

			truncateTail(t, dir, cut)

			_, rep, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("replay after a %d-byte tear: %v", cut, err)
			}

			// The three original entries must always survive; whether the
			// fourth does depends on how much of it was cut away.
			if len(rep.Entries) < len(base) {
				t.Fatalf("a %d-byte tear lost committed entries: %s",
					cut, formatEntries(rep.Entries))
			}
			assertEntries(t, rep.Entries[:len(base)], base)
		})
	}
}

func TestRepairedWALAcceptsFurtherAppends(t *testing.T) {
	// Repair is not just about reading. The file has to be truncated on disk
	// too, or the next append lands after the garbage and every future replay
	// stops at it.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 3)); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.AppendEntries(entries(1, 4, 1)); err != nil {
		t.Fatalf("appending doomed entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	truncateTail(t, dir, 5)

	w2, rep := openWAL(t, dir, Options{})
	if !rep.Repaired {
		t.Fatal("expected a repair")
	}

	// Write past the repaired tail and confirm it replays cleanly.
	next := entries(2, raft.Index(len(rep.Entries))+1, 2)
	if err := w2.AppendEntries(next); err != nil {
		t.Fatalf("appending after repair: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep2 := openWAL(t, dir, Options{})
	if rep2.Repaired {
		t.Fatal("a second replay reported a repair; the file was not truncated on disk")
	}
	assertEntries(t, rep2.Entries, append(entries(1, 1, 3), next...))
}

func TestCorruptionInsideSegmentIsRejected(t *testing.T) {
	// Damage that is not at the tail cannot be explained by an interrupted
	// append. Silently truncating there would discard committed entries, so
	// it must surface instead.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 10)); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	// Flip a bit in the middle of the file, well away from the tail.
	path := segmentPaths(t, dir)[0]
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading segment: %v", err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(path, data, fileMode); err != nil {
		t.Fatalf("writing corrupted segment: %v", err)
	}

	_, _, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("opening a WAL corrupted mid-file gave %v, want ErrCorruptWAL", err)
	}
}

func TestCorruptionInOlderSegmentIsRejected(t *testing.T) {
	// The same rule across files: only the newest segment's tail may be
	// repaired, because only it could have been mid-write.
	dir := t.TempDir()

	// A tiny segment size forces several rollovers.
	w, _ := openWAL(t, dir, Options{SegmentSize: 256})
	appendEach(t, w, entries(1, 1, 20))
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	paths := segmentPaths(t, dir)
	if len(paths) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(paths))
	}

	// Chop the tail off the *first* segment. In the newest file this would be
	// a repairable tear; here it is unexplained damage.
	info, err := os.Stat(paths[0])
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(paths[0], info.Size()-3); err != nil {
		t.Fatalf("truncating: %v", err)
	}

	_, _, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("opening a WAL damaged in an older segment gave %v, want ErrCorruptWAL", err)
	}
}

func TestSegmentsRollOver(t *testing.T) {
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 512})
	want := entries(1, 1, 100)
	appendEach(t, w, want)
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	if got := len(segmentPaths(t, dir)); got < 5 {
		t.Fatalf("100 entries at a 512-byte segment size produced %d segments, want several", got)
	}

	// Rollover must be invisible to replay: the log is one sequence however
	// many files it happens to span.
	_, rep := openWAL(t, dir, Options{SegmentSize: 512})
	assertEntries(t, rep.Entries, want)
}

func TestTruncateBeforeDropsSupersededSegments(t *testing.T) {
	// Compaction after a snapshot: whole segments below the snapshot point
	// are deleted rather than rewritten.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})
	appendEach(t, w, entries(1, 1, 100))

	before := w.SegmentCount()
	if before < 3 {
		t.Fatalf("expected several segments, got %d", before)
	}

	if err := w.TruncateBefore(90); err != nil {
		t.Fatalf("truncating: %v", err)
	}

	if after := w.SegmentCount(); after >= before {
		t.Fatalf("segment count went from %d to %d, want fewer", before, after)
	}
	if got := len(segmentPaths(t, dir)); got != w.SegmentCount() {
		t.Fatalf("%d segment files on disk but %d tracked", got, w.SegmentCount())
	}
}

func TestTruncateBeforePreservesHardState(t *testing.T) {
	// Hard state records live in segments alongside entries, so deleting an
	// old segment could take the most recent vote with it. Losing it would
	// let a restarted node vote a second time in a term it had already voted
	// in, which breaks Election Safety.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})

	want := raft.HardState{Term: 9, VotedFor: 4}
	if err := w.SaveHardState(want); err != nil {
		t.Fatalf("saving hard state: %v", err)
	}

	// Push the hard state well into an older segment.
	appendEach(t, w, entries(1, 1, 100))
	if err := w.TruncateBefore(90); err != nil {
		t.Fatalf("truncating: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{SegmentSize: 256})
	if rep.HardState != want {
		t.Fatalf("hard state after compaction = %+v, want %+v; the vote was lost with a "+
			"deleted segment", rep.HardState, want)
	}
}

func TestTruncateBeforeKeepsNeededEntries(t *testing.T) {
	// Compaction must never remove an entry at or above the truncation point.
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})
	appendEach(t, w, entries(1, 1, 100))
	if err := w.TruncateBefore(50); err != nil {
		t.Fatalf("truncating: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{SegmentSize: 256})

	if len(rep.Entries) == 0 {
		t.Fatal("compaction removed the entire log")
	}
	last := rep.Entries[len(rep.Entries)-1]
	if last.Index != 100 {
		t.Fatalf("last surviving index = %d, want 100", last.Index)
	}
	for _, e := range rep.Entries {
		if e.Index > 50 && string(e.Data) != fmt.Sprintf("cmd-%d", e.Index) {
			t.Fatalf("entry %d was corrupted by compaction: %+v", e.Index, e)
		}
	}
	// Everything from the truncation point on must still be present.
	for want := raft.Index(51); want <= 100; want++ {
		found := false
		for _, e := range rep.Entries {
			if e.Index == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("entry %d was removed but is at or above the truncation point", want)
		}
	}
}

func TestSnapshotMetaSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	want := SnapshotMeta{Index: 42, Term: 7}

	w, _ := openWAL(t, dir, Options{})
	if err := w.SaveSnapshotMeta(want); err != nil {
		t.Fatalf("saving snapshot meta: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	_, rep := openWAL(t, dir, Options{})
	if rep.Snapshot != want {
		t.Fatalf("replayed snapshot meta %+v, want %+v", rep.Snapshot, want)
	}
}

func TestNonContiguousAppendIsRejected(t *testing.T) {
	// A gap in the log would be undetectable later, so it is refused at the
	// point where the caller can still do something about it.
	dir := t.TempDir()
	w, _ := openWAL(t, dir, Options{})

	gapped := []raft.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 3},
	}
	if err := w.AppendEntries(gapped); err == nil {
		t.Fatal("appending entries with a gap succeeded, want an error")
	}
}

func TestUnexpectedFileInDirectoryIsRejected(t *testing.T) {
	// A file that does not parse as a segment might be a half-created segment
	// or someone else's data. Guessing is worse than refusing.
	dir := t.TempDir()
	openWAL(t, dir, Options{})

	bad := filepath.Join(dir, "not-a-segment"+segmentSuffix)
	if err := os.WriteFile(bad, []byte("junk"), fileMode); err != nil {
		t.Fatalf("writing stray file: %v", err)
	}

	if _, _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("opening a WAL with an unrecognized file succeeded, want an error")
	}
}

func TestOperationsAfterCloseAreRejected(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	if err := w.AppendEntries(entries(1, 1, 1)); err == nil {
		t.Error("AppendEntries succeeded after Close")
	}
	if err := w.SaveHardState(raft.HardState{Term: 1}); err == nil {
		t.Error("SaveHardState succeeded after Close")
	}
	if err := w.Sync(); err == nil {
		t.Error("Sync succeeded after Close")
	}
	if err := w.Close(); err != nil {
		t.Errorf("a second Close returned %v, want nil", err)
	}
}

func TestSyncNeverStillSurvivesProcessCrash(t *testing.T) {
	// SyncNever leaves flushing to the operating system, so data is in the
	// page cache rather than on the platter. That is enough to survive the
	// process dying — which is what makes it a usable test setting — but not
	// a power loss, which this cannot simulate and the docs are explicit
	// about.
	dir := t.TempDir()

	w, _, err := Open(Options{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	want := entries(1, 1, 5)
	if err := w.AppendEntries(want); err != nil {
		t.Fatalf("appending: %v", err)
	}
	// Deliberately abandoned without Close, as a killed process would.

	_, rep := openWAL(t, dir, Options{Sync: SyncNever})
	assertEntries(t, rep.Entries, want)
}

func TestReopenAfterAbandonWithoutClose(t *testing.T) {
	// The realistic crash: no Close, no truncation, just a process that
	// stopped. Everything acknowledged must still be there.
	dir := t.TempDir()

	w, _, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if err := w.SaveHardState(raft.HardState{Term: 4, VotedFor: 2}); err != nil {
		t.Fatalf("saving hard state: %v", err)
	}
	want := entries(3, 1, 7)
	if err := w.AppendEntries(want); err != nil {
		t.Fatalf("appending: %v", err)
	}

	_, rep := openWAL(t, dir, Options{})
	assertEntries(t, rep.Entries, want)
	if rep.HardState != (raft.HardState{Term: 4, VotedFor: 2}) {
		t.Fatalf("hard state = %+v, want {4 2}", rep.HardState)
	}
	if rep.Repaired {
		t.Fatal("a cleanly abandoned WAL reported a repair")
	}
}
