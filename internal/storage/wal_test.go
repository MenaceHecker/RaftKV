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

func appendEach(t *testing.T, w *WAL, es []raft.Entry) {
	t.Helper()
	for _, e := range es {
		if err := w.AppendEntries([]raft.Entry{e}); err != nil {
			t.Fatalf("appending entry %d: %v", e.Index, err)
		}
	}
}

func segmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		t.Fatalf("listing segments: %v", err)
	}
	sort.Strings(paths)
	return paths
}

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
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 5)); err != nil {
		t.Fatalf("appending original: %v", err)
	}

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

	if n := len(rep.Entries); n != 4 {
		t.Fatalf("replayed %d entries, want 4; the truncated suffix was not dropped", n)
	}
}

func TestTornTailIsRepaired(t *testing.T) {
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

			if len(rep.Entries) < len(base) {
				t.Fatalf("a %d-byte tear lost committed entries: %s",
					cut, formatEntries(rep.Entries))
			}
			assertEntries(t, rep.Entries[:len(base)], base)
		})
	}
}

func TestRepairedWALAcceptsFurtherAppends(t *testing.T) {
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
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 10)); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

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
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})
	appendEach(t, w, entries(1, 1, 20))
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	paths := segmentPaths(t, dir)
	if len(paths) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(paths))
	}

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

	_, rep := openWAL(t, dir, Options{SegmentSize: 512})
	assertEntries(t, rep.Entries, want)
}

func TestTruncateBeforeDropsSupersededSegments(t *testing.T) {
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
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})

	want := raft.HardState{Term: 9, VotedFor: 4}
	if err := w.SaveHardState(want); err != nil {
		t.Fatalf("saving hard state: %v", err)
	}

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
	dir := t.TempDir()

	w, _, err := Open(Options{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	want := entries(1, 1, 5)
	if err := w.AppendEntries(want); err != nil {
		t.Fatalf("appending: %v", err)
	}

	_, rep := openWAL(t, dir, Options{Sync: SyncNever})
	assertEntries(t, rep.Entries, want)
}

func TestReopenAfterAbandonWithoutClose(t *testing.T) {
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
