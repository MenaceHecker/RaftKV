package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for snapshot storage.
//
// A snapshot's job is to be either completely there or not there at all. Most
// of these tests attack that: they damage files, leave partial writes behind,
// and check that recovery either reads a whole snapshot or moves on to an
// older one, but never hands back something half-formed.

// newSnapshotter creates a Snapshotter in a temporary directory.
func newSnapshotter(t *testing.T, dir string) *Snapshotter {
	t.Helper()
	s, err := NewSnapshotter(dir)
	if err != nil {
		t.Fatalf("creating snapshotter: %v", err)
	}
	return s
}

// saveSnapshot stores a snapshot, failing the test on error.
func saveSnapshot(t *testing.T, s *Snapshotter, index raft.Index, term raft.Term, data string) Snapshot {
	t.Helper()
	snap := Snapshot{
		Meta: SnapshotMeta{Index: index, Term: term},
		Data: []byte(data),
	}
	if err := s.Save(snap); err != nil {
		t.Fatalf("saving snapshot at index %d: %v", index, err)
	}
	return snap
}

// snapshotFiles lists the published snapshot files in a directory.
func snapshotFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+snapshotSuffix))
	if err != nil {
		t.Fatalf("listing snapshots: %v", err)
	}
	return matches
}

func TestLoadWithNoSnapshots(t *testing.T) {
	s := newSnapshotter(t, t.TempDir())

	if _, err := s.Load(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Load on an empty directory gave %v, want ErrNoSnapshot", err)
	}
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	want := saveSnapshot(t, s, 42, 7, "the state machine contents")

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Meta != want.Meta {
		t.Fatalf("metadata = %+v, want %+v", got.Meta, want.Meta)
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("data = %q, want %q", got.Data, want.Data)
	}
}

func TestSaveEmptyData(t *testing.T) {
	// A snapshot of an empty state machine is legitimate, and must not be
	// confused with a missing or truncated one.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	if err := s.Save(Snapshot{Meta: SnapshotMeta{Index: 1, Term: 1}}); err != nil {
		t.Fatalf("saving an empty snapshot: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Meta.Index != 1 {
		t.Fatalf("index = %d, want 1", got.Meta.Index)
	}
	if len(got.Data) != 0 {
		t.Fatalf("data = %q, want empty", got.Data)
	}
}

func TestLoadReturnsNewest(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	// Saved out of order, to be sure ordering comes from the metadata rather
	// than from the order files happen to be listed in.
	saveSnapshot(t, s, 20, 2, "middle")
	saveSnapshot(t, s, 40, 4, "newest")
	saveSnapshot(t, s, 10, 1, "oldest")

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Meta.Index != 40 {
		t.Fatalf("loaded index %d, want 40 (the newest)", got.Meta.Index)
	}
	if string(got.Data) != "newest" {
		t.Fatalf("loaded %q, want %q", got.Data, "newest")
	}
}

func TestSnapshotsSurviveReopen(t *testing.T) {
	dir := t.TempDir()

	s := newSnapshotter(t, dir)
	saveSnapshot(t, s, 100, 9, "durable state")

	// A fresh Snapshotter over the same directory is what a restarted process
	// sees.
	reopened := newSnapshotter(t, dir)
	got, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load after reopen: %v", err)
	}
	if got.Meta.Index != 100 || string(got.Data) != "durable state" {
		t.Fatalf("loaded %+v %q after reopen", got.Meta, got.Data)
	}
}

func TestCorruptNewestFallsBackToOlder(t *testing.T) {
	// The reason more than one snapshot is kept. An older image plus the log
	// entries after it rebuilds exactly the same state, so falling back costs
	// replay time and nothing else — far better than refusing to start.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "older")
	saveSnapshot(t, s, 20, 2, "newer")

	path := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 20, Term: 2}))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading newest snapshot: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, fileMode); err != nil {
		t.Fatalf("corrupting newest snapshot: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load did not fall back to the older snapshot: %v", err)
	}
	if got.Meta.Index != 10 || string(got.Data) != "older" {
		t.Fatalf("fell back to %+v %q, want the index-10 snapshot", got.Meta, got.Data)
	}
}

func TestAllSnapshotsCorruptIsAnError(t *testing.T) {
	// Falling back is only reasonable while something readable remains.
	// With nothing intact, the caller has to be told rather than handed an
	// empty state machine that looks legitimate.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "one")
	saveSnapshot(t, s, 20, 2, "two")

	for _, path := range snapshotFiles(t, dir) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		data[len(data)-1] ^= 0xff
		if err := os.WriteFile(path, data, fileMode); err != nil {
			t.Fatalf("corrupting %s: %v", path, err)
		}
	}

	if _, err := s.Load(); err == nil {
		t.Fatal("Load succeeded with every snapshot corrupt, want an error")
	}
}

func TestTruncatedSnapshotIsRejected(t *testing.T) {
	// A snapshot cut short at any point must be rejected, never partially
	// decoded. Handing back a truncated state machine image would silently
	// lose committed data.
	full := func() []byte {
		dir := t.TempDir()
		s := newSnapshotter(t, dir)
		saveSnapshot(t, s, 5, 1, "some reasonably long snapshot payload")
		data, err := os.ReadFile(filepath.Join(dir, snapshotName(SnapshotMeta{Index: 5, Term: 1})))
		if err != nil {
			t.Fatalf("reading reference snapshot: %v", err)
		}
		return data
	}()

	for cut := range len(full) {
		dir := t.TempDir()
		s := newSnapshotter(t, dir)

		path := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 5, Term: 1}))
		if err := os.WriteFile(path, full[:cut], fileMode); err != nil {
			t.Fatalf("writing truncated snapshot: %v", err)
		}

		if _, err := s.Load(); err == nil {
			t.Fatalf("Load accepted a snapshot truncated to %d of %d bytes", cut, len(full))
		}
	}
}

func TestPartialWriteIsSweptOnStartup(t *testing.T) {
	// A crash during Save leaves a file under the temporary name, never under
	// the real one. Startup must remove it: a partial snapshot is worth
	// nothing, and leaving it risks a later save colliding with it.
	dir := t.TempDir()
	newSnapshotter(t, dir)

	temp := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 7, Term: 2})+tempSuffix)
	if err := os.WriteFile(temp, []byte("half a snapshot"), fileMode); err != nil {
		t.Fatalf("writing partial snapshot: %v", err)
	}

	newSnapshotter(t, dir)

	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("partial snapshot %s was not swept away", filepath.Base(temp))
	}
}

func TestPartialWriteIsNeverLoaded(t *testing.T) {
	// The atomicity guarantee stated directly: a file still under the
	// temporary name is invisible to Load, so an interrupted save can never
	// be mistaken for a finished one.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "complete")

	temp := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 99, Term: 9})+tempSuffix)
	if err := os.WriteFile(temp, []byte("interrupted"), fileMode); err != nil {
		t.Fatalf("writing partial snapshot: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Meta.Index != 10 {
		t.Fatalf("loaded index %d, want 10; a partial write was treated as a real snapshot",
			got.Meta.Index)
	}
}

func TestSaveAfterInterruptedSaveSucceeds(t *testing.T) {
	// The sweep has to leave the directory usable, not merely tidy: a retry
	// of the same snapshot must not collide with the leftover temporary file.
	dir := t.TempDir()
	newSnapshotter(t, dir)

	meta := SnapshotMeta{Index: 7, Term: 2}
	temp := filepath.Join(dir, snapshotName(meta)+tempSuffix)
	if err := os.WriteFile(temp, []byte("half"), fileMode); err != nil {
		t.Fatalf("writing partial snapshot: %v", err)
	}

	s := newSnapshotter(t, dir)
	if err := s.Save(Snapshot{Meta: meta, Data: []byte("retried")}); err != nil {
		t.Fatalf("retrying a save after an interrupted one: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got.Data) != "retried" {
		t.Fatalf("loaded %q, want %q", got.Data, "retried")
	}
}

func TestMetadataMismatchIsRejected(t *testing.T) {
	// If the filename and the contents disagree, a file was renamed by hand
	// or tampered with. Guessing which one to believe is worse than refusing.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "state")

	from := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 10, Term: 1}))
	to := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 99, Term: 1}))
	if err := os.Rename(from, to); err != nil {
		t.Fatalf("renaming snapshot: %v", err)
	}

	if _, err := s.Load(); err == nil {
		t.Fatal("Load accepted a snapshot whose name disagrees with its contents")
	}
}

func TestListIsNewestFirst(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	for _, m := range []SnapshotMeta{{Index: 30, Term: 3}, {Index: 10, Term: 1}, {Index: 20, Term: 2}} {
		saveSnapshot(t, s, m.Index, m.Term, "x")
	}

	metas, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []raft.Index{30, 20, 10}
	if len(metas) != len(want) {
		t.Fatalf("listed %d snapshots, want %d", len(metas), len(want))
	}
	for i := range want {
		if metas[i].Index != want[i] {
			t.Fatalf("position %d = index %d, want %d", i, metas[i].Index, want[i])
		}
	}
}

func TestPurgeKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	for i := 1; i <= 5; i++ {
		saveSnapshot(t, s, raft.Index(i*10), raft.Term(i), fmt.Sprintf("state-%d", i))
	}

	if err := s.Purge(2); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	metas, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("kept %d snapshots, want 2", len(metas))
	}
	if metas[0].Index != 50 || metas[1].Index != 40 {
		t.Fatalf("kept indexes %d and %d, want 50 and 40", metas[0].Index, metas[1].Index)
	}

	// The surviving snapshots must still be readable, not merely present.
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load after purge: %v", err)
	}
	if string(got.Data) != "state-5" {
		t.Fatalf("loaded %q after purge, want %q", got.Data, "state-5")
	}
}

func TestPurgeWithFewerThanKeepIsANoOp(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "only")

	if err := s.Purge(3); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if got := len(snapshotFiles(t, dir)); got != 1 {
		t.Fatalf("%d snapshots remain, want 1", got)
	}
}

func TestPurgeRefusesToKeepNothing(t *testing.T) {
	// Keeping at least one is what makes fallback possible, so a request for
	// zero is a mistake rather than an instruction.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)
	saveSnapshot(t, s, 10, 1, "state")

	if err := s.Purge(0); err == nil {
		t.Fatal("Purge(0) succeeded, want an error")
	}
	if got := len(snapshotFiles(t, dir)); got != 1 {
		t.Fatalf("Purge(0) deleted snapshots; %d remain, want 1", got)
	}
}

func TestOversizedSnapshotIsRejected(t *testing.T) {
	// The in-memory encoding bounds a snapshot at one record. Enforcing that
	// on write turns a future unreadable file into an immediate, explicit
	// error.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	huge := Snapshot{
		Meta: SnapshotMeta{Index: 1, Term: 1},
		Data: make([]byte, maxRecordSize+1),
	}
	err := s.Save(huge)
	if !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("saving an oversized snapshot gave %v, want ErrSnapshotTooLarge", err)
	}
	if got := len(snapshotFiles(t, dir)); got != 0 {
		t.Fatalf("a rejected save left %d files behind", got)
	}
}

func TestUnrecognizedFilesAreIgnored(t *testing.T) {
	// Unlike the WAL directory, a stray file here is skipped rather than
	// fatal: snapshots are redundant with the log, so an unknown file is no
	// reason to refuse to start.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "state")

	stray := filepath.Join(dir, "notes"+snapshotSuffix)
	if err := os.WriteFile(stray, []byte("not a snapshot"), fileMode); err != nil {
		t.Fatalf("writing stray file: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load with a stray file present: %v", err)
	}
	if got.Meta.Index != 10 {
		t.Fatalf("loaded index %d, want 10", got.Meta.Index)
	}
}

func TestLoadAtSelectsASpecificSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	saveSnapshot(t, s, 10, 1, "older")
	saveSnapshot(t, s, 20, 2, "newer")

	got, err := s.LoadAt(SnapshotMeta{Index: 10, Term: 1})
	if err != nil {
		t.Fatalf("LoadAt: %v", err)
	}
	if string(got.Data) != "older" {
		t.Fatalf("loaded %q, want %q", got.Data, "older")
	}

	if _, err := s.LoadAt(SnapshotMeta{Index: 999, Term: 9}); err == nil {
		t.Fatal("LoadAt succeeded for a snapshot that does not exist")
	}
}

func TestLargeSnapshotRoundTrips(t *testing.T) {
	// Well under the limit, but large enough to cross buffer boundaries and
	// catch an encoding that only works for short payloads.
	dir := t.TempDir()
	s := newSnapshotter(t, dir)

	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i)
	}

	if err := s.Save(Snapshot{Meta: SnapshotMeta{Index: 1, Term: 1}, Data: data}); err != nil {
		t.Fatalf("saving a 1 MiB snapshot: %v", err)
	}

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatalf("1 MiB snapshot did not round-trip intact")
	}
}
