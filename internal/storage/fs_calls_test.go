package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// runScriptedStorage drives one fixed scenario through open, which hides how
// the filesystem is observed. The same scenario was recorded against the
// pre-refactor os calls to produce testdata/fs_calls.golden.
func runScriptedStorage(t *testing.T, dir string, open func(dir string) (*DiskStorage, Snapshot, error)) {
	t.Helper()

	snapDir := filepath.Join(dir, snapshotSubdir)
	if err := os.MkdirAll(snapDir, dirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "x"+snapshotSuffix+tempSuffix), []byte("partial"), fileMode); err != nil {
		t.Fatal(err)
	}

	s, _, err := open(dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := s.Append(logEntries(1, 1, 3)); err != nil {
		t.Fatalf("append 3: %v", err)
	}
	if err := s.SetHardState(raft.HardState{Term: 1, VotedFor: 1}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	if err := s.Append(logEntries(1, 4, 6)); err != nil {
		t.Fatalf("append 6 (rolls segments): %v", err)
	}
	if err := s.CreateSnapshot(8, []byte("state"), testConf); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s, _, err = open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestStorageCallOrderIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	fsys := newWrapFS(nil)

	runScriptedStorage(t, dir, func(dir string) (*DiskStorage, Snapshot, error) {
		return OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncAlways, SegmentSize: 64, FS: fsys})
	})

	got := strings.ReplaceAll(strings.Join(fsys.log(), "\n"), dir, "$DIR") + "\n"

	want, err := os.ReadFile(filepath.Join("testdata", "fs_calls.golden"))
	if err != nil {
		t.Fatalf("reading the golden file: %v", err)
	}
	if got != string(want) {
		t.Fatalf("the sequence of filesystem calls changed.\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}
