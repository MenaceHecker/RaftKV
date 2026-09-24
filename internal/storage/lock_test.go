package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func TestASecondOpenOfTheSameDirectoryIsRefused(t *testing.T) {
	dir := t.TempDir()

	first, _, err := OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening a fresh directory: %v", err)
	}
	defer first.Close()

	_, _, err = OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncNever})
	if err == nil {
		t.Fatal("a second process opened a directory already in use")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("the error was %q, which does not say the directory is in use", err)
	}
}

func TestTheDirectoryCanBeReopenedAfterClosing(t *testing.T) {
	dir := t.TempDir()

	first, _, err := OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.SetHardState(raft.HardState{Term: 3, VotedFor: 1}); err != nil {
		t.Fatalf("writing hard state: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	second, _, err := OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("reopening after a clean close: %v", err)
	}
	defer second.Close()

	hs, err := second.InitialState()
	if err != nil {
		t.Fatalf("reading hard state: %v", err)
	}
	if hs.Term != 3 || hs.VotedFor != 1 {
		t.Errorf("recovered term=%d votedFor=%d, want 3 and 1", hs.Term, hs.VotedFor)
	}
}

func TestDifferentDirectoriesDoNotBlockEachOther(t *testing.T) {
	a, _, err := OpenDiskStorage(DiskConfig{Dir: t.TempDir(), Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening the first directory: %v", err)
	}
	defer a.Close()

	b, _, err := OpenDiskStorage(DiskConfig{Dir: t.TempDir(), Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening a second, separate directory: %v", err)
	}
	defer b.Close()
}

func TestTheLockFileSurvivesAndIsReused(t *testing.T) {
	dir := t.TempDir()

	s, _, err := OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncNever})
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	path := filepath.Join(dir, lockFileName)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no lock file while the directory is held: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the lock file was removed on close: %v", err)
	}
}
