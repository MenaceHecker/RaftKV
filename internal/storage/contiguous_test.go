package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func logOf(indexes ...raft.Index) []raft.Entry {
	entries := make([]raft.Entry, 0, len(indexes))
	for _, i := range indexes {
		entries = append(entries, raft.Entry{Index: i, Term: 1})
	}
	return entries
}

func TestAnUnbrokenLogIsAccepted(t *testing.T) {
	s := &DiskStorage{entries: logOf(5, 6, 7, 8)}
	if err := s.validateContiguous(); err != nil {
		t.Fatalf("a contiguous log was rejected: %v", err)
	}
}

func TestALogWithAHoleIsRefused(t *testing.T) {
	s := &DiskStorage{entries: logOf(5, 6, 8)}

	err := s.validateContiguous()
	if err == nil {
		t.Fatal("a log that jumps from 6 to 8 was accepted")
	}
	if !errors.Is(err, ErrCorruptWAL) {
		t.Errorf("error is %v, which does not report corruption", err)
	}
	if !strings.Contains(err.Error(), "6") || !strings.Contains(err.Error(), "8") {
		t.Errorf("the error does not say where the gap is: %v", err)
	}
}

func TestARepeatedIndexIsRefused(t *testing.T) {
	s := &DiskStorage{entries: logOf(5, 6, 6)}
	if err := s.validateContiguous(); err == nil {
		t.Fatal("a log with a repeated index was accepted")
	}
}

func TestALogThatGoesBackwardsIsRefused(t *testing.T) {
	s := &DiskStorage{entries: logOf(5, 7, 6)}
	if err := s.validateContiguous(); err == nil {
		t.Fatal("a log that goes backwards was accepted")
	}
}

func TestALogTooShortToHaveAGapIsAccepted(t *testing.T) {
	for _, entries := range [][]raft.Entry{logOf(), logOf(9)} {
		s := &DiskStorage{entries: entries}
		if err := s.validateContiguous(); err != nil {
			t.Fatalf("a log of %d entries was rejected: %v", len(entries), err)
		}
	}
}

func TestOpeningWithoutADirectoryIsRefused(t *testing.T) {
	_, _, err := OpenDiskStorage(DiskConfig{})
	if err == nil {
		t.Fatal("opening with no directory succeeded")
	}
	if !strings.Contains(err.Error(), "Dir") && !strings.Contains(err.Error(), "directory") {
		t.Errorf("the error does not say what is missing: %v", err)
	}
}
