package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Recovery rebuilds the log from a snapshot point plus whatever the
// write-ahead log still holds, and the two have to meet exactly. A hole
// between them is not something recovery can repair: the entries are gone,
// and a node that started anyway would answer questions about indexes it has
// no record of, with a log that looks sound to everything above it.
//
// The check that refuses is three lines and, until now, the branch that
// reports a gap had never run. Its loop starts at one and compares against
// the entry before, which is exactly the shape that is silently correct on an
// empty log, silently correct on a single entry, and wrong in a way nothing
// notices if the comparison is off.

// logOf builds the entries slice as recovery leaves it: a placeholder at
// element zero carrying the last compacted position, then the real entries.
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
	// The message has to name the boundary, because the operator's next
	// question is which segment to look at.
	if !strings.Contains(err.Error(), "6") || !strings.Contains(err.Error(), "8") {
		t.Errorf("the error does not say where the gap is: %v", err)
	}
}

func TestARepeatedIndexIsRefused(t *testing.T) {
	// Two entries claiming the same index is not a gap, but it is the same
	// kind of impossible: the log no longer maps one index to one entry.
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
	// The placeholder alone, which is what a node that has compacted
	// everything and written nothing since looks like. The loop must not
	// read element minus one.
	for _, entries := range [][]raft.Entry{logOf(), logOf(9)} {
		s := &DiskStorage{entries: entries}
		if err := s.validateContiguous(); err != nil {
			t.Fatalf("a log of %d entries was rejected: %v", len(entries), err)
		}
	}
}

func TestOpeningWithoutADirectoryIsRefused(t *testing.T) {
	// An empty directory would otherwise be resolved against the process's
	// working directory, so a node started with the flag missing would write
	// its log wherever it happened to be launched from.
	_, _, err := OpenDiskStorage(DiskConfig{})
	if err == nil {
		t.Fatal("opening with no directory succeeded")
	}
	if !strings.Contains(err.Error(), "Dir") && !strings.Contains(err.Error(), "directory") {
		t.Errorf("the error does not say what is missing: %v", err)
	}
}
