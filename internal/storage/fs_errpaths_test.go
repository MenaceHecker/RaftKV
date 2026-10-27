package storage

import (
	"errors"
	"path/filepath"
	"testing"
)

var errInjected = errors.New("injected failure")

// indexAfter returns the index of the first entry equal to want at or after
// from, or -1.
func indexAfter(calls []string, from int, want string) int {
	for i := from; i < len(calls); i++ {
		if calls[i] == want {
			return i
		}
	}
	return -1
}

// The AST test cannot see whether an error path releases its file handle, so
// each case injects one failure and asserts the handle's Close follows it.
func TestStorageErrorPathsCloseTheirHandles(t *testing.T) {
	t.Run("Stat failure in openActive closes the opened segment", func(t *testing.T) {
		dir := t.TempDir()
		seg := filepath.Join(dir, segmentName(1, 1))

		w, _, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("creating the segment: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}

		fsys := newWrapFS(func(op, name string) error {
			if op == "Stat" && name == seg {
				return errInjected
			}
			return nil
		})
		if _, _, err := Open(Options{Dir: dir, FS: fsys}); !errors.Is(err, errInjected) {
			t.Fatalf("Open error = %v, want one wrapping the injected Stat failure", err)
		}

		calls := fsys.log()
		stat := indexAfter(calls, 0, "Stat "+seg)
		if stat < 0 || indexAfter(calls, stat, "Close "+seg) < 0 {
			t.Fatalf("no Close of %s after the failed Stat; calls:\n%v", seg, calls)
		}
	})

	t.Run("SyncDir failure in roll closes the new segment", func(t *testing.T) {
		dir := t.TempDir()
		seg := filepath.Join(dir, segmentName(1, 1))

		fsys := newWrapFS(func(op, name string) error {
			if op == "SyncDir" && name == dir {
				return errInjected
			}
			return nil
		})
		if _, _, err := Open(Options{Dir: dir, FS: fsys}); !errors.Is(err, errInjected) {
			t.Fatalf("Open error = %v, want one wrapping the injected SyncDir failure", err)
		}

		calls := fsys.log()
		sync := indexAfter(calls, 0, "SyncDir "+dir)
		if sync < 0 || indexAfter(calls, sync, "Close "+seg) < 0 {
			t.Fatalf("no Close of %s after the failed SyncDir; calls:\n%v", seg, calls)
		}
	})

	t.Run("Close failure of the previous segment closes the new segment", func(t *testing.T) {
		dir := t.TempDir()
		first := filepath.Join(dir, segmentName(1, 1))

		fsys := newWrapFS(func(op, name string) error {
			if op == "Close" && name == first {
				return errInjected
			}
			return nil
		})
		w, _, err := Open(Options{Dir: dir, SegmentSize: 64, FS: fsys})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = w.Close() }()

		var appendErr error
		for i := 0; i < 20 && appendErr == nil; i++ {
			appendErr = w.AppendEntries(logEntries(1, w.lastIndex+1, 1))
		}
		if !errors.Is(appendErr, errInjected) {
			t.Fatalf("append error = %v, want one wrapping the injected Close failure", appendErr)
		}

		calls := fsys.log()
		failed := indexAfter(calls, 0, "Close "+first)
		if failed < 0 {
			t.Fatalf("the previous segment was never closed; calls:\n%v", calls)
		}
		next := ""
		for _, c := range calls[failed:] {
			if len(c) > 5 && c[:5] == "Close" && c != "Close "+first {
				next = c
			}
		}
		if next == "" {
			t.Fatalf("the new segment was not closed after the failed Close; calls:\n%v", calls[failed:])
		}
	})
}
