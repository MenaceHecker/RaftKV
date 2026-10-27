package storage

import (
	"errors"
	"strings"
	"testing"
)

// The one deliberate behavior change in the FS seam: the partial-snapshot
// sweep used filepath.Glob, which silently swallowed directory-read errors.
func TestSweepSurfacesReadDirError(t *testing.T) {
	dir := t.TempDir()
	cause := errors.New("injected readdir failure")
	fsys := newWrapFS(func(op, name string) error {
		if op == "ReadDir" && name == dir {
			return cause
		}
		return nil
	})

	_, err := newSnapshotterFS(fsys, dir)
	if err == nil {
		t.Fatal("newSnapshotterFS succeeded although the directory could not be read")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want it to wrap the ReadDir failure", err)
	}
	if !strings.Contains(err.Error(), "storage: scanning for partial snapshots") {
		t.Fatalf("error = %q, want the scanning-for-partial-snapshots prefix", err)
	}
}
