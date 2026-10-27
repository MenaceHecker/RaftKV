package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A deliberate behavior change in the FS seam: the partial-snapshot sweep used
// filepath.Glob, which silently swallowed directory-read errors.
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

// The sweep's second deliberate difference: filepath.Glob read the configured
// directory as part of a pattern, so a name such as "data[" made it fail with
// ErrBadPattern (and "*" or "?" could scan other directories). Listing the
// directory takes the name literally. This pins that the sweep works there.
func TestSweepTreatsDirectoryNamesLiterally(t *testing.T) {
	for _, name := range []string{"data[", "snap*", "what?"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			if err := os.MkdirAll(dir, dirMode); err != nil {
				t.Fatal(err)
			}
			partial := filepath.Join(dir, "x"+snapshotSuffix+tempSuffix)
			if err := os.WriteFile(partial, []byte("partial"), fileMode); err != nil {
				t.Fatal(err)
			}

			if _, err := NewSnapshotter(dir); err != nil {
				t.Fatalf("NewSnapshotter(%q): %v", dir, err)
			}
			if _, err := os.Stat(partial); !os.IsNotExist(err) {
				t.Fatalf("partial snapshot in %q was not swept: stat err = %v", dir, err)
			}
		})
	}
}

// With Glob, "snap*" in the configured directory's name would have matched
// sibling directories and swept their partial snapshots. The sweep must touch
// only the directory it was given.
func TestSweepDoesNotReachSiblingDirectories(t *testing.T) {
	for _, c := range []struct{ name, sibling string }{
		{"snap*", "snap1"},
		{"what?", "whatX"},
		{"data[ab]", "dataa"},
	} {
		t.Run(c.name, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, c.name)
			sibling := filepath.Join(parent, c.sibling)

			var sentinel string
			for _, d := range []string{dir, sibling} {
				if err := os.MkdirAll(d, dirMode); err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(d, "x"+snapshotSuffix+tempSuffix)
				if err := os.WriteFile(p, []byte("partial"), fileMode); err != nil {
					t.Fatal(err)
				}
				sentinel = p
			}

			if _, err := NewSnapshotter(dir); err != nil {
				t.Fatalf("NewSnapshotter(%q): %v", dir, err)
			}
			if _, err := os.Stat(sentinel); err != nil {
				t.Fatalf("the sibling directory's file was swept: %v", err)
			}
		})
	}
}
