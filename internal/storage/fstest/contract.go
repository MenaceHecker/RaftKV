// Package fstest is the contract every storage.FS implementation must meet.
// The rows are copies of the rows in package storage's TestOSFSContract and
// TestOSFSLockContract, exported so the real OS filesystem (storage.NewOSFS)
// and faultfs run the same table. A row where an implementation disagrees
// with the real filesystem is a bug in that implementation, not in the row.
package fstest

import (
	"errors"
	iofs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/storage"
)

const (
	dirMode  = 0o755
	fileMode = 0o644
)

// Factory returns a fresh, empty filesystem and an existing directory in it.
type Factory func(t *testing.T) (fsys storage.FS, dir string)

type contractCase struct {
	name string
	run  func(t *testing.T, fsys storage.FS, dir string)
}

func writeAll(t *testing.T, fsys storage.FS, path string, flag int, data string) {
	t.Helper()
	f, err := fsys.OpenFile(path, flag, fileMode)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		_ = f.Close()
		t.Fatalf("Write(%s): %v", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("Sync(%s): %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close(%s): %v", path, err)
	}
}

func readString(t *testing.T, fsys storage.FS, path string) string {
	t.Helper()
	data, err := fsys.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	return string(data)
}

var contractCases = []contractCase{
	{"create-excl on an existing file fails", func(t *testing.T, fsys storage.FS, dir string) {
		path := filepath.Join(dir, "a")
		flag := os.O_CREATE | os.O_EXCL | os.O_WRONLY | os.O_APPEND
		writeAll(t, fsys, path, flag, "x")

		f, err := fsys.OpenFile(path, flag, fileMode)
		if err == nil {
			_ = f.Close()
			t.Fatal("create-excl on an existing file succeeded")
		}
		if !errors.Is(err, iofs.ErrExist) {
			t.Fatalf("error = %v, want one matching fs.ErrExist", err)
		}
	}},
	{"appended writes land at the end", func(t *testing.T, fsys storage.FS, dir string) {
		path := filepath.Join(dir, "a")
		flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
		writeAll(t, fsys, path, flag, "ab")
		writeAll(t, fsys, path, os.O_WRONLY|os.O_APPEND, "cd")

		if got := readString(t, fsys, path); got != "abcd" {
			t.Fatalf("contents = %q, want %q", got, "abcd")
		}
	}},
	{"Truncate shrinks", func(t *testing.T, fsys storage.FS, dir string) {
		path := filepath.Join(dir, "a")
		writeAll(t, fsys, path, os.O_CREATE|os.O_WRONLY, "abcdef")

		if err := fsys.Truncate(path, 2); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		if got := readString(t, fsys, path); got != "ab" {
			t.Fatalf("contents = %q, want %q", got, "ab")
		}
		info, err := fsys.Stat(path)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if info.Size() != 2 {
			t.Fatalf("size = %d, want 2", info.Size())
		}
	}},
	{"Rename over an existing target replaces it", func(t *testing.T, fsys storage.FS, dir string) {
		from := filepath.Join(dir, "from")
		to := filepath.Join(dir, "to")
		writeAll(t, fsys, from, os.O_CREATE|os.O_WRONLY, "new")
		writeAll(t, fsys, to, os.O_CREATE|os.O_WRONLY, "old")

		if err := fsys.Rename(from, to); err != nil {
			t.Fatalf("Rename: %v", err)
		}
		if got := readString(t, fsys, to); got != "new" {
			t.Fatalf("target = %q, want %q", got, "new")
		}
		if _, err := fsys.Stat(from); !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("Stat(source) error = %v, want one matching fs.ErrNotExist", err)
		}
	}},
	{"ReadDir is sorted", func(t *testing.T, fsys storage.FS, dir string) {
		names := []string{"c", "a", "e", "b", "d"}
		for _, n := range names {
			writeAll(t, fsys, filepath.Join(dir, n), os.O_CREATE|os.O_WRONLY, n)
		}

		entries, err := fsys.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		var got []string
		for _, e := range entries {
			got = append(got, e.Name())
		}
		if !sort.StringsAreSorted(got) || len(got) != len(names) {
			t.Fatalf("entries = %v, want %d names in sorted order", got, len(names))
		}
	}},
	{"SyncDir on a missing directory errors", func(t *testing.T, fsys storage.FS, dir string) {
		if err := fsys.SyncDir(filepath.Join(dir, "missing")); err == nil {
			t.Fatal("SyncDir on a missing directory succeeded")
		}
	}},
	{"SyncDir on an existing directory succeeds", func(t *testing.T, fsys storage.FS, dir string) {
		if err := fsys.SyncDir(dir); err != nil {
			t.Fatalf("SyncDir: %v", err)
		}
	}},
	{"Remove of a missing file reports not-exist", func(t *testing.T, fsys storage.FS, dir string) {
		err := fsys.Remove(filepath.Join(dir, "missing"))
		if !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("error = %v, want one matching fs.ErrNotExist", err)
		}
	}},
	{"MkdirAll creates nested directories and is idempotent", func(t *testing.T, fsys storage.FS, dir string) {
		nested := filepath.Join(dir, "x", "y")
		for i := 0; i < 2; i++ {
			if err := fsys.MkdirAll(nested, dirMode); err != nil {
				t.Fatalf("MkdirAll #%d: %v", i+1, err)
			}
		}
		info, err := fsys.Stat(nested)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if !info.IsDir() {
			t.Fatal("MkdirAll did not create a directory")
		}
	}},
}

// RunContract runs every non-lock row of the FS contract against fsys.
func RunContract(t *testing.T, newFS Factory) {
	t.Helper()
	for _, tc := range contractCases {
		t.Run(tc.name, func(t *testing.T) {
			fsys, dir := newFS(t)
			tc.run(t, fsys, dir)
		})
	}
}

// RunLockContract runs the Lock rows. The no-op lock of non-unix builds of the
// real filesystem cannot refuse a second holder, so callers gate it on unix.
func RunLockContract(t *testing.T, newFS Factory) {
	t.Helper()
	t.Run("Lock on a held directory fails with already in use", func(t *testing.T) {
		fsys, dir := newFS(t)
		first, err := fsys.Lock(dir)
		if err != nil {
			t.Fatalf("first Lock: %v", err)
		}
		defer func() { _ = first() }()

		second, err := fsys.Lock(dir)
		if err == nil {
			_ = second()
			t.Fatal("a second Lock on a held directory succeeded")
		}
		if !strings.Contains(err.Error(), "already in use") {
			t.Fatalf("error = %q, want it to say the directory is already in use", err)
		}
	})

	t.Run("release then re-lock succeeds", func(t *testing.T) {
		fsys, dir := newFS(t)
		first, err := fsys.Lock(dir)
		if err != nil {
			t.Fatalf("first Lock: %v", err)
		}
		if err := first(); err != nil {
			t.Fatalf("release: %v", err)
		}

		second, err := fsys.Lock(dir)
		if err != nil {
			t.Fatalf("Lock after release: %v", err)
		}
		if err := second(); err != nil {
			t.Fatalf("second release: %v", err)
		}
	})
}
