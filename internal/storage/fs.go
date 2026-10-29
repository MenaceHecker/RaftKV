package storage

import (
	"fmt"
	"io"
	"os"
)

// FS is the set of filesystem operations the WAL, snapshotter and disk store
// perform. osFS is the real implementation; tests substitute wrappers.
type FS interface {
	MkdirAll(dir string, perm os.FileMode) error

	OpenFile(name string, flag int, perm os.FileMode) (File, error)

	ReadFile(name string) ([]byte, error)

	ReadDir(dir string) ([]os.DirEntry, error)

	Remove(name string) error

	Rename(oldpath, newpath string) error

	Truncate(name string, size int64) error

	SyncDir(dir string) error

	Stat(name string) (os.FileInfo, error)

	// Lock takes an exclusive lock on dir, creating it if needed, and fails
	// with an "already in use" error while another holder has it.
	// release gives the lock up; calling it again is a no-op.
	Lock(dir string) (release func() error, err error)
}

// File is an open file as the storage layer uses it: write, fsync, stat, close.
type File interface {
	io.Writer

	// Stat describes the open file itself, not whatever its path names now.
	Stat() (os.FileInfo, error)

	Sync() error

	Close() error
}

type osFS struct{}

func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm) //nolint:gosec // paths are built by the storage layer from its configured directory
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) } //nolint:gosec // paths are built by the storage layer from its configured directory

func (osFS) ReadDir(dir string) ([]os.DirEntry, error) { return os.ReadDir(dir) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) Truncate(name string, size int64) error { return os.Truncate(name, size) }

func (osFS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is the storage layer's configured directory
	if err != nil {
		return fmt.Errorf("storage: opening directory for sync: %w", err)
	}
	defer func() { _ = d.Close() }() // read-only handle; the fsync result is what matters

	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: syncing directory: %w", err)
	}
	return nil
}
