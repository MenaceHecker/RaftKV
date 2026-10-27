package storage

import (
	"fmt"
	"io"
	"os"
)

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
}

type File interface {
	io.Writer

	Sync() error

	Close() error
}

type osFS struct{}

func (osFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }

func (osFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

func (osFS) ReadDir(dir string) ([]os.DirEntry, error) { return os.ReadDir(dir) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) Rename(oldpath, newpath string) error { return os.Rename(oldpath, newpath) }

func (osFS) Truncate(name string, size int64) error { return os.Truncate(name, size) }

func (osFS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: opening directory for sync: %w", err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: syncing directory: %w", err)
	}
	return nil
}
