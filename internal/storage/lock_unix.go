//go:build unix

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const lockFileName = "LOCK"

type dirLock struct {
	f *os.File
}

func lockDir(dir string) (*dirLock, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("storage: creating %s: %w", dir, err)
	}

	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, fmt.Errorf("storage: opening the lock file: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: %s is already in use by another process "+
			"(could not lock %s): %w", dir, path, err)
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
