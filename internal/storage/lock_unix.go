//go:build unix

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockFileName is the file whose lock represents ownership of a data
// directory. Its contents are never read; only the lock matters.
const lockFileName = "LOCK"

// dirLock is an exclusive claim on a data directory, held for as long as the
// process lives.
type dirLock struct {
	f *os.File
}

// lockDir takes an exclusive lock on dir, failing if another process holds it.
//
// Nothing else stops two processes sharing a directory, and sharing one is
// quietly catastrophic. Both append to the same write-ahead log, so the
// records interleave, and the last hard state written wins. Two nodes each
// record a vote in the same term and the survivor inherits the other's: a
// node then restarts believing it voted for a candidate it never heard from,
// which is exactly the record that stops two leaders existing at once.
//
// The lock is advisory and held by the file descriptor, which means the
// kernel drops it when the process dies however it dies. That matters more
// than it sounds: a lock file containing a PID would survive a crash and
// leave a node unable to restart without manual cleanup, turning a recoverable
// outage into an operator's problem at three in the morning.
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

// release drops the lock. The file is left behind deliberately: creating and
// removing it on every start would race with another process opening it.
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
