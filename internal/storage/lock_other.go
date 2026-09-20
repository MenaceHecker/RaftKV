//go:build !unix

package storage

// On platforms without flock the directory is not locked.
//
// This is a gap rather than a decision, and it is left open rather than
// papered over: the alternative, a lock file holding a process ID, survives a
// crash and leaves a node that cannot restart without someone deleting a file
// by hand. The supported deployment targets, the Linux container and
// development on macOS, both take the real lock above.
type dirLock struct{}

func lockDir(dir string) (*dirLock, error) { return &dirLock{}, nil }

func (l *dirLock) release() error { return nil }
