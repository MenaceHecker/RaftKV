//go:build !unix

package storage

type dirLock struct{}

func lockDir(dir string) (*dirLock, error) { return &dirLock{}, nil }

func (l *dirLock) release() error { return nil }

// Lock is a no-op where flock is unavailable; see lockDir.
func (osFS) Lock(dir string) (func() error, error) {
	l, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	return l.release, nil
}
