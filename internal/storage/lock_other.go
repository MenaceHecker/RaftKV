//go:build !unix

package storage

type dirLock struct{}

func lockDir(dir string) (*dirLock, error) { return &dirLock{}, nil }

func (l *dirLock) release() error { return nil }
