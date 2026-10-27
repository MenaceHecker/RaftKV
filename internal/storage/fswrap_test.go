package storage

import (
	"os"
	"sync"
)

// wrapFS decorates an FS for tests: it can fail chosen operations and records
// every call as "op name". The hook sees the operation name ("ReadDir",
// "File.Close", ...) and its path argument; a non-nil return fails the call.
type wrapFS struct {
	FS
	fail func(op, name string) error

	mu    sync.Mutex
	calls []string
}

func newWrapFS(fail func(op, name string) error) *wrapFS {
	return &wrapFS{FS: osFS{}, fail: fail}
}

func (w *wrapFS) record(op, name string) error {
	w.mu.Lock()
	w.calls = append(w.calls, op+" "+name)
	w.mu.Unlock()
	if w.fail == nil {
		return nil
	}
	return w.fail(op, name)
}

func (w *wrapFS) ReadDir(dir string) ([]os.DirEntry, error) {
	if err := w.record("ReadDir", dir); err != nil {
		return nil, err
	}
	return w.FS.ReadDir(dir)
}
