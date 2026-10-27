package storage

import (
	"os"
	"sync"
)

// wrapFS decorates an FS for tests: it can fail chosen operations and records
// every call as "op name". The hook sees the operation name ("ReadDir",
// "File.Close", ...) and its path argument; a non-nil return fails the call
// (for File.Close the file is still closed, as a real failed Close would be).
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

func (w *wrapFS) log() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.calls...)
}

func (w *wrapFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := w.record("MkdirAll", dir); err != nil {
		return err
	}
	return w.FS.MkdirAll(dir, perm)
}

func (w *wrapFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if err := w.record("OpenFile", name); err != nil {
		return nil, err
	}
	f, err := w.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &wrapFile{File: f, fs: w, name: name}, nil
}

func (w *wrapFS) ReadFile(name string) ([]byte, error) {
	if err := w.record("ReadFile", name); err != nil {
		return nil, err
	}
	return w.FS.ReadFile(name)
}

func (w *wrapFS) ReadDir(dir string) ([]os.DirEntry, error) {
	if err := w.record("ReadDir", dir); err != nil {
		return nil, err
	}
	return w.FS.ReadDir(dir)
}

func (w *wrapFS) Remove(name string) error {
	if err := w.record("Remove", name); err != nil {
		return err
	}
	return w.FS.Remove(name)
}

func (w *wrapFS) Rename(oldpath, newpath string) error {
	if err := w.record("Rename", oldpath); err != nil {
		return err
	}
	return w.FS.Rename(oldpath, newpath)
}

func (w *wrapFS) Truncate(name string, size int64) error {
	if err := w.record("Truncate", name); err != nil {
		return err
	}
	return w.FS.Truncate(name, size)
}

func (w *wrapFS) SyncDir(dir string) error {
	if err := w.record("SyncDir", dir); err != nil {
		return err
	}
	return w.FS.SyncDir(dir)
}

func (w *wrapFS) Stat(name string) (os.FileInfo, error) {
	if err := w.record("Stat", name); err != nil {
		return nil, err
	}
	return w.FS.Stat(name)
}

func (w *wrapFS) Lock(dir string) (*dirLock, error) {
	if err := w.record("Lock", dir); err != nil {
		return nil, err
	}
	return w.FS.Lock(dir)
}

type wrapFile struct {
	File
	fs   *wrapFS
	name string
}

func (f *wrapFile) Write(p []byte) (int, error) {
	if err := f.fs.record("Write", f.name); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

func (f *wrapFile) Stat() (os.FileInfo, error) {
	if err := f.fs.record("Stat", f.name); err != nil {
		return nil, err
	}
	return f.File.Stat()
}

func (f *wrapFile) Sync() error {
	if err := f.fs.record("Sync", f.name); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *wrapFile) Close() error {
	hookErr := f.fs.record("Close", f.name)
	if err := f.File.Close(); err != nil {
		return err
	}
	return hookErr
}
