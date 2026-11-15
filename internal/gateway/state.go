package gateway

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// errKeyCap means the registry already holds the maximum number of keys.
var errKeyCap = errors.New("gateway: key registry is full")

const registryFile = "keys.log"

// registry is the persisted set of public keys. The cluster has no scan or
// count RPC, so this is the only way to enforce the key cap and to reset the
// keyspace (D5 card, Q3).
//
// It is an append-only log of "put <key>" and "del <key>" lines, fsynced on
// every append and loaded at start. A last line cut short by a crash (no
// newline) is dropped and the file is truncated back to the last whole line.
// The log is rewritten (temp file, fsync, rename, directory fsync) once it is
// more than twice the size of the live set.
type registry struct {
	mu    sync.Mutex
	dir   string
	f     *os.File
	keys  map[string]struct{}
	lines int
	max   int
	shut  bool
}

func openRegistry(dir string, capacity int) (*registry, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("state dir: %w", err)
	}
	r := &registry{dir: dir, keys: map[string]struct{}{}, max: capacity}
	path := filepath.Join(dir, registryFile)

	data, err := os.ReadFile(path) //nolint:gosec // inside the operator's state dir
	created := false
	switch {
	case errors.Is(err, os.ErrNotExist):
		created = true
	case err != nil:
		return nil, fmt.Errorf("read %s: %w", registryFile, err)
	}

	good := bytes.LastIndexByte(data, '\n') + 1
	sc := bufio.NewScanner(bytes.NewReader(data[:good]))
	for sc.Scan() {
		op, key, ok := strings.Cut(sc.Text(), " ")
		switch {
		case !ok || key == "":
			return nil, fmt.Errorf("%s: malformed line", registryFile)
		case op == "put":
			r.keys[key] = struct{}{}
		case op == "del":
			delete(r.keys, key)
		default:
			return nil, fmt.Errorf("%s: unknown operation", registryFile)
		}
		r.lines++
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if good < len(data) { // a torn last line
		if err := os.Truncate(path, int64(good)); err != nil {
			return nil, fmt.Errorf("repair %s: %w", registryFile, err)
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // inside the operator's state dir
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if created || good < len(data) {
		if err := syncDir(dir); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	r.f = f
	return r, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // inside the operator's state dir
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func (r *registry) appendLocked(op, key string) error {
	if _, err := r.f.WriteString(op + " " + key + "\n"); err != nil {
		return err
	}
	if err := r.f.Sync(); err != nil {
		return err
	}
	r.lines++
	return nil
}

// add records key before it is written to the cluster. It reports whether the
// key is new; a new key over the cap is refused.
func (r *registry) add(key string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.keys[key]; ok {
		return false, nil
	}
	if len(r.keys) >= r.max {
		return false, errKeyCap
	}
	if err := r.appendLocked("put", key); err != nil {
		return false, err
	}
	r.keys[key] = struct{}{}
	return true, r.compactLocked()
}

// remove forgets key, freeing a slot.
func (r *registry) remove(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.keys[key]; !ok {
		return nil
	}
	if err := r.appendLocked("del", key); err != nil {
		return err
	}
	delete(r.keys, key)
	return r.compactLocked()
}

func (r *registry) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.keys))
	for k := range r.keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// compact rewrites the log so it holds only the live keys.
func (r *registry) compact() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rewriteLocked()
}

func (r *registry) compactLocked() error {
	if r.lines > 64 && r.lines > 2*len(r.keys) {
		return r.rewriteLocked()
	}
	return nil
}

func (r *registry) rewriteLocked() error {
	path := filepath.Join(r.dir, registryFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // inside the operator's state dir
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(r.keys))
	for k := range r.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w := bufio.NewWriter(f)
	for _, k := range keys {
		_, _ = w.WriteString("put " + k + "\n")
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := syncDir(r.dir); err != nil {
		return err
	}
	_ = r.f.Close()
	nf, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // inside the operator's state dir
	if err != nil {
		return err
	}
	r.f = nf
	r.lines = len(keys)
	return nil
}

func (r *registry) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shut {
		return nil
	}
	r.shut = true
	return r.f.Close()
}
