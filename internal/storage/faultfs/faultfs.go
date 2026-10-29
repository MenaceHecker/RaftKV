// Package faultfs is an in-memory, seeded, fault-injecting filesystem for
// storage.FS. It separates what a process sees (live state) from what survives
// power loss (durable state), so tests can crash it at any operation and check
// what recovery finds.
//
// # Model
//
// Every file has durable and live bytes. Write extends live; File.Sync makes
// the file's bytes and length durable. Truncate changes live only and is not
// durable until the file is synced. Directories are inodes with live and
// durable entries (name to inode). Create, Remove and Rename change live
// entries; SyncDir(d) makes d's entries durable and nothing else. MkdirAll
// adds the missing components to their parents' live entries, so a directory
// exists after a crash only if SyncDir(parent) ran after it was created.
// Rename is atomic. Data, directory entry and directory creation durability
// are independent: a synced file whose entry was never dir-synced vanishes, a
// synced file inside a directory whose own entry was never synced vanishes
// with it, and a durable entry whose data was never synced comes back with its
// durable bytes, possibly none.
//
// The model is deliberately conservative. Everything it does is permitted by
// POSIX, and it is stricter than ext4 in practice (for example, ext4 usually
// orders data before a rename that replaces a file; here nothing is ordered
// unless the code syncs it). A recovery path that survives this model may
// still not be proven correct on every real filesystem, but one that fails
// here is wrong.
//
// # Crashing
//
// Crash applies power loss: every file and directory falls back to its
// durable state, all open handles become stale (later calls on them return
// ErrStaleHandle and never touch the new state) and the lock table is cleared,
// as a dead process releases its flocks. What survives of unsynced bytes
// depends on CrashOptions: they are dropped, cut at a seeded point (TornTail),
// kept at full length but replaced by random bytes (GarbageTail) or kept at
// full length but zeroed (ZeroTail). Unsynced truncation or rewriting does not
// extend the durable bytes, so a file whose live bytes are not an extension of
// its durable bytes comes back as exactly one of the two, chosen by the seed or
// forced with TruncateLost or TruncatePersisted. Crash computes every outcome
// from the live state as it was before the crash, visiting files in sorted
// path order, so one seed always gives one result.
//
// CrashAt(n) and CrashAfter(n) do not change file state. They put the FS in a
// crashed state at operation n (before or after it takes effect), after which
// every call fails with ErrCrashed. The next Crash(opts) applies the power loss
// and clears the crashed state; ProcessCrash clears it too. Every FS and File
// method counts as an operation, in the order Ops reports.
//
// ProcessCrash models the process dying while the kernel survives: handles
// and locks are invalidated, file state is untouched and live data persists.
//
// Faults are injected with Inject. With Options.Fsyncgate, a failed Sync
// leaves the bytes it could not write permanently non-durable even if a later
// Sync reports success: they read back as what the disk held before (zeros
// beyond the old length) while the file length persists.
//
// Out of scope: cross-directory rename (Rename returns ErrCrossDirRename),
// hard links, permissions, timestamps, reordering unsynced writes across
// files, and Windows paths (paths are slash-separated).
//
// Randomness comes only from the *rand.Rand passed to New; the package reads
// no clock (time is imported for the time.Time in fs.FileInfo only), no global
// generator and no operating system state.
package faultfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/MenaceHecker/raftkv/internal/storage"
)

var _ storage.FS = (*FS)(nil)

// Errors a faultfs returns or injects.
var (
	// ErrCrashed is returned by every call after CrashAt or CrashAfter fired,
	// until the next Crash or ProcessCrash.
	ErrCrashed = errors.New("faultfs: crashed")

	// ErrStaleHandle is returned by a File or lock release obtained before the
	// last Crash or ProcessCrash.
	ErrStaleHandle = errors.New("faultfs: stale handle from before a crash")

	// ErrCrossDirRename is returned when Rename's paths are in different
	// directories, which the model does not cover.
	ErrCrossDirRename = errors.New("faultfs: cross-directory rename is not modelled")

	// ErrSync is an injected fsync failure (EIO).
	ErrSync = fmt.Errorf("faultfs: injected sync failure: %w", syscall.EIO)

	// ErrSyncDir is an injected directory fsync failure (EIO).
	ErrSyncDir = fmt.Errorf("faultfs: injected directory sync failure: %w", syscall.EIO)

	// ErrRename is an injected rename failure (EIO).
	ErrRename = fmt.Errorf("faultfs: injected rename failure: %w", syscall.EIO)

	// ErrNoSpace is an injected ENOSPC for Write, OpenFile, Truncate and MkdirAll.
	ErrNoSpace = fmt.Errorf("faultfs: injected: %w", syscall.ENOSPC)
)

// ShortWriteError is the error a Write fails with when ShortWrite injected it.
type ShortWriteError struct {
	// Keep is how many bytes the failing Write still stores.
	Keep int
}

func (e *ShortWriteError) Error() string {
	return fmt.Sprintf("faultfs: injected short write after %d bytes", e.Keep)
}

// Unwrap lets errors.Is(err, io.ErrShortWrite) hold.
func (*ShortWriteError) Unwrap() error { return io.ErrShortWrite }

// ShortWrite returns an error that makes a Write store k bytes and then fail
// with a non-nil error.
func ShortWrite(k int) error { return &ShortWriteError{Keep: k} }

// Op names a method for fault rules.
type Op int

// The operations a Rule can match.
const (
	OpAny Op = iota
	OpMkdirAll
	OpOpenFile
	OpReadFile
	OpReadDir
	OpRemove
	OpRename
	OpTruncate
	OpSyncDir
	OpStat
	OpLock
	OpRelease
	OpWrite
	OpSync
	OpFileStat
	OpClose
)

var opNames = [...]string{"Any", "MkdirAll", "OpenFile", "ReadFile", "ReadDir", "Remove",
	"Rename", "Truncate", "SyncDir", "Stat", "Lock", "Release", "Write", "Sync", "File.Stat", "Close"}

func (o Op) String() string {
	if o < 0 || int(o) >= len(opNames) {
		return fmt.Sprintf("Op(%d)", int(o))
	}
	return opNames[o]
}

// Rule makes matching calls fail with Err. It matches calls of Op (OpAny
// matches all) whose path matches PathGlob (path.Match against the full path,
// or against the base name when the glob has no slash; empty matches all).
// Exactly one trigger applies:
//
//   - Nth > 0: the Nth matching call fails, once; later calls succeed.
//   - Nth > 0 and Sticky: the Nth matching call and every one after it fail.
//   - Sticky alone: every matching call fails.
//   - Prob > 0: each matching call fails with that probability, drawn from the seed.
//
// A failing call has no effect, except Write with ShortWrite(k) (stores k
// bytes, then fails) and Sync (the file's bytes stay unsynced; see Options.Fsyncgate).
type Rule struct {
	Op       Op
	PathGlob string
	Nth      int
	Prob     float64
	Sticky   bool
	Err      error
}

// TailMode says what Crash does with a file's unsynced appended bytes.
type TailMode int

// The tail modes.
const (
	// DropTail loses all unsynced appended bytes. The zero value.
	DropTail TailMode = iota
	// TornTail keeps a seeded prefix of them (possibly none, possibly all).
	TornTail
	// GarbageTail keeps their length but replaces them with seeded random bytes.
	GarbageTail
	// ZeroTail keeps their length but they read back as zeros.
	ZeroTail
)

// TruncateMode says what Crash does with a file whose live bytes are not an
// extension of its durable bytes (unsynced truncate or rewrite).
type TruncateMode int

// The truncate modes.
const (
	// TruncateBySeed picks the durable or the live bytes per file, by seed. The zero value.
	TruncateBySeed TruncateMode = iota
	// TruncateLost always reverts to the durable bytes.
	TruncateLost
	// TruncatePersisted always keeps the live bytes.
	TruncatePersisted
)

// CrashOptions selects the power-loss outcome.
type CrashOptions struct {
	Tail     TailMode
	Truncate TruncateMode

	// TornTailAt keeps exactly this many unsynced appended bytes of the named
	// file (full path, or base name), clamped to what exists. It overrides Tail
	// for that file.
	TornTailAt map[string]int
}

// Options configure New.
type Options struct {
	// Fsyncgate: after a failed Sync, those bytes stay permanently non-durable
	// even if a later Sync returns success.
	Fsyncgate bool
}

type span struct{ lo, hi int }

type inode struct {
	dir  bool
	mode fs.FileMode

	durable, live []byte
	poisoned      []span
	syncFailed    bool

	liveEntries, durableEntries map[string]*inode
}

type rule struct {
	Rule
	seen int
}

// FS is the in-memory filesystem. It implements storage.FS.
type FS struct {
	mu   sync.Mutex
	rng  *rand.Rand
	opts Options

	root  *inode
	epoch int

	locks  map[string]int
	nextID int

	ops          int
	log          []string
	crashAt      int
	crashAfter   int
	pendingCrash bool
	crashed      bool

	rules []*rule
}

// New returns an empty filesystem whose only directory is "/". All randomness
// comes from rng.
func New(rng *rand.Rand, opts Options) *FS {
	return &FS{
		rng:   rng,
		opts:  opts,
		root:  newDir(0o755),
		locks: map[string]int{},
	}
}

func newDir(mode fs.FileMode) *inode {
	return &inode{dir: true, mode: mode, liveEntries: map[string]*inode{}, durableEntries: map[string]*inode{}}
}

// Ops reports how many operations have been counted so far. CrashAt and
// CrashAfter take absolute operation numbers on this count.
func (f *FS) Ops() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ops
}

// Log returns the counted operations so far as "n Op path", to say which
// operation a crash point n was.
func (f *FS) Log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

// CrashAt makes operation number n (as counted by Ops, starting at 1) fail
// with ErrCrashed before it takes effect, and every later call too.
func (f *FS) CrashAt(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashAt, f.crashAfter = n, 0
}

// CrashAfter is CrashAt except operation n takes effect first. The caller
// still sees ErrCrashed.
func (f *FS) CrashAfter(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashAt, f.crashAfter = 0, n
}

// Crashed reports whether CrashAt or CrashAfter has fired and no Crash or
// ProcessCrash has cleared it.
func (f *FS) Crashed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.crashed
}

// Inject adds a fault rule. It panics on a rule with no trigger or no error,
// which is a mistake in the test, not a runtime condition.
func (f *FS) Inject(r Rule) {
	if r.Err == nil {
		panic("faultfs: Inject needs an Err")
	}
	if r.Nth <= 0 && r.Prob <= 0 && !r.Sticky {
		panic("faultfs: Inject needs one of Nth, Prob or Sticky")
	}
	if r.Prob > 0 && (r.Nth > 0 || r.Sticky) {
		panic("faultfs: Prob cannot be combined with Nth or Sticky")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, &rule{Rule: r})
}

// ClearRules removes every fault rule.
func (f *FS) ClearRules() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
}

// SyncAll makes every file and directory durable, for test setup.
func (f *FS) SyncAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[*inode]bool{}
	var walk func(n *inode)
	walk = func(n *inode) {
		if seen[n] {
			return
		}
		seen[n] = true
		if !n.dir {
			n.durable = clone(n.live)
			n.poisoned = nil
			n.syncFailed = false
			return
		}
		n.durableEntries = cloneEntries(n.liveEntries)
		for _, c := range n.liveEntries {
			walk(c)
		}
	}
	walk(f.root)
}

// Unsynced reports how many appended bytes of the file at name are not
// durable: zero when the live bytes are not an extension of the durable ones,
// or when the file does not exist.
func (f *FS) Unsynced(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.lookup(name)
	if err != nil || n.dir || !n.extendsDurable() {
		return 0
	}
	return len(n.live) - len(n.durable)
}

// ProcessCrash models the process dying with the machine up: handles and
// locks are invalidated and the crashed state is cleared, but no file state
// changes, so live data persists.
func (f *FS) ProcessCrash() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidate()
}

// Crash applies power loss as described in the package comment.
func (f *FS) Crash(o CrashOptions) {
	f.mu.Lock()
	defer f.mu.Unlock()

	type result struct {
		ino  *inode
		data []byte
	}
	var files []result
	var dirs []*inode
	seen := map[*inode]bool{}

	// Pass 1 reads only the pre-crash state and decides every outcome.
	var walk func(dir *inode, p string)
	walk = func(dir *inode, p string) {
		if seen[dir] {
			return
		}
		seen[dir] = true
		dirs = append(dirs, dir)
		for _, name := range sortedNames(dir.durableEntries) {
			child := dir.durableEntries[name]
			cp := path.Join(p, name)
			if child.dir {
				walk(child, cp)
				continue
			}
			if seen[child] {
				continue
			}
			seen[child] = true
			files = append(files, result{child, f.outcome(child, cp, o)})
		}
	}
	walk(f.root, "/")

	// Pass 2 installs them.
	for _, r := range files {
		r.ino.durable = clone(r.data)
		r.ino.live = clone(r.data)
		r.ino.poisoned = nil
		r.ino.syncFailed = false
	}
	for _, d := range dirs {
		d.liveEntries = cloneEntries(d.durableEntries)
	}
	f.invalidate()
}

func (f *FS) invalidate() {
	f.epoch++
	f.locks = map[string]int{}
	f.crashed = false
	f.pendingCrash = false
	f.crashAt, f.crashAfter = 0, 0
}

// outcome is the durable (and so live) content of file n after power loss.
func (f *FS) outcome(n *inode, p string, o CrashOptions) []byte {
	d, l := n.durable, n.live
	if !n.extendsDurable() {
		switch o.Truncate {
		case TruncateLost:
			return clone(d)
		case TruncatePersisted:
			return clone(l)
		default:
			if f.rng.Intn(2) == 0 {
				return clone(d)
			}
			return clone(l)
		}
	}

	tail := l[len(d):]
	if len(tail) == 0 {
		return clone(d)
	}
	if keep, ok := tornAt(o.TornTailAt, p); ok {
		return append(clone(d), tail[:clamp(keep, len(tail))]...)
	}
	switch o.Tail {
	case TornTail:
		return append(clone(d), tail[:f.rng.Intn(len(tail)+1)]...)
	case GarbageTail:
		out := clone(d)
		for range tail {
			out = append(out, byte(f.rng.Uint32()&0xff))
		}
		return out
	case ZeroTail:
		return append(clone(d), make([]byte, len(tail))...)
	default:
		// A failed Sync may have flushed some of the bytes before it failed,
		// so the seed decides between keeping all of them and none.
		if n.syncFailed && f.rng.Intn(2) == 0 {
			return clone(l)
		}
		return clone(d)
	}
}

func tornAt(m map[string]int, p string) (int, bool) {
	if k, ok := m[p]; ok {
		return k, true
	}
	k, ok := m[path.Base(p)]
	return k, ok
}

func clamp(k, n int) int {
	return max(0, min(k, n))
}

// extendsDurable reports whether the live bytes are the durable bytes followed
// by zero or more bytes. Positions in a poisoned span (see Options.Fsyncgate)
// are not compared: their durable bytes were never written, so live differing
// there is not an unsynced change.
func (n *inode) extendsDurable() bool {
	if len(n.live) < len(n.durable) {
		return false
	}
	for i := range n.durable {
		if n.durable[i] != n.live[i] && !inSpans(n.poisoned, i) {
			return false
		}
	}
	return true
}

func inSpans(spans []span, i int) bool {
	for _, s := range spans {
		if i >= s.lo && i < s.hi {
			return true
		}
	}
	return false
}

func clone(b []byte) []byte {
	return append([]byte{}, b...)
}

func cloneEntries(m map[string]*inode) map[string]*inode {
	out := make(map[string]*inode, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func sortedNames(m map[string]*inode) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// gate does the bookkeeping every call shares: stale and crashed checks,
// operation counting, CrashAt/CrashAfter, and fault rules. A non-nil err means
// the call must fail without effect; inj is the rule's error, if one fired.
func (f *FS) gate(op Op, name string, h *handle) (inj, err error) {
	if h != nil && h.epoch != f.epoch {
		return nil, ErrStaleHandle
	}
	if f.crashed {
		return nil, ErrCrashed
	}
	f.ops++
	f.log = append(f.log, fmt.Sprintf("%d %s %s", f.ops, op, name))
	if f.crashAt != 0 && f.ops == f.crashAt {
		f.crashed = true
		return nil, ErrCrashed
	}
	if f.crashAfter != 0 && f.ops == f.crashAfter {
		f.pendingCrash = true
	}
	return f.match(op, name), nil
}

func (f *FS) match(op Op, name string) error {
	var inj error
	for _, r := range f.rules {
		if (r.Op != OpAny && r.Op != op) || !globMatch(r.PathGlob, name) {
			continue
		}
		r.seen++
		var fire bool
		switch {
		case r.Prob > 0:
			fire = f.rng.Float64() < r.Prob
		case r.Sticky:
			fire = r.seen >= max(r.Nth, 1)
		default:
			fire = r.seen == r.Nth
		}
		if fire && inj == nil {
			inj = r.Err
		}
	}
	return inj
}

func globMatch(glob, name string) bool {
	if glob == "" {
		return true
	}
	target := name
	if !strings.Contains(glob, "/") {
		target = path.Base(name)
	}
	ok, err := path.Match(glob, target)
	return err == nil && ok
}

// fin turns a CrashAfter that fired during the call into ErrCrashed. It runs
// before the mutex is released.
func (f *FS) fin(err *error) {
	if f.pendingCrash {
		f.pendingCrash = false
		f.crashed = true
		*err = ErrCrashed
	}
}

func perr(op, name string, err error) error {
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func split(name string) []string {
	p := path.Clean("/" + name)
	if p == "/" {
		return nil
	}
	return strings.Split(p[1:], "/")
}

// lookup resolves name in the live tree.
func (f *FS) lookup(name string) (*inode, error) {
	cur := f.root
	for _, c := range split(name) {
		if !cur.dir {
			return nil, syscall.ENOTDIR
		}
		next, ok := cur.liveEntries[c]
		if !ok {
			return nil, fs.ErrNotExist
		}
		cur = next
	}
	return cur, nil
}

// lookupParent resolves the directory holding name and name's base.
func (f *FS) lookupParent(name string) (*inode, string, error) {
	p := path.Clean("/" + name)
	if p == "/" {
		return nil, "", syscall.EINVAL
	}
	dir, err := f.lookup(path.Dir(p))
	if err != nil {
		return nil, "", err
	}
	if !dir.dir {
		return nil, "", syscall.ENOTDIR
	}
	return dir, path.Base(p), nil
}

// MkdirAll creates dir and any missing parents in the live tree.
func (f *FS) MkdirAll(dir string, perm fs.FileMode) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpMkdirAll, dir, nil)
	if err != nil {
		return err
	}
	if inj != nil {
		return perr("mkdir", dir, inj)
	}
	return f.mkdirAll(dir, perm)
}

func (f *FS) mkdirAll(dir string, perm fs.FileMode) error {
	cur := f.root
	for _, c := range split(dir) {
		next, ok := cur.liveEntries[c]
		if !ok {
			next = newDir(perm)
			cur.liveEntries[c] = next
		}
		if !next.dir {
			return perr("mkdir", dir, syscall.ENOTDIR)
		}
		cur = next
	}
	return nil
}

// OpenFile opens name, creating it in the live tree if flag asks.
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (file storage.File, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpOpenFile, name, nil)
	if err != nil {
		return nil, err
	}
	if inj != nil {
		return nil, perr("open", name, inj)
	}

	parent, base, err := f.lookupParent(name)
	if err != nil {
		return nil, perr("open", name, err)
	}
	ino, exists := parent.liveEntries[base]
	switch {
	case exists && flag&syscall.O_CREAT != 0 && flag&syscall.O_EXCL != 0:
		return nil, perr("open", name, fs.ErrExist)
	case exists && ino.dir:
		return nil, perr("open", name, syscall.EISDIR)
	case !exists && flag&syscall.O_CREAT == 0:
		return nil, perr("open", name, fs.ErrNotExist)
	case !exists:
		ino = &inode{mode: perm}
		parent.liveEntries[base] = ino
	}
	if flag&syscall.O_TRUNC != 0 && writable(flag) {
		ino.live = ino.live[:0]
		ino.poisoned = nil
	}
	return &handle{fs: f, ino: ino, name: name, flag: flag, epoch: f.epoch}, nil
}

func writable(flag int) bool {
	return flag&(syscall.O_WRONLY|syscall.O_RDWR) != 0
}

// ReadFile returns a copy of the live bytes of name.
func (f *FS) ReadFile(name string) (data []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpReadFile, name, nil)
	if err != nil {
		return nil, err
	}
	if inj != nil {
		return nil, perr("read", name, inj)
	}
	ino, err := f.lookup(name)
	if err != nil {
		return nil, perr("open", name, err)
	}
	if ino.dir {
		return nil, perr("read", name, syscall.EISDIR)
	}
	return clone(ino.live), nil
}

// ReadDir lists the live entries of dir, sorted by name.
func (f *FS) ReadDir(dir string) (entries []fs.DirEntry, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpReadDir, dir, nil)
	if err != nil {
		return nil, err
	}
	if inj != nil {
		return nil, perr("open", dir, inj)
	}
	ino, err := f.lookup(dir)
	if err != nil {
		return nil, perr("open", dir, err)
	}
	if !ino.dir {
		return nil, perr("readdir", dir, syscall.ENOTDIR)
	}
	for _, n := range sortedNames(ino.liveEntries) {
		entries = append(entries, fs.FileInfoToDirEntry(infoOf(n, ino.liveEntries[n])))
	}
	return entries, nil
}

// Remove deletes name (a file or an empty directory) from the live tree.
func (f *FS) Remove(name string) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpRemove, name, nil)
	if err != nil {
		return err
	}
	if inj != nil {
		return perr("remove", name, inj)
	}
	parent, base, err := f.lookupParent(name)
	if err != nil {
		return perr("remove", name, err)
	}
	ino, ok := parent.liveEntries[base]
	if !ok {
		return perr("remove", name, fs.ErrNotExist)
	}
	if ino.dir && len(ino.liveEntries) > 0 {
		return perr("remove", name, syscall.ENOTEMPTY)
	}
	delete(parent.liveEntries, base)
	return nil
}

// Rename atomically moves oldpath to newpath in the live tree, replacing a
// file at newpath. Both must be in the same directory.
func (f *FS) Rename(oldpath, newpath string) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpRename, oldpath, nil)
	if err != nil {
		return err
	}
	if inj != nil {
		return perr("rename", oldpath+" -> "+newpath, inj)
	}
	linkErr := func(e error) error { return perr("rename", oldpath+" -> "+newpath, e) }

	op, np := path.Clean("/"+oldpath), path.Clean("/"+newpath)
	if path.Dir(op) != path.Dir(np) {
		return linkErr(ErrCrossDirRename)
	}
	parent, oldBase, err := f.lookupParent(op)
	if err != nil {
		return linkErr(err)
	}
	ino, ok := parent.liveEntries[oldBase]
	if !ok {
		return linkErr(fs.ErrNotExist)
	}
	newBase := path.Base(np)
	if target, exists := parent.liveEntries[newBase]; exists {
		if target.dir && !ino.dir {
			return linkErr(syscall.EISDIR)
		}
		if ino.dir && !target.dir {
			return linkErr(syscall.ENOTDIR)
		}
		if target.dir && len(target.liveEntries) > 0 {
			return linkErr(syscall.ENOTEMPTY)
		}
	}
	delete(parent.liveEntries, oldBase)
	parent.liveEntries[newBase] = ino
	return nil
}

// Truncate resizes name in the live tree only; it is durable once the file is synced.
func (f *FS) Truncate(name string, size int64) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpTruncate, name, nil)
	if err != nil {
		return err
	}
	if inj != nil {
		return perr("truncate", name, inj)
	}
	ino, err := f.lookup(name)
	if err != nil {
		return perr("truncate", name, err)
	}
	if ino.dir {
		return perr("truncate", name, syscall.EISDIR)
	}
	if size < 0 {
		return perr("truncate", name, syscall.EINVAL)
	}
	ino.resize(int(size))
	return nil
}

func (n *inode) resize(size int) {
	if size <= len(n.live) {
		n.live = n.live[:size]
	} else {
		n.live = append(n.live, make([]byte, size-len(n.live))...)
	}
	n.poisoned = clip(n.poisoned, size)
}

// SyncDir makes the live entries of dir durable.
func (f *FS) SyncDir(dir string) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpSyncDir, dir, nil)
	if err != nil {
		return err
	}
	if inj != nil {
		return perr("sync", dir, inj)
	}
	ino, err := f.lookup(dir)
	if err != nil {
		return perr("open", dir, err)
	}
	if !ino.dir {
		return perr("sync", dir, syscall.ENOTDIR)
	}
	ino.durableEntries = cloneEntries(ino.liveEntries)
	return nil
}

// Stat describes name in the live tree.
func (f *FS) Stat(name string) (info fs.FileInfo, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpStat, name, nil)
	if err != nil {
		return nil, err
	}
	if inj != nil {
		return nil, perr("stat", name, inj)
	}
	ino, err := f.lookup(name)
	if err != nil {
		return nil, perr("stat", name, err)
	}
	return infoOf(path.Base(path.Clean("/"+name)), ino), nil
}

// Lock takes the simulated exclusive lock on dir, creating dir and its LOCK
// file in the live tree, like the real filesystem does.
func (f *FS) Lock(dir string) (release func() error, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpLock, dir, nil)
	if err != nil {
		return nil, err
	}
	if inj != nil {
		return nil, perr("lock", dir, inj)
	}
	key := path.Clean("/" + dir)
	if _, held := f.locks[key]; held {
		return nil, fmt.Errorf("faultfs: %s is already in use by another holder", dir)
	}
	if err := f.mkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d, err := f.lookup(dir)
	if err != nil {
		return nil, err
	}
	if _, ok := d.liveEntries["LOCK"]; !ok {
		d.liveEntries["LOCK"] = &inode{mode: 0o644}
	}

	f.nextID++
	id, epoch, released := f.nextID, f.epoch, false
	f.locks[key] = id
	return func() (rerr error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		defer f.fin(&rerr)

		if released {
			return nil
		}
		if epoch != f.epoch {
			return ErrStaleHandle
		}
		if _, err := f.gate(OpRelease, dir, nil); err != nil {
			return err
		}
		released = true
		if f.locks[key] == id {
			delete(f.locks, key)
		}
		return nil
	}, nil
}

type handle struct {
	fs     *FS
	ino    *inode
	name   string
	flag   int
	off    int
	epoch  int
	closed bool
}

// Write stores p at the handle's offset (or at the end, for O_APPEND) in the
// live bytes.
func (h *handle) Write(p []byte) (n int, err error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpWrite, h.name, h)
	if err != nil {
		return 0, err
	}
	if h.closed {
		return 0, perr("write", h.name, fs.ErrClosed)
	}
	if !writable(h.flag) {
		return 0, perr("write", h.name, syscall.EBADF)
	}
	if inj != nil {
		var sw *ShortWriteError
		if errors.As(inj, &sw) {
			k := clamp(sw.Keep, len(p))
			h.put(p[:k])
			return k, inj
		}
		return 0, perr("write", h.name, inj)
	}
	h.put(p)
	return len(p), nil
}

func (h *handle) put(p []byte) {
	n := h.ino
	off := h.off
	if h.flag&syscall.O_APPEND != 0 {
		off = len(n.live)
	}
	if off > len(n.live) {
		n.live = append(n.live, make([]byte, off-len(n.live))...)
	}
	end := off + len(p)
	if end > len(n.live) {
		n.live = append(n.live, make([]byte, end-len(n.live))...)
	}
	copy(n.live[off:], p)
	n.poisoned = subtract(n.poisoned, off, end)
	h.off = end
}

// Stat describes the open file itself.
func (h *handle) Stat() (info fs.FileInfo, err error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpFileStat, h.name, h)
	if err != nil {
		return nil, err
	}
	if h.closed {
		return nil, perr("stat", h.name, fs.ErrClosed)
	}
	if inj != nil {
		return nil, perr("stat", h.name, inj)
	}
	return infoOf(path.Base(path.Clean("/"+h.name)), h.ino), nil
}

// Sync makes the file's live bytes and length durable. A failed Sync leaves
// them unsynced; with Options.Fsyncgate the failed bytes also stay non-durable
// through later successful Syncs.
func (h *handle) Sync() (err error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpSync, h.name, h)
	if err != nil {
		return err
	}
	if h.closed {
		return perr("sync", h.name, fs.ErrClosed)
	}
	n := h.ino
	if inj != nil {
		n.syncFailed = true
		if f.opts.Fsyncgate {
			if lo := firstDiff(n.durable, n.live); lo < len(n.live) {
				n.poisoned = append(n.poisoned, span{lo, len(n.live)})
			}
		}
		return perr("sync", h.name, inj)
	}

	d := clone(n.live)
	for _, s := range n.poisoned {
		for i := s.lo; i < s.hi; i++ {
			if i < len(n.durable) {
				d[i] = n.durable[i]
			} else {
				d[i] = 0
			}
		}
	}
	n.durable = d
	n.syncFailed = false
	return nil
}

// Close closes the handle without syncing. A rule-injected error still closes it.
func (h *handle) Close() (err error) {
	f := h.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.fin(&err)

	inj, err := f.gate(OpClose, h.name, h)
	if err != nil {
		return err
	}
	if h.closed {
		return perr("close", h.name, fs.ErrClosed)
	}
	h.closed = true
	if inj != nil {
		return perr("close", h.name, inj)
	}
	return nil
}

func firstDiff(d, l []byte) int {
	n := min(len(d), len(l))
	for i := 0; i < n; i++ {
		if d[i] != l[i] {
			return i
		}
	}
	return n
}

// subtract removes [lo, hi) from the spans.
func subtract(spans []span, lo, hi int) []span {
	var out []span
	for _, s := range spans {
		if s.hi <= lo || s.lo >= hi {
			out = append(out, s)
			continue
		}
		if s.lo < lo {
			out = append(out, span{s.lo, lo})
		}
		if s.hi > hi {
			out = append(out, span{hi, s.hi})
		}
	}
	return out
}

// clip drops everything at or beyond size.
func clip(spans []span, size int) []span {
	var out []span
	for _, s := range spans {
		if s.lo >= size {
			continue
		}
		out = append(out, span{s.lo, min(s.hi, size)})
	}
	return out
}

type fileInfo struct {
	name string
	size int64
	mode fs.FileMode
	dir  bool
}

func infoOf(name string, n *inode) fileInfo {
	fi := fileInfo{name: name, mode: n.mode.Perm(), dir: n.dir}
	if n.dir {
		fi.mode |= fs.ModeDir
	} else {
		fi.size = int64(len(n.live))
	}
	return fi
}

func (i fileInfo) Name() string      { return i.name }
func (i fileInfo) Size() int64       { return i.size }
func (i fileInfo) Mode() fs.FileMode { return i.mode }
func (fileInfo) ModTime() time.Time  { return time.Time{} }
func (i fileInfo) IsDir() bool       { return i.dir }
func (fileInfo) Sys() any            { return nil }
