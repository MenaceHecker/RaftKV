package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The write-ahead log.
//
// A WAL is a directory of append-only segment files. Nothing is ever modified
// in place: a follower whose log conflicts with the leader's does not rewrite
// history, it simply appends the leader's version, and replay resolves the
// conflict by letting the later record for an index win. That keeps every
// write sequential and makes the crash story simple, because a torn record can
// only ever be the last one in the newest segment.
//
// Segments exist so that log compaction is a file deletion rather than a
// rewrite. Once a snapshot covers everything in a segment, that whole file can
// be unlinked.

// SyncPolicy controls how aggressively the WAL forces data to stable storage.
//
// This is the durability/throughput dial, and it is deliberately explicit
// rather than tuned: Raft's guarantees hold only as far as the fsync does. A
// node running with SyncNever can acknowledge a write, lose power, and come
// back missing an entry it already told the leader it had — which breaks
// exactly the assumption the commit rule depends on.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every append before returning. This is what
	// Raft's safety argument assumes, and it is the only correct setting for
	// a real deployment.
	SyncAlways SyncPolicy = iota

	// SyncNever leaves flushing to the operating system. Writes survive a
	// process crash, because the data is already in the page cache, but not
	// a power loss or kernel panic. Useful for tests and for benchmarking
	// the cost of durability, not for production.
	SyncNever
)

func (p SyncPolicy) String() string {
	switch p {
	case SyncAlways:
		return "always"
	case SyncNever:
		return "never"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
	}
}

// Default sizing for a WAL.
const (
	// DefaultSegmentSize is how large a segment grows before the WAL rolls
	// over to a new one. Smaller segments make compaction finer-grained;
	// larger ones mean fewer file creations.
	DefaultSegmentSize = 16 << 20 // 16 MiB

	segmentSuffix = ".wal"
	dirMode       = 0o755
	fileMode      = 0o644
)

// ErrCorruptWAL means a segment is damaged somewhere other than the tail of
// the newest file. A torn tail is an expected consequence of a crash and is
// repaired silently; damage anywhere else was not caused by an interrupted
// append, so it is surfaced rather than papered over.
var ErrCorruptWAL = errors.New("storage: write-ahead log is corrupt")

// Options configures a WAL.
type Options struct {
	// Dir is the directory holding the segment files. It is created if it
	// does not exist.
	Dir string

	// Sync selects the durability policy. The zero value is SyncAlways,
	// so the safe setting is what you get by default.
	Sync SyncPolicy

	// SegmentSize is the rollover threshold in bytes. Zero means
	// DefaultSegmentSize.
	SegmentSize int64
}

// Replay is the state reconstructed from the WAL at startup.
type Replay struct {
	// HardState is the most recently persisted term and vote. It is the zero
	// value if the node has never voted.
	HardState raft.HardState

	// Entries is the log, in index order, with conflicts already resolved.
	Entries []raft.Entry

	// Snapshot is the most recent snapshot point recorded in the log, or the
	// zero value if none was ever taken.
	Snapshot SnapshotMeta

	// Repaired reports whether a torn record was found and truncated. It is
	// true exactly when the previous process died mid-append, which makes it
	// worth surfacing in metrics rather than leaving silent.
	Repaired bool
}

// segment is one file in the log.
type segment struct {
	// seq orders the segments. It increases by one on every rollover.
	seq uint64

	// firstIndex is the index of the first entry this segment can contain.
	// It is recorded in the filename so that compaction can decide which
	// files are fully superseded without opening any of them.
	firstIndex raft.Index

	name string
}

// WAL is an append-only log spread across segment files in a directory.
//
// It is safe for concurrent use. In practice the Raft core drives it from a
// single goroutine, but the lock also protects against a concurrent Sync or
// compaction from a background task.
type WAL struct {
	mu sync.Mutex

	dir         string
	policy      SyncPolicy
	segmentSize int64

	// segments is ordered by sequence number; the last is the active one.
	segments []segment

	// active is the open file handle for the newest segment. All writes go
	// here.
	active     *os.File
	activeSize int64

	// hardState is the most recent hard state written. Compaction rewrites
	// it into the active segment before deleting older files, so that
	// deleting the file that happened to hold it cannot lose the vote.
	hardState raft.HardState

	// lastIndex is the highest entry index written so far, used to name new
	// segments.
	lastIndex raft.Index

	closed bool
}

// Open opens or creates a WAL in the given directory and replays it.
//
// A crash that left a partially written record at the tail is repaired here:
// the file is truncated back to the last complete record, so subsequent
// appends do not follow garbage. The repair is reported through Replay.
func Open(opts Options) (*WAL, Replay, error) {
	if opts.Dir == "" {
		return nil, Replay{}, errors.New("storage: WAL directory must not be empty")
	}
	if opts.SegmentSize == 0 {
		opts.SegmentSize = DefaultSegmentSize
	}
	if opts.SegmentSize < 0 {
		return nil, Replay{}, fmt.Errorf("storage: segment size must be positive, got %d", opts.SegmentSize)
	}

	if err := os.MkdirAll(opts.Dir, dirMode); err != nil {
		return nil, Replay{}, fmt.Errorf("storage: creating WAL directory: %w", err)
	}

	w := &WAL{
		dir:         opts.Dir,
		policy:      opts.Sync,
		segmentSize: opts.SegmentSize,
	}

	segments, err := listSegments(opts.Dir)
	if err != nil {
		return nil, Replay{}, err
	}
	w.segments = segments

	replay, err := w.replay()
	if err != nil {
		return nil, Replay{}, err
	}

	w.hardState = replay.HardState
	if n := len(replay.Entries); n > 0 {
		w.lastIndex = replay.Entries[n-1].Index
	} else {
		w.lastIndex = replay.Snapshot.Index
	}

	if err := w.openActive(); err != nil {
		return nil, Replay{}, err
	}

	return w, replay, nil
}

// listSegments returns the segment files in the directory, ordered by
// sequence number.
func listSegments(dir string) ([]segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("storage: reading WAL directory: %w", err)
	}

	var segments []segment
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentSuffix) {
			continue
		}
		s, err := parseSegmentName(e.Name())
		if err != nil {
			// An unrecognized file in the WAL directory is not something to
			// guess about: it might be a partially created segment, or it
			// might be someone else's data.
			return nil, fmt.Errorf("storage: unexpected file %q in WAL directory: %w", e.Name(), err)
		}
		segments = append(segments, s)
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })
	return segments, nil
}

// segmentName builds the filename for a segment. Both the sequence number and
// the first index are zero-padded so that lexical order matches numeric order,
// which makes a directory listing readable in the order the log was written.
func segmentName(seq uint64, firstIndex raft.Index) string {
	return fmt.Sprintf("%016d-%016d%s", seq, uint64(firstIndex), segmentSuffix)
}

// parseSegmentName recovers a segment's sequence number and first index from
// its filename.
func parseSegmentName(name string) (segment, error) {
	base := strings.TrimSuffix(name, segmentSuffix)
	seqStr, idxStr, ok := strings.Cut(base, "-")
	if !ok {
		return segment{}, errors.New("expected a name of the form <seq>-<index>.wal")
	}

	var seq, idx uint64
	if _, err := fmt.Sscanf(seqStr, "%d", &seq); err != nil {
		return segment{}, fmt.Errorf("parsing sequence number: %w", err)
	}
	if _, err := fmt.Sscanf(idxStr, "%d", &idx); err != nil {
		return segment{}, fmt.Errorf("parsing first index: %w", err)
	}

	return segment{seq: seq, firstIndex: raft.Index(idx), name: name}, nil
}

// replay reads every segment in order and reconstructs the log.
func (w *WAL) replay() (Replay, error) {
	var rep Replay

	for i, seg := range w.segments {
		isLast := i == len(w.segments)-1

		data, err := os.ReadFile(filepath.Join(w.dir, seg.name))
		if err != nil {
			return Replay{}, fmt.Errorf("storage: reading segment %s: %w", seg.name, err)
		}

		good, err := w.replaySegment(data, &rep)
		if err == nil {
			continue
		}

		// A torn or corrupt record is only explicable at the very end of the
		// newest segment, where a crash could have interrupted the write.
		// Anywhere else the file was damaged by something other than an
		// interrupted append, and silently discarding the remainder could
		// throw away committed entries.
		if !isLast {
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d: %w",
				ErrCorruptWAL, seg.name, good, err)
		}
		if !errors.Is(err, ErrTornRecord) {
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d: %w",
				ErrCorruptWAL, seg.name, good, err)
		}

		// Truncate away the partial record so later appends start from a
		// clean boundary.
		path := filepath.Join(w.dir, seg.name)
		if err := os.Truncate(path, int64(good)); err != nil {
			return Replay{}, fmt.Errorf("storage: truncating torn segment %s: %w", seg.name, err)
		}
		if err := syncDir(w.dir); err != nil {
			return Replay{}, err
		}
		rep.Repaired = true
	}

	return rep, nil
}

// replaySegment decodes one segment's records into rep. It returns the offset
// of the first byte that is not part of a complete, valid record, which is
// where the file must be truncated if that byte marks a torn tail.
func (w *WAL) replaySegment(data []byte, rep *Replay) (int, error) {
	offset := 0

	for offset < len(data) {
		typ, payload, n, err := readRecord(data[offset:])
		if err != nil {
			return offset, err
		}

		switch typ {
		case recordEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return offset, err
			}
			rep.Entries = appendResolvingConflict(rep.Entries, e)

		case recordHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return offset, err
			}
			// Later records supersede earlier ones; the last one written is
			// the node's real term and vote.
			rep.HardState = hs

		case recordSnapshotMeta:
			meta, err := decodeSnapshotMeta(payload)
			if err != nil {
				return offset, err
			}
			rep.Snapshot = meta

		default:
			return offset, fmt.Errorf("%w: unknown record type %s", ErrCorruptWAL, typ)
		}

		offset += n
	}

	return offset, nil
}

// appendResolvingConflict adds e to entries, honouring the rule that a later
// record for an index supersedes an earlier one.
//
// This is what lets the WAL stay purely append-only. When a follower's log
// diverges from the leader's, the Raft core overwrites the conflicting suffix;
// on disk that overwrite is just more appends, and replay reconstructs the
// intended result by dropping everything from the repeated index onward before
// adding the newer entry.
func appendResolvingConflict(entries []raft.Entry, e raft.Entry) []raft.Entry {
	if n := len(entries); n > 0 && e.Index <= entries[n-1].Index {
		// Find the first entry the new one supersedes and cut from there.
		cut := sort.Search(n, func(i int) bool { return entries[i].Index >= e.Index })
		entries = entries[:cut]
	}
	return append(entries, e)
}

// openActive opens the newest segment for appending, creating the first one if
// the directory is empty.
func (w *WAL) openActive() error {
	if len(w.segments) == 0 {
		return w.roll(1, w.lastIndex+1)
	}

	seg := w.segments[len(w.segments)-1]
	path := filepath.Join(w.dir, seg.name)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return fmt.Errorf("storage: opening active segment %s: %w", seg.name, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("storage: stat active segment %s: %w", seg.name, err)
	}

	w.active = f
	w.activeSize = info.Size()
	return nil
}

// roll creates a new segment and makes it active.
func (w *WAL) roll(seq uint64, firstIndex raft.Index) error {
	name := segmentName(seq, firstIndex)
	path := filepath.Join(w.dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return fmt.Errorf("storage: creating segment %s: %w", name, err)
	}

	// Creating a file is a directory modification, and on most filesystems
	// that is not durable until the directory itself is synced. Without this,
	// a crash could leave the segment's contents on disk but the directory
	// entry missing, and the records would be unreachable.
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return err
	}

	if w.active != nil {
		if err := w.active.Close(); err != nil {
			f.Close()
			return fmt.Errorf("storage: closing previous segment: %w", err)
		}
	}

	w.active = f
	w.activeSize = 0
	w.segments = append(w.segments, segment{seq: seq, firstIndex: firstIndex, name: name})
	return nil
}

// syncDir fsyncs a directory so that file creations and deletions within it
// are durable.
func syncDir(dir string) error {
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

// AppendEntries durably appends entries to the log.
//
// Entries need not continue from the previous append: a follower resolving a
// conflict re-appends from an earlier index, and replay resolves it. What is
// required is that entries be contiguous among themselves.
func (w *WAL) AppendEntries(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("storage: entries are not contiguous: %d follows %d",
				entries[i].Index, entries[i-1].Index)
		}
	}

	// Build the whole batch first, so a multi-entry append is one write
	// syscall and one fsync rather than one of each per entry.
	var buf []byte
	for _, e := range entries {
		buf = appendRecord(buf, recordEntry, encodeEntry(nil, e))
	}

	if err := w.write(buf); err != nil {
		return err
	}

	w.lastIndex = entries[len(entries)-1].Index
	return w.maybeRoll()
}

// SaveHardState durably records the term and vote.
//
// The Raft core calls this before responding to a vote request or acting on a
// new term, so this is the write whose durability Election Safety rests on.
func (w *WAL) SaveHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}

	buf := appendRecord(nil, recordHardState, encodeHardState(nil, hs))
	if err := w.write(buf); err != nil {
		return err
	}

	w.hardState = hs
	return w.maybeRoll()
}

// SaveSnapshotMeta records that a snapshot was taken through a given index, so
// that replay knows where the log's meaningful history begins.
func (w *WAL) SaveSnapshotMeta(meta SnapshotMeta) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}

	buf := appendRecord(nil, recordSnapshotMeta, encodeSnapshotMeta(nil, meta))
	if err := w.write(buf); err != nil {
		return err
	}
	return w.maybeRoll()
}

// write appends raw bytes to the active segment and applies the sync policy.
// The caller must hold the lock.
func (w *WAL) write(buf []byte) error {
	n, err := w.active.Write(buf)
	w.activeSize += int64(n)
	if err != nil {
		return fmt.Errorf("storage: writing to WAL: %w", err)
	}

	if w.policy == SyncAlways {
		if err := w.active.Sync(); err != nil {
			return fmt.Errorf("storage: syncing WAL: %w", err)
		}
	}
	return nil
}

// maybeRoll starts a new segment once the active one has grown past the
// threshold. The caller must hold the lock.
func (w *WAL) maybeRoll() error {
	if w.activeSize < w.segmentSize {
		return nil
	}
	last := w.segments[len(w.segments)-1]
	return w.roll(last.seq+1, w.lastIndex+1)
}

// Sync forces everything written so far to stable storage. It is a no-op under
// SyncAlways, where every append has already been synced.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}
	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("storage: syncing WAL: %w", err)
	}
	return nil
}

// TruncateBefore deletes segments made redundant by a snapshot covering
// everything up to and including index.
//
// A segment can only go if every entry it holds is at or below index, which is
// knowable from the next segment's first index without opening any file. The
// active segment is never deleted.
func (w *WAL) TruncateBefore(index raft.Index) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}

	// Hard state records live in segments alongside entries, so deleting an
	// old segment could take the most recent vote with it. Rewriting it into
	// the active segment first makes the deletion safe regardless of which
	// file happened to hold it.
	if !w.hardState.IsEmpty() {
		buf := appendRecord(nil, recordHardState, encodeHardState(nil, w.hardState))
		if err := w.write(buf); err != nil {
			return err
		}
	}

	keepFrom := 0
	for i := 0; i+1 < len(w.segments); i++ {
		// Segment i holds entries below segments[i+1].firstIndex. If that
		// boundary is already covered by the snapshot, nothing in segment i
		// is still needed.
		if w.segments[i+1].firstIndex > index+1 {
			break
		}
		keepFrom = i + 1
	}

	if keepFrom == 0 {
		return nil
	}

	for _, seg := range w.segments[:keepFrom] {
		if err := os.Remove(filepath.Join(w.dir, seg.name)); err != nil {
			return fmt.Errorf("storage: removing superseded segment %s: %w", seg.name, err)
		}
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}

	w.segments = append([]segment(nil), w.segments[keepFrom:]...)
	return nil
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.active == nil {
		return nil
	}
	if err := w.active.Sync(); err != nil {
		w.active.Close()
		return fmt.Errorf("storage: syncing WAL on close: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return fmt.Errorf("storage: closing WAL: %w", err)
	}
	return nil
}

// SegmentCount reports how many segment files exist, for tests and metrics.
func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segments)
}

// interface assertion: the WAL must satisfy io.Closer so a node's shutdown
// path can treat it uniformly with its other resources.
var _ io.Closer = (*WAL)(nil)
