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

type SyncPolicy int

const (
	SyncAlways SyncPolicy = iota

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

const (
	DefaultSegmentSize = 16 << 20

	segmentSuffix = ".wal"
	dirMode       = 0o755
	fileMode      = 0o644
)

var ErrCorruptWAL = errors.New("storage: write-ahead log is corrupt")

type Options struct {
	Dir string

	Sync SyncPolicy

	SegmentSize int64
}

type Replay struct {
	HardState raft.HardState

	Entries []raft.Entry

	Snapshot SnapshotMeta

	Repaired bool
}

type segment struct {
	seq uint64

	firstIndex raft.Index

	name string
}

type WAL struct {
	mu sync.Mutex

	dir         string
	policy      SyncPolicy
	segmentSize int64

	segments []segment

	active     *os.File
	activeSize int64

	hardState raft.HardState

	lastIndex raft.Index

	closed bool
}

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
			return nil, fmt.Errorf("storage: unexpected file %q in WAL directory: %w", e.Name(), err)
		}
		segments = append(segments, s)
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].seq < segments[j].seq })
	return segments, nil
}

func segmentName(seq uint64, firstIndex raft.Index) string {
	return fmt.Sprintf("%016d-%016d%s", seq, uint64(firstIndex), segmentSuffix)
}

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

		if !isLast {
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d: %w",
				ErrCorruptWAL, seg.name, good, err)
		}
		if !errors.Is(err, ErrTornRecord) {
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d: %w",
				ErrCorruptWAL, seg.name, good, err)
		}

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

func appendResolvingConflict(entries []raft.Entry, e raft.Entry) []raft.Entry {
	if n := len(entries); n > 0 && e.Index <= entries[n-1].Index {
		cut := sort.Search(n, func(i int) bool { return entries[i].Index >= e.Index })
		entries = entries[:cut]
	}
	return append(entries, e)
}

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

func (w *WAL) roll(seq uint64, firstIndex raft.Index) error {
	name := segmentName(seq, firstIndex)
	path := filepath.Join(w.dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return fmt.Errorf("storage: creating segment %s: %w", name, err)
	}

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

func (w *WAL) maybeRoll() error {
	if w.activeSize < w.segmentSize {
		return nil
	}
	last := w.segments[len(w.segments)-1]
	return w.roll(last.seq+1, w.lastIndex+1)
}

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

func (w *WAL) TruncateBefore(index raft.Index) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("storage: WAL is closed")
	}

	if !w.hardState.IsEmpty() {
		buf := appendRecord(nil, recordHardState, encodeHardState(nil, w.hardState))
		if err := w.write(buf); err != nil {
			return err
		}
	}

	keepFrom := 0
	for i := 0; i+1 < len(w.segments); i++ {
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

func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segments)
}

var _ io.Closer = (*WAL)(nil)
