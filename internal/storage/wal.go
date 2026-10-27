package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

	// FS is the filesystem the log lives on. Nil means the real OS.
	FS FS

	// OnRepair, if set, is called once for every tail repair Open performs,
	// after the repair is durable. It means "a repair was performed": the
	// caller may still fail to start afterwards (for example on a snapshot
	// check in OpenDiskStorage), and the repair is not undone.
	OnRepair func(RepairInfo)
}

// RepairInfo describes one repair of a bad record at the tail of the last
// segment: Segment is the file name, Offset the first dropped byte, Bytes how
// many bytes were dropped and Cause the error that made the record unreadable.
type RepairInfo struct {
	Segment string
	Offset  int64
	Bytes   int64
	Cause   error
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

	fs          FS
	dir         string
	policy      SyncPolicy
	segmentSize int64

	segments []segment

	onRepair func(RepairInfo)

	active     File
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

	fsys := opts.FS
	if fsys == nil {
		fsys = osFS{}
	}

	if err := fsys.MkdirAll(opts.Dir, dirMode); err != nil {
		return nil, Replay{}, fmt.Errorf("storage: creating WAL directory: %w", err)
	}

	w := &WAL{
		fs:          fsys,
		dir:         opts.Dir,
		policy:      opts.Sync,
		segmentSize: opts.SegmentSize,
		onRepair:    opts.OnRepair,
	}

	segments, err := listSegments(fsys, opts.Dir)
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

func listSegments(fsys FS, dir string) ([]segment, error) {
	entries, err := fsys.ReadDir(dir)
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

		data, err := w.fs.ReadFile(filepath.Join(w.dir, seg.name))
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

		// Only the last segment can hold a torn write, and only a framing
		// failure (incomplete, bad checksum, oversize length) at the record
		// boundary can be one. Key on what readRecord says about the bytes at
		// good, not on errors.Is of the replay error: the payload decoders
		// also wrap ErrCorruptRecord, and a record whose checksum is valid but
		// whose type or payload is bad is corruption, not a tear (RUNBOOK 7.3).
		_, _, _, framing := readRecord(data[good:])
		if framing == nil {
			return Replay{}, fmt.Errorf("%w: segment %s holds an undecodable record at offset %d: %w",
				ErrCorruptWAL, seg.name, good, err)
		}

		// A crash can leave garbage only at the end of the file. If anything
		// after the bad record is a valid record, the damage is mid-log and
		// truncating would silently drop data that was acknowledged
		// (RUNBOOK 7.2 item 1, 7.3 #2): refuse and leave the file alone.
		found, exhausted := validRecordFollows(data, good+1)
		if found {
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d with valid records after it: %w",
				ErrCorruptWAL, seg.name, good, framing)
		}
		if exhausted {
			// Could not show that nothing valid follows within the checksum
			// budget. Refusing is the safe direction; truncating when unsure
			// could drop acknowledged records.
			return Replay{}, fmt.Errorf("%w: segment %s is damaged at offset %d and the tail is too large to prove it holds no valid record: %w",
				ErrCorruptWAL, seg.name, good, framing)
		}

		// Residual risk: a record that was fsynced and later rotted on the
		// media, with nothing valid after it, looks exactly like a tear and is
		// dropped here. Requiring valid data to follow keeps that to the final
		// record(s) of the log.
		if err := w.repairTail(seg.name, int64(good), int64(len(data)), framing); err != nil {
			return Replay{}, err
		}
		rep.Repaired = true
	}

	return rep, nil
}

// repairTail cuts the segment back to good and makes the new size durable
// before Open returns (RUNBOOK 7.3 #2): truncate, fsync the file, fsync the
// directory.
func (w *WAL) repairTail(name string, good, size int64, cause error) error {
	path := filepath.Join(w.dir, name)

	if err := w.fs.Truncate(path, good); err != nil {
		return fmt.Errorf("storage: truncating damaged segment %s: %w", name, err)
	}

	f, err := w.fs.OpenFile(path, os.O_WRONLY, fileMode)
	if err != nil {
		return fmt.Errorf("storage: opening repaired segment %s to sync it: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: syncing repaired segment %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: closing repaired segment %s: %w", name, err)
	}

	if err := w.fs.SyncDir(w.dir); err != nil {
		return err
	}

	info := RepairInfo{Segment: name, Offset: good, Bytes: size - good, Cause: cause}
	slog.Warn("repaired the tail of the last WAL segment",
		"segment", info.Segment, "offset", info.Offset, "dropped_bytes", info.Bytes, "cause", info.Cause)
	if w.onRepair != nil {
		w.onRepair(info)
	}
	return nil
}

// maxScanChecksumBytes bounds the total bytes the tail scan may checksum. Each
// candidate offset can claim a body of up to maxRecordSize that overlaps the
// next candidate's, so without a bound the scan is quadratic: a tail of
// 0x01 bytes makes every offset checksum about 16 MiB. A real torn or rotted
// tail trips only a handful of candidates. When the bound is hit the scan
// reports exhausted and the caller refuses to start, never truncates.
const maxScanChecksumBytes = 64 << 20

// Smallest legal body (type byte plus payload) per record type, and the
// exact body size of the fixed-size types, from the decoders in record.go.
const (
	minEntryBody   = typeSize + 4*8 // term, index, type, data length
	fixedStateBody = typeSize + 2*8 // HardState and SnapshotMeta
)

// validRecordFollows reports whether any offset at or after from holds a
// record that frames correctly, has a known type and decodes. Candidates are
// first filtered on their header (known type, length that fits the file and
// is possible for the type) without touching the body, and each remaining
// candidate's checksum is charged against maxScanChecksumBytes. If the budget
// runs out before the scan finishes, exhausted is true and found is false.
func validRecordFollows(data []byte, from int) (found, exhausted bool) {
	budget := maxScanChecksumBytes
	for p := from; p < len(data); p++ {
		if len(data)-p < headerSize+typeSize {
			break
		}
		bodyLen := int(binary.LittleEndian.Uint32(data[p : p+lenSize]))
		if bodyLen < typeSize || bodyLen > maxRecordSize || bodyLen > len(data)-p-headerSize {
			continue
		}
		switch recordType(data[p+headerSize]) {
		case recordEntry:
			if bodyLen < minEntryBody {
				continue
			}
		case recordHardState, recordSnapshotMeta:
			if bodyLen != fixedStateBody {
				continue
			}
		default:
			continue
		}

		if bodyLen > budget {
			return false, true
		}
		budget -= bodyLen

		typ, payload, _, err := readRecord(data[p:])
		if err != nil {
			continue
		}
		if validatePayload(typ, payload) == nil {
			return true, false
		}
	}
	return false, false
}

// validatePayload reports whether payload decodes as record type typ. It is
// the one place that knows which record types exist besides replaySegment.
func validatePayload(typ recordType, payload []byte) error {
	var err error
	switch typ {
	case recordEntry:
		_, err = decodeEntry(payload)
	case recordHardState:
		_, err = decodeHardState(payload)
	case recordSnapshotMeta:
		_, err = decodeSnapshotMeta(payload)
	default:
		err = fmt.Errorf("%w: unknown record type %s", ErrCorruptWAL, typ)
	}
	return err
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

	f, err := w.fs.OpenFile(path, os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return fmt.Errorf("storage: opening active segment %s: %w", seg.name, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: stat active segment %s: %w", seg.name, err)
	}

	w.active = f
	w.activeSize = info.Size()
	return nil
}

func (w *WAL) roll(seq uint64, firstIndex raft.Index) error {
	name := segmentName(seq, firstIndex)
	path := filepath.Join(w.dir, name)

	f, err := w.fs.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return fmt.Errorf("storage: creating segment %s: %w", name, err)
	}

	if err := w.fs.SyncDir(w.dir); err != nil {
		_ = f.Close()
		return err
	}

	if w.active != nil {
		if err := w.active.Close(); err != nil {
			_ = f.Close()
			return fmt.Errorf("storage: closing previous segment: %w", err)
		}
	}

	w.active = f
	w.activeSize = 0
	w.segments = append(w.segments, segment{seq: seq, firstIndex: firstIndex, name: name})
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
		if err := w.fs.Remove(filepath.Join(w.dir, seg.name)); err != nil {
			return fmt.Errorf("storage: removing superseded segment %s: %w", seg.name, err)
		}
	}
	if err := w.fs.SyncDir(w.dir); err != nil {
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
		_ = w.active.Close()
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
