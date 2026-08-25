package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// DiskStorage binds the write-ahead log and the snapshot store into a single
// raft.Storage, giving the consensus core durability without teaching it
// anything about files.
//
// Reads are served from memory and writes go to disk first. That split is the
// point: Raft asks for arbitrary ranges of the log constantly — every heartbeat
// to a lagging follower re-reads entries — and answering those from the page
// cache through a decoder would make replication cost far more than it should.
// The WAL is the authority on what survives a crash; the in-memory copy is a
// cache that is rebuilt from it at startup and can never diverge, because
// nothing updates memory unless the disk write already succeeded.
type DiskStorage struct {
	mu sync.RWMutex

	wal       *WAL
	snapshots *Snapshotter

	hardState raft.HardState

	// entries holds the log in index order, with one subtlety: entries[0] is
	// a placeholder rather than a real entry. It carries the index and term
	// of the last compacted position — the snapshot point — so that questions
	// about the boundary have an answer after the entry itself is gone.
	//
	// A leader replicating to a follower asks for the term at the index
	// immediately before what it is sending, and that index can be exactly
	// the snapshot point. Without the placeholder that lookup fails and the
	// follower can never be caught up.
	entries []raft.Entry

	closed bool
}

// DiskConfig configures a DiskStorage.
type DiskConfig struct {
	// Dir is the node's data directory. The write-ahead log and snapshots
	// live in subdirectories of it, so one node's entire durable state is one
	// directory that can be copied, archived, or deleted as a unit.
	Dir string

	// Sync selects the WAL's durability policy. The zero value, SyncAlways,
	// is what Raft's guarantees assume.
	Sync SyncPolicy

	// SegmentSize is the WAL's rollover threshold. Zero means the default.
	SegmentSize int64

	// SnapshotsKept is how many snapshots to retain after compaction. More
	// than one means a damaged newest snapshot is survivable. Zero means the
	// default.
	SnapshotsKept int
}

// DefaultSnapshotsKept is how many snapshots are retained by default. Keeping
// a spare is what lets recovery fall back when the newest one cannot be read.
const DefaultSnapshotsKept = 3

const (
	walSubdir      = "wal"
	snapshotSubdir = "snap"
)

var errClosed = errors.New("storage: disk storage is closed")

// OpenDiskStorage opens or creates a node's durable state and reconstructs it.
//
// It returns the snapshot the caller must restore into its state machine
// before applying anything from the log. That snapshot is the zero value when
// the node has never taken one, which a caller distinguishes by its Index
// being zero.
//
// Recovery order matters and is the whole reason this function exists rather
// than the caller wiring the two stores together: the snapshot establishes the
// state as of some index, and only entries after that index are replayed on
// top. Replaying from the start instead would be correct but slow; replaying
// the wrong prefix would be silently wrong.
func OpenDiskStorage(cfg DiskConfig) (*DiskStorage, Snapshot, error) {
	if cfg.Dir == "" {
		return nil, Snapshot{}, errors.New("storage: data directory must not be empty")
	}
	if cfg.SnapshotsKept == 0 {
		cfg.SnapshotsKept = DefaultSnapshotsKept
	}
	if cfg.SnapshotsKept < 1 {
		return nil, Snapshot{}, fmt.Errorf("storage: SnapshotsKept must be at least 1, got %d",
			cfg.SnapshotsKept)
	}

	snapshots, err := NewSnapshotter(filepath.Join(cfg.Dir, snapshotSubdir))
	if err != nil {
		return nil, Snapshot{}, err
	}

	// Load the snapshot first: it sets the floor below which log entries are
	// no longer interesting.
	snap, err := snapshots.Load()
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		return nil, Snapshot{}, err
	}

	wal, replay, err := Open(Options{
		Dir:         filepath.Join(cfg.Dir, walSubdir),
		Sync:        cfg.Sync,
		SegmentSize: cfg.SegmentSize,
	})
	if err != nil {
		return nil, Snapshot{}, err
	}

	// The WAL records where the last snapshot was taken. If a snapshot file
	// was lost but the log still remembers it, the log alone cannot rebuild
	// the state it covered, and starting from an empty state machine would
	// silently lose committed data.
	if replay.Snapshot.Index > snap.Meta.Index {
		wal.Close()
		return nil, Snapshot{}, fmt.Errorf(
			"%w: the log records a snapshot at index %d but the newest readable snapshot is at %d",
			ErrCorruptWAL, replay.Snapshot.Index, snap.Meta.Index)
	}

	s := &DiskStorage{
		wal:       wal,
		snapshots: snapshots,
		hardState: replay.HardState,
		entries:   []raft.Entry{{Index: snap.Meta.Index, Term: snap.Meta.Term}},
	}

	// Keep only the entries the snapshot does not already account for.
	for _, e := range replay.Entries {
		if e.Index <= snap.Meta.Index {
			continue
		}
		s.entries = append(s.entries, e)
	}

	if err := s.validateContiguous(); err != nil {
		wal.Close()
		return nil, Snapshot{}, err
	}

	return s, snap, nil
}

// validateContiguous checks that the reconstructed log has no holes. A gap
// would mean the snapshot and the log do not meet, which recovery cannot
// repair and must not paper over.
func (s *DiskStorage) validateContiguous() error {
	for i := 1; i < len(s.entries); i++ {
		if s.entries[i].Index != s.entries[i-1].Index+1 {
			return fmt.Errorf("%w: recovered log jumps from index %d to %d",
				ErrCorruptWAL, s.entries[i-1].Index, s.entries[i].Index)
		}
	}
	return nil
}

// offset returns the index of the placeholder, which is the last compacted
// position. The caller must hold the lock.
func (s *DiskStorage) offset() raft.Index {
	return s.entries[0].Index
}

// InitialState implements raft.Storage.
func (s *DiskStorage) InitialState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

// SetHardState implements raft.Storage.
func (s *DiskStorage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}

	// Disk first. Updating memory before the write succeeded would let the
	// node act on a term it has not durably recorded, which is precisely the
	// situation the hard state exists to prevent.
	if err := s.wal.SaveHardState(hs); err != nil {
		return err
	}
	s.hardState = hs
	return nil
}

// FirstIndex implements raft.Storage.
func (s *DiskStorage) FirstIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset() + 1
}

// LastIndex implements raft.Storage.
func (s *DiskStorage) LastIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *DiskStorage) lastIndexLocked() raft.Index {
	return s.offset() + raft.Index(len(s.entries)) - 1
}

// Term implements raft.Storage.
func (s *DiskStorage) Term(i raft.Index) (raft.Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// The placeholder answers for the compaction boundary, which is what a
	// leader asks about when replicating the first entry after a snapshot.
	if i < s.offset() {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndexLocked() {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-s.offset()].Term, nil
}

// Entries implements raft.Storage.
func (s *DiskStorage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, nil
	}
	// lo must name a real entry, not the placeholder.
	if lo <= s.offset() {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndexLocked()+1 {
		return nil, raft.ErrUnavailable
	}

	// Copy: the caller must not be able to reach into the cache, and a later
	// Append that grows the backing array would otherwise alias it.
	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-s.offset():hi-s.offset()])
	return out, nil
}

// Append implements raft.Storage.
func (s *DiskStorage) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}

	first := entries[0].Index
	last := s.lastIndexLocked()

	if first > last+1 {
		return fmt.Errorf("storage: append at index %d leaves a gap after last index %d",
			first, last)
	}

	// Entries at or below the compaction point are already reflected in the
	// snapshot. A stale retransmission can carry them, and re-adding them
	// would put the cache behind the snapshot it is supposed to follow.
	if first <= s.offset() {
		trim := s.offset() + 1 - first
		if int(trim) >= len(entries) {
			return nil
		}
		entries = entries[trim:]
		first = entries[0].Index
	}

	// The WAL is append-only even when the Raft core is overwriting a
	// conflicting suffix: replay resolves it by letting the later record for
	// an index win. So the disk write is the same in both cases.
	if err := s.wal.AppendEntries(entries); err != nil {
		return err
	}

	// Drop any conflicting suffix from the cache and add the new entries. On
	// a leader this is a plain append; on a follower it is the §5.3 overwrite.
	keep := first - s.offset()
	s.entries = append(s.entries[:keep:keep], entries...)
	return nil
}

// CreateSnapshot records a snapshot of the state machine at index and compacts
// everything up to it out of the log.
//
// The caller supplies the serialized state and the cluster configuration as of
// that index, and must have applied exactly through index to produce them. The
// configuration has to be recorded here because compaction is about to delete
// the conf-change entries it was derived from.
//
// Ordering is deliberate and load-bearing:
// the snapshot file is published first, then the log records that it exists,
// and only then is anything deleted. A crash between any two steps leaves the
// node recoverable, because nothing is discarded until its replacement is
// durable.
func (s *DiskStorage) CreateSnapshot(index raft.Index, data []byte, conf raft.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}
	if index <= s.offset() {
		return fmt.Errorf("storage: snapshot at index %d is at or before the last one at %d",
			index, s.offset())
	}
	if index > s.lastIndexLocked() {
		return fmt.Errorf("storage: cannot snapshot index %d, the log ends at %d",
			index, s.lastIndexLocked())
	}

	term := s.entries[index-s.offset()].Term
	meta := SnapshotMeta{Index: index, Term: term}

	// 1. Publish the snapshot. Until this succeeds nothing else may change.
	if err := s.snapshots.Save(Snapshot{Meta: meta, Data: data, Conf: conf}); err != nil {
		return err
	}

	// 2. Record it in the log, so recovery knows the snapshot should exist and
	//    can refuse to start quietly without it.
	if err := s.wal.SaveSnapshotMeta(meta); err != nil {
		return err
	}

	// 3. Now the entries it covers are redundant. Compact the cache first,
	//    since that cannot fail.
	keep := s.entries[index-s.offset():]
	compacted := make([]raft.Entry, len(keep))
	copy(compacted, keep)
	// The first surviving element becomes the new placeholder: its index and
	// term describe the boundary, and it is no longer a queryable entry.
	compacted[0] = raft.Entry{Index: meta.Index, Term: meta.Term}
	s.entries = compacted

	// 4. Delete the superseded segments. A failure here wastes disk but loses
	//    nothing, so it is reported rather than rolled back.
	if err := s.wal.TruncateBefore(index); err != nil {
		return err
	}

	return s.snapshots.Purge(DefaultSnapshotsKept)
}

// Snapshot implements raft.Storage.
//
// It reads the image back from disk rather than keeping one in memory. A
// snapshot is only wanted when a follower has fallen behind the compaction
// point, which is rare, and holding the whole state machine image resident to
// serve that case would double a node's memory for no ordinary benefit.
func (s *DiskStorage) Snapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	meta := SnapshotMeta{Index: s.entries[0].Index, Term: s.entries[0].Term}
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return raft.Snapshot{}, errClosed
	}
	if meta.Index == 0 {
		return raft.Snapshot{}, raft.ErrSnapshotUnavailable
	}

	snap, err := s.snapshots.LoadAt(meta)
	if err != nil {
		return raft.Snapshot{}, err
	}
	return raft.Snapshot{
		Index: snap.Meta.Index,
		Term:  snap.Meta.Term,
		Conf:  snap.Conf,
		Data:  snap.Data,
	}, nil
}

// ApplySnapshot implements raft.Storage.
//
// Ordering mirrors CreateSnapshot for the same reason: the image is made
// durable before anything is discarded, so a crash part-way through leaves the
// node recoverable rather than holding neither the old log nor the new state.
func (s *DiskStorage) ApplySnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}
	if snap.Index <= s.offset() {
		// Older than what this node already has. A delayed message makes this
		// reachable, and applying it would discard entries accepted since.
		return fmt.Errorf("storage: snapshot at index %d is not newer than the local state at %d",
			snap.Index, s.offset())
	}

	meta := SnapshotMeta{Index: snap.Index, Term: snap.Term}

	// 1. Publish the image. Until this succeeds nothing is thrown away.
	if err := s.snapshots.Save(Snapshot{Meta: meta, Data: snap.Data, Conf: snap.Conf}); err != nil {
		return err
	}

	// 2. Record it in the log, so recovery knows the snapshot must exist.
	if err := s.wal.SaveSnapshotMeta(meta); err != nil {
		return err
	}

	// 3. Replace the cached log. Everything is superseded: the snapshot covers
	//    a committed prefix, and whatever followed has to come from the leader.
	s.entries = []raft.Entry{{Index: snap.Index, Term: snap.Term}}

	// 4. Drop the segments the snapshot has made redundant.
	if err := s.wal.TruncateBefore(snap.Index); err != nil {
		return err
	}
	return s.snapshots.Purge(DefaultSnapshotsKept)
}

// SnapshotMeta returns the position of the most recent snapshot, or the zero
// value if none has been taken.
func (s *DiskStorage) SnapshotMeta() SnapshotMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SnapshotMeta{Index: s.entries[0].Index, Term: s.entries[0].Term}
}

// Sync flushes the write-ahead log.
func (s *DiskStorage) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errClosed
	}
	return s.wal.Sync()
}

// Close releases the underlying files.
func (s *DiskStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.Close()
}

// DiskStorage must satisfy the interface the Raft core depends on. Asserting
// it here means a change to either side is a compile error rather than a
// runtime surprise.
var _ raft.Storage = (*DiskStorage)(nil)
