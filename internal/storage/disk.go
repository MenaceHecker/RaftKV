package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

type DiskStorage struct {
	mu sync.RWMutex

	wal       *WAL
	snapshots *Snapshotter

	lock *dirLock

	hardState raft.HardState

	entries []raft.Entry

	closed bool
}

type DiskConfig struct {
	Dir string

	Sync SyncPolicy

	SegmentSize int64

	SnapshotsKept int
}

const DefaultSnapshotsKept = 3

const (
	walSubdir      = "wal"
	snapshotSubdir = "snap"
)

var errClosed = errors.New("storage: disk storage is closed")

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

	lock, err := lockDir(cfg.Dir)
	if err != nil {
		return nil, Snapshot{}, err
	}

	snapshots, err := NewSnapshotter(filepath.Join(cfg.Dir, snapshotSubdir))
	if err != nil {
		lock.release()
		return nil, Snapshot{}, err
	}

	snap, err := snapshots.Load()
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		lock.release()
		return nil, Snapshot{}, err
	}

	wal, replay, err := Open(Options{
		Dir:         filepath.Join(cfg.Dir, walSubdir),
		Sync:        cfg.Sync,
		SegmentSize: cfg.SegmentSize,
	})
	if err != nil {
		lock.release()
		return nil, Snapshot{}, err
	}

	if replay.Snapshot.Index > snap.Meta.Index {
		wal.Close()
		lock.release()
		return nil, Snapshot{}, fmt.Errorf(
			"%w: the log records a snapshot at index %d but the newest readable snapshot is at %d",
			ErrCorruptWAL, replay.Snapshot.Index, snap.Meta.Index)
	}

	s := &DiskStorage{
		wal:       wal,
		lock:      lock,
		snapshots: snapshots,
		hardState: replay.HardState,
		entries:   []raft.Entry{{Index: snap.Meta.Index, Term: snap.Meta.Term}},
	}

	for _, e := range replay.Entries {
		if e.Index <= snap.Meta.Index {
			continue
		}
		s.entries = append(s.entries, e)
	}

	if err := s.validateContiguous(); err != nil {
		wal.Close()
		lock.release()
		return nil, Snapshot{}, err
	}

	return s, snap, nil
}

func (s *DiskStorage) validateContiguous() error {
	for i := 1; i < len(s.entries); i++ {
		if s.entries[i].Index != s.entries[i-1].Index+1 {
			return fmt.Errorf("%w: recovered log jumps from index %d to %d",
				ErrCorruptWAL, s.entries[i-1].Index, s.entries[i].Index)
		}
	}
	return nil
}

func (s *DiskStorage) offset() raft.Index {
	return s.entries[0].Index
}

func (s *DiskStorage) InitialState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

func (s *DiskStorage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}

	if err := s.wal.SaveHardState(hs); err != nil {
		return err
	}
	s.hardState = hs
	return nil
}

func (s *DiskStorage) FirstIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset() + 1
}

func (s *DiskStorage) LastIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *DiskStorage) lastIndexLocked() raft.Index {
	return s.offset() + raft.Index(len(s.entries)) - 1
}

func (s *DiskStorage) Term(i raft.Index) (raft.Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i < s.offset() {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndexLocked() {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-s.offset()].Term, nil
}

func (s *DiskStorage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, nil
	}
	if lo <= s.offset() {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndexLocked()+1 {
		return nil, raft.ErrUnavailable
	}

	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-s.offset():hi-s.offset()])
	return out, nil
}

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

	if first <= s.offset() {
		trim := s.offset() + 1 - first
		if int(trim) >= len(entries) {
			return nil
		}
		entries = entries[trim:]
		first = entries[0].Index
	}

	if err := s.wal.AppendEntries(entries); err != nil {
		return err
	}

	keep := first - s.offset()
	s.entries = append(s.entries[:keep:keep], entries...)
	return nil
}

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

	if err := s.snapshots.Save(Snapshot{Meta: meta, Data: data, Conf: conf}); err != nil {
		return err
	}

	if err := s.wal.SaveSnapshotMeta(meta); err != nil {
		return err
	}

	keep := s.entries[index-s.offset():]
	compacted := make([]raft.Entry, len(keep))
	copy(compacted, keep)
	compacted[0] = raft.Entry{Index: meta.Index, Term: meta.Term}
	s.entries = compacted

	if err := s.wal.TruncateBefore(index); err != nil {
		return err
	}

	return s.snapshots.Purge(DefaultSnapshotsKept)
}

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

func (s *DiskStorage) ApplySnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}
	if snap.Index <= s.offset() {
		return fmt.Errorf("storage: snapshot at index %d is not newer than the local state at %d",
			snap.Index, s.offset())
	}

	meta := SnapshotMeta{Index: snap.Index, Term: snap.Term}

	if err := s.snapshots.Save(Snapshot{Meta: meta, Data: snap.Data, Conf: snap.Conf}); err != nil {
		return err
	}

	if err := s.wal.SaveSnapshotMeta(meta); err != nil {
		return err
	}

	s.entries = []raft.Entry{{Index: snap.Index, Term: snap.Term}}

	if err := s.wal.TruncateBefore(snap.Index); err != nil {
		return err
	}
	return s.snapshots.Purge(DefaultSnapshotsKept)
}

func (s *DiskStorage) SnapshotMeta() SnapshotMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SnapshotMeta{Index: s.entries[0].Index, Term: s.entries[0].Term}
}

func (s *DiskStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	err := s.wal.Close()
	if lerr := s.lock.release(); err == nil {
		err = lerr
	}
	return err
}

var _ raft.Storage = (*DiskStorage)(nil)
