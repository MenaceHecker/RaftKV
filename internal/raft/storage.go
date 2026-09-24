package raft

import (
	"errors"
	"fmt"
	"sync"
)

type Storage interface {
	InitialState() (HardState, error)

	SetHardState(hs HardState) error

	FirstIndex() Index

	LastIndex() Index

	Term(i Index) (Term, error)

	Entries(lo, hi Index) ([]Entry, error)

	Append(entries []Entry) error

	Snapshot() (Snapshot, error)

	ApplySnapshot(snap Snapshot) error
}

var (
	ErrCompacted = errors.New("raft: requested index is compacted")

	ErrUnavailable = errors.New("raft: requested index is unavailable")

	ErrSnapshotUnavailable = errors.New("raft: no snapshot is available")

	ErrStorage = errors.New("raft: storage failure")
)

type HardState struct {
	Term     Term
	VotedFor NodeID
}

func (h HardState) IsEmpty() bool {
	return h.Term == 0 && h.VotedFor == None
}

type MemoryStorage struct {
	mu        sync.RWMutex
	hardState HardState

	snapshot Snapshot

	entries []Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (s *MemoryStorage) InitialState() (HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

func (s *MemoryStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	return nil
}

func (s *MemoryStorage) FirstIndex() Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked()
}

func (s *MemoryStorage) firstIndexLocked() Index {
	if len(s.entries) == 0 {
		return s.snapshot.Index + 1
	}
	return s.entries[0].Index
}

func (s *MemoryStorage) LastIndex() Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *MemoryStorage) lastIndexLocked() Index {
	if len(s.entries) == 0 {
		return s.snapshot.Index
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *MemoryStorage) Term(i Index) (Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i == 0 {
		return 0, nil
	}
	if i == s.snapshot.Index {
		return s.snapshot.Term, nil
	}
	if i < s.firstIndexLocked() {
		return 0, ErrCompacted
	}
	if i > s.lastIndexLocked() {
		return 0, ErrUnavailable
	}
	return s.entries[i-s.firstIndexLocked()].Term, nil
}

func (s *MemoryStorage) Entries(lo, hi Index) ([]Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo >= hi {
		return nil, nil
	}
	first := s.firstIndexLocked()
	if lo < first {
		return nil, ErrCompacted
	}
	if hi > s.lastIndexLocked()+1 {
		return nil, ErrUnavailable
	}

	out := make([]Entry, hi-lo)
	copy(out, s.entries[lo-first:hi-first])
	return out, nil
}

func (s *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	first := s.firstIndexLocked()
	last := s.lastIndexLocked()

	if entries[0].Index > last+1 {
		return fmt.Errorf("raft: append at index %d leaves a gap after last index %d",
			entries[0].Index, last)
	}
	if entries[0].Index < first {
		return fmt.Errorf("raft: append at index %d precedes first index %d",
			entries[0].Index, first)
	}

	keep := entries[0].Index - first
	s.entries = append(s.entries[:keep:keep], entries...)
	return nil
}

func (s *MemoryStorage) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot.Index == 0 {
		return Snapshot{}, ErrSnapshotUnavailable
	}
	return s.snapshot, nil
}

func (s *MemoryStorage) CreateSnapshot(index Index, data []byte, conf ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index <= s.snapshot.Index {
		return fmt.Errorf("raft: snapshot at index %d is at or before the last one at %d",
			index, s.snapshot.Index)
	}
	if index > s.lastIndexLocked() {
		return fmt.Errorf("raft: cannot snapshot index %d, the log ends at %d",
			index, s.lastIndexLocked())
	}

	offset := s.firstIndexLocked()
	term := s.entries[index-offset].Term

	s.snapshot = Snapshot{Index: index, Term: term, Conf: conf, Data: data}
	s.entries = append([]Entry(nil), s.entries[index-offset+1:]...)
	return nil
}

func (s *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap.Index <= s.snapshot.Index {
		return fmt.Errorf("raft: snapshot at index %d is not newer than the one at %d",
			snap.Index, s.snapshot.Index)
	}

	s.snapshot = snap
	s.entries = nil
	return nil
}
