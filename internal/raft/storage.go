package raft

import (
	"errors"
	"fmt"
	"sync"
)

// Storage is the durability boundary of the Raft core. Everything Raft needs
// to survive a crash lives behind this interface: the hard state (current term
// and vote) and the log entries themselves.
//
// The core calls into Storage synchronously and treats a returned error as
// fatal. Raft's safety argument assumes that once state is reported persisted
// it really is, so there is no correct way to carry on after a failed write.
//
// Phase 1 ships MemoryStorage, which satisfies the interface without touching
// disk and keeps the deterministic tests fast. Phase 2 adds a WAL-backed
// implementation; nothing in the core changes when it is swapped in.
type Storage interface {
	// InitialState returns the hard state persisted by a previous
	// incarnation of this node. A fresh node returns the zero HardState.
	InitialState() (HardState, error)

	// SetHardState durably records the current term and vote. It must not
	// return until the write is stable, because a node may not vote twice in
	// one term even across a crash.
	SetHardState(hs HardState) error

	// FirstIndex is the index of the oldest entry still retained. It is 1
	// for a log that has never been compacted; once snapshotting lands in
	// Phase 2 it becomes one past the last compacted entry.
	FirstIndex() Index

	// LastIndex is the index of the newest entry, or 0 if the log is empty.
	LastIndex() Index

	// Term returns the term of the entry at index i. Term(0) is 0, the
	// sentinel term of the position before the first entry. It reports
	// ErrCompacted if i precedes FirstIndex, and ErrUnavailable if i is past
	// LastIndex.
	Term(i Index) (Term, error)

	// Entries returns the entries in the half-open range [lo, hi). An empty
	// range returns nil. It reports ErrCompacted or ErrUnavailable if the
	// range falls outside what is stored.
	Entries(lo, hi Index) ([]Entry, error)

	// Append writes entries to the log. They must be contiguous and start no
	// later than LastIndex+1. Any existing entry at or after
	// entries[0].Index is overwritten, which is how a follower resolves a
	// conflict with the leader's log (§5.3).
	Append(entries []Entry) error

	// Snapshot returns the most recent state machine image, for sending to a
	// follower that has fallen behind the compaction point. It reports
	// ErrSnapshotUnavailable when none has been taken.
	Snapshot() (Snapshot, error)

	// ApplySnapshot replaces the stored state with a snapshot received from a
	// leader.
	//
	// This is the one operation that moves the log backwards, and it is safe
	// only because the snapshot is by definition a committed prefix: the
	// leader would not hold it otherwise. Everything the receiver had is
	// discarded, because reconciling entry by entry is exactly what became
	// impossible when the leader compacted.
	ApplySnapshot(snap Snapshot) error
}

var (
	// ErrCompacted means the requested index is older than the oldest entry
	// still retained, because a snapshot has superseded it.
	ErrCompacted = errors.New("raft: requested index is compacted")

	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("raft: requested index is unavailable")

	// ErrSnapshotUnavailable means no snapshot has been taken yet, so there is
	// nothing to send a lagging follower. The leader falls back to the log.
	ErrSnapshotUnavailable = errors.New("raft: no snapshot is available")
)

// HardState is the subset of a node's state that Raft requires to be on stable
// storage before it responds to any RPC (Figure 2). Losing it would let a node
// vote twice in a term or forget a term it has already seen, either of which
// breaks Election Safety.
//
// The commit index is deliberately absent: it is recoverable by replaying the
// log and relearning it from the leader, so persisting it is an optimization
// rather than a requirement for correctness.
type HardState struct {
	// Term is the latest term this node has seen.
	Term Term
	// VotedFor is the candidate this node voted for in Term, or None if it
	// has not voted in that term.
	VotedFor NodeID
}

// IsEmpty reports whether the hard state is the zero value, which is what a
// node that has never persisted anything reads back on startup.
func (h HardState) IsEmpty() bool {
	return h.Term == 0 && h.VotedFor == None
}

// MemoryStorage is a Storage that keeps everything in memory. It backs the
// deterministic test harness and any node configured without durability.
//
// It guards its state with a mutex so a transport layer can read while the
// core writes, even though the Phase 1 harness drives everything from a single
// goroutine.
type MemoryStorage struct {
	mu        sync.RWMutex
	hardState HardState

	// snapshot is the last image installed, if any. Its index is the floor
	// below which entries no longer exist, and its term answers for that
	// boundary position — which a leader asks about when replicating the first
	// entry after it.
	snapshot Snapshot

	// entries holds the log in index order, all of it above snapshot.Index.
	entries []Entry
}

// NewMemoryStorage returns an empty store, as a node booting for the first
// time would see.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// InitialState implements Storage.
func (s *MemoryStorage) InitialState() (HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

// SetHardState implements Storage.
func (s *MemoryStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	return nil
}

// FirstIndex implements Storage.
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

// LastIndex implements Storage.
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

// Term implements Storage.
func (s *MemoryStorage) Term(i Index) (Term, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Index 0 is the position before the first entry. Giving it term 0 lets
	// callers compare against an empty log without a special case of their
	// own — an empty follower legitimately matches PrevLogIndex 0.
	if i == 0 {
		return 0, nil
	}
	// The snapshot answers for the compaction boundary. Without this a leader
	// replicating the first entry after a snapshot could never find a matching
	// position, and the follower could never be caught up.
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

// Entries implements Storage.
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

	// Copy rather than sub-slice. The caller must not be able to mutate the
	// log through the returned slice, and a later Append that grows the
	// backing array would otherwise alias it.
	out := make([]Entry, hi-lo)
	copy(out, s.entries[lo-first:hi-first])
	return out, nil
}

// Append implements Storage.
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

	// Drop anything at or after the first new index. On a follower this is
	// the conflict resolution of §5.3 — the leader's log wins, so the
	// follower's divergent suffix goes. On a leader it is a no-op, since a
	// leader only ever appends past its own last index (Leader Append-Only).
	//
	// The three-index slice caps capacity, so the append allocates a fresh
	// array instead of overwriting entries a concurrent reader still holds.
	keep := entries[0].Index - first
	s.entries = append(s.entries[:keep:keep], entries...)
	return nil
}

// Snapshot implements Storage.
func (s *MemoryStorage) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot.Index == 0 {
		return Snapshot{}, ErrSnapshotUnavailable
	}
	return s.snapshot, nil
}

// ApplySnapshot implements Storage.
func (s *MemoryStorage) ApplySnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A snapshot older than one already installed carries nothing new, and
	// applying it would throw away entries this node has since accepted.
	// Delayed messages make this reachable, so it is refused rather than
	// assumed impossible.
	if snap.Index <= s.snapshot.Index {
		return fmt.Errorf("raft: snapshot at index %d is not newer than the one at %d",
			snap.Index, s.snapshot.Index)
	}

	s.snapshot = snap
	// Every entry is superseded: the snapshot covers a committed prefix, and
	// anything after it has to come from the leader.
	s.entries = nil
	return nil
}
