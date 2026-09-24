package raft

import "fmt"

type raftLog struct {
	storage Storage

	committed Index

	applied Index
}

func newRaftLog(storage Storage) *raftLog {
	boundary := storage.FirstIndex() - 1
	return &raftLog{
		storage:   storage,
		committed: boundary,
		applied:   boundary,
	}
}

func (l *raftLog) firstIndex() Index {
	return l.storage.FirstIndex()
}

func (l *raftLog) lastIndex() Index {
	return l.storage.LastIndex()
}

func (l *raftLog) term(i Index) (Term, error) {
	return l.storage.Term(i)
}

func (l *raftLog) lastTerm() Term {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("raft: term of last index %d unavailable: %v", l.lastIndex(), err))
	}
	return t
}

func (l *raftLog) entries(lo, hi Index) ([]Entry, error) {
	return l.storage.Entries(lo, hi)
}

func (l *raftLog) entriesFrom(lo Index) ([]Entry, error) {
	return l.entries(lo, l.lastIndex()+1)
}

func (l *raftLog) matches(i Index, term Term) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) isUpToDate(lastIdx Index, lastTerm Term) bool {
	ourTerm := l.lastTerm()
	if lastTerm != ourTerm {
		return lastTerm > ourTerm
	}
	return lastIdx >= l.lastIndex()
}

func (l *raftLog) append(entries []Entry) (Index, error) {
	if len(entries) == 0 {
		return l.lastIndex(), nil
	}
	if err := l.storage.Append(entries); err != nil {
		return 0, fmt.Errorf("raft: appending entries: %w: %w", ErrStorage, err)
	}
	return l.lastIndex(), nil
}

type appendResult struct {
	lastIndex    Index
	firstWritten Index
	truncated    bool
}

func (l *raftLog) maybeAppend(prevIdx Index, prevTerm Term, leaderCommit Index, entries []Entry) (appendResult, bool, error) {
	if !l.matches(prevIdx, prevTerm) {
		return appendResult{}, false, nil
	}

	lastNewIdx := prevIdx + Index(len(entries))
	res := appendResult{lastIndex: lastNewIdx}

	conflict := l.findConflict(entries)
	switch {
	case conflict == 0:
	case conflict <= l.committed:
		panic(fmt.Sprintf("raft: entry %d conflicts with committed index %d", conflict, l.committed))
	default:
		res.truncated = conflict <= l.lastIndex()
		res.firstWritten = conflict

		offset := conflict - (prevIdx + 1)
		if err := l.storage.Append(entries[offset:]); err != nil {
			return appendResult{}, false, fmt.Errorf("raft: appending entries: %w: %w", ErrStorage, err)
		}
	}

	l.commitTo(min(leaderCommit, lastNewIdx))
	return res, true, nil
}

func (l *raftLog) findConflict(entries []Entry) Index {
	for _, e := range entries {
		if !l.matches(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

func (l *raftLog) conflictHint(prevIdx Index) (Index, Term) {
	if prevIdx > l.lastIndex() {
		return l.lastIndex() + 1, 0
	}

	conflictTerm, err := l.term(prevIdx)
	if err != nil {
		return l.firstIndex(), 0
	}

	first := l.firstIndex()
	idx := prevIdx
	for idx > first {
		t, err := l.term(idx - 1)
		if err != nil || t != conflictTerm {
			break
		}
		idx--
	}
	return idx, conflictTerm
}

func (l *raftLog) commitTo(i Index) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit index %d is past last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

func (l *raftLog) appliedTo(i Index) {
	if i == 0 {
		return
	}
	if i > l.committed || i < l.applied {
		panic(fmt.Sprintf("raft: applied index %d out of range (applied %d, committed %d)",
			i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *raftLog) nextCommitted(max int) ([]Entry, error) {
	if l.committed <= l.applied {
		return nil, nil
	}
	hi := l.committed + 1
	if max > 0 && hi-l.applied-1 > Index(max) {
		hi = l.applied + 1 + Index(max)
	}
	return l.entries(l.applied+1, hi)
}

func (l *raftLog) hasUnapplied() bool { return l.committed > l.applied }
