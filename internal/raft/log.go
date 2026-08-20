package raft

import "fmt"

// raftLog layers Raft's log semantics over a Storage. Storage knows how to
// hold entries; raftLog knows the rules that make those entries a Raft log —
// which prefix is committed, which prefix has reached the state machine, when
// one log counts as more up-to-date than another, and how a follower
// reconciles its log with a leader's.
//
// It holds no lock of its own. A Node owns exactly one raftLog and touches it
// only from the goroutine driving Tick and Step.
type raftLog struct {
	storage Storage

	// committed is the highest index known to be stored on a majority.
	// Entries at or below it can never be lost, so they are safe to apply.
	committed Index

	// applied is the highest index handed to the state machine. It trails
	// committed by however long the caller takes to consume Ready.
	applied Index
}

// newRaftLog builds a log over storage.
//
// The committed and applied cursors start at the storage's compaction
// boundary, not at zero. A snapshot is only ever taken over entries that were
// already committed and applied, so everything below that boundary is both by
// definition — and the entries themselves are gone, so a cursor left at zero
// would send the log looking for entries that no longer exist the moment it
// tried to report what was newly committed.
//
// Above the boundary a restarting node relearns its commit index from the
// leader's next AppendEntries rather than reading it back from disk.
// Re-applying entries the state machine has already seen is harmless, because
// application is deterministic and the state machine ignores anything at or
// below its own applied index.
func newRaftLog(storage Storage) *raftLog {
	boundary := storage.FirstIndex() - 1
	return &raftLog{
		storage:   storage,
		committed: boundary,
		applied:   boundary,
	}
}

// firstIndex is the oldest index still retained.
func (l *raftLog) firstIndex() Index {
	return l.storage.FirstIndex()
}

// lastIndex is the index of the newest entry, or 0 for an empty log.
func (l *raftLog) lastIndex() Index {
	return l.storage.LastIndex()
}

// term returns the term of the entry at index i. Index 0 has term 0. An index
// that is compacted or not yet present reports an error, which callers read as
// "this node cannot speak for that position".
func (l *raftLog) term(i Index) (Term, error) {
	return l.storage.Term(i)
}

// lastTerm is the term of the newest entry, or 0 for an empty log.
func (l *raftLog) lastTerm() Term {
	t, err := l.term(l.lastIndex())
	if err != nil {
		// lastIndex always names a retained entry, or the 0 sentinel, so a
		// failure here means Storage broke its own contract.
		panic(fmt.Sprintf("raft: term of last index %d unavailable: %v", l.lastIndex(), err))
	}
	return t
}

// entries returns the entries in [lo, hi).
func (l *raftLog) entries(lo, hi Index) ([]Entry, error) {
	return l.storage.Entries(lo, hi)
}

// entriesFrom returns every entry from lo through the end of the log. The
// leader uses it to build the payload of an AppendEntries.
func (l *raftLog) entriesFrom(lo Index) ([]Entry, error) {
	return l.entries(lo, l.lastIndex()+1)
}

// matches reports whether this log holds an entry at index i with the given
// term. This is the check a follower runs against a leader's PrevLogIndex and
// PrevLogTerm: agreement at one position implies agreement on everything
// before it, which is the Log Matching Property (§5.3).
func (l *raftLog) matches(i Index, term Term) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

// isUpToDate reports whether a candidate whose last entry is
// (lastIdx, lastTerm) has a log at least as up-to-date as this one, which is
// the precondition for granting it a vote (§5.4.1).
//
// The comparison looks only at the last entry: a higher term wins, and on a tie
// the longer log wins. This restriction is what guarantees that whoever wins an
// election already holds every committed entry, so a new leader never has to
// overwrite committed data.
func (l *raftLog) isUpToDate(lastIdx Index, lastTerm Term) bool {
	ourTerm := l.lastTerm()
	if lastTerm != ourTerm {
		return lastTerm > ourTerm
	}
	return lastIdx >= l.lastIndex()
}

// append writes entries at the end of the log and returns the new last index.
// This is the leader's path only: a leader never overwrites its own entries
// (Leader Append-Only, §5.2).
func (l *raftLog) append(entries []Entry) (Index, error) {
	if len(entries) == 0 {
		return l.lastIndex(), nil
	}
	if err := l.storage.Append(entries); err != nil {
		return 0, err
	}
	return l.lastIndex(), nil
}

// appendResult describes what an accepted AppendEntries actually did to the
// log.
//
// The detail matters because a configuration change takes effect as soon as it
// is appended, not when it commits (§6). An append that overwrites entries can
// therefore remove a configuration this node is already using, so the caller
// has to know whether anything was discarded.
type appendResult struct {
	// lastIndex is the index of the last entry now agreed with.
	lastIndex Index
	// firstWritten is the index of the first entry actually written, or zero
	// if the payload was already present in full.
	firstWritten Index
	// truncated reports that entries this node already held were discarded,
	// which can revert a configuration change.
	truncated bool
}

// maybeAppend is the follower's side of AppendEntries. It checks that the log
// agrees at (prevIdx, prevTerm) and, if so, merges entries in and advances the
// commit index to min(leaderCommit, last index now held).
//
// On failure it returns ok=false, and the caller replies with a conflict hint
// so the leader can back up efficiently.
func (l *raftLog) maybeAppend(prevIdx Index, prevTerm Term, leaderCommit Index, entries []Entry) (appendResult, bool, error) {
	if !l.matches(prevIdx, prevTerm) {
		return appendResult{}, false, nil
	}

	lastNewIdx := prevIdx + Index(len(entries))
	res := appendResult{lastIndex: lastNewIdx}

	// Write only the suffix that actually differs. This is a correctness
	// requirement, not an optimization: a delayed or duplicated AppendEntries
	// must not truncate entries the follower has since accepted from the same
	// leader, which is exactly what writing the whole payload blindly would
	// do.
	conflict := l.findConflict(entries)
	switch {
	case conflict == 0:
		// Every entry in the payload is already present. Nothing to write.
	case conflict <= l.committed:
		// A conflict inside the committed prefix would mean two leaders had
		// committed different entries at the same index. State Machine
		// Safety is already broken at that point and there is nothing safe
		// left to do, so fail loudly rather than corrupt the log.
		panic(fmt.Sprintf("raft: entry %d conflicts with committed index %d", conflict, l.committed))
	default:
		// Anything at or after the conflict that this node already held is
		// about to be replaced.
		res.truncated = conflict <= l.lastIndex()
		res.firstWritten = conflict

		offset := conflict - (prevIdx + 1)
		if err := l.storage.Append(entries[offset:]); err != nil {
			return appendResult{}, false, err
		}
	}

	// The leader's commit index can run ahead of what this follower has
	// received, so clamp it to the entries actually in hand.
	l.commitTo(min(leaderCommit, lastNewIdx))
	return res, true, nil
}

// findConflict returns the index of the first entry in entries that this log
// disagrees with — either the terms differ at that index, or the log ends
// before it. It returns 0 when every entry is already present with a matching
// term.
func (l *raftLog) findConflict(entries []Entry) Index {
	for _, e := range entries {
		if !l.matches(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// conflictHint describes, for a rejected AppendEntries, where the leader should
// retry. Reporting a whole term's worth of backup rather than a single index
// keeps a badly diverged follower from costing one round trip per entry (§5.3).
//
// A zero term means the follower's log is simply too short, and the returned
// index is one past its last entry. Otherwise the term is that of the
// conflicting entry, and the index is the first entry the follower holds in
// that term.
func (l *raftLog) conflictHint(prevIdx Index) (Index, Term) {
	if prevIdx > l.lastIndex() {
		return l.lastIndex() + 1, 0
	}

	conflictTerm, err := l.term(prevIdx)
	if err != nil {
		// The position is compacted here, so this node can say nothing useful
		// about it. Point the leader at the oldest entry still retained.
		return l.firstIndex(), 0
	}

	// Walk back to the first entry of the conflicting term so the leader can
	// skip that entire term in one step.
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

// commitTo advances the commit index. Moving it backwards would un-commit an
// entry the state machine may already have applied, so that is a programming
// error rather than a condition to tolerate.
func (l *raftLog) commitTo(i Index) {
	if i <= l.committed {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit index %d is past last index %d", i, l.lastIndex()))
	}
	l.committed = i
}

// appliedTo records that the state machine has consumed through index i.
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

// nextCommitted returns the committed entries not yet applied, which is what a
// Ready hands to the state machine.
func (l *raftLog) nextCommitted() ([]Entry, error) {
	if l.committed <= l.applied {
		return nil, nil
	}
	return l.entries(l.applied+1, l.committed+1)
}
