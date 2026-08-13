package statemachine

import (
	"fmt"
	"sort"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Client sessions and request deduplication (§6.3).
//
// A client that does not hear back from a write cannot tell whether it was
// lost on the way in, applied and lost on the way out, or is merely slow. Its
// only option is to retry, so the system has to make a retry harmless.
//
// "Harmless" is not the same as "idempotent". Put and Delete are individually
// idempotent, so applying one twice changes nothing on its own. The damage
// comes from reordering: a client sends Put(x, 1), times out, and retries;
// meanwhile it has already sent Put(x, 2). If the retried Put(x, 1) is
// appended after Put(x, 2), the log now says x is 1 and the client's newer
// write has been silently undone. Nothing about that is detectable after the
// fact.
//
// Each client therefore tags its requests with an ID and a strictly increasing
// sequence number, and the state machine ignores any request whose sequence it
// has already passed.
//
// This has to happen inside the state machine rather than at the server. A
// server-side check would only filter duplicates arriving at the node that saw
// the original, and the entry would still be committed and applied everywhere
// else — the replicas would diverge, which is far worse than a duplicate.

// DefaultMaxSessions bounds how many clients are remembered at once. The table
// is part of the snapshot, so it cannot grow without bound; see the note on
// sessions about what eviction costs.
const DefaultMaxSessions = 4096

// session records what the state machine has already done for one client.
type session struct {
	// lastSeq is the highest sequence number applied for this client. A
	// request at or below it is a duplicate.
	lastSeq uint64

	// lastIndex is the log index at which this client was last seen. It
	// exists only to make eviction deterministic: the least recently used
	// session is evicted, and "least recently" has to mean the same thing on
	// every replica, so it is measured in log positions rather than wall time.
	lastIndex raft.Index
}

// sessions is the per-client deduplication table.
//
// Its size is bounded, which is a real tradeoff rather than a detail. An
// unbounded table would grow with every client that ever connected and would
// have to be written into every snapshot. A bounded one means a client that
// stays silent long enough to be evicted will have a subsequent retry treated
// as a fresh request — the exact hazard the table exists to prevent, just for
// a client that has been idle for thousands of writes.
//
// The paper's answer is explicit session registration and expiry, with clients
// told when their session is gone so they can stop assuming exactly-once. That
// is the right long-term design; this is the smaller version of it, with the
// limitation stated rather than hidden.
type sessions struct {
	entries map[uint64]*session
	max     int
}

func newSessions(max int) *sessions {
	if max <= 0 {
		max = DefaultMaxSessions
	}
	return &sessions{
		entries: make(map[uint64]*session),
		max:     max,
	}
}

// NoClient is the client ID meaning "not part of a session". Commands carrying
// it are never deduplicated, which suits internal commands and tests where
// exactly-once delivery is not being claimed.
const NoClient uint64 = 0

// shouldApply reports whether a request is new, and records it if so.
//
// Sequence numbers must increase per client but need not be contiguous: a
// client that abandons a request and moves on simply leaves a gap, and the
// next request is still newer than everything applied.
func (s *sessions) shouldApply(clientID, seq uint64, index raft.Index) bool {
	if clientID == NoClient {
		return true
	}

	existing, ok := s.entries[clientID]
	if ok {
		if seq <= existing.lastSeq {
			// Already applied, or superseded by a later request from the same
			// client. Either way, doing it again could only undo newer work.
			return false
		}
		existing.lastSeq = seq
		existing.lastIndex = index
		return true
	}

	s.evictIfFull(index)
	s.entries[clientID] = &session{lastSeq: seq, lastIndex: index}
	return true
}

// evictIfFull makes room for a new client by dropping the least recently used
// session.
//
// Eviction is a state machine transition like any other, so it must be
// identical on every replica. The victim is chosen by the log index at which a
// client was last seen, with the client ID breaking ties — both of which every
// replica agrees on exactly. Anything derived from wall time, map order, or
// arrival order would diverge the cluster.
func (s *sessions) evictIfFull(index raft.Index) {
	if len(s.entries) < s.max {
		return
	}

	var victim uint64
	var victimIndex raft.Index
	first := true

	for id, sess := range s.entries {
		switch {
		case first,
			sess.lastIndex < victimIndex,
			sess.lastIndex == victimIndex && id < victim:
			victim, victimIndex, first = id, sess.lastIndex, false
		}
	}

	if !first {
		delete(s.entries, victim)
	}
}

// len reports how many clients are currently tracked.
func (s *sessions) len() int {
	return len(s.entries)
}

// lastSeq returns the highest sequence applied for a client, and whether that
// client is tracked at all.
func (s *sessions) lastSeq(clientID uint64) (uint64, bool) {
	sess, ok := s.entries[clientID]
	if !ok {
		return 0, false
	}
	return sess.lastSeq, true
}

// encode appends the session table to dst.
//
// Clients are written in ID order for the same reason the key-value pairs are
// written in key order: the snapshot must be a deterministic function of the
// state, or two replicas holding identical state would produce different bytes
// and there would be no cheap way to tell convergence from divergence.
func (s *sessions) encode(dst []byte) []byte {
	ids := make([]uint64, 0, len(s.entries))
	for id := range s.entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	dst = appendUint64(dst, uint64(len(ids)))
	for _, id := range ids {
		sess := s.entries[id]
		dst = appendUint64(dst, id)
		dst = appendUint64(dst, sess.lastSeq)
		dst = appendUint64(dst, uint64(sess.lastIndex))
	}
	return dst
}

// decodeSessions reads a session table written by encode.
func decodeSessions(r *reader, max int) (*sessions, error) {
	count, err := r.uint64()
	if err != nil {
		return nil, fmt.Errorf("reading session count: %w", err)
	}
	if count > maxFieldSize {
		return nil, fmt.Errorf("implausible session count %d", count)
	}

	s := newSessions(max)
	for i := uint64(0); i < count; i++ {
		id, err := r.uint64()
		if err != nil {
			return nil, fmt.Errorf("reading session %d client ID: %w", i, err)
		}
		seq, err := r.uint64()
		if err != nil {
			return nil, fmt.Errorf("reading session %d sequence: %w", i, err)
		}
		index, err := r.uint64()
		if err != nil {
			return nil, fmt.Errorf("reading session %d index: %w", i, err)
		}
		s.entries[id] = &session{lastSeq: seq, lastIndex: raft.Index(index)}
	}
	return s, nil
}
