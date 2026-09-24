package statemachine

import (
	"fmt"
	"sort"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

const DefaultMaxSessions = 4096

type session struct {
	lastSeq uint64

	lastIndex raft.Index
}

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

const NoClient uint64 = 0

func (s *sessions) shouldApply(clientID, seq uint64, index raft.Index) bool {
	if clientID == NoClient {
		return true
	}

	existing, ok := s.entries[clientID]
	if ok {
		if seq <= existing.lastSeq {
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

func (s *sessions) len() int {
	return len(s.entries)
}

func (s *sessions) lastSeq(clientID uint64) (uint64, bool) {
	sess, ok := s.entries[clientID]
	if !ok {
		return 0, false
	}
	return sess.lastSeq, true
}

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

func decodeSessions(r *reader, max int) (*sessions, error) {
	count, err := r.uint64()
	if err != nil {
		return nil, fmt.Errorf("reading session count: %w", err)
	}
	if count > maxFieldSize {
		return nil, fmt.Errorf("implausible session count %d", count)
	}

	const bytesPerSession = 24
	if remaining := uint64(len(r.b) - r.pos); count > remaining/bytesPerSession {
		return nil, fmt.Errorf("%d sessions declared but only %d bytes remain",
			count, remaining)
	}

	s := newSessions(max)
	var previous uint64
	for i := uint64(0); i < count; i++ {
		id, err := r.uint64()
		if err != nil {
			return nil, fmt.Errorf("reading session %d client ID: %w", i, err)
		}

		if i > 0 && id <= previous {
			return nil, fmt.Errorf("session client ID %d follows %d, but IDs must ascend",
				id, previous)
		}
		previous = id
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
