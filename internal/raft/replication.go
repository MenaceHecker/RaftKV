package raft

import (
	"fmt"
	"sort"
)

func (n *Node) propose(entries []Entry) error {
	if n.state != Leader {
		return ErrNotLeader
	}
	if len(entries) == 0 {
		return nil
	}

	next := n.log.lastIndex() + 1
	stamped := make([]Entry, len(entries))
	for i, e := range entries {
		e.Term = n.term
		e.Index = next + Index(i)
		stamped[i] = e
	}

	if _, err := n.log.append(stamped); err != nil {
		return err
	}
	n.progress[n.id].match = n.log.lastIndex()
	n.progress[n.id].next = n.log.lastIndex() + 1

	if n.isSoleVoter() {
		n.maybeCommit()
		return nil
	}

	n.broadcastAppend()
	return nil
}

func (n *Node) broadcastAppend() {
	for _, p := range n.conf.members() {
		if p == n.id {
			continue
		}
		n.sendAppend(p)
	}
}

func (n *Node) broadcastHeartbeat() {
	n.broadcastAppend()

	if n.readOnly.outstanding != "" {
		if round, ok := n.readOnly.rounds[n.readOnly.outstanding]; ok {
			n.sendReadHeartbeats(round.context)
		}
	}
}

func (n *Node) sendAppend(to NodeID) {
	pr := n.progress[to]
	if pr == nil {
		return
	}

	prevIdx := pr.next - 1
	prevTerm, err := n.log.term(prevIdx)
	if err != nil {
		n.sendSnapshot(to)
		return
	}

	entries, err := n.log.entriesFrom(pr.next)
	if err != nil {
		n.sendSnapshot(to)
		return
	}

	limited := limitEntries(entries, n.maxAppendBytes)
	pr.heldBack = len(limited) < len(entries)
	entries = limited

	n.send(Message{
		Type:         MsgAppendRequest,
		To:           to,
		Term:         n.term,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		CommitIndex:  n.log.committed,
	})
}

func (n *Node) handleAppendRequest(m Message) error {
	switch n.state {
	case Leader:
		return fmt.Errorf("raft: node %d received an append from %d in its own leader term %d",
			n.id, m.From, n.term)
	case PreCandidate, Candidate:
		if err := n.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}

	n.leader = m.From
	n.electionElapsed = 0

	res, ok, err := n.log.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.CommitIndex, m.Entries)
	if err != nil {
		return err
	}

	if !ok {
		conflictIdx, conflictTerm := n.log.conflictHint(m.PrevLogIndex)
		n.send(Message{
			Type:          MsgAppendResponse,
			To:            m.From,
			Term:          n.term,
			Success:       false,
			ConflictIndex: conflictIdx,
			ConflictTerm:  conflictTerm,
		})
		return nil
	}

	if res.truncated || containsConfChange(m.Entries) {
		if err := n.rebuildConfig(); err != nil {
			return err
		}
	}

	n.send(Message{
		Type:       MsgAppendResponse,
		To:         m.From,
		Term:       n.term,
		Success:    true,
		MatchIndex: res.lastIndex,
	})
	return nil
}

func (n *Node) handleAppendResponse(m Message) error {
	if n.state != Leader {
		return nil
	}
	pr := n.progress[m.From]
	if pr == nil {
		return nil
	}

	pr.active = true

	if !m.Success {
		n.backoff(pr, m)
		n.sendAppend(m.From)
		return nil
	}

	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	if pr.match+1 > pr.next {
		pr.next = pr.match + 1
	}

	if n.maybeCommit() {
		if err := n.maybeFinishConfChange(); err != nil {
			return err
		}

		n.broadcastAppend()
	} else if pr.heldBack && pr.next <= n.log.lastIndex() {
		n.sendAppend(m.From)
	}
	return nil
}

func (n *Node) backoff(pr *progress, m Message) {
	var next Index

	switch last, ok := n.lastIndexInTerm(m.ConflictTerm); {
	case m.ConflictTerm == 0:
		next = m.ConflictIndex

	case ok:
		next = last + 1

	default:
		next = m.ConflictIndex
	}

	if next < 1 {
		next = 1
	}
	if next < pr.next {
		pr.next = next
	}
}

func (n *Node) lastIndexInTerm(term Term) (Index, bool) {
	first := n.log.firstIndex()
	for i := n.log.lastIndex(); i >= first; i-- {
		t, err := n.log.term(i)
		if err != nil {
			break
		}
		if t == term {
			return i, true
		}
		if t < term {
			break
		}
	}
	return 0, false
}

func (n *Node) maybeCommit() bool {
	matchFor := func(id NodeID) Index {
		if pr := n.progress[id]; pr != nil {
			return pr.match
		}
		return 0
	}

	members := n.conf.members()
	candidates := make([]Index, 0, len(members))
	for _, p := range members {
		if m := matchFor(p); m > n.log.committed {
			candidates = append(candidates, m)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })

	for _, candidate := range candidates {
		if !n.conf.commitReady(candidate, matchFor) {
			continue
		}

		t, err := n.log.term(candidate)
		if err != nil || t != n.term {
			return false
		}

		n.log.commitTo(candidate)
		return true
	}
	return false
}
