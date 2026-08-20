package raft

import (
	"fmt"
	"sort"
)

// Log replication (§5.3, §5.4.2).
//
// The leader holds the authoritative log. For each follower it tracks the
// highest index known to be replicated there (match) and where the next append
// should start (next). A follower accepts an append only if its log agrees at
// the entry immediately before it, which by induction means the whole prefix
// agrees — the Log Matching Property. Once a majority of match indices reach
// some index, and the entry there belongs to the leader's own term, it is
// committed.

// propose appends client commands to the leader's log and starts replicating
// them. Entries arrive with no term or index; the leader assigns both, which is
// what makes it the single ordering authority for its term.
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

	// When this node is the whole cluster its own append is already a
	// majority, and no response will ever arrive to advance the commit index.
	if n.isSoleVoter() {
		n.maybeCommit()
		return nil
	}

	n.broadcastAppend()
	return nil
}

// broadcastAppend sends every follower whatever it is missing.
//
// The set is drawn from the configuration, so during a joint transition a node
// that belongs to only the incoming configuration is replicated to as well. It
// has to be: it cannot contribute to the new majority until its log has caught
// up.
func (n *Node) broadcastAppend() {
	for _, p := range n.conf.members() {
		if p == n.id {
			continue
		}
		n.sendAppend(p)
	}
}

// broadcastHeartbeat is the leader's periodic contact with its followers.
//
// It is deliberately the same operation as broadcastAppend: a heartbeat is
// just an AppendEntries that happens to carry nothing when the follower is
// caught up. Sharing the path means a dropped append is retried on the next
// heartbeat, instead of leaving that follower stalled until the next client
// write.
func (n *Node) broadcastHeartbeat() {
	n.broadcastAppend()
}

// sendAppend sends one follower the entries from its next index onward, along
// with the (index, term) of the entry immediately before them so the follower
// can verify its log agrees at that point.
func (n *Node) sendAppend(to NodeID) {
	pr := n.progress[to]
	if pr == nil {
		return
	}

	prevIdx := pr.next - 1
	prevTerm, err := n.log.term(prevIdx)
	if err != nil {
		// The entry this follower needs has been compacted away, so the log
		// alone cannot catch it up. Phase 2 answers this with a snapshot
		// transfer; until compaction exists it is unreachable.
		return
	}

	entries, err := n.log.entriesFrom(pr.next)
	if err != nil {
		return
	}

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

// handleAppendRequest is the follower's side of replication. Step has already
// applied the term rules, so m.Term equals n.term here.
func (n *Node) handleAppendRequest(m Message) error {
	switch n.state {
	case Leader:
		// Two leaders in one term would violate Election Safety, so this is
		// a bug in this implementation rather than a condition to tolerate.
		return fmt.Errorf("raft: node %d received an append from %d in its own leader term %d",
			n.id, m.From, n.term)
	case Candidate:
		// Someone else won this term's election. Concede.
		if err := n.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}

	// Contact from the current leader, so the election timer restarts even if
	// the append itself is rejected. The leader being alive and reachable is
	// the only thing that timer is watching for; whether the logs happen to
	// agree yet is a separate question.
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

	// A configuration change takes effect on append, so accepting entries can
	// change this node's membership — and truncating can revert one it was
	// already using.
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

// handleAppendResponse updates the leader's view of one follower and, on
// success, reconsiders what is committed.
func (n *Node) handleAppendResponse(m Message) error {
	if n.state != Leader {
		return nil
	}
	pr := n.progress[m.From]
	if pr == nil {
		// Not a known member. Phase 4 makes this reachable during a
		// membership change; for now it means a stray message.
		return nil
	}

	if !m.Success {
		n.backoff(pr, m)
		n.sendAppend(m.From)
		return nil
	}

	// A delayed response can report a lower match than one already recorded.
	// match must never move backwards, or the leader could un-commit an entry
	// it has already told clients about.
	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	if pr.match+1 > pr.next {
		pr.next = pr.match + 1
	}

	if n.maybeCommit() {
		// Tell the followers about the new commit index now rather than
		// waiting for the next heartbeat, so they can apply without that
		// extra delay.
		n.broadcastAppend()
	}
	return nil
}

// backoff rewinds a follower's next index after a rejected append, using the
// conflict hint to skip a whole term at a time instead of stepping back one
// index per round trip (§5.3).
func (n *Node) backoff(pr *progress, m Message) {
	var next Index

	switch last, ok := n.lastIndexInTerm(m.ConflictTerm); {
	case m.ConflictTerm == 0:
		// The follower's log is shorter than PrevLogIndex, or that position
		// is compacted on its side. ConflictIndex is the first index it
		// cannot speak for, so resume there.
		next = m.ConflictIndex

	case ok:
		// The leader also holds entries in the conflicting term. Everything
		// through its last entry in that term matches, so resume just after
		// it.
		next = last + 1

	default:
		// The leader has no entries in that term at all, so the follower's
		// entire run of that term is wrong. Skip past all of it in one step.
		next = m.ConflictIndex
	}

	if next < 1 {
		next = 1
	}
	// Only ever move backwards. A reordered rejection arriving after a later
	// success must not push next forward past what has already been confirmed.
	if next < pr.next {
		pr.next = next
	}
}

// lastIndexInTerm returns the highest index in the leader's log whose entry has
// the given term.
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
			// Terms never decrease as the index grows, so once the log drops
			// below the target term there is nothing left to find.
			break
		}
	}
	return 0, false
}

// maybeCommit advances the commit index to the highest index replicated on a
// majority, reporting whether it moved.
//
// The term check is the subtle part (§5.4.2). A leader may not commit an entry
// from an earlier term merely because a majority now stores it — such an entry
// can still be overwritten by a future leader, and committing it would let two
// state machines diverge. Only once an entry from the leader's own term is
// committed does the whole prefix become safe, which is exactly why
// becomeLeader appends a no-op: it gives every new leader something in its own
// term to commit immediately.
func (n *Node) maybeCommit() bool {
	matchFor := func(id NodeID) Index {
		if pr := n.progress[id]; pr != nil {
			return pr.match
		}
		return 0
	}

	// The commit index can only ever be an index some member has actually
	// reached, so those are the only candidates worth testing. Trying every
	// index between the current commit point and the highest match would be
	// unbounded work after an election with a backlog, and would test indexes
	// no majority could possibly hold.
	//
	// They are tested highest first, and the configuration decides whether
	// each is held by a majority — which during a joint transition means a
	// majority of both voter sets. Asking the configuration is what makes the
	// two-set rule expressible: there is no single position in one sorted
	// ordering that answers a double-majority question, which is why the
	// previous sort-and-index approach could not be extended.
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

		// §5.4.2: replica count alone does not commit. An entry from an
		// earlier term can sit on a majority and still be overwritten, so
		// only an entry from the leader's own term may advance the commit
		// index — and it carries the whole inherited prefix with it.
		t, err := n.log.term(candidate)
		if err != nil || t != n.term {
			return false
		}

		n.log.commitTo(candidate)
		return true
	}
	return false
}
