package raft

// Leader election (§5.2, §5.4.1).
//
// A node that stops hearing from a leader promotes itself to candidate, bumps
// the term, votes for itself, and asks everyone else for a vote. A majority
// makes it leader. Two rules keep that safe:
//
//   - One vote per node per term. Since any two majorities intersect, at most
//     one candidate can collect a majority in a given term, so there is at most
//     one leader per term (Election Safety).
//   - A vote goes only to a candidate whose log is at least as up-to-date as
//     the voter's. That guarantees the winner already holds every committed
//     entry, so no leader ever has to overwrite committed data (Leader
//     Completeness).

// campaign starts an election for the next term. It runs on an election
// timeout in Tick, or on an explicit MsgCampaign — which is how tests force an
// election at a chosen moment instead of waiting one out.
func (n *Node) campaign() error {
	if n.state == Leader {
		// Already leading. A leader has no reason to disrupt its own term.
		return nil
	}

	if err := n.becomeCandidate(); err != nil {
		return err
	}

	// A node that is the whole cluster is its own majority, so the self-vote
	// has already decided this election.
	if n.isSoleVoter() {
		return n.becomeLeader()
	}

	// Sample the log once: every request in this round describes the same
	// log, and nothing can append to it before the responses arrive.
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

	// Every member of every active configuration is asked. During a joint
	// transition that includes nodes present in only one of the two sets:
	// their votes are needed for that set's majority.
	for _, p := range n.conf.members() {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVoteRequest,
			To:           p,
			Term:         n.term,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
	return nil
}

// handleVoteRequest decides whether to grant a vote. Step has already applied
// the term rules, so m.Term equals n.term by the time this runs.
func (n *Node) handleVoteRequest(m Message) error {
	// A node votes at most once per term. Re-granting to the same candidate
	// is both allowed and necessary: the original response may have been
	// lost, and the retry has to get the same answer.
	canVote := n.vote == None || n.vote == m.From

	// The election restriction — refuse any candidate whose log is behind
	// ours, however new its term.
	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	granted := canVote && upToDate

	if granted {
		// The vote must be durable before the response goes out. A node that
		// replied and then crashed before persisting could wake up and vote
		// for a different candidate in the same term, and two leaders could
		// be elected.
		if err := n.persist(n.term, m.From); err != nil {
			return err
		}

		// Only a granted vote resets the election timer. Resetting on a
		// rejection too would let a node with a stale log keep healthy
		// followers from ever timing out and starting a useful election.
		n.electionElapsed = 0
	}

	n.send(Message{
		Type:    MsgVoteResponse,
		To:      m.From,
		Term:    n.term,
		Granted: granted,
	})
	return nil
}

// handleVoteResponse tallies a vote and resolves the election once the outcome
// is settled either way.
func (n *Node) handleVoteResponse(m Message) error {
	if n.state != Candidate {
		// The election is already over: this node won, lost, or moved to a
		// later term. Either way the vote no longer means anything.
		return nil
	}

	// Record only the first response from each voter, so a retransmission
	// cannot be counted twice.
	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.Granted
	}

	// The tally is judged by the configuration, not by a raw count. During a
	// joint transition a candidate must carry a majority of both voter sets:
	// winning on one alone would let the other set elect a different leader in
	// the same term, which is exactly the split joint consensus prevents.
	switch {
	case n.conf.voteGranted(n.votes):
		return n.becomeLeader()

	case n.conf.voteLost(n.votes):
		// A majority has become unreachable in at least one active
		// configuration, so this election cannot be won. Standing down now,
		// rather than waiting out the election timeout, gets this node back
		// to accepting a real leader's heartbeats sooner.
		return n.becomeFollower(n.term, None)
	}
	return nil
}
