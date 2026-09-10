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

	// With pre-vote on, campaigning always means asking first. Note this
	// applies to a node that is already a pre-candidate too: a pre-vote
	// round that nobody answered must retry as another pre-vote, never
	// escalate into a real one. Escalating on retry would raise the term of
	// exactly the isolated node pre-vote exists to keep quiet.
	if n.preVote {
		return n.preCampaign()
	}

	return n.realCampaign()
}

// realCampaign starts an actual election: it raises the term, votes for
// itself, and asks for votes. Everything it does is visible to the rest of
// the cluster and forces a leader to step down, which is why pre-vote guards
// the path to it.
func (n *Node) realCampaign() error {
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

// Pre-vote (§9.6).
//
// The problem it solves is that campaigning is destructive. A vote request
// carries a higher term, and §5.1 obliges every recipient to step down to it,
// so a node that has been partitioned away or has merely restarted can depose
// a leader that a majority is perfectly happy with. The cluster then spends an
// election deciding, very often, exactly what it had already decided.
//
// The fix is to ask first. A pre-vote round runs the same election arithmetic
// against a term nobody adopts: the sender does not raise its term, the
// receivers do not record a vote or reset their election timers, and if the
// answer is no then nothing happened at all. Only a node that would actually
// win goes on to raise its term for real.

// preCampaign asks whether an election would be won, without starting one.
func (n *Node) preCampaign() error {
	if err := n.becomePreCandidate(); err != nil {
		return err
	}

	// A sole voter has nobody to ask and cannot be disrupting anyone, so the
	// hypothetical round would be pure ceremony.
	if n.isSoleVoter() {
		if err := n.becomeCandidate(); err != nil {
			return err
		}
		return n.becomeLeader()
	}

	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

	for _, p := range n.conf.members() {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type: MsgPreVoteRequest,
			To:   p,
			// The term this node would campaign in, not the term it is in.
			// The receiver needs it to judge whether the candidate would be
			// current, and must not mistake it for a term anyone holds.
			Term:         n.term + 1,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
	return nil
}

// handlePreVoteRequest answers the hypothetical question.
//
// Unlike handleVoteRequest this writes nothing, changes no term, and does not
// reset the election timer. A pre-vote must leave the responder exactly as it
// was, or it would be the very disruption it is meant to avoid.
func (n *Node) handlePreVoteRequest(m Message) error {
	// A leader that is still leading, and any follower still hearing from
	// one, refuses. This is the rule that does the actual work: it is what
	// stops a rejoining node from unseating a leader the rest of the cluster
	// can still hear. A node only entertains the question once it has gone
	// long enough without contact to be considering an election itself.
	//
	// Note this is judged from the responder's own timer, not from anything
	// the candidate claims, so a node cannot talk its way past it.
	leaderIsAlive := n.state == Leader ||
		(n.leader != None && n.electionElapsed < n.randomizedElectionTimeout)

	// A term no higher than this node's own describes an election that is
	// already settled or stale.
	current := m.Term > n.term

	// The election restriction still applies (§5.4.1). There is no point
	// agreeing to an election that would have to be refused for real.
	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	granted := !leaderIsAlive && current && upToDate

	// A grant echoes the hypothetical term so the sender can match it to the
	// round it asked about. A rejection carries this node's real term, which
	// is how a node campaigning on stale information learns the truth
	// without having disturbed anybody to find it out.
	term := n.term
	if granted {
		term = m.Term
	}

	n.send(Message{
		Type:    MsgPreVoteResponse,
		To:      m.From,
		Term:    term,
		Granted: granted,
	})
	return nil
}

// handlePreVoteResponse tallies the answers and, if they are favourable,
// starts the real election.
func (n *Node) handlePreVoteResponse(m Message) error {
	if n.state != PreCandidate {
		// The round is over, or this node has moved on. Either way the
		// answer no longer refers to a question being asked.
		return nil
	}
	if m.Granted && m.Term != n.term+1 {
		// An answer to some earlier round, arriving late.
		return nil
	}

	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.Granted
	}

	switch {
	case n.conf.voteGranted(n.votes):
		// The election would be won, so it is now worth the disruption of
		// actually raising the term.
		return n.realCampaign()

	case n.conf.voteLost(n.votes):
		// It would not be won. Nothing was raised and nothing was written,
		// so standing down costs the cluster nothing at all, which is the
		// whole point.
		return n.becomeFollower(n.term, None)
	}
	return nil
}
