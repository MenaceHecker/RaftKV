package raft

func (n *Node) campaign() error {
	if n.state == Leader {
		return nil
	}

	if n.preVote {
		return n.preCampaign()
	}

	return n.realCampaign()
}

func (n *Node) realCampaign() error {
	if err := n.becomeCandidate(); err != nil {
		return err
	}

	if n.isSoleVoter() {
		return n.becomeLeader()
	}

	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

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

func (n *Node) handleVoteRequest(m Message) error {
	canVote := n.vote == None || n.vote == m.From

	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	granted := canVote && upToDate

	if granted {
		if err := n.persist(n.term, m.From); err != nil {
			return err
		}

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

func (n *Node) handleVoteResponse(m Message) error {
	if n.state != Candidate {
		return nil
	}

	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.Granted
	}

	switch {
	case n.conf.voteGranted(n.votes):
		return n.becomeLeader()

	case n.conf.voteLost(n.votes):
		return n.becomeFollower(n.term, None)
	}
	return nil
}

func (n *Node) preCampaign() error {
	if err := n.becomePreCandidate(); err != nil {
		return err
	}

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
			Type:         MsgPreVoteRequest,
			To:           p,
			Term:         n.term + 1,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
	return nil
}

func (n *Node) handlePreVoteRequest(m Message) error {
	leaderIsAlive := n.state == Leader ||
		(n.leader != None && n.electionElapsed < n.randomizedElectionTimeout)

	current := m.Term > n.term

	upToDate := n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	granted := !leaderIsAlive && current && upToDate

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

func (n *Node) handlePreVoteResponse(m Message) error {
	if n.state != PreCandidate {
		return nil
	}
	if m.Granted && m.Term != n.term+1 {
		return nil
	}

	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = m.Granted
	}

	switch {
	case n.conf.voteGranted(n.votes):
		return n.realCampaign()

	case n.conf.voteLost(n.votes):
		return n.becomeFollower(n.term, None)
	}
	return nil
}
