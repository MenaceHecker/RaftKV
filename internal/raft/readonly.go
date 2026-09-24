package raft

import "errors"

var (
	ErrLeaderNotReady = errors.New("raft: leader has not yet committed an entry in its term")

	ErrReadIndexInFlight = errors.New("raft: a read with this context is already in flight")
)

type ReadState struct {
	Index   Index
	Context []byte
}

type readIndexRound struct {
	index Index

	acks map[NodeID]bool

	context []byte
}

type readOnly struct {
	rounds map[string]*readIndexRound
	order  []string

	outstanding string
}

func newReadOnly() *readOnly {
	return &readOnly{rounds: make(map[string]*readIndexRound)}
}

func (r *readOnly) reset() {
	r.rounds = make(map[string]*readIndexRound)
	r.order = nil
	r.outstanding = ""
}

func (n *Node) ReadIndex(context []byte) error {
	return n.Step(Message{
		Type:    MsgReadIndex,
		From:    n.id,
		Context: context,
	})
}

func (n *Node) handleReadIndex(m Message) error {
	if n.state != Leader {
		return ErrNotLeader
	}
	if len(m.Context) == 0 {
		return errors.New("raft: a read index requires a non-empty context")
	}

	if !n.hasCommittedInCurrentTerm() {
		return ErrLeaderNotReady
	}

	if n.isSoleVoter() {
		n.readStates = append(n.readStates, ReadState{
			Index:   n.log.committed,
			Context: cloneBytes(m.Context),
		})
		return nil
	}

	key := string(m.Context)
	if _, exists := n.readOnly.rounds[key]; exists {
		return ErrReadIndexInFlight
	}

	round := &readIndexRound{
		index:   n.log.committed,
		acks:    map[NodeID]bool{n.id: true},
		context: cloneBytes(m.Context),
	}
	n.readOnly.rounds[key] = round
	n.readOnly.order = append(n.readOnly.order, key)

	if n.readOnly.outstanding == "" {
		n.startReadRound(key)
	}
	return nil
}

func (n *Node) startReadRound(key string) {
	round, ok := n.readOnly.rounds[key]
	if !ok {
		return
	}
	n.readOnly.outstanding = key
	n.sendReadHeartbeats(round.context)
}

func (n *Node) sendReadHeartbeats(context []byte) {
	for _, p := range n.conf.members() {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:    MsgHeartbeat,
			To:      p,
			Term:    n.term,
			Context: context,
		})
	}
}

func (n *Node) hasCommittedInCurrentTerm() bool {
	if n.log.committed == 0 {
		return false
	}
	t, err := n.log.term(n.log.committed)
	return err == nil && t == n.term
}

func (n *Node) handleHeartbeat(m Message) error {
	switch n.state {
	case Leader:
		return errors.New("raft: received a heartbeat from a peer in this node's own leader term")
	case PreCandidate, Candidate:
		if err := n.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}

	n.leader = m.From
	n.electionElapsed = 0

	if m.CommitIndex > n.log.committed {
		n.log.commitTo(min(m.CommitIndex, n.log.lastIndex()))
	}

	n.send(Message{
		Type:    MsgHeartbeatResponse,
		To:      m.From,
		Term:    n.term,
		Context: m.Context,
	})
	return nil
}

func (n *Node) handleHeartbeatResponse(m Message) error {
	if n.state != Leader || len(m.Context) == 0 {
		return nil
	}

	if pr := n.progress[m.From]; pr != nil {
		pr.active = true
	}

	key := string(m.Context)
	round, ok := n.readOnly.rounds[key]
	if !ok {
		return nil
	}

	round.acks[m.From] = true

	if !n.conf.voteGranted(round.acks) {
		return nil
	}

	cut := 0
	for i, k := range n.readOnly.order {
		pending, ok := n.readOnly.rounds[k]
		if !ok {
			continue
		}
		n.readStates = append(n.readStates, ReadState{
			Index:   pending.index,
			Context: pending.context,
		})
		delete(n.readOnly.rounds, k)
		cut = i + 1
		if k == key {
			break
		}
	}
	n.readOnly.order = n.readOnly.order[cut:]
	n.readOnly.outstanding = ""

	if len(n.readOnly.order) > 0 {
		n.startReadRound(n.readOnly.order[len(n.readOnly.order)-1])
	}
	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
