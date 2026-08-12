package raft

import "errors"

// Linearizable reads via the read-index protocol (§6.4).
// Here is a commit log of the design notes that explain the protocol in more detail:
// Serving a read from the leader's local state looks safe and is not. A leader
// that has been partitioned away and deposed still believes it leads, and its
// state machine still holds whatever it last applied — so it would happily
// answer with data the rest of the cluster has already moved past. The read
// would be stale, and nothing would notice.
//
// Read-index closes that hole without writing anything to the log:
//
//  1. The leader records its current commit index as the read index.
//  2. It exchanges a round of heartbeats and waits for a majority to
//     acknowledge that round specifically.
//  3. Once the state machine has applied through the read index, the read is
//     served.
//
// Step 2 is the proof. A majority acknowledged this node as leader after the
// read arrived, and any competing leader would need a majority of its own —
// the two must overlap, so no other leader could have committed anything in
// between. Step 3 is what makes the recorded index meaningful: the state
// machine must actually reflect everything committed as of that moment.
//
// The cost is one round trip per read (batched across concurrent reads), and
// no disk write at all. The alternative, a leader lease, avoids the round trip
// by trusting clocks not to drift more than a bounded amount — faster, but it
// trades a network assumption for a timing assumption. Read-index is the
// default here for that reason; §7 of the design notes records the tradeoff.

var (
	// ErrLeaderNotReady means a newly elected leader has not yet committed an
	// entry from its own term, so it cannot know which entries from previous
	// terms are actually committed and has no safe read index to hand out.
	//
	// It resolves on its own within a heartbeat or so, once the no-op entry
	// appended on election commits. Callers should retry rather than fail the
	// client request.
	ErrLeaderNotReady = errors.New("raft: leader has not yet committed an entry in its term")

	// ErrReadIndexInFlight means a read was requested with a context already
	// belonging to an outstanding read. Contexts must be unique while in
	// flight, since they are what match an acknowledgement to its round.
	ErrReadIndexInFlight = errors.New("raft: a read with this context is already in flight")
)

// ReadState is a completed read index, reported through Ready.
//
// The caller must wait until its state machine has applied through Index
// before serving the read. Ignoring that and reading immediately would give
// back state from before the entries the read index promises, which is exactly
// the staleness the protocol exists to prevent.
type ReadState struct {
	// Index is the log index the read must observe.
	Index Index
	// Context is the token supplied to ReadIndex, echoed back so the caller
	// can match this to the request that asked for it.
	Context []byte
}

// readIndexRound tracks one in-flight leadership confirmation.
type readIndexRound struct {
	// index is the commit index captured when the read was registered. It is
	// captured at registration rather than on completion because it is the
	// earliest index the read is allowed to observe; anything committed later
	// is fine to see but not required.
	index Index

	// acks records which nodes have confirmed this specific round.
	acks map[NodeID]bool

	context []byte
}

// readOnly holds the read-index rounds a leader is currently confirming.
//
// Rounds are kept in arrival order so that completing one also completes every
// earlier round: acknowledgements are monotonic, so if a later round reached a
// majority then every earlier one did too. That is what makes concurrent reads
// cost one shared round trip rather than one each.
type readOnly struct {
	rounds map[string]*readIndexRound
	order  []string
}

func newReadOnly() *readOnly {
	return &readOnly{rounds: make(map[string]*readIndexRound)}
}

// reset drops every in-flight round. A node that stops being leader can no
// longer confirm anything, so the rounds are abandoned rather than left to
// complete against stale acknowledgements.
func (r *readOnly) reset() {
	r.rounds = make(map[string]*readIndexRound)
	r.order = nil
}

// ReadIndex asks the leader to establish a read index for a linearizable read.
//
// The context identifies this read; it is echoed back in the resulting
// ReadState and must be unique among reads currently in flight. It returns
// ErrNotLeader on a node that is not the leader, so a client can be redirected.
func (n *Node) ReadIndex(context []byte) error {
	return n.Step(Message{
		Type:    MsgReadIndex,
		From:    n.id,
		Context: context,
	})
}

// handleReadIndex registers a read and starts a confirmation round.
func (n *Node) handleReadIndex(m Message) error {
	if n.state != Leader {
		return ErrNotLeader
	}
	if len(m.Context) == 0 {
		return errors.New("raft: a read index requires a non-empty context")
	}

	// A leader may not trust its own commit index until it has committed an
	// entry from its current term. Before that it cannot tell which entries
	// inherited from previous terms are genuinely committed (§5.4.2), so any
	// read index it produced could point at an entry that is later
	// overwritten.
	if !n.hasCommittedInCurrentTerm() {
		return ErrLeaderNotReady
	}

	// A single-node cluster is its own majority. There is nobody to hear
	// from, so the read is confirmed the moment it is asked for.
	if n.quorum() == 1 {
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

	// Ask every follower to confirm leadership for this round specifically.
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:    MsgHeartbeat,
			To:      p,
			Term:    n.term,
			Context: round.context,
		})
	}
	return nil
}

// hasCommittedInCurrentTerm reports whether the leader has committed at least
// one entry in its own term, which is the precondition for its commit index
// being a meaningful read index.
func (n *Node) hasCommittedInCurrentTerm() bool {
	if n.log.committed == 0 {
		return false
	}
	t, err := n.log.term(n.log.committed)
	return err == nil && t == n.term
}

// handleHeartbeat is a follower's reply to a leadership check. Step has
// already applied the term rules, so m.Term equals n.term here.
func (n *Node) handleHeartbeat(m Message) error {
	switch n.state {
	case Leader:
		// Two leaders in one term would break Election Safety.
		return errors.New("raft: received a heartbeat from a peer in this node's own leader term")
	case Candidate:
		// Someone else won this term. Concede.
		if err := n.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}

	n.leader = m.From
	n.electionElapsed = 0

	// A heartbeat carries the leader's commit index, so a follower whose
	// appends are all delivered still learns what has become committed even
	// when there is nothing new to replicate.
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

// handleHeartbeatResponse counts an acknowledgement toward its round.
func (n *Node) handleHeartbeatResponse(m Message) error {
	if n.state != Leader || len(m.Context) == 0 {
		return nil
	}

	key := string(m.Context)
	round, ok := n.readOnly.rounds[key]
	if !ok {
		// A response to a round that has already completed, or one from
		// before this node became leader. Either way there is nothing to
		// count it toward.
		return nil
	}

	round.acks[m.From] = true
	if len(round.acks) < n.quorum() {
		return nil
	}

	// This round is confirmed, and so is every round registered before it:
	// they were all outstanding while these same acknowledgements arrived, so
	// each has at least this many. Completing them together is what lets
	// concurrent reads share a single round trip.
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
	return nil
}

// cloneBytes copies a caller-supplied context so that later mutation of the
// caller's buffer cannot change what a round is keyed on.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
