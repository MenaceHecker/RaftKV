package raft

import (
	"errors"
	"fmt"
	"math/rand"
)

// ErrNotLeader is returned when a proposal reaches a node that is not the
// leader. Phase 3's transport turns this into a redirect to the address of the
// node named by Leader.
var ErrNotLeader = errors.New("raft: node is not the leader")

// Config describes one node's participation in a cluster. Every field except
// Rand is required.
type Config struct {
	// ID is this node's identifier. It must be non-zero and must appear in
	// Peers.
	ID NodeID

	// Peers is the cluster membership this node is started with. It is used
	// only when there is no configuration to restore, which means the very
	// first boot of a cluster.
	//
	// It must contain this node's ID. Once membership has changed, the log and
	// any snapshot are authoritative and this field is ignored.
	Peers []NodeID

	// InitialConfState is the membership recovered from a snapshot, if there
	// was one. It supersedes Peers.
	//
	// A node restarting after its log was compacted past a membership change
	// cannot derive that change from the log any more, because the entry is
	// gone. The snapshot is the only remaining record of it, so restoring from
	// there is what keeps a compacted node's view of the cluster in step with
	// everyone else's.
	InitialConfState *ConfState

	// ElectionTick is how many Tick calls a follower tolerates without
	// hearing from a leader before it starts an election. The effective
	// timeout is randomized in [ElectionTick, 2*ElectionTick) so nodes
	// rarely campaign simultaneously and split the vote (§5.2).
	ElectionTick int

	// HeartbeatTick is how many Tick calls pass between a leader's
	// heartbeats. It must be well below ElectionTick, or followers will time
	// out under a perfectly healthy leader.
	HeartbeatTick int

	// Storage holds the persistent state. The node reads its hard state from
	// it at startup and writes through it on every term change and vote.
	Storage Storage

	// Rand supplies the randomized election timeout. Tests pass a seeded
	// source so a whole cluster run is reproducible; leaving it nil derives
	// a source from the node ID.
	Rand *rand.Rand
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: config ID must be non-zero")
	}

	// A restored configuration replaces the peer list entirely, so the peer
	// list only has to make sense when there is nothing to restore.
	if c.InitialConfState != nil && !c.InitialConfState.IsEmpty() {
		if c.Storage == nil {
			return errors.New("raft: config Storage must not be nil")
		}
		if c.ElectionTick <= 0 {
			return errors.New("raft: config ElectionTick must be positive")
		}
		if c.HeartbeatTick <= 0 {
			return errors.New("raft: config HeartbeatTick must be positive")
		}
		if c.HeartbeatTick >= c.ElectionTick {
			return fmt.Errorf("raft: config HeartbeatTick (%d) must be less than ElectionTick (%d)",
				c.HeartbeatTick, c.ElectionTick)
		}
		return nil
	}

	if len(c.Peers) == 0 {
		return errors.New("raft: config Peers must not be empty")
	}
	found := false
	seen := make(map[NodeID]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p == None {
			return errors.New("raft: config Peers must not contain the zero ID")
		}
		if seen[p] {
			return fmt.Errorf("raft: config Peers contains duplicate ID %d", p)
		}
		seen[p] = true
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("raft: config Peers must contain this node's ID %d", c.ID)
	}
	if c.ElectionTick <= 0 {
		return errors.New("raft: config ElectionTick must be positive")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("raft: config HeartbeatTick must be positive")
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return fmt.Errorf("raft: config HeartbeatTick (%d) must be less than ElectionTick (%d)",
			c.HeartbeatTick, c.ElectionTick)
	}
	if c.Storage == nil {
		return errors.New("raft: config Storage must not be nil")
	}
	return nil
}

// progress is a leader's bookkeeping for one follower.
type progress struct {
	// next is the index of the next entry to send. It is optimistic: the
	// leader guesses its own last index plus one and backs off on rejection.
	next Index
	// match is the highest index known to be replicated on that follower. It
	// is conservative — only a successful append moves it. Commit decisions
	// are made from match values, never from next.
	match Index
}

// Node is a single Raft peer, and it is a pure state machine: it never blocks,
// never spawns a goroutine, and never reads the wall clock. The caller drives
// it by calling Tick to advance logical time and Step to deliver a message,
// then drains the resulting effects with Ready.
//
// That design is what makes a five-node cluster reproducible in one goroutine,
// and it is the foundation the Phase 5 chaos harness needs: with no real time
// and no real network anywhere in the core, a failing scenario replays exactly.
//
// A Node is not safe for concurrent use. The intended pattern is one goroutine
// per node owning all of Tick, Step, Ready, and Advance.
type Node struct {
	id NodeID

	// conf is the cluster membership. Every quorum decision — commit,
	// election, read-index confirmation — is asked of it rather than computed
	// from a peer count, because during a joint transition there are two
	// voter sets and a majority of one is not a decision.
	conf config

	// jointEntryIndex is the log index of the entry that opened the current
	// transition, or zero when none is open. The leader waits for it to commit
	// before finishing the transition.
	jointEntryIndex Index

	// baseConf is the membership as of the point the log begins: the
	// configuration every conf-change entry still in the log is applied on top
	// of. conf is always baseConf plus those entries, which is what lets a
	// truncation that removes a change be undone by simply deriving conf
	// again.
	//
	// It comes from the snapshot when there is one, and from the statically
	// configured peer set only on a cluster's very first boot. That is what
	// makes compaction safe alongside membership changes: an entry that has
	// been compacted away is already reflected in the base.
	baseConf config

	state State
	// term is the node's current term, mirroring the persisted hard state.
	term Term
	// vote is who this node voted for in term, or None.
	vote NodeID
	// leader is the leader this node currently recognizes, or None if it
	// knows of none in this term.
	leader NodeID

	log *raftLog

	// votes records responses to this node's own campaign, keyed by voter.
	// Only meaningful while Candidate.
	votes map[NodeID]bool

	// progress tracks replication to each peer. Only meaningful while
	// Leader; rebuilt from scratch on election.
	progress map[NodeID]*progress

	// readOnly tracks in-flight read-index confirmations. Only meaningful
	// while Leader; abandoned on any state change.
	readOnly *readOnly

	// readStates accumulates completed read indexes until Ready collects
	// them, mirroring how msgs accumulates outbound messages.
	readStates []ReadState

	// pendingSnapshot holds a snapshot installed from a leader, waiting for
	// Ready to hand it to the state machine.
	pendingSnapshot *Snapshot

	electionElapsed  int
	heartbeatElapsed int
	electionTick     int
	heartbeatTick    int
	// randomizedElectionTimeout is redrawn on every state change, so a
	// repeated split vote does not repeat the same timing.
	randomizedElectionTimeout int

	rand    *rand.Rand
	storage Storage

	// msgs accumulates outbound messages until Ready collects them.
	msgs []Message
}

// NewNode builds a node from cfg and restores whatever the previous
// incarnation persisted. A node always starts as a follower, even if it was
// leader before it crashed: leadership is not durable, only the term and vote
// are.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	hs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: reading initial state: %w", err)
	}

	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(int64(cfg.ID)))
	}

	// A restored configuration wins over the static peer list: it reflects
	// every membership change that committed before the snapshot was taken,
	// including ones whose log entries have since been compacted away.
	base := newConfig(cfg.Peers)
	if cfg.InitialConfState != nil && !cfg.InitialConfState.IsEmpty() {
		base = configFromState(*cfg.InitialConfState)
	}

	n := &Node{
		id:            cfg.ID,
		conf:          base.clone(),
		baseConf:      base,
		state:         Follower,
		term:          hs.Term,
		vote:          hs.VotedFor,
		leader:        None,
		log:           newRaftLog(cfg.Storage),
		votes:         make(map[NodeID]bool),
		progress:      make(map[NodeID]*progress),
		readOnly:      newReadOnly(),
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		rand:          rng,
		storage:       cfg.Storage,
	}
	n.resetElectionTimeout()

	// A restarting node's membership lives in its log, not in the peer list it
	// was started with. Deriving it here means a node that crashed part-way
	// through a membership change comes back believing whatever its log says,
	// which is the same thing every other node derives from the same entries.
	if err := n.rebuildConfig(); err != nil {
		return nil, err
	}

	return n, nil
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// State returns the node's current role.
func (n *Node) State() State { return n.state }

// Term returns the node's current term.
func (n *Node) Term() Term { return n.term }

// Leader returns the leader this node currently recognizes, or None if it
// knows of none. A follower learns the leader from the first AppendEntries of
// the term.
func (n *Node) Leader() NodeID { return n.leader }

// LastIndex returns the index of the last entry in this node's log.
func (n *Node) LastIndex() Index { return n.log.lastIndex() }

// CommitIndex returns the highest index this node knows to be committed.
func (n *Node) CommitIndex() Index { return n.log.committed }

// Members returns every node in any currently active configuration, sorted.
func (n *Node) Members() []NodeID { return n.conf.members() }

// InJointConfiguration reports whether a membership change is in progress.
func (n *Node) InJointConfiguration() bool { return n.conf.inJoint() }

// isSoleVoter reports whether this node alone constitutes every majority the
// cluster needs.
//
// A single-node cluster has nobody to hear from, so decisions that would
// otherwise wait for acknowledgements are settled immediately. Asking the
// configuration rather than counting peers keeps that true during a joint
// transition, where being the only member of one set is not enough.
func (n *Node) isSoleVoter() bool {
	members := n.conf.members()
	return len(members) == 1 && members[0] == n.id
}

// Ready is the batch of effects produced since the previous Ready. The caller
// sends Messages, applies CommittedEntries in order, then calls Advance.
type Ready struct {
	// Messages are the messages to deliver to other nodes. Anything these
	// promise has already been persisted, so they are safe to send the
	// moment they are handed over.
	Messages []Message

	// CommittedEntries are the entries newly known to be committed, in index
	// order, ready for the state machine.
	CommittedEntries []Entry

	// ReadStates are read indexes whose leadership has been confirmed. The
	// caller must wait until the state machine has applied through each
	// Index before serving the corresponding read.
	ReadStates []ReadState

	// Snapshot is a state machine image received from the leader, or nil.
	//
	// When present it must be restored *before* CommittedEntries are applied:
	// it replaces the state machine wholesale, and anything applied first
	// would be overwritten by it.
	Snapshot *Snapshot
}

// IsEmpty reports whether there is nothing for the caller to do.
func (r Ready) IsEmpty() bool {
	return len(r.Messages) == 0 && len(r.CommittedEntries) == 0 &&
		len(r.ReadStates) == 0 && r.Snapshot == nil
}

// Ready drains the pending effects.
//
// It deliberately does not mark the committed entries as applied. The caller
// does that through Advance once they are durable in the state machine, so a
// crash part-way through applying replays those entries rather than skipping
// them.
func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs, ReadStates: n.readStates, Snapshot: n.pendingSnapshot}
	n.msgs = nil
	n.readStates = nil
	n.pendingSnapshot = nil

	committed, err := n.log.nextCommitted()
	if err != nil {
		panic(fmt.Sprintf("raft: reading committed entries: %v", err))
	}
	rd.CommittedEntries = committed
	return rd
}

// Advance reports that the entries from rd have been applied.
func (n *Node) Advance(rd Ready) {
	if rd.Snapshot != nil {
		// The state machine has been rebuilt from the image, so it is applied
		// through the snapshot's index whether or not entries followed.
		if rd.Snapshot.Index > n.log.applied {
			n.log.applied = rd.Snapshot.Index
		}
	}
	if len(rd.CommittedEntries) > 0 {
		n.log.appliedTo(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
	}
}

// Tick advances the node's logical clock by one unit. A follower or candidate
// that goes its whole election timeout without contact starts an election; a
// leader sends heartbeats every HeartbeatTick ticks.
//
// It returns an error only when starting an election fails to persist the new
// term. That is unrecoverable: the node must not carry on as though the term
// change had happened.
func (n *Node) Tick() error {
	switch n.state {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedElectionTimeout {
			return n.campaign()
		}
	}
	return nil
}

// Step delivers a message to the node.
//
// Every message is filtered through the term rules of §5.1 before its type is
// considered: a higher term always makes this node a follower of that term,
// and a lower term is stale. Handling this once, here, is what keeps the
// per-message handlers free of term checks.
func (n *Node) Step(m Message) error {
	switch {
	case m.Type == MsgCampaign:
		return n.campaign()

	case m.Type == MsgPropose:
		return n.propose(m.Entries)

	case m.Type == MsgReadIndex:
		return n.handleReadIndex(m)

	case m.Term > n.term:
		// A newer term means this node's information is out of date,
		// whatever its role. Step down, then handle the message as a
		// follower of the new term.
		//
		// The leader is taken from the message only when the sender must be
		// the leader. A vote request proves an election is under way, not
		// who won it, so in that case the leader stays unknown.
		leader := m.From
		if m.Type == MsgVoteRequest {
			leader = None
		}
		if err := n.becomeFollower(m.Term, leader); err != nil {
			return err
		}

	case m.Term < n.term:
		// Stale message. A vote request gets an explicit rejection so the
		// sender learns the real term and steps down promptly. Anything else
		// is dropped: the sender will find out from its own traffic.
		if m.Type == MsgVoteRequest {
			n.send(Message{
				Type:    MsgVoteResponse,
				To:      m.From,
				Term:    n.term,
				Granted: false,
			})
		}
		return nil
	}

	switch m.Type {
	case MsgVoteRequest:
		return n.handleVoteRequest(m)
	case MsgVoteResponse:
		return n.handleVoteResponse(m)
	case MsgAppendRequest:
		return n.handleAppendRequest(m)
	case MsgAppendResponse:
		return n.handleAppendResponse(m)
	case MsgHeartbeat:
		return n.handleHeartbeat(m)
	case MsgHeartbeatResponse:
		return n.handleHeartbeatResponse(m)
	case MsgInstallSnapshot:
		return n.handleInstallSnapshot(m)
	case MsgInstallSnapshotResponse:
		return n.handleInstallSnapshotResponse(m)
	default:
		return fmt.Errorf("raft: unhandled message type %s", m.Type)
	}
}

// Propose asks a leader to append a command to the log. It returns
// ErrNotLeader on any other node.
func (n *Node) Propose(data []byte) error {
	return n.Step(Message{
		Type:    MsgPropose,
		From:    n.id,
		Entries: []Entry{{Type: EntryNormal, Data: data}},
	})
}

// send queues a message for delivery. From is filled in here so no caller can
// forget it.
func (n *Node) send(m Message) {
	m.From = n.id
	n.msgs = append(n.msgs, m)
}

// becomeFollower steps down to term, recognizing leader, which may be None. It
// persists the term change before returning, because a node may not act on a
// term it has not durably recorded.
func (n *Node) becomeFollower(term Term, leader NodeID) error {
	if term < n.term {
		return fmt.Errorf("raft: cannot step down from term %d to %d", n.term, term)
	}

	// Entering a new term clears the vote, since this node has not voted in
	// it yet. Staying in the same term keeps the existing vote, which is what
	// stops a node voting twice in one term.
	if term > n.term {
		if err := n.persist(term, None); err != nil {
			return err
		}
	}

	n.state = Follower
	n.leader = leader
	n.reset()
	return nil
}

// becomeCandidate advances to the next term and votes for itself.
func (n *Node) becomeCandidate() error {
	if n.state == Leader {
		return errors.New("raft: a leader cannot become a candidate")
	}
	if err := n.persist(n.term+1, n.id); err != nil {
		return err
	}

	n.state = Candidate
	n.leader = None
	n.reset()
	n.votes = map[NodeID]bool{n.id: true}
	return nil
}

// becomeLeader takes leadership of the current term and appends the no-op entry
// that lets a new leader commit entries carried over from earlier terms
// (§5.4.2).
func (n *Node) becomeLeader() error {
	if n.state != Candidate {
		return fmt.Errorf("raft: cannot become leader from %s", n.state)
	}

	n.state = Leader
	n.leader = n.id
	n.reset()

	// Reset replication state: next is optimistic, match is empty. A new
	// leader knows nothing about its followers' logs until they respond.
	members := n.conf.members()
	n.progress = make(map[NodeID]*progress, len(members))
	for _, p := range members {
		n.progress[p] = &progress{next: n.log.lastIndex() + 1}
	}

	noop := Entry{
		Term:  n.term,
		Index: n.log.lastIndex() + 1,
		Type:  EntryNoOp,
	}
	if _, err := n.log.append([]Entry{noop}); err != nil {
		return err
	}
	// A leader trivially holds its own entries.
	n.progress[n.id].match = n.log.lastIndex()
	n.progress[n.id].next = n.log.lastIndex() + 1

	// Reconsider the commit index before sending anything. In a single-node
	// cluster the leader's own append is already a majority, and no response
	// will ever arrive to trigger this later — without it such a cluster
	// elects a leader that can never commit its own no-op, and therefore
	// never commits anything at all.
	//
	// In a larger cluster this is a no-op: the followers' match indexes are
	// still zero, so no majority exists yet.
	n.maybeCommit()

	n.broadcastAppend()
	return nil
}

// persist records a term and vote to stable storage, updating the in-memory
// copy only after the write succeeds so the two can never disagree in the
// dangerous direction.
func (n *Node) persist(term Term, vote NodeID) error {
	if err := n.storage.SetHardState(HardState{Term: term, VotedFor: vote}); err != nil {
		return fmt.Errorf("raft: persisting hard state: %w", err)
	}
	n.term = term
	n.vote = vote
	return nil
}

// reset clears the per-role timers and vote tally on a state change.
func (n *Node) reset() {
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.votes = make(map[NodeID]bool)

	// In-flight read confirmations belong to the role being left. A node that
	// is no longer leader cannot confirm anything, and one that has just
	// become leader must not inherit acknowledgements collected under a
	// previous term.
	n.readOnly.reset()

	n.resetElectionTimeout()
}

// resetElectionTimeout draws a fresh timeout from
// [ElectionTick, 2*ElectionTick). Redrawing on every reset is what breaks the
// symmetry of a split vote: two candidates that tied will almost certainly not
// tie again.
func (n *Node) resetElectionTimeout() {
	n.randomizedElectionTimeout = n.electionTick + n.rand.Intn(n.electionTick)
}
