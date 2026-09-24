package raft

import (
	"errors"
	"fmt"
	"math/rand"
)

var ErrNotLeader = errors.New("raft: node is not the leader")

const DefaultMaxAppendBytes = 1 << 20

const DefaultMaxCommittedEntries = 1000

const entryOverheadBytes = 32

func limitEntries(entries []Entry, budget int) []Entry {
	total := 0
	for i, e := range entries {
		size := len(e.Data) + entryOverheadBytes
		if i > 0 && total+size > budget {
			return entries[:i]
		}
		total += size
	}
	return entries
}

type Config struct {
	ID NodeID

	Peers []NodeID

	InitialConfState *ConfState

	ElectionTick int

	HeartbeatTick int

	MaxCommittedEntries int

	MaxAppendBytes int

	CheckQuorum bool

	PreVote bool

	Storage Storage

	Rand *rand.Rand
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("raft: config ID must be non-zero")
	}

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

type progress struct {
	next     Index
	match    Index
	heldBack bool
	active   bool
}

type Node struct {
	id NodeID

	conf config

	confSeq uint64

	jointEntryIndex Index

	baseConf config

	state  State
	term   Term
	vote   NodeID
	leader NodeID

	log *raftLog

	votes map[NodeID]bool

	progress map[NodeID]*progress

	readOnly *readOnly

	readStates []ReadState

	pendingSnapshot *Snapshot

	electionElapsed  int
	heartbeatElapsed int
	electionTick     int

	preVote bool

	checkQuorum bool

	maxAppendBytes int

	maxCommittedEntries       int
	heartbeatTick             int
	randomizedElectionTimeout int

	rand    *rand.Rand
	storage Storage

	msgs []Message
}

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

	maxAppendBytes := cfg.MaxAppendBytes
	if maxAppendBytes <= 0 {
		maxAppendBytes = DefaultMaxAppendBytes
	}

	maxCommittedEntries := cfg.MaxCommittedEntries
	if maxCommittedEntries <= 0 {
		maxCommittedEntries = DefaultMaxCommittedEntries
	}

	base := newConfig(cfg.Peers)
	if cfg.InitialConfState != nil && !cfg.InitialConfState.IsEmpty() {
		base = configFromState(*cfg.InitialConfState)
	}

	n := &Node{
		id:                  cfg.ID,
		conf:                base.clone(),
		baseConf:            base,
		state:               Follower,
		term:                hs.Term,
		vote:                hs.VotedFor,
		leader:              None,
		log:                 newRaftLog(cfg.Storage),
		votes:               make(map[NodeID]bool),
		progress:            make(map[NodeID]*progress),
		readOnly:            newReadOnly(),
		electionTick:        cfg.ElectionTick,
		heartbeatTick:       cfg.HeartbeatTick,
		preVote:             cfg.PreVote,
		checkQuorum:         cfg.CheckQuorum,
		maxAppendBytes:      maxAppendBytes,
		maxCommittedEntries: maxCommittedEntries,
		rand:                rng,
		storage:             cfg.Storage,
	}
	n.resetElectionTimeout()

	if err := n.rebuildConfig(); err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) State() State { return n.state }

func (n *Node) Term() Term { return n.term }

func (n *Node) Leader() NodeID { return n.leader }

func (n *Node) LastIndex() Index { return n.log.lastIndex() }

func (n *Node) CommitIndex() Index { return n.log.committed }

func (n *Node) TermAt(i Index) (Term, error) { return n.log.term(i) }

func (n *Node) Members() []NodeID { return n.conf.members() }

func (n *Node) InJointConfiguration() bool { return n.conf.inJoint() }

func (n *Node) isSoleVoter() bool {
	members := n.conf.members()
	return len(members) == 1 && members[0] == n.id
}

type Ready struct {
	Messages []Message

	CommittedEntries []Entry

	ReadStates []ReadState

	Snapshot *Snapshot
}

func (r Ready) IsEmpty() bool {
	return len(r.Messages) == 0 && len(r.CommittedEntries) == 0 &&
		len(r.ReadStates) == 0 && r.Snapshot == nil
}

func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs, ReadStates: n.readStates, Snapshot: n.pendingSnapshot}
	n.msgs = nil
	n.readStates = nil
	n.pendingSnapshot = nil

	committed, err := n.log.nextCommitted(n.maxCommittedEntries)
	if err != nil {
		panic(fmt.Sprintf("raft: reading committed entries: %v", err))
	}
	rd.CommittedEntries = committed
	return rd
}

func (n *Node) quorumActive() bool {
	active := make(map[NodeID]bool, len(n.progress))
	active[n.id] = true
	for id, pr := range n.progress {
		if pr.active {
			active[id] = true
		}
	}
	return n.conf.voteGranted(active)
}

func (n *Node) clearActive() {
	for _, pr := range n.progress {
		pr.active = false
	}
}

func (n *Node) HasUnapplied() bool { return n.log.hasUnapplied() }

func (n *Node) Advance(rd Ready) {
	if rd.Snapshot != nil {
		if rd.Snapshot.Index > n.log.applied {
			n.log.applied = rd.Snapshot.Index
		}
	}
	if len(rd.CommittedEntries) > 0 {
		n.log.appliedTo(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
	}
}

func (n *Node) Tick() error {
	switch n.state {
	case Leader:
		if n.checkQuorum {
			n.electionElapsed++
			if n.electionElapsed >= n.randomizedElectionTimeout {
				n.electionElapsed = 0
				if !n.quorumActive() {
					return n.becomeFollower(n.term, None)
				}
				n.clearActive()
			}
		}

		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
	case Follower, PreCandidate, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedElectionTimeout {
			return n.campaign()
		}
	}
	return nil
}

func (n *Node) Step(m Message) error {
	switch {
	case m.Type == MsgCampaign:
		return n.campaign()

	case m.Type == MsgPropose:
		return n.propose(m.Entries)

	case m.Type == MsgReadIndex:
		return n.handleReadIndex(m)

	case m.Type == MsgPreVoteRequest:
		return n.handlePreVoteRequest(m)

	case m.Type == MsgPreVoteResponse && m.Granted:
		return n.handlePreVoteResponse(m)

	case m.Type == MsgPreVoteResponse && m.Term > n.term:
		return n.becomeFollower(m.Term, None)

	case m.Type == MsgPreVoteResponse:
		return n.handlePreVoteResponse(m)

	case m.Term > n.term:
		leader := m.From
		if m.Type == MsgVoteRequest {
			leader = None
		}
		if err := n.becomeFollower(m.Term, leader); err != nil {
			return err
		}

	case m.Term < n.term:
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
	case MsgPreVoteRequest:
		return n.handlePreVoteRequest(m)
	case MsgPreVoteResponse:
		return n.handlePreVoteResponse(m)
	default:
		return fmt.Errorf("raft: unhandled message type %s", m.Type)
	}
}

func (n *Node) Propose(data []byte) error {
	return n.ProposeBatch([][]byte{data})
}

func (n *Node) ProposeBatch(datas [][]byte) error {
	if len(datas) == 0 {
		return nil
	}
	entries := make([]Entry, len(datas))
	for i, d := range datas {
		entries[i] = Entry{Type: EntryNormal, Data: d}
	}
	return n.Step(Message{
		Type:    MsgPropose,
		From:    n.id,
		Entries: entries,
	})
}

func (n *Node) send(m Message) {
	m.From = n.id
	n.msgs = append(n.msgs, m)
}

func (n *Node) becomeFollower(term Term, leader NodeID) error {
	if term < n.term {
		return fmt.Errorf("raft: cannot step down from term %d to %d", n.term, term)
	}

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

func (n *Node) becomePreCandidate() error {
	if n.state == Leader {
		return errors.New("raft: a leader cannot become a pre-candidate")
	}

	n.state = PreCandidate
	n.leader = None
	n.reset()
	n.votes = map[NodeID]bool{n.id: true}
	return nil
}

func (n *Node) becomeLeader() error {
	if n.state != Candidate {
		return fmt.Errorf("raft: cannot become leader from %s", n.state)
	}

	n.state = Leader
	n.leader = n.id
	n.reset()

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
	n.progress[n.id].match = n.log.lastIndex()
	n.progress[n.id].next = n.log.lastIndex() + 1

	n.maybeCommit()

	n.broadcastAppend()
	return nil
}

func (n *Node) persist(term Term, vote NodeID) error {
	if err := n.storage.SetHardState(HardState{Term: term, VotedFor: vote}); err != nil {
		return fmt.Errorf("raft: persisting hard state: %w: %w", ErrStorage, err)
	}
	n.term = term
	n.vote = vote
	return nil
}

func (n *Node) reset() {
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.votes = make(map[NodeID]bool)

	n.readOnly.reset()

	n.resetElectionTimeout()
}

func (n *Node) resetElectionTimeout() {
	n.randomizedElectionTimeout = n.electionTick + n.rand.Intn(n.electionTick)
}
