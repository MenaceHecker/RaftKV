// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm") from first principles.
//
// The core is deliberately free of side effects: it owns no goroutines, opens
// no sockets, and reads no clocks. Time advances only when the caller invokes
// Tick, inbound messages arrive only through Step, and every outbound effect
// (messages to send, entries to apply) is returned from Ready. That makes a
// whole cluster reproducible inside a single process and a single goroutine,
// which is what the deterministic test harness, and later the chaos harness,
// are built on.
package raft

import (
	"encoding/binary"
	"fmt"
)

// NodeID identifies a single member of the cluster. IDs are stable for the
// lifetime of a node and must be non-zero; zero is reserved to mean "no node",
// for example a vote that has not been cast.
type NodeID uint64

// None is the zero NodeID, used wherever "no node" needs to be expressed.
const None NodeID = 0

// Term is a Raft term number: a logical clock that increases monotonically and
// divides time into election epochs. Every message carries a term, and a node
// that sees a term higher than its own always steps down to follower.
type Term uint64

// Index is a position in the replicated log. The log is 1-indexed; index 0 is
// the sentinel "before the first entry" position, so a node with an empty log
// reports a last index of 0.
type Index uint64

// State is the role a node currently plays. A Raft node is always in exactly
// one of these three states (§5.1).
type State uint8

const (
	// Follower is passive: it responds to candidates and leaders but issues
	// no requests of its own. All nodes start here.
	Follower State = iota
	// Candidate is campaigning for leadership of a particular term.
	Candidate
	// Leader handles all client requests and replicates them to followers.
	// There is at most one leader per term (Election Safety).
	Leader
)

// String renders the state for logs and test failure messages.
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// EntryType distinguishes ordinary state-machine commands from entries the
// Raft layer itself interprets.
type EntryType uint8

const (
	// EntryNormal carries an opaque command for the state machine. The Raft
	// core never inspects Data; only the KV state machine does.
	EntryNormal EntryType = iota
	// EntryNoOp is the empty entry a new leader appends to its own log on
	// election. Committing it commits everything before it from earlier
	// terms, which is what makes the leader's commit index safe to advance
	// (§5.4.2).
	EntryNoOp
	// EntryConfChange carries a cluster membership change. Reserved for
	// Phase 4 (joint consensus); the core does not act on it yet.
	EntryConfChange
)

// Entry is a single record in the replicated log. The pair (Term, Index)
// uniquely identifies an entry across the whole cluster: if two logs hold an
// entry with the same index and term, those entries are identical and every
// preceding entry is identical too — the Log Matching Property (§5.3).
type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

// MessageType enumerates everything that crosses a node boundary. Raft defines
// two RPCs; each is modelled here as a request/response pair. Node-local
// signals (campaign, propose) share the same envelope so that a node can be
// driven entirely through Step, which is what lets tests trigger an election
// at an exact moment instead of waiting one out.
type MessageType uint8

const (
	// MsgVoteRequest is a candidate soliciting a vote (RequestVote, §5.2).
	MsgVoteRequest MessageType = iota
	// MsgVoteResponse answers a MsgVoteRequest.
	MsgVoteResponse
	// MsgAppendRequest is a leader replicating entries, or a heartbeat when
	// Entries is empty (AppendEntries, §5.3).
	MsgAppendRequest
	// MsgAppendResponse answers a MsgAppendRequest.
	MsgAppendResponse
	// MsgHeartbeat is a leader asking its followers to confirm, right now,
	// that they still recognize it. It carries no entries and replicates
	// nothing.
	//
	// This is deliberately separate from MsgAppendRequest even though an
	// empty append also serves as a heartbeat. A linearizable read has to
	// know that a majority acknowledged leadership *after* the read was
	// registered, and an append response cannot prove that: it may have been
	// sent before the read arrived and merely be slow, during which time
	// another leader could have been elected. The echoed Context is what
	// makes an acknowledgement attributable to a specific round.
	MsgHeartbeat
	// MsgHeartbeatResponse answers a MsgHeartbeat, echoing its Context.
	MsgHeartbeatResponse
	// MsgCampaign is a local signal telling a node to start an election now
	// rather than waiting out its election timeout.
	MsgCampaign
	// MsgPropose is a local signal carrying a client command for a leader to
	// append to its log.
	MsgPropose
	// MsgReadIndex is a local signal asking the leader to establish a read
	// index for a linearizable read (§6.4).
	MsgReadIndex
)

// String renders the message type for logs and test failure messages.
func (t MessageType) String() string {
	switch t {
	case MsgVoteRequest:
		return "VoteRequest"
	case MsgVoteResponse:
		return "VoteResponse"
	case MsgAppendRequest:
		return "AppendRequest"
	case MsgAppendResponse:
		return "AppendResponse"
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatResponse:
		return "HeartbeatResponse"
	case MsgCampaign:
		return "Campaign"
	case MsgPropose:
		return "Propose"
	case MsgReadIndex:
		return "ReadIndex"
	default:
		return "Unknown"
	}
}

// Message is the single envelope for every inter-node interaction. Not all
// fields are meaningful for every type; the comments say which types use which.
// Keeping one flat struct means the transport — and later the chaos harness
// that delays, drops, and reorders traffic — has only one shape to understand.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID

	// Term is the sender's term. It is checked before anything else: a
	// higher term forces the receiver to step down, a lower term makes the
	// message stale (§5.1).
	Term Term

	// LastLogIndex and LastLogTerm describe the candidate's log in a
	// MsgVoteRequest. A voter refuses a candidate whose log is less
	// up-to-date than its own — the election restriction (§5.4.1).
	LastLogIndex Index
	LastLogTerm  Term

	// PrevLogIndex and PrevLogTerm identify the entry immediately before
	// Entries in a MsgAppendRequest. The receiver rejects the append unless
	// its log holds a matching entry at that position (§5.3).
	PrevLogIndex Index
	PrevLogTerm  Term

	// Entries carries the log entries being replicated in a
	// MsgAppendRequest, or the command being proposed in a MsgPropose. Empty
	// in a MsgAppendRequest means the message is a heartbeat.
	Entries []Entry

	// CommitIndex is the leader's commit index in a MsgAppendRequest, which
	// is how followers learn what is safe to apply.
	CommitIndex Index

	// Granted reports whether a vote was given, in a MsgVoteResponse.
	Granted bool

	// Success reports whether an append was accepted, in a
	// MsgAppendResponse.
	Success bool

	// MatchIndex is the highest index the responder now agrees with, in a
	// successful MsgAppendResponse. The leader advances its commit index
	// once a majority of match indices reach a given entry.
	MatchIndex Index

	// ConflictIndex and ConflictTerm let a rejecting follower tell the leader
	// enough to back up by a whole term at a time rather than one index per
	// round trip (§5.3). ConflictTerm is zero when the follower's log is
	// simply too short, in which case ConflictIndex is one past its last
	// entry.
	ConflictIndex Index
	ConflictTerm  Term

	// Context is an opaque token carried by MsgReadIndex, MsgHeartbeat, and
	// MsgHeartbeatResponse. The leader mints one per read-index round and
	// followers echo it back unchanged, which is what lets the leader count
	// only the acknowledgements belonging to that round.
	//
	// The Raft core never interprets it; the layer above uses it to match a
	// completed read index back to the client request that asked for it.
	Context []byte
}

// ConfChangeType describes what a membership change does.
type ConfChangeType uint8

const (
	// ConfChangeAddNode adds a new voting member to the cluster.
	ConfChangeAddNode ConfChangeType = iota
	// ConfChangeRemoveNode removes an existing voting member.
	ConfChangeRemoveNode
	// ConfChangeLeaveJoint is the second half of a joint-consensus transition.
	// It carries no NodeID; the leader proposes it automatically once the
	// enter-joint entry commits. Its commit finalises the move from C_joint to
	// C_new.
	ConfChangeLeaveJoint
)

// ConfChange is the payload stored in an EntryConfChange log entry. Every
// cluster reconfiguration — add, remove, or finalise — travels through the
// log as a ConfChange so the transition is replicated and durable before
// taking effect.
type ConfChange struct {
	// Type describes the operation.
	Type ConfChangeType
	// NodeID is the node being added or removed. It is zero for a
	// ConfChangeLeaveJoint, which carries no target node.
	NodeID NodeID
	// Addr is the network address of the node being added (e.g. "host:port").
	// It is empty for removals and leave-joint entries. The transport uses it
	// to open a connection to the new peer once the entry is applied.
	Addr string
}

// Encode serialises cc into a compact binary form for storage in a log
// entry's Data field. The layout is:
//
//	[type: 1 byte] [nodeID: 8 bytes, big-endian uint64]
//	[addrLen: 4 bytes, big-endian uint32] [addr: addrLen bytes]
func (cc ConfChange) Encode() []byte {
	addr := []byte(cc.Addr)
	b := make([]byte, 1+8+4+len(addr))
	b[0] = byte(cc.Type)
	binary.BigEndian.PutUint64(b[1:], uint64(cc.NodeID))
	binary.BigEndian.PutUint32(b[9:], uint32(len(addr)))
	copy(b[13:], addr)
	return b
}

// DecodeConfChange deserialises a ConfChange from bytes written by Encode.
func DecodeConfChange(b []byte) (ConfChange, error) {
	const minLen = 1 + 8 + 4
	if len(b) < minLen {
		return ConfChange{}, fmt.Errorf("raft: conf change payload too short (%d bytes)", len(b))
	}
	addrLen := int(binary.BigEndian.Uint32(b[9:13]))
	if len(b) < minLen+addrLen {
		return ConfChange{}, fmt.Errorf("raft: conf change payload truncated: have %d bytes, need %d",
			len(b), minLen+addrLen)
	}
	return ConfChange{
		Type:   ConfChangeType(b[0]),
		NodeID: NodeID(binary.BigEndian.Uint64(b[1:9])),
		Addr:   string(b[13 : 13+addrLen]),
	}, nil
}
