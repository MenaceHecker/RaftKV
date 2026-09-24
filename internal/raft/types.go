package raft

import (
	"encoding/binary"
	"fmt"
)

type NodeID uint64

const None NodeID = 0

type Term uint64

type Index uint64

type State uint8

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case PreCandidate:
		return "PreCandidate"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryNoOp
	EntryConfChange
)

func (t EntryType) Valid() bool {
	switch t {
	case EntryNormal, EntryNoOp, EntryConfChange:
		return true
	default:
		return false
	}
}

type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

type MessageType uint8

const (
	MsgVoteRequest MessageType = iota
	MsgVoteResponse
	MsgAppendRequest
	MsgAppendResponse
	MsgHeartbeat
	MsgHeartbeatResponse
	MsgCampaign
	MsgPropose
	MsgReadIndex
	MsgInstallSnapshot
	MsgInstallSnapshotResponse
	MsgPreVoteRequest
	MsgPreVoteResponse
)

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
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResponse:
		return "InstallSnapshotResponse"
	case MsgPreVoteRequest:
		return "PreVoteRequest"
	case MsgPreVoteResponse:
		return "PreVoteResponse"
	default:
		return "Unknown"
	}
}

type Message struct {
	Type MessageType
	From NodeID
	To   NodeID

	Term Term

	LastLogIndex Index
	LastLogTerm  Term

	PrevLogIndex Index
	PrevLogTerm  Term

	Entries []Entry

	CommitIndex Index

	Granted bool

	Success bool

	MatchIndex Index

	ConflictIndex Index
	ConflictTerm  Term

	Context []byte

	Snapshot *Snapshot
}

type Snapshot struct {
	Index Index
	Term  Term
	Conf  ConfState
	Data  []byte
}

func (s *Snapshot) IsEmpty() bool { return s == nil || s.Index == 0 }

type ConfChangeType uint8

const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
	ConfChangeLeaveJoint
)

func (t ConfChangeType) Valid() bool {
	switch t {
	case ConfChangeAddNode, ConfChangeRemoveNode, ConfChangeLeaveJoint:
		return true
	default:
		return false
	}
}

type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
	Addr   string
}

func (cc ConfChange) Encode() []byte {
	addr := []byte(cc.Addr)
	b := make([]byte, 1+8+4+len(addr))
	b[0] = byte(cc.Type)
	binary.BigEndian.PutUint64(b[1:], uint64(cc.NodeID))
	binary.BigEndian.PutUint32(b[9:], uint32(len(addr)))
	copy(b[13:], addr)
	return b
}

func DecodeConfChange(b []byte) (ConfChange, error) {
	const minLen = 1 + 8 + 4
	if len(b) < minLen {
		return ConfChange{}, fmt.Errorf("raft: conf change payload too short (%d bytes)", len(b))
	}
	addrLen := int(binary.BigEndian.Uint32(b[9:13]))
	if len(b) != minLen+addrLen {
		return ConfChange{}, fmt.Errorf("raft: conf change payload is %d bytes, but its "+
			"address length declares %d", len(b), minLen+addrLen)
	}

	typ := ConfChangeType(b[0])
	if !typ.Valid() {
		return ConfChange{}, fmt.Errorf("raft: conf change type %d is not a known type", b[0])
	}

	return ConfChange{
		Type:   typ,
		NodeID: NodeID(binary.BigEndian.Uint64(b[1:9])),
		Addr:   string(b[13 : 13+addrLen]),
	}, nil
}
