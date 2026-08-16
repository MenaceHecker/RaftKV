// Package transport carries Raft messages between cluster members over gRPC,
// and exposes the key-value API to clients.
//
// It is the only place that knows both the consensus core's types and the wire
// format. That separation is deliberate: the core stays free of generated code
// and protobuf semantics, and the wire format can change — a new field, a
// renamed enum — without any of it reaching the algorithm.
package transport

import (
	"fmt"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Conversion between the core's types and the wire types.
//
// The two representations are kept separate rather than having the core use
// generated structs directly. Protobuf's semantics do not match the
// algorithm's: proto3 cannot tell an unset field from a zero one, generated
// types carry mutable internal state, and the wire format has to stay
// compatible across versions in ways the in-memory representation does not.
// Converting at the boundary costs a copy per message and keeps every one of
// those concerns out of the consensus logic.
//
// Both directions are total and explicit. An unrecognized enum value is an
// error rather than a silent zero: a message from a newer or corrupted peer
// must not be quietly reinterpreted as a different, valid message.

// ErrUnknownEnum means a wire value has no counterpart in the core, which
// happens if a peer speaks a newer protocol version or a message is damaged.
type ErrUnknownEnum struct {
	Field string
	Value int32
}

func (e *ErrUnknownEnum) Error() string {
	return fmt.Sprintf("transport: unknown %s value %d", e.Field, e.Value)
}

// messageTypeToWire maps a core message type onto the wire enum.
//
// Only inter-node messages appear here. MsgCampaign, MsgPropose, and
// MsgReadIndex are local signals a node sends to itself, and putting them on
// the wire would let a peer drive another node's internal state machine
// directly — so they have no wire representation at all.
func messageTypeToWire(t raft.MessageType) (raftkvv1.MessageType, error) {
	switch t {
	case raft.MsgVoteRequest:
		return raftkvv1.MessageType_MESSAGE_TYPE_VOTE_REQUEST, nil
	case raft.MsgVoteResponse:
		return raftkvv1.MessageType_MESSAGE_TYPE_VOTE_RESPONSE, nil
	case raft.MsgAppendRequest:
		return raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST, nil
	case raft.MsgAppendResponse:
		return raftkvv1.MessageType_MESSAGE_TYPE_APPEND_RESPONSE, nil
	case raft.MsgHeartbeat:
		return raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT, nil
	case raft.MsgHeartbeatResponse:
		return raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE, nil
	default:
		return raftkvv1.MessageType_MESSAGE_TYPE_UNSPECIFIED,
			fmt.Errorf("transport: %s is a node-local signal and has no wire form", t)
	}
}

// messageTypeFromWire maps a wire enum onto a core message type.
func messageTypeFromWire(t raftkvv1.MessageType) (raft.MessageType, error) {
	switch t {
	case raftkvv1.MessageType_MESSAGE_TYPE_VOTE_REQUEST:
		return raft.MsgVoteRequest, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_VOTE_RESPONSE:
		return raft.MsgVoteResponse, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST:
		return raft.MsgAppendRequest, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_APPEND_RESPONSE:
		return raft.MsgAppendResponse, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT:
		return raft.MsgHeartbeat, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE:
		return raft.MsgHeartbeatResponse, nil
	default:
		return 0, &ErrUnknownEnum{Field: "MessageType", Value: int32(t)}
	}
}

// entryTypeToWire maps a core entry type onto the wire enum.
func entryTypeToWire(t raft.EntryType) (raftkvv1.EntryType, error) {
	switch t {
	case raft.EntryNormal:
		return raftkvv1.EntryType_ENTRY_TYPE_NORMAL, nil
	case raft.EntryNoOp:
		return raftkvv1.EntryType_ENTRY_TYPE_NO_OP, nil
	case raft.EntryConfChange:
		return raftkvv1.EntryType_ENTRY_TYPE_CONF_CHANGE, nil
	default:
		return raftkvv1.EntryType_ENTRY_TYPE_UNSPECIFIED,
			&ErrUnknownEnum{Field: "EntryType", Value: int32(t)}
	}
}

// entryTypeFromWire maps a wire enum onto a core entry type.
func entryTypeFromWire(t raftkvv1.EntryType) (raft.EntryType, error) {
	switch t {
	case raftkvv1.EntryType_ENTRY_TYPE_NORMAL:
		return raft.EntryNormal, nil
	case raftkvv1.EntryType_ENTRY_TYPE_NO_OP:
		return raft.EntryNoOp, nil
	case raftkvv1.EntryType_ENTRY_TYPE_CONF_CHANGE:
		return raft.EntryConfChange, nil
	default:
		return 0, &ErrUnknownEnum{Field: "EntryType", Value: int32(t)}
	}
}

// stateToWire maps a core role onto the wire enum, for Status responses.
func stateToWire(s raft.State) raftkvv1.NodeState {
	switch s {
	case raft.Follower:
		return raftkvv1.NodeState_NODE_STATE_FOLLOWER
	case raft.Candidate:
		return raftkvv1.NodeState_NODE_STATE_CANDIDATE
	case raft.Leader:
		return raftkvv1.NodeState_NODE_STATE_LEADER
	default:
		return raftkvv1.NodeState_NODE_STATE_UNSPECIFIED
	}
}

// entryToWire converts one log entry.
func entryToWire(e raft.Entry) (*raftkvv1.Entry, error) {
	typ, err := entryTypeToWire(e.Type)
	if err != nil {
		return nil, err
	}
	return &raftkvv1.Entry{
		Term:  uint64(e.Term),
		Index: uint64(e.Index),
		Type:  typ,
		Data:  e.Data,
	}, nil
}

// entryFromWire converts one log entry.
func entryFromWire(e *raftkvv1.Entry) (raft.Entry, error) {
	if e == nil {
		return raft.Entry{}, fmt.Errorf("transport: nil entry")
	}
	typ, err := entryTypeFromWire(e.GetType())
	if err != nil {
		return raft.Entry{}, err
	}
	return raft.Entry{
		Term:  raft.Term(e.GetTerm()),
		Index: raft.Index(e.GetIndex()),
		Type:  typ,
		Data:  e.GetData(),
	}, nil
}

// MessageToWire converts a Raft message into its wire form.
func MessageToWire(m raft.Message) (*raftkvv1.Message, error) {
	typ, err := messageTypeToWire(m.Type)
	if err != nil {
		return nil, err
	}

	var entries []*raftkvv1.Entry
	if len(m.Entries) > 0 {
		entries = make([]*raftkvv1.Entry, len(m.Entries))
		for i, e := range m.Entries {
			we, err := entryToWire(e)
			if err != nil {
				return nil, fmt.Errorf("transport: encoding entry %d: %w", e.Index, err)
			}
			entries[i] = we
		}
	}

	return &raftkvv1.Message{
		Type:          typ,
		From:          uint64(m.From),
		To:            uint64(m.To),
		Term:          uint64(m.Term),
		LastLogIndex:  uint64(m.LastLogIndex),
		LastLogTerm:   uint64(m.LastLogTerm),
		PrevLogIndex:  uint64(m.PrevLogIndex),
		PrevLogTerm:   uint64(m.PrevLogTerm),
		Entries:       entries,
		CommitIndex:   uint64(m.CommitIndex),
		Granted:       m.Granted,
		Success:       m.Success,
		MatchIndex:    uint64(m.MatchIndex),
		ConflictIndex: uint64(m.ConflictIndex),
		ConflictTerm:  uint64(m.ConflictTerm),
		Context:       m.Context,
	}, nil
}

// MessageFromWire converts a wire message into the core's form.
//
// It rejects anything it cannot represent exactly. A message that decodes into
// something subtly different from what was sent is worse than one that fails
// to decode at all, because the receiver would act on it.
func MessageFromWire(m *raftkvv1.Message) (raft.Message, error) {
	if m == nil {
		return raft.Message{}, fmt.Errorf("transport: nil message")
	}

	typ, err := messageTypeFromWire(m.GetType())
	if err != nil {
		return raft.Message{}, err
	}

	var entries []raft.Entry
	if len(m.GetEntries()) > 0 {
		entries = make([]raft.Entry, len(m.GetEntries()))
		for i, we := range m.GetEntries() {
			e, err := entryFromWire(we)
			if err != nil {
				return raft.Message{}, fmt.Errorf("transport: decoding entry %d: %w", i, err)
			}
			entries[i] = e
		}
	}

	return raft.Message{
		Type:          typ,
		From:          raft.NodeID(m.GetFrom()),
		To:            raft.NodeID(m.GetTo()),
		Term:          raft.Term(m.GetTerm()),
		LastLogIndex:  raft.Index(m.GetLastLogIndex()),
		LastLogTerm:   raft.Term(m.GetLastLogTerm()),
		PrevLogIndex:  raft.Index(m.GetPrevLogIndex()),
		PrevLogTerm:   raft.Term(m.GetPrevLogTerm()),
		Entries:       entries,
		CommitIndex:   raft.Index(m.GetCommitIndex()),
		Granted:       m.GetGranted(),
		Success:       m.GetSuccess(),
		MatchIndex:    raft.Index(m.GetMatchIndex()),
		ConflictIndex: raft.Index(m.GetConflictIndex()),
		ConflictTerm:  raft.Term(m.GetConflictTerm()),
		Context:       m.GetContext(),
	}, nil
}
