package transport

import (
	"fmt"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

type ErrUnknownEnum struct {
	Field string
	Value int32
}

func (e *ErrUnknownEnum) Error() string {
	return fmt.Sprintf("transport: unknown %s value %d", e.Field, e.Value)
}

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
	case raft.MsgInstallSnapshot:
		return raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT, nil
	case raft.MsgInstallSnapshotResponse:
		return raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT_RESPONSE, nil
	case raft.MsgPreVoteRequest:
		return raftkvv1.MessageType_MESSAGE_TYPE_PRE_VOTE_REQUEST, nil
	case raft.MsgPreVoteResponse:
		return raftkvv1.MessageType_MESSAGE_TYPE_PRE_VOTE_RESPONSE, nil
	default:
		return raftkvv1.MessageType_MESSAGE_TYPE_UNSPECIFIED,
			fmt.Errorf("transport: %s is a node-local signal and has no wire form", t)
	}
}

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
	case raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT:
		return raft.MsgInstallSnapshot, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT_RESPONSE:
		return raft.MsgInstallSnapshotResponse, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_PRE_VOTE_REQUEST:
		return raft.MsgPreVoteRequest, nil
	case raftkvv1.MessageType_MESSAGE_TYPE_PRE_VOTE_RESPONSE:
		return raft.MsgPreVoteResponse, nil
	default:
		return 0, &ErrUnknownEnum{Field: "MessageType", Value: int32(t)}
	}
}

func confStateToWire(cs raft.ConfState) *raftkvv1.ConfState {
	out := &raftkvv1.ConfState{Joint: cs.Joint}

	if len(cs.Voters) > 0 {
		out.Voters = make([]uint64, len(cs.Voters))
		for i, id := range cs.Voters {
			out.Voters[i] = uint64(id)
		}
	}
	if len(cs.Incoming) > 0 {
		out.Incoming = make([]uint64, len(cs.Incoming))
		for i, id := range cs.Incoming {
			out.Incoming[i] = uint64(id)
		}
	}
	if len(cs.Addrs) > 0 {
		out.Addrs = make(map[uint64]string, len(cs.Addrs))
		for id, addr := range cs.Addrs {
			out.Addrs[uint64(id)] = addr
		}
	}
	return out
}

func confStateFromWire(cs *raftkvv1.ConfState) raft.ConfState {
	if cs == nil {
		return raft.ConfState{}
	}

	out := raft.ConfState{Joint: cs.GetJoint()}
	if v := cs.GetVoters(); len(v) > 0 {
		out.Voters = make([]raft.NodeID, len(v))
		for i, id := range v {
			out.Voters[i] = raft.NodeID(id)
		}
	}
	if v := cs.GetIncoming(); len(v) > 0 {
		out.Incoming = make([]raft.NodeID, len(v))
		for i, id := range v {
			out.Incoming[i] = raft.NodeID(id)
		}
	}
	if a := cs.GetAddrs(); len(a) > 0 {
		out.Addrs = make(map[raft.NodeID]string, len(a))
		for id, addr := range a {
			out.Addrs[raft.NodeID(id)] = addr
		}
	}
	return out
}

func snapshotToWire(s *raft.Snapshot) *raftkvv1.Snapshot {
	if s == nil {
		return nil
	}
	return &raftkvv1.Snapshot{
		Index: uint64(s.Index),
		Term:  uint64(s.Term),
		Conf:  confStateToWire(s.Conf),
		Data:  s.Data,
	}
}

func snapshotFromWire(s *raftkvv1.Snapshot) (*raft.Snapshot, error) {
	if s == nil {
		return nil, nil
	}
	if s.GetIndex() == 0 {
		return nil, fmt.Errorf("transport: snapshot has no index")
	}
	return &raft.Snapshot{
		Index: raft.Index(s.GetIndex()),
		Term:  raft.Term(s.GetTerm()),
		Conf:  confStateFromWire(s.GetConf()),
		Data:  s.GetData(),
	}, nil
}

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

func stateToWire(s raft.State) raftkvv1.NodeState {
	switch s {
	case raft.Follower:
		return raftkvv1.NodeState_NODE_STATE_FOLLOWER
	case raft.PreCandidate:
		return raftkvv1.NodeState_NODE_STATE_CANDIDATE
	case raft.Candidate:
		return raftkvv1.NodeState_NODE_STATE_CANDIDATE
	case raft.Leader:
		return raftkvv1.NodeState_NODE_STATE_LEADER
	default:
		return raftkvv1.NodeState_NODE_STATE_UNSPECIFIED
	}
}

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
		Snapshot:      snapshotToWire(m.Snapshot),
	}, nil
}

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

	snap, err := snapshotFromWire(m.GetSnapshot())
	if err != nil {
		return raft.Message{}, err
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
		Snapshot:      snap,
	}, nil
}
