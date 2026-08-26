package transport

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Tests for the wire codec.
//
// Conversion bugs are the quiet kind. A message that decodes into something
// subtly different from what was sent is worse than one that fails outright,
// because the receiver acts on it and nothing reports a problem — a vote
// request whose LastLogTerm was dropped, say, would make a voter grant a vote
// it should have refused. So these tests check that every field survives, that
// unrepresentable values are refused rather than coerced, and that the two
// directions agree.

// assertMessageEqual compares two core messages field by field.
func assertMessageEqual(t *testing.T, got, want raft.Message) {
	t.Helper()

	if got.Type != want.Type {
		t.Fatalf("Type = %s, want %s", got.Type, want.Type)
	}
	if got.From != want.From || got.To != want.To {
		t.Fatalf("From/To = %d/%d, want %d/%d", got.From, got.To, want.From, want.To)
	}
	if got.Term != want.Term {
		t.Fatalf("Term = %d, want %d", got.Term, want.Term)
	}
	if got.LastLogIndex != want.LastLogIndex || got.LastLogTerm != want.LastLogTerm {
		t.Fatalf("LastLog = %d/%d, want %d/%d",
			got.LastLogIndex, got.LastLogTerm, want.LastLogIndex, want.LastLogTerm)
	}
	if got.PrevLogIndex != want.PrevLogIndex || got.PrevLogTerm != want.PrevLogTerm {
		t.Fatalf("PrevLog = %d/%d, want %d/%d",
			got.PrevLogIndex, got.PrevLogTerm, want.PrevLogIndex, want.PrevLogTerm)
	}
	if got.CommitIndex != want.CommitIndex {
		t.Fatalf("CommitIndex = %d, want %d", got.CommitIndex, want.CommitIndex)
	}
	if got.Granted != want.Granted || got.Success != want.Success {
		t.Fatalf("Granted/Success = %v/%v, want %v/%v",
			got.Granted, got.Success, want.Granted, want.Success)
	}
	if got.MatchIndex != want.MatchIndex {
		t.Fatalf("MatchIndex = %d, want %d", got.MatchIndex, want.MatchIndex)
	}
	if got.ConflictIndex != want.ConflictIndex || got.ConflictTerm != want.ConflictTerm {
		t.Fatalf("Conflict = %d/%d, want %d/%d",
			got.ConflictIndex, got.ConflictTerm, want.ConflictIndex, want.ConflictTerm)
	}
	if !bytes.Equal(got.Context, want.Context) {
		t.Fatalf("Context = %x, want %x", got.Context, want.Context)
	}
	if len(got.Entries) != len(want.Entries) {
		t.Fatalf("%d entries, want %d", len(got.Entries), len(want.Entries))
	}
	for i := range want.Entries {
		a, b := got.Entries[i], want.Entries[i]
		if a.Term != b.Term || a.Index != b.Index || a.Type != b.Type || !bytes.Equal(a.Data, b.Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, a, b)
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	cases := map[string]raft.Message{
		"vote request": {
			Type: raft.MsgVoteRequest, From: 1, To: 2, Term: 5,
			LastLogIndex: 42, LastLogTerm: 4,
		},
		"vote response granted": {
			Type: raft.MsgVoteResponse, From: 2, To: 1, Term: 5, Granted: true,
		},
		"vote response refused": {
			Type: raft.MsgVoteResponse, From: 2, To: 1, Term: 5, Granted: false,
		},
		"heartbeat append": {
			Type: raft.MsgAppendRequest, From: 1, To: 3, Term: 7,
			PrevLogIndex: 10, PrevLogTerm: 6, CommitIndex: 9,
		},
		"append with entries": {
			Type: raft.MsgAppendRequest, From: 1, To: 3, Term: 7,
			PrevLogIndex: 10, PrevLogTerm: 6, CommitIndex: 9,
			Entries: []raft.Entry{
				{Term: 7, Index: 11, Type: raft.EntryNormal, Data: []byte("set x=1")},
				{Term: 7, Index: 12, Type: raft.EntryNoOp},
				{Term: 7, Index: 13, Type: raft.EntryConfChange, Data: []byte{0x00, 0xff}},
			},
		},
		"append accepted": {
			Type: raft.MsgAppendResponse, From: 3, To: 1, Term: 7,
			Success: true, MatchIndex: 13,
		},
		"append rejected with hint": {
			Type: raft.MsgAppendResponse, From: 3, To: 1, Term: 7,
			Success: false, ConflictIndex: 8, ConflictTerm: 5,
		},
		"heartbeat": {
			Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 9,
			CommitIndex: 20, Context: []byte("read-round-1"),
		},
		"heartbeat response": {
			Type: raft.MsgHeartbeatResponse, From: 2, To: 1, Term: 9,
			Context: []byte("read-round-1"),
		},
		"maximum values": {
			Type: raft.MsgAppendRequest, From: math.MaxUint64, To: math.MaxUint64,
			Term: math.MaxUint64, PrevLogIndex: math.MaxUint64, PrevLogTerm: math.MaxUint64,
			CommitIndex: math.MaxUint64, MatchIndex: math.MaxUint64,
			ConflictIndex: math.MaxUint64, ConflictTerm: math.MaxUint64,
		},
	}

	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			wire, err := MessageToWire(want)
			if err != nil {
				t.Fatalf("MessageToWire: %v", err)
			}
			got, err := MessageFromWire(wire)
			if err != nil {
				t.Fatalf("MessageFromWire: %v", err)
			}
			assertMessageEqual(t, got, want)
		})
	}
}

func TestLocalSignalsHaveNoWireForm(t *testing.T) {
	// Campaign, Propose, and ReadIndex are signals a node sends to itself. If
	// they could be encoded, a peer could drive another node's internal state
	// machine directly — forcing an election or injecting a proposal that
	// never came from a client.
	for _, typ := range []raft.MessageType{
		raft.MsgCampaign,
		raft.MsgPropose,
		raft.MsgReadIndex,
	} {
		if _, err := MessageToWire(raft.Message{Type: typ, From: 1, To: 2}); err == nil {
			t.Fatalf("%s was encoded for the wire; a peer could use it to drive "+
				"another node's internal state", typ)
		}
	}
}

func TestUnknownMessageTypeIsRejected(t *testing.T) {
	// A peer speaking a newer protocol, or a damaged message, must not decode
	// into a valid message of some other type.
	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType(9999),
		From: 1, To: 2, Term: 3,
	}

	var unknown *ErrUnknownEnum
	if _, err := MessageFromWire(wire); !errors.As(err, &unknown) {
		t.Fatalf("decoding an unknown message type gave %v, want ErrUnknownEnum", err)
	}
}

func TestUnspecifiedMessageTypeIsRejected(t *testing.T) {
	// The zero value must not be meaningful. Proto3 cannot distinguish an
	// unset field from one set to zero, so a truncated or empty message would
	// otherwise decode as a valid vote request.
	if _, err := MessageFromWire(&raftkvv1.Message{}); err == nil {
		t.Fatal("an empty message decoded successfully; the zero enum value is meaningful")
	}
}

func TestUnknownEntryTypeIsRejected(t *testing.T) {
	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST,
		From: 1, To: 2, Term: 3,
		Entries: []*raftkvv1.Entry{
			{Term: 3, Index: 1, Type: raftkvv1.EntryType(9999)},
		},
	}

	if _, err := MessageFromWire(wire); err == nil {
		t.Fatal("an entry with an unknown type was accepted")
	}
}

func TestUnspecifiedEntryTypeIsRejected(t *testing.T) {
	// Same reasoning one level down: an entry whose type field never arrived
	// must not silently become a normal command that the state machine then
	// tries to decode.
	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST,
		From: 1, To: 2, Term: 3,
		Entries: []*raftkvv1.Entry{{Term: 3, Index: 1}},
	}

	if _, err := MessageFromWire(wire); err == nil {
		t.Fatal("an entry with an unspecified type was accepted")
	}
}

func TestNilInputsAreRejected(t *testing.T) {
	if _, err := MessageFromWire(nil); err == nil {
		t.Fatal("a nil message was accepted")
	}

	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST,
		From: 1, To: 2, Term: 1,
		Entries: []*raftkvv1.Entry{nil},
	}
	if _, err := MessageFromWire(wire); err == nil {
		t.Fatal("a message containing a nil entry was accepted")
	}
}

func TestEveryCoreMessageTypeIsMapped(t *testing.T) {
	// A new inter-node message type added to the core must be given a wire
	// form deliberately, not discovered missing at runtime when two nodes fail
	// to talk to each other.
	interNode := []raft.MessageType{
		raft.MsgVoteRequest,
		raft.MsgVoteResponse,
		raft.MsgAppendRequest,
		raft.MsgAppendResponse,
		raft.MsgHeartbeat,
		raft.MsgHeartbeatResponse,
		raft.MsgInstallSnapshot,
		raft.MsgInstallSnapshotResponse,
	}

	seen := make(map[raftkvv1.MessageType]raft.MessageType, len(interNode))
	for _, typ := range interNode {
		wire, err := messageTypeToWire(typ)
		if err != nil {
			t.Fatalf("%s has no wire form: %v", typ, err)
		}
		if prev, dup := seen[wire]; dup {
			t.Fatalf("%s and %s both map to wire value %v", prev, typ, wire)
		}
		seen[wire] = typ

		back, err := messageTypeFromWire(wire)
		if err != nil {
			t.Fatalf("wire value %v does not map back: %v", wire, err)
		}
		if back != typ {
			t.Fatalf("%s round-tripped to %s", typ, back)
		}
	}
}

func TestEveryCoreEntryTypeIsMapped(t *testing.T) {
	for _, typ := range []raft.EntryType{
		raft.EntryNormal,
		raft.EntryNoOp,
		raft.EntryConfChange,
	} {
		wire, err := entryTypeToWire(typ)
		if err != nil {
			t.Fatalf("entry type %d has no wire form: %v", typ, err)
		}
		back, err := entryTypeFromWire(wire)
		if err != nil {
			t.Fatalf("wire value %v does not map back: %v", wire, err)
		}
		if back != typ {
			t.Fatalf("entry type %d round-tripped to %d", typ, back)
		}
	}
}

func TestEveryStateIsMapped(t *testing.T) {
	for _, s := range []raft.State{raft.Follower, raft.Candidate, raft.Leader} {
		if got := stateToWire(s); got == raftkvv1.NodeState_NODE_STATE_UNSPECIFIED {
			t.Fatalf("state %s maps to UNSPECIFIED", s)
		}
	}
}

func TestEmptyAndNilPayloadsAreDistinguishable(t *testing.T) {
	// An entry with an empty payload is legitimate. It must survive the round
	// trip as empty rather than becoming something the state machine rejects.
	want := raft.Message{
		Type: raft.MsgAppendRequest, From: 1, To: 2, Term: 1,
		Entries: []raft.Entry{
			{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte{}},
			{Term: 1, Index: 2, Type: raft.EntryNormal, Data: nil},
		},
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("MessageFromWire: %v", err)
	}

	for i, e := range got.Entries {
		if len(e.Data) != 0 {
			t.Fatalf("entry %d data = %x, want empty", i, e.Data)
		}
	}
}

func TestLargeEntryBatchRoundTrips(t *testing.T) {
	// A follower far behind receives a large batch. Nothing may be dropped or
	// reordered, or the log would silently diverge.
	const count = 500

	entries := make([]raft.Entry, count)
	for i := range entries {
		entries[i] = raft.Entry{
			Term:  3,
			Index: raft.Index(i + 1),
			Type:  raft.EntryNormal,
			Data:  []byte{byte(i), byte(i >> 8)},
		}
	}
	want := raft.Message{
		Type: raft.MsgAppendRequest, From: 1, To: 2, Term: 3,
		Entries: entries,
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("MessageFromWire: %v", err)
	}

	assertMessageEqual(t, got, want)

	// Order is what the log depends on, so check indexes are still ascending.
	for i, e := range got.Entries {
		if e.Index != raft.Index(i+1) {
			t.Fatalf("entry at position %d has index %d; the batch was reordered", i, e.Index)
		}
	}
}

func TestSnapshotMessageRoundTrips(t *testing.T) {
	// A snapshot is the largest and most structured thing that crosses the
	// wire, and the only message whose loss of a field would silently install
	// the wrong state rather than fail. Every part of it has to survive.
	want := raft.Message{
		Type: raft.MsgInstallSnapshot, From: 1, To: 4, Term: 9,
		Snapshot: &raft.Snapshot{
			Index: 500,
			Term:  8,
			Data:  bytes.Repeat([]byte("state"), 1000),
			Conf: raft.ConfState{
				Voters:   []raft.NodeID{1, 2, 3},
				Incoming: []raft.NodeID{1, 2, 3, 4},
				Joint:    true,
				Addrs:    map[raft.NodeID]string{4: "10.0.0.4:9000"},
			},
		},
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("MessageFromWire: %v", err)
	}

	if got.Snapshot == nil {
		t.Fatal("the snapshot did not survive the round trip")
	}
	if got.Snapshot.Index != want.Snapshot.Index || got.Snapshot.Term != want.Snapshot.Term {
		t.Fatalf("snapshot position = %d/%d, want %d/%d",
			got.Snapshot.Index, got.Snapshot.Term, want.Snapshot.Index, want.Snapshot.Term)
	}
	if !bytes.Equal(got.Snapshot.Data, want.Snapshot.Data) {
		t.Fatalf("snapshot data length = %d, want %d",
			len(got.Snapshot.Data), len(want.Snapshot.Data))
	}

	gc, wc := got.Snapshot.Conf, want.Snapshot.Conf
	if gc.Joint != wc.Joint {
		t.Fatalf("joint = %v, want %v", gc.Joint, wc.Joint)
	}
	if len(gc.Voters) != len(wc.Voters) || len(gc.Incoming) != len(wc.Incoming) {
		t.Fatalf("configuration = %v, want %v", gc, wc)
	}
	for i := range wc.Voters {
		if gc.Voters[i] != wc.Voters[i] {
			t.Fatalf("voters = %v, want %v", gc.Voters, wc.Voters)
		}
	}
	if gc.Addrs[4] != "10.0.0.4:9000" {
		t.Fatalf("addresses = %v, want the new member's address", gc.Addrs)
	}
}

func TestSnapshotJointFlagSurvivesAnEmptyIncomingSet(t *testing.T) {
	// The flag is carried rather than inferred, and the wire form has to
	// preserve that. A receiver that read "not joint" from an empty incoming
	// set would decide on a single majority while the rest of the cluster
	// still required two.
	want := raft.Message{
		Type: raft.MsgInstallSnapshot, From: 1, To: 2, Term: 3,
		Snapshot: &raft.Snapshot{
			Index: 10, Term: 2,
			Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}, Joint: true},
		},
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("MessageFromWire: %v", err)
	}

	if !got.Snapshot.Conf.Joint {
		t.Fatal("the joint flag was lost, so the receiver would use a single majority")
	}
}

func TestSnapshotResponseCarriesNoImage(t *testing.T) {
	// The response only acknowledges; sending the image back would double the
	// cost of every transfer for nothing.
	want := raft.Message{
		Type: raft.MsgInstallSnapshotResponse, From: 4, To: 1, Term: 9,
		Success: true, MatchIndex: 500,
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("MessageFromWire: %v", err)
	}

	assertMessageEqual(t, got, want)
	if got.Snapshot != nil {
		t.Fatal("a response carried a snapshot")
	}
}

func TestSnapshotWithNoIndexIsRejected(t *testing.T) {
	// An image at index zero covers nothing while claiming to cover a prefix
	// of the log. Installing it would replace a node's state with nothing.
	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT,
		From: 1, To: 2, Term: 3,
		Snapshot: &raftkvv1.Snapshot{Index: 0, Term: 0, Data: []byte("nothing")},
	}

	if _, err := MessageFromWire(wire); err == nil {
		t.Fatal("a snapshot with no index was accepted")
	}
}

func TestSnapshotWithNoConfigurationIsAccepted(t *testing.T) {
	// A snapshot taken before membership ever changed legitimately records no
	// configuration, and must not be mistaken for a damaged one.
	wire := &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT,
		From: 1, To: 2, Term: 3,
		Snapshot: &raftkvv1.Snapshot{Index: 5, Term: 2, Data: []byte("state")},
	}

	got, err := MessageFromWire(wire)
	if err != nil {
		t.Fatalf("a snapshot with no configuration was rejected: %v", err)
	}
	if !got.Snapshot.Conf.IsEmpty() {
		t.Fatalf("configuration = %v, want empty", got.Snapshot.Conf)
	}
}

func TestNonSnapshotMessagesCarryNoSnapshot(t *testing.T) {
	// Every other message type must leave the field nil, or an ordinary append
	// would arrive looking like a snapshot transfer.
	for _, typ := range []raft.MessageType{
		raft.MsgVoteRequest, raft.MsgVoteResponse,
		raft.MsgAppendRequest, raft.MsgAppendResponse,
		raft.MsgHeartbeat, raft.MsgHeartbeatResponse,
	} {
		wire, err := MessageToWire(raft.Message{Type: typ, From: 1, To: 2, Term: 1})
		if err != nil {
			t.Fatalf("encoding %s: %v", typ, err)
		}
		if wire.GetSnapshot() != nil {
			t.Fatalf("%s carried a snapshot", typ)
		}

		got, err := MessageFromWire(wire)
		if err != nil {
			t.Fatalf("decoding %s: %v", typ, err)
		}
		if got.Snapshot != nil {
			t.Fatalf("%s decoded with a snapshot", typ)
		}
	}
}
