package transport

import (
	"testing"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Fuzz target for the wire decoder.
//
// This is the only decoder in the system reachable from the network before
// anything else has looked at the bytes. A peer opens a connection and sends
// a message; gRPC unmarshals it and hands it here, and whatever comes out is
// stepped into the consensus core. Protobuf guarantees the framing parses, it
// guarantees nothing about whether the fields make sense together: a message
// can arrive with an unknown type, a snapshot with no index, entries whose
// type byte names nothing, or every field left at its zero value.
//
// The property is the same as for the other decoders. Anything the core would
// not accept must be rejected here with an error, never a panic, because a
// panic in this path is a node killed by a single malformed packet.

func FuzzMessageFromWire(f *testing.F) {
	// Seeds are marshalled from real messages so the fuzzer starts inside the
	// protobuf grammar rather than spending its budget rediscovering it.
	seed := func(m *raftkvv1.Message) {
		b, err := proto.Marshal(m)
		if err != nil {
			f.Fatalf("marshalling a seed: %v", err)
		}
		f.Add(b)
	}

	seed(&raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT,
		From: 1, To: 2, Term: 3,
	})
	seed(&raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST,
		From: 1, To: 2, Term: 3,
		Entries: []*raftkvv1.Entry{
			{Term: 1, Index: 1, Type: raftkvv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("x")},
		},
	})
	seed(&raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_INSTALL_SNAPSHOT,
		From: 1, To: 2, Term: 3,
		Snapshot: &raftkvv1.Snapshot{
			Index: 5, Term: 2,
			Conf: &raftkvv1.ConfState{Voters: []uint64{1, 2, 3}},
			Data: []byte("state"),
		},
	})
	seed(&raftkvv1.Message{})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var m raftkvv1.Message
		if err := proto.Unmarshal(data, &m); err != nil {
			// Not a protobuf message at all; gRPC would have rejected it
			// before the codec ever saw it.
			return
		}

		msg, err := MessageFromWire(&m)
		if err != nil {
			return
		}

		// Whatever survives must be something the encoder can express, since
		// responses travel back out through it. A value that decodes but
		// cannot be re-encoded is one the two halves disagree about.
		if _, err := MessageToWire(msg); err != nil {
			t.Fatalf("a message decoded from the wire could not be encoded back: %v\nmessage: %+v",
				err, msg)
		}

		// Entry types must be ones the core defines, or the entry would be
		// appended to a log and replayed forever as something nobody can
		// interpret.
		for i, e := range msg.Entries {
			if !e.Type.Valid() {
				t.Fatalf("entry %d decoded with undefined type %d", i, e.Type)
			}
		}
	})
}
