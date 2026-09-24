package transport

import (
	"testing"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func FuzzMessageFromWire(f *testing.F) {
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
			return
		}

		msg, err := MessageFromWire(&m)
		if err != nil {
			return
		}

		if _, err := MessageToWire(msg); err != nil {
			t.Fatalf("a message decoded from the wire could not be encoded back: %v\nmessage: %+v",
				err, msg)
		}

		for i, e := range msg.Entries {
			if !e.Type.Valid() {
				t.Fatalf("entry %d decoded with undefined type %d", i, e.Type)
			}
		}
	})
}
