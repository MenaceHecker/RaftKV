package raft

import (
	"bytes"
	"testing"
)

// Fuzz target for the configuration change decoder.
//
// A conf change travels in a log entry, so it is read back off disk after a
// crash and replayed on every replica. It decides who may vote, which makes it
// the one payload where accepting something the encoder could not have written
// changes who is allowed to elect a leader.

func FuzzDecodeConfChange(f *testing.F) {
	f.Add(ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "127.0.0.1:9004"}.Encode())
	f.Add(ConfChange{Type: ConfChangeRemoveNode, NodeID: 2}.Encode())
	f.Add(ConfChange{Type: ConfChangeLeaveJoint}.Encode())
	f.Add([]byte{})
	f.Add(make([]byte, 13))

	f.Fuzz(func(t *testing.T, data []byte) {
		cc, err := DecodeConfChange(data)
		if err != nil {
			return
		}
		again := cc.Encode()
		if !bytes.Equal(again, data) {
			t.Fatalf("conf change round-tripped to different bytes:\n in: %x\nout: %x",
				data, again)
		}
	})
}

func TestDecodeConfChangeRejectsMalformedPayloads(t *testing.T) {
	good := ConfChange{Type: ConfChangeAddNode, NodeID: 4, Addr: "host:1"}.Encode()
	if _, err := DecodeConfChange(good); err != nil {
		t.Fatalf("a well-formed change was rejected: %v", err)
	}

	// A payload longer than its declared address does not describe itself,
	// and accepting it would let two byte strings name one membership change.
	if _, err := DecodeConfChange(append(good, 0)); err == nil {
		t.Error("a conf change with a trailing byte was accepted")
	}

	// A type byte naming no known operation must not reach the configuration
	// machinery to be interpreted by whichever branch happens to catch it.
	unknown := ConfChange{Type: ConfChangeType(200), NodeID: 1}.Encode()
	if _, err := DecodeConfChange(unknown); err == nil {
		t.Error("a conf change with an undefined type was accepted")
	}
}
