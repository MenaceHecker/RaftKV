package raft

import (
	"bytes"
	"testing"
)

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

	if _, err := DecodeConfChange(append(good, 0)); err == nil {
		t.Error("a conf change with a trailing byte was accepted")
	}

	unknown := ConfChange{Type: ConfChangeType(200), NodeID: 1}.Encode()
	if _, err := DecodeConfChange(unknown); err == nil {
		t.Error("a conf change with an undefined type was accepted")
	}
}
