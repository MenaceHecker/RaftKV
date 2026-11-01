package storage

import (
	"encoding/hex"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The snapshot file's Conf encoding must not change with T3.17: only the value
// stored in the field does.
func TestSnapshotConfEncodingIsStable(t *testing.T) {
	cs := raft.ConfState{
		Voters:   []raft.NodeID{1, 2, 3},
		Incoming: []raft.NodeID{1, 2, 3, 4},
		Joint:    true,
		Addrs:    map[raft.NodeID]string{4: "host:4"},
	}
	got := hex.EncodeToString(appendConfState(nil, cs))
	const want = "0300000000000000010000000000000002000000000000000300000000000000040000000000000001000000000000000200000000000000030000000000000004000000000000000100000000000000010000000000000004000000000000000600000000000000686f73743a34"
	if got != want {
		t.Fatalf("encoded Conf changed:\n got %s\nwant %s", got, want)
	}
}
