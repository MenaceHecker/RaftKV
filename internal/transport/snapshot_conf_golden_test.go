package transport

import (
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The InstallSnapshot wire encoding of a snapshot must not change with T3.17.
func TestSnapshotWireEncodingIsStable(t *testing.T) {
	snap := &raft.Snapshot{
		Index: 7,
		Term:  3,
		Conf: raft.ConfState{
			Voters:   []raft.NodeID{1, 2, 3},
			Incoming: []raft.NodeID{1, 2, 3, 4},
			Joint:    true,
			Addrs:    map[raft.NodeID]string{4: "host:4"},
		},
		Data: []byte("state"),
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshotToWire(snap))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = "080710031a190a030102031204010203041801220a08041206686f73743a3422057374617465"
	if got := hex.EncodeToString(b); got != want {
		t.Fatalf("wire encoding changed:\n got %s\nwant %s", got, want)
	}
}
