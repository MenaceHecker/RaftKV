package node

// T3.11 test 3e: membership and quorum after a restart from a stale tail that
// holds an uncommitted ConfChange. Everything goes through Start, the real open
// path (it builds InitialConfState from the snapshot's Conf), on a real DiskStorage.

import (
	"slices"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

// discardTransport drops everything the node sends; the test feeds it messages by hand.
type discardTransport struct{}

func (discardTransport) Send([]raft.Message) {}

// acksNeeded lists, per voter set of the configuration, how many acknowledgements
// an entry needs to commit: one majority when the configuration is plain, both
// majorities when it is joint (raft config.commitReady).
func acksNeeded(cs raft.ConfState) []int {
	need := []int{len(cs.Voters)/2 + 1}
	if cs.Joint {
		need = append(need, len(cs.Incoming)/2+1)
	}
	return need
}

func sameIDs(a, b []raft.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// confShape is the membership a node must present and what that means for quorum.
type confShape struct {
	joint    bool
	voters   []raft.NodeID
	incoming []raft.NodeID // joint only
	acks     []int         // acks needed per voter set
}

var (
	voters5 = []raft.NodeID{1, 2, 3, 4, 5}
	voters6 = []raft.NodeID{1, 2, 3, 4, 5, 6}

	shapeAE      = confShape{voters: voters5, acks: []int{3}}
	shapeJointEF = confShape{joint: true, voters: voters5, incoming: voters6, acks: []int{3, 4}}
	shapeAF      = confShape{voters: voters6, acks: []int{4}}
)

// staleConfTail runs the three-point check for node D (4) of voters A-E. D holds
// a term-1 log through 10 whose entries from index 6 are given by confs (nil means
// a normal entry); the committed snapshot (5,2) carries Conf A-E and mismatches
// D's entry 5. before is the shape the uncommitted tail puts D in; after install
// and after restart D must be A-E: the stale tail is not part of the cluster's log.
func staleConfTail(t *testing.T, confs map[raft.Index]raft.ConfChange, before confShape) {
	const d = raft.NodeID(4)
	dir := t.TempDir()

	// D's disk before the install.
	store, _, err := storage.OpenDiskStorage(storage.DiskConfig{Dir: dir, Sync: storage.SyncAlways})
	if err != nil {
		t.Fatalf("opening D's storage: %v", err)
	}
	var tail []raft.Entry
	for i := raft.Index(1); i <= 10; i++ {
		e := raft.Entry{Term: 1, Index: i, Type: raft.EntryNormal,
			Data: statemachine.Command{Op: statemachine.OpPut, Key: "k", Value: []byte("v")}.Encode()}
		if cc, ok := confs[i]; ok {
			e.Type = raft.EntryConfChange
			e.Data = cc.Encode()
		}
		tail = append(tail, e)
	}
	if err := store.Append(tail); err != nil {
		t.Fatalf("appending D's log: %v", err)
	}
	if err := store.SetHardState(raft.HardState{Term: 1}); err != nil {
		t.Fatalf("setting D's hard state: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing D's storage: %v", err)
	}

	// The snapshot state machine image: an empty KV is enough, D never reads it.
	kvImage, err := statemachine.New().Snapshot()
	if err != nil {
		t.Fatalf("snapshotting an empty state machine: %v", err)
	}

	start := func() *Node {
		t.Helper()
		n, err := Start(Config{
			ID:        d,
			Peers:     voters5,
			DataDir:   dir,
			Transport: discardTransport{},
			// No tick may fire: D must not campaign while the test inspects it.
			TickInterval: time.Hour,
			Sync:         storage.SyncAlways,
		})
		if err != nil {
			t.Fatalf("starting D: %v", err)
		}
		return n
	}

	expect := func(point string, cs raft.ConfState, want confShape) {
		t.Helper()
		acks := acksNeeded(cs)
		t.Logf("%s: %v, acks needed %v", point, cs, acks)
		if cs.Joint != want.joint || !sameIDs(cs.Voters, want.voters) || !sameIDs(cs.Incoming, want.incoming) {
			t.Errorf("%s: membership %v, want joint=%v voters=%v incoming=%v",
				point, cs, want.joint, want.voters, want.incoming)
		}
		if !slices.Equal(acks, want.acks) {
			t.Errorf("%s: acks needed %v, want %v", point, acks, want.acks)
		}
	}

	n := start()
	stopped := false
	defer func() {
		if !stopped {
			_ = n.Stop() // best-effort cleanup
		}
	}()

	// (1) Before the install the uncommitted ConfChanges are in effect.
	expect("(1) before the install", n.Status().Members, before)

	// Install the committed snapshot (5,2). D's entry 5 has term 1: a mismatch.
	n.Step(raft.Message{
		Type: raft.MsgInstallSnapshot, From: 1, Term: 2,
		Snapshot: &raft.Snapshot{Index: 5, Term: 2, Data: kvImage, Conf: raft.ConfState{Voters: voters5}},
	})
	deadline := time.Now().Add(settleTimeout)
	for n.Status().SnapshotsReceived == 0 {
		if time.Now().After(deadline) {
			t.Fatal("D did not take the snapshot")
		}
		time.Sleep(time.Millisecond)
	}

	// (2) After the install, before any restart.
	st := n.Status()
	if st.Commit != 5 {
		t.Fatalf("commit index after the install is %d, want 5", st.Commit)
	}
	expect("(2) after the install", st.Members, shapeAE)

	// (3) After a restart, before any leader repairs the suffix. Expected RED on
	// today's code: Open resurrects entries 6.., and they are ConfChanges.
	if err := n.Stop(); err != nil {
		t.Fatalf("stopping D: %v", err)
	}
	stopped = true
	n = start()
	stopped = false
	expect("(3) after the restart", n.Status().Members, shapeAE)
}

// TestStaleConfChangeTailMembershipAndQuorum (T3.11 test 3e, required): the
// uncommitted tail holds add-F at index 6, so D is joint A-E/A-F (3 and 4 acks).
func TestStaleConfChangeTailMembershipAndQuorum(t *testing.T) {
	staleConfTail(t, map[raft.Index]raft.ConfChange{
		6: {Type: raft.ConfChangeAddNode, NodeID: 6},
	}, shapeJointEF)
}

// TestStaleLeaveJointTailMembershipAndQuorum (T3.11 test 3f): the uncommitted tail
// holds add-F at 6 and the entry that leaves the joint configuration at 7, so D
// is plain A-F (4 acks). A leader can write that tail: the automatic leave
// (confchange.go maybeFinishConfChange) waits for the joint entry to commit, but
// ProposeConfChange only requires n.conf.inJoint() for a leave (confchange.go:10-13),
// and internal/node passes a client's change straight to it (node.go:645).
func TestStaleLeaveJointTailMembershipAndQuorum(t *testing.T) {
	staleConfTail(t, map[raft.Index]raft.ConfChange{
		6: {Type: raft.ConfChangeAddNode, NodeID: 6},
		7: {Type: raft.ConfChangeLeaveJoint},
	}, shapeAF)
}
