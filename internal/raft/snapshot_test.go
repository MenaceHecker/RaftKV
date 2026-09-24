package raft

import (
	"errors"
	"fmt"
	"testing"
)

func compactLeader(t *testing.T, c *cluster, id NodeID, data string) Index {
	t.Helper()

	n := c.node(id)
	at := n.CommitIndex()
	term, err := n.log.term(at)
	if err != nil {
		t.Fatalf("reading the term at the compaction point: %v", err)
	}

	if err := c.storages[id].ApplySnapshot(Snapshot{
		Index: at,
		Term:  term,
		Conf:  n.ConfState(),
		Data:  []byte(data),
	}); err != nil {
		t.Fatalf("compacting node %d: %v", id, err)
	}
	return at
}

func isolateOne(t *testing.T, c *cluster, keepLeading NodeID) NodeID {
	t.Helper()

	var lagging NodeID
	var rest []NodeID
	for _, id := range c.ids {
		if id != keepLeading && lagging == None {
			lagging = id
			continue
		}
		rest = append(rest, id)
	}
	c.partition(rest, []NodeID{lagging})
	return lagging
}

func TestLaggingFollowerIsCaughtUpByASnapshot(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 900})
	leader := c.awaitLeader(defaultElectionTick * 2)

	lagging := isolateOne(t, c, leader)
	for i := range 10 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	behind := c.node(lagging).LastIndex()
	snapIndex := compactLeader(t, c, leader, "image")
	if behind >= snapIndex {
		t.Fatalf("the follower is at %d and the snapshot at %d; it has not fallen behind",
			behind, snapIndex)
	}
	if first := c.storages[leader].FirstIndex(); first <= behind+1 {
		t.Fatalf("the leader still holds the entries the follower needs (first index %d); "+
			"the test is not exercising snapshot transfer", first)
	}

	c.heal()
	c.tickN(defaultElectionTick)

	if len(c.snapshots[lagging]) == 0 {
		t.Fatalf("the follower was never sent a snapshot\n%s", c.dump())
	}
	if got := c.node(lagging).CommitIndex(); got < snapIndex {
		t.Fatalf("the follower committed through %d, want at least the snapshot index %d\n%s",
			got, snapIndex, c.dump())
	}
}

func TestSnapshotIsNotSentWhenTheLogSuffices(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 901})
	leader := c.awaitLeader(defaultElectionTick * 2)

	lagging := isolateOne(t, c, leader)
	for i := range 5 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	c.heal()
	c.tickN(defaultElectionTick)

	if got := len(c.snapshots[lagging]); got != 0 {
		t.Fatalf("the follower was sent %d snapshots although the log could have "+
			"caught it up\n%s", got, c.dump())
	}
	if got := c.node(lagging).CommitIndex(); got != c.node(leader).CommitIndex() {
		t.Fatalf("the follower committed through %d, want %d\n%s",
			got, c.node(leader).CommitIndex(), c.dump())
	}
}

func TestStaleSnapshotDoesNotRegressAFollower(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 902})
	leader := c.awaitLeader(defaultElectionTick * 2)

	for i := range 5 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	f := c.node(follower)

	committed := f.CommitIndex()
	last := f.LastIndex()
	if committed == 0 {
		t.Fatal("the follower has committed nothing, so there is nothing to regress")
	}

	err := f.Step(Message{
		Type: MsgInstallSnapshot,
		From: leader,
		To:   follower,
		Term: f.Term(),
		Snapshot: &Snapshot{
			Index: committed - 1,
			Term:  1,
			Conf:  f.ConfState(),
			Data:  []byte("stale"),
		},
	})
	if err != nil {
		t.Fatalf("stepping a stale snapshot: %v", err)
	}

	if got := f.CommitIndex(); got != committed {
		t.Fatalf("commit index moved from %d to %d on a stale snapshot", committed, got)
	}
	if got := f.LastIndex(); got != last {
		t.Fatalf("last index moved from %d to %d on a stale snapshot", last, got)
	}
	if got := len(c.snapshots[follower]); got != 0 {
		t.Fatalf("a stale snapshot was handed to the state machine")
	}
}

func TestInstallingASnapshotMovesEveryCursorTogether(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 903})
	f := c.node(2)

	snap := Snapshot{
		Index: 40,
		Term:  5,
		Conf:  ConfState{Voters: []NodeID{1, 2, 3, 4}, Addrs: map[NodeID]string{4: "h:4"}},
		Data:  []byte("image"),
	}

	err := f.Step(Message{
		Type: MsgInstallSnapshot, From: 1, To: 2, Term: 5, Snapshot: &snap,
	})
	if err != nil {
		t.Fatalf("stepping the snapshot: %v", err)
	}

	if got := f.CommitIndex(); got != snap.Index {
		t.Fatalf("commit index = %d, want %d", got, snap.Index)
	}
	if got := f.LastIndex(); got != snap.Index {
		t.Fatalf("last index = %d, want %d", got, snap.Index)
	}
	if got, err := f.log.term(snap.Index); err != nil || got != snap.Term {
		t.Fatalf("term at the snapshot index = %d (err %v), want %d", got, err, snap.Term)
	}
	if got := len(f.Members()); got != 4 {
		t.Fatalf("members = %v, want the four from the snapshot", f.Members())
	}
	if f.conf.addrs[4] != "h:4" {
		t.Fatal("the snapshot's addresses were not adopted")
	}
}

func TestSnapshotIsSurfacedForTheStateMachine(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 904})
	f := c.node(2)

	err := f.Step(Message{
		Type: MsgInstallSnapshot, From: 1, To: 2, Term: 3,
		Snapshot: &Snapshot{Index: 12, Term: 3, Conf: f.ConfState(), Data: []byte("payload")},
	})
	if err != nil {
		t.Fatalf("stepping the snapshot: %v", err)
	}

	rd := f.Ready()
	if rd.Snapshot == nil {
		t.Fatal("Ready did not surface the snapshot; the state machine would never see it")
	}
	if string(rd.Snapshot.Data) != "payload" {
		t.Fatalf("snapshot data = %q, want payload", rd.Snapshot.Data)
	}
	if rd.IsEmpty() {
		t.Fatal("a Ready carrying a snapshot reports itself as empty")
	}

	f.Advance(rd)
	if got := f.log.applied; got != 12 {
		t.Fatalf("applied = %d after acknowledging a snapshot at 12", got)
	}
}

func TestSnapshotCarriesTheConfiguration(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 905})
	f := c.node(2)

	before := len(f.Members())

	err := f.Step(Message{
		Type: MsgInstallSnapshot, From: 1, To: 2, Term: 4,
		Snapshot: &Snapshot{
			Index: 20, Term: 4, Data: []byte("image"),
			Conf: ConfState{
				Voters:   []NodeID{1, 2, 3},
				Incoming: []NodeID{1, 2, 3, 4},
				Joint:    true,
			},
		},
	})
	if err != nil {
		t.Fatalf("stepping the snapshot: %v", err)
	}

	if !f.InJointConfiguration() {
		t.Fatal("a snapshot taken mid-transition did not put the receiver in that transition")
	}
	if got := len(f.Members()); got == before {
		t.Fatalf("membership did not change: still %d members", got)
	}
}

func TestLeaderFallsBackWhenNoSnapshotExists(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 906})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	if _, err := c.storages[leader].Snapshot(); !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("a storage with no snapshot gave %v, want ErrSnapshotUnavailable", err)
	}

	n.Ready()
	n.sendSnapshot(c.ids[1])

	rd := n.Ready()
	for _, m := range rd.Messages {
		if m.Type == MsgInstallSnapshot {
			t.Fatal("a snapshot was sent although none exists")
		}
	}
}

func TestEmptySnapshotIsRejected(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 907})
	f := c.node(2)

	err := f.Step(Message{
		Type: MsgInstallSnapshot, From: 1, To: 2, Term: 2,
		Snapshot: &Snapshot{Index: 0, Term: 0},
	})
	if err == nil {
		t.Fatal("an empty snapshot was accepted")
	}
}

func TestLeaderRejectsASnapshotInItsOwnTerm(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 908})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	err := n.Step(Message{
		Type: MsgInstallSnapshot, From: 2, To: leader, Term: n.Term(),
		Snapshot: &Snapshot{Index: 5, Term: 1, Data: []byte("x")},
	})
	if err == nil {
		t.Fatal("a leader accepted a snapshot from a peer in its own term")
	}
}

func TestSnapshotResponseAdvancesReplicationState(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 909})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	installed := n.CommitIndex()
	n.progress[follower].match = 0
	n.progress[follower].next = 1

	err := n.Step(Message{
		Type: MsgInstallSnapshotResponse, From: follower, To: leader,
		Term: n.Term(), Success: true, MatchIndex: installed,
	})
	if err != nil {
		t.Fatalf("stepping the response: %v", err)
	}

	if got := n.progress[follower].match; got != installed {
		t.Fatalf("match = %d after a successful install, want %d", got, installed)
	}
	if got := n.progress[follower].next; got != installed+1 {
		t.Fatalf("next = %d, want %d", got, installed+1)
	}
}

func TestFailedSnapshotResponseBacksOff(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 910})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	n.progress[follower].match = 3
	n.progress[follower].next = 50

	err := n.Step(Message{
		Type: MsgInstallSnapshotResponse, From: follower, To: leader,
		Term: n.Term(), Success: false,
	})
	if err != nil {
		t.Fatalf("stepping the response: %v", err)
	}

	if got := n.progress[follower].next; got != 4 {
		t.Fatalf("next = %d after a refused snapshot, want one past the known match (4)", got)
	}
}

func TestStorageRefusesAnOlderSnapshot(t *testing.T) {
	s := NewMemoryStorage()

	if err := s.ApplySnapshot(Snapshot{Index: 10, Term: 2, Data: []byte("a")}); err != nil {
		t.Fatalf("applying the first snapshot: %v", err)
	}
	if err := s.ApplySnapshot(Snapshot{Index: 5, Term: 1, Data: []byte("b")}); err == nil {
		t.Fatal("an older snapshot was accepted")
	}
	if err := s.ApplySnapshot(Snapshot{Index: 10, Term: 2, Data: []byte("c")}); err == nil {
		t.Fatal("a snapshot at the same index was accepted")
	}

	got, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got.Index != 10 || string(got.Data) != "a" {
		t.Fatalf("stored snapshot = index %d data %q, want index 10 data a", got.Index, got.Data)
	}
}

func TestStorageAnswersForTheCompactionBoundary(t *testing.T) {
	s := NewMemoryStorage()
	if err := s.ApplySnapshot(Snapshot{Index: 10, Term: 3, Data: []byte("x")}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	if got, err := s.Term(10); err != nil || got != 3 {
		t.Fatalf("Term(10) = %d (err %v), want 3", got, err)
	}
	if _, err := s.Term(9); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term(9) gave %v, want ErrCompacted", err)
	}
	if got := s.FirstIndex(); got != 11 {
		t.Fatalf("FirstIndex = %d, want 11", got)
	}
	if got := s.LastIndex(); got != 10 {
		t.Fatalf("LastIndex = %d, want 10", got)
	}

	if err := s.Append([]Entry{{Index: 11, Term: 3}}); err != nil {
		t.Fatalf("appending after the boundary: %v", err)
	}
	if got := s.LastIndex(); got != 11 {
		t.Fatalf("LastIndex = %d after appending, want 11", got)
	}
}
