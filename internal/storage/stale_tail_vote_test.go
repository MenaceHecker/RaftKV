package storage_test

// T3.11 tests 3a and 7 on a real reopen. The raft package cannot import
// storage, and once ApplySnapshot writes a durable discard the state those tests
// seeded into a MemoryStorage (snapshot (5,2) with the stale tail 6..10 at term
// 1) is no longer reachable, so the vote assertions live here, on the log a real
// DiskStorage recovers after a mismatching install and a restart.

import (
	"math/rand"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

var staleVoters = []raft.NodeID{1, 2, 3, 4, 5}

func staleSnapshot() raft.Snapshot {
	return raft.Snapshot{Index: 5, Term: 2, Conf: raft.ConfState{Voters: staleVoters}, Data: []byte("state@5")}
}

// reopenAfterMismatchingInstall builds node D's disk: entries 1..10 at term 1,
// hard state hs, the mismatching snapshot (5,2) installed, then close and reopen.
func reopenAfterMismatchingInstall(t *testing.T, hs raft.HardState) (*storage.DiskStorage, string) {
	t.Helper()
	dir := t.TempDir()
	s, _ := openOnDisk(t, dir)
	requireOK(t, "append", s.Append(logEntries(1, 1, 10)))
	requireOK(t, "hard state", s.SetHardState(hs))
	requireOK(t, "ApplySnapshot", s.ApplySnapshot(staleSnapshot()))
	requireOK(t, "close", s.Close())

	s2, _ := openOnDisk(t, dir)
	t.Cleanup(func() { _ = s2.Close() })
	return s2, dir
}

func staleTailNode(t *testing.T, id raft.NodeID, st raft.Storage) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(raft.Config{
		ID:            id,
		Peers:         staleVoters,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       st,
		Rand:          rand.New(rand.NewSource(int64(id) * 7919)), //nolint:gosec // deterministic test
	})
	if err != nil {
		t.Fatalf("creating node %d: %v", id, err)
	}
	return n
}

func drain(n *raft.Node) []raft.Message {
	rd := n.Ready()
	n.Advance(rd)
	return rd.Messages
}

func onlyVoteResponse(t *testing.T, ms []raft.Message) raft.Message {
	t.Helper()
	if len(ms) != 1 || ms[0].Type != raft.MsgVoteResponse {
		t.Fatalf("got %d messages %+v, want exactly one vote response", len(ms), ms)
	}
	return ms[0]
}

// TestStaleTailRestartVoteOnRealReopen (T3.11 test 3a): a restarted voter whose
// disk took the mismatching snapshot (5,2) compares candidates against (5,2),
// not against the (10,1) of the discarded tail. One fresh fixture per case.
func TestStaleTailRestartVoteOnRealReopen(t *testing.T) {
	const candidate = raft.NodeID(5)

	for _, tc := range []struct {
		name    string
		idx     raft.Index
		term    raft.Term
		granted bool
	}{
		{"(12,1) must be rejected", 12, 1, false},
		{"(7,1) must be rejected", 7, 1, false},
		{"(4,1) must be rejected", 4, 1, false},
		{"control (5,2) is granted", 5, 2, true},
		{"control (6,2) is granted", 6, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, _ := reopenAfterMismatchingInstall(t, raft.HardState{Term: 2})
			if v := viewOf(t, st); len(v.tail) != 0 {
				t.Fatalf("precondition: the reopened log is %v, want nothing above the snapshot", v)
			}

			n := staleTailNode(t, 4, st)
			if err := n.Step(raft.Message{Type: raft.MsgVoteRequest, From: candidate, Term: 3,
				LastLogIndex: tc.idx, LastLogTerm: tc.term}); err != nil {
				t.Fatalf("step: %v", err)
			}
			resp := onlyVoteResponse(t, drain(n))
			if resp.Term != 3 || resp.Granted != tc.granted {
				t.Errorf("response term %d granted %v, want term 3 granted %v", resp.Term, resp.Granted, tc.granted)
			}

			wantVote := raft.None
			if tc.granted {
				wantVote = candidate
			}
			if hs, _ := st.InitialState(); hs.Term != 3 || hs.VotedFor != wantVote {
				t.Errorf("persisted hard state %+v, want term 3 VotedFor %d", hs, wantVote)
			}
		})
	}
}

// TestVoteRegressionAfterInstallSnapshotRestartOnRealReopen (T3.11 test 7): D
// holds 1..10 at term 1, takes the leader's snapshot (5,2) through the raft
// core, rejects E's (12,1) at term 4, restarts from its disk and must reject it
// again.
func TestVoteRegressionAfterInstallSnapshotRestartOnRealReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := openOnDisk(t, dir)
	requireOK(t, "append", s.Append(logEntries(1, 1, 10)))
	requireOK(t, "hard state", s.SetHardState(raft.HardState{Term: 3, VotedFor: 1}))

	snap := staleSnapshot()
	if term, err := s.Term(5); err != nil || term == snap.Term {
		t.Fatalf("precondition: D's term at 5 is %d (%v), the install must be a mismatch", term, err)
	}

	d := staleTailNode(t, 4, s)
	if err := d.Step(raft.Message{Type: raft.MsgInstallSnapshot, From: 1, Term: 3, Snapshot: &snap}); err != nil {
		t.Fatalf("installing the snapshot on D: %v", err)
	}
	ms := drain(d)
	if len(ms) != 1 || ms[0].Type != raft.MsgInstallSnapshotResponse || !ms[0].Success {
		t.Fatalf("D did not accept the snapshot: %+v", ms)
	}

	req := raft.Message{Type: raft.MsgVoteRequest, From: 5, Term: 4, LastLogIndex: 12, LastLogTerm: 1}
	if err := d.Step(req); err != nil {
		t.Fatalf("step: %v", err)
	}
	if resp := onlyVoteResponse(t, drain(d)); resp.Term != 4 || resp.Granted {
		t.Fatalf("before the restart: D answered term %d granted=%v to (12,1), want a rejection", resp.Term, resp.Granted)
	}
	requireOK(t, "close", s.Close())

	s2, _ := openOnDisk(t, dir)
	defer func() { _ = s2.Close() }()
	if v := viewOf(t, s2); len(v.tail) != 0 {
		t.Fatalf("D restarted with %v, want nothing above the snapshot", v)
	}
	restarted := staleTailNode(t, 4, s2)
	if err := restarted.Step(req); err != nil {
		t.Fatalf("step: %v", err)
	}
	if resp := onlyVoteResponse(t, drain(restarted)); resp.Term != 4 || resp.Granted {
		t.Errorf("after the restart: D answered term %d granted=%v to (12,1), want a rejection", resp.Term, resp.Granted)
	}
}
