package storage_test

// T3.11: the unacknowledged window of ApplySnapshot, and why a matching snapshot
// that leaves its suffix on disk is benign.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

func nodeOn(t *testing.T, peers []raft.NodeID, st raft.Storage) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(raft.Config{
		ID: 1, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, Storage: st,
		Rand: rand.New(rand.NewSource(7)), //nolint:gosec // deterministic test
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}
	return n
}

// voteBattery asks st's node for its vote on each candidate log at successive
// terms starting at 3 and returns what it said. Votes mutate only the hard state.
func voteBattery(t *testing.T, peers []raft.NodeID, st raft.Storage, term raft.Term) []string {
	t.Helper()
	var out []string
	for i, c := range [][2]uint64{{12, 1}, {7, 1}, {4, 1}, {5, 2}, {6, 2}} {
		n := nodeOn(t, peers, st)
		req := raft.Message{Type: raft.MsgVoteRequest, From: 3, Term: term + raft.Term(i),
			LastLogIndex: raft.Index(c[0]), LastLogTerm: raft.Term(c[1])}
		if err := n.Step(req); err != nil {
			t.Fatalf("vote step: %v", err)
		}
		ms := drain(n)
		if len(ms) != 1 || ms[0].Type != raft.MsgVoteResponse {
			t.Fatalf("got %+v, want one vote response", ms)
		}
		out = append(out, fmt.Sprintf("(%d,%d)@%d: term %d granted %v", c[0], c[1], req.Term, ms[0].Term, ms[0].Granted))
	}
	return out
}

func memoryWith(t *testing.T, snap *raft.Snapshot, es []raft.Entry) *raft.MemoryStorage {
	t.Helper()
	st := raft.NewMemoryStorage()
	if snap != nil {
		requireOK(t, "seeding the snapshot", st.ApplySnapshot(*snap))
	}
	if len(es) > 0 {
		requireOK(t, "seeding entries", st.Append(es))
	}
	return st
}

// leaderAppend steps an AppendEntries from leader 2 at term and returns the reply.
func leaderAppend(t *testing.T, n *raft.Node, term raft.Term, prevIdx raft.Index, prevTerm raft.Term,
	es []raft.Entry, commit raft.Index) raft.Message {
	t.Helper()
	if err := n.Step(raft.Message{Type: raft.MsgAppendRequest, From: 2, Term: term,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: es, CommitIndex: commit}); err != nil {
		t.Fatalf("append step: %v", err)
	}
	ms := drain(n)
	if len(ms) != 1 || ms[0].Type != raft.MsgAppendResponse {
		t.Fatalf("got %+v, want one append response", ms)
	}
	return ms[0]
}

// TestUnacknowledgedInstallCrashWindowIsSafe (T3.11): ApplySnapshot of the
// mismatching snapshot (5,2) over 1..10 at term 1, crashed at every filesystem
// operation (drop and torn x seeds 1-5, SyncAlways). The call never returned, so
// the recovered store is (a) the pre-install log, (b/c) snapshot (5,2) with the
// pre-install tail, or (d) snapshot (5,2) with an empty tail, with no hole.
// For (b) and (c), a raft node on it is held to the weaker oracle: the claims it
// makes are none the pre-install node could not have made, except that it may
// acknowledge the committed boundary (5,2).
func TestUnacknowledgedInstallCrashWindowIsSafe(t *testing.T) {
	leader := logEntries(2, 1, 12)
	stale := logEntries(1, 6, 5)
	preCall := logEntries(1, 1, 10)
	voters := testConf.Voters

	counts := map[string]int{}
	crashEverywhere(t, scenario{
		policy: storage.SyncAlways,
		prepare: func(r *rig) {
			requireOK(t, "open", r.open())
			requireOK(t, "append", r.append(preCall))
		},
		work: func(r *rig) error { return r.applySnapshot(leader, 5) },
		then: func(t *testing.T, r *rig, ctx string, _ crashMode) {
			v := viewOf(t, r.s)
			switch {
			case r.snap.Meta.Index == 0:
				if !sameEntries(v.tail, preCall) {
					t.Fatalf("%s: state (a) must be the pre-install log, got %v", ctx, v)
				}
				counts["(a) no snapshot, pre-install log"]++
			case len(v.tail) == 0:
				if v.boundary != 5 || v.term != 2 {
					t.Fatalf("%s: state (d) boundary is (%d,%d)", ctx, v.boundary, v.term)
				}
				counts["(d) snapshot, empty tail"]++
			case sameEntries(v.tail, stale):
				if v.boundary != 5 || v.term != 2 {
					t.Fatalf("%s: states (b)/(c) boundary is (%d,%d)", ctx, v.boundary, v.term)
				}
				counts["(b)/(c) snapshot, pre-install tail"]++
				weakerOracle(t, ctx, r.s, voters, preCall)
			default:
				t.Fatalf("%s: recovered %v is none of the states (a)-(d)", ctx, v)
			}
		},
	}, append([]crashMode{{"drop", 1, faultfs.CrashOptions{}}}, torn(1, 2, 3, 4, 5)...))
	t.Logf("recovered states over all crash points: %v", counts)

	for _, state := range []string{"(a) no snapshot, pre-install log", "(b)/(c) snapshot, pre-install tail", "(d) snapshot, empty tail"} {
		if counts[state] == 0 {
			t.Errorf("no crash point reached state %s: the test would prove nothing about it", state)
		}
	}
}

// weakerOracle: a node on the recovered (b)/(c) store (snapshot (5,2), tail 6..10
// at term 1) against the pre-install node (1..10 at term 1).
func weakerOracle(t *testing.T, ctx string, s *storage.DiskStorage, voters []raft.NodeID, preCall []raft.Entry) {
	t.Helper()

	// 4. Commit never exceeds the snapshot index; the configuration is the same.
	n := nodeOn(t, voters, s)
	if n.CommitIndex() > 5 {
		t.Errorf("%s: commit index %d exceeds the snapshot index 5", ctx, n.CommitIndex())
	}
	if snap, err := s.Snapshot(); err != nil || fmt.Sprint(snap.Conf.Voters) != fmt.Sprint(voters) {
		t.Errorf("%s: recovered configuration %+v (%v), want voters %v", ctx, snap.Conf, err, voters)
	}

	// 1. Votes identical to the pre-install node's.
	got := voteBattery(t, voters, s, 3)
	want := voteBattery(t, voters, memoryWith(t, nil, preCall), 3)
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: vote %s differs from the pre-install node's %s", ctx, got[i], want[i])
		}
	}

	// 3. Nothing beyond the boundary is acknowledged: a heartbeat advertising
	// (8,2) or (10,2) is rejected, as the pre-install node does. (10,1) matches
	// both nodes' stored entry, so it is no weaker than before the install.
	pre := memoryWith(t, nil, preCall)
	for _, adv := range [][2]uint64{{8, 2}, {10, 2}, {10, 1}} {
		g := leaderAppend(t, nodeOn(t, voters, s), 9, raft.Index(adv[0]), raft.Term(adv[1]), nil, 10)
		w := leaderAppend(t, nodeOn(t, voters, pre), 9, raft.Index(adv[0]), raft.Term(adv[1]), nil, 10)
		if g.Success != w.Success || g.MatchIndex != w.MatchIndex {
			t.Errorf("%s: heartbeat prev=(%d,%d): recovered success=%v match=%d, pre-install success=%v match=%d",
				ctx, adv[0], adv[1], g.Success, g.MatchIndex, w.Success, w.MatchIndex)
		}
	}

	// 2. The boundary may be acknowledged: prev=(5,2), no entries.
	hb := nodeOn(t, voters, s)
	if r := leaderAppend(t, hb, 10, 5, 2, nil, 10); !r.Success || r.MatchIndex != 5 {
		t.Errorf("%s: heartbeat prev=(5,2): success=%v match=%d, want success with match 5", ctx, r.Success, r.MatchIndex)
	}
	if hb.CommitIndex() != 5 {
		t.Errorf("%s: commit index %d after the boundary heartbeat, want 5", ctx, hb.CommitIndex())
	}

	// 3. Entries 6.. at term 2: the reply's match is prev+len and storage has
	// already replaced the conflicting term-1 entries.
	app := nodeOn(t, voters, s)
	r := leaderAppend(t, app, 11, 5, 2, logEntries(2, 6, 2), 7)
	if !r.Success || r.MatchIndex != 7 {
		t.Errorf("%s: append prev=(5,2) entries 6..7: success=%v match=%d, want success with match 7", ctx, r.Success, r.MatchIndex)
	}
	tail, err := s.Entries(s.FirstIndex(), s.LastIndex()+1)
	if err != nil || !sameEntries(tail, logEntries(2, 6, 2)) {
		t.Errorf("%s: storage after the append holds [%s] (%v), want 6/2 7/2", ctx, describe(tail), err)
	}
}

// TestMatchingSnapshotMemoryDiskDifferenceIsBenign (T3.11): a matching install
// (5,1) drops the suffix in memory but leaves it in the WAL, so a restart
// recovers 6..10 at term 1. The card decides this is expected: the recovered
// tail is entries the cluster already agrees on. Held to the node that legitimately
// keeps them (snapshot (5,1) plus 6..10 at term 1, the paper's section 7 outcome):
// same entries, same votes, same replies to the same leader messages, and a leader
// append over them converges.
func TestMatchingSnapshotMemoryDiskDifferenceIsBenign(t *testing.T) {
	dir := t.TempDir()
	s, _ := openOnDisk(t, dir)
	requireOK(t, "append", s.Append(logEntries(1, 1, 10)))
	snap := raft.Snapshot{Index: 5, Term: 1, Conf: testConf, Data: []byte("state@5")}
	if term, err := s.Term(5); err != nil || term != snap.Term {
		t.Fatalf("precondition: local Term(5) = %d (%v), the install must be a match", term, err)
	}
	requireOK(t, "ApplySnapshot", s.ApplySnapshot(snap))
	requireOK(t, "close", s.Close())

	reference := func() *raft.MemoryStorage {
		return memoryWith(t, &snap, logEntries(1, 6, 5))
	}
	// The directory lock allows one open store at a time: each probe opens the
	// directory, runs f and closes it, so it starts from what a restart recovers.
	withDisk := func(f func(d *storage.DiskStorage)) {
		d, _ := openOnDisk(t, dir)
		defer func() { requireOK(t, "close", d.Close()) }()
		f(d)
	}
	voters := testConf.Voters

	withDisk(func(d *storage.DiskStorage) {
		got := viewOf(t, d)
		if got.boundary != 5 || got.term != 1 || !sameEntries(got.tail, logEntries(1, 6, 5)) {
			t.Fatalf("reopened log is %v, want snapshot (5,1) + the matching 6..10 at term 1", got)
		}
		// The recovered entries carry the index and term the cluster agreed on: they
		// equal what the reference node, which keeps the suffix by the paper's rule,
		// holds.
		ref := reference()
		want, err := ref.Entries(ref.FirstIndex(), ref.LastIndex()+1)
		if err != nil || !sameEntries(got.tail, want) {
			t.Fatalf("recovered tail [%s], the node that keeps the suffix holds [%s] (%v)", describe(got.tail), describe(want), err)
		}

		g, w := voteBattery(t, voters, d, 3), voteBattery(t, voters, reference(), 3)
		for i := range w {
			if g[i] != w[i] {
				t.Errorf("vote %s differs from the node that keeps the matching suffix: %s", g[i], w[i])
			}
		}
	})

	for _, adv := range [][3]uint64{{5, 1, 10}, {10, 1, 10}, {8, 2, 10}, {5, 2, 10}} {
		var gr raft.Message
		withDisk(func(d *storage.DiskStorage) {
			gr = leaderAppend(t, nodeOn(t, voters, d), 9, raft.Index(adv[0]), raft.Term(adv[1]), nil, raft.Index(adv[2]))
		})
		wr := leaderAppend(t, nodeOn(t, voters, reference()), 9, raft.Index(adv[0]), raft.Term(adv[1]), nil, raft.Index(adv[2]))
		if gr.Success != wr.Success || gr.MatchIndex != wr.MatchIndex {
			t.Errorf("heartbeat prev=(%d,%d): recovered success=%v match=%d, reference success=%v match=%d",
				adv[0], adv[1], gr.Success, gr.MatchIndex, wr.Success, wr.MatchIndex)
		}
	}

	// A leader that disagrees about 6.. at term 2 converges to its log.
	withDisk(func(d *storage.DiskStorage) {
		r := leaderAppend(t, nodeOn(t, voters, d), 9, 5, 1, logEntries(2, 6, 2), 7)
		if !r.Success || r.MatchIndex != 7 {
			t.Errorf("append over the recovered tail: success=%v match=%d, want success with match 7", r.Success, r.MatchIndex)
		}
		tail, err := d.Entries(d.FirstIndex(), d.LastIndex()+1)
		if err != nil || !sameEntries(tail, logEntries(2, 6, 2)) {
			t.Errorf("after the leader's append the log holds [%s] (%v), want 6/2 7/2", describe(tail), err)
		}
	})
}
