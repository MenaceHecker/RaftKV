package raft

// T3.11 tests 3a-3d, 4, 5 and 7. The recovered state of a node that installed a
// mismatching snapshot (5, term 2) and restarted is snapshot (5,2) plus the
// stale entries 6..10 at term 1 that DiskStorage.Open brings back from the WAL.
// Storage here is a MemoryStorage seeded with exactly that state (internal/node
// and internal/storage tests cover the real disk path). Tests 3a and 7 state the
// correct behavior and are red today: that is evidence, not a defect of the test.
// None of them claims a Leader Completeness violation.

import (
	"fmt"
	"math/rand"
	"testing"
)

var staleVoters = []NodeID{1, 2, 3, 4, 5}

func snapshot52() *Snapshot {
	return &Snapshot{Index: 5, Term: 2, Conf: ConfState{Voters: staleVoters}, Data: []byte("state@5")}
}

func mkEntries(term Term, from, to Index) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Term: term, Index: i, Type: EntryNormal, Data: []byte(fmt.Sprintf("e%d@%d", i, term))})
	}
	return out
}

// seed is the durable state a node starts from.
type seed struct {
	snap    *Snapshot
	entries []Entry
	hs      HardState
}

// recovered is node D after the restart: snapshot (5,2), stale 6..10 at term 1,
// term 2, no vote. Every case below builds its own, because an earlier append or
// vote in a shared fixture would change the log, the commit index and the vote.
func recovered() seed {
	return seed{snap: snapshot52(), entries: mkEntries(1, 6, 10), hs: HardState{Term: 2, VotedFor: None}}
}

func (s seed) storage(t *testing.T) *MemoryStorage {
	t.Helper()
	st := NewMemoryStorage()
	if s.snap != nil {
		if err := st.ApplySnapshot(*s.snap); err != nil {
			t.Fatalf("seeding snapshot: %v", err)
		}
	}
	if len(s.entries) > 0 {
		if err := st.Append(s.entries); err != nil {
			t.Fatalf("seeding entries: %v", err)
		}
	}
	if err := st.SetHardState(s.hs); err != nil {
		t.Fatalf("seeding hard state: %v", err)
	}
	return st
}

func seededNode(t *testing.T, id NodeID, st Storage) *Node {
	t.Helper()
	n, err := NewNode(Config{
		ID:            id,
		Peers:         staleVoters,
		ElectionTick:  defaultElectionTick,
		HeartbeatTick: defaultHeartbeatTick,
		Storage:       st,
		Rand:          rand.New(rand.NewSource(int64(id) * 7919)), //nolint:gosec // deterministic test
	})
	if err != nil {
		t.Fatalf("creating node %d: %v", id, err)
	}
	return n
}

func seededCluster(t *testing.T, seeds map[NodeID]seed) *cluster {
	t.Helper()
	c := &cluster{
		t:          t,
		ids:        staleVoters,
		nodes:      map[NodeID]*Node{},
		storages:   map[NodeID]*MemoryStorage{},
		applied:    map[NodeID][]Entry{},
		readStates: map[NodeID][]ReadState{},
		snapshots:  map[NodeID][]*Snapshot{},
	}
	for _, id := range staleVoters {
		st := seeds[id].storage(t)
		c.storages[id] = st
		c.nodes[id] = seededNode(t, id, st)
	}
	return c
}

// messages drains what n wants to send.
func messages(n *Node) []Message {
	rd := n.Ready()
	n.Advance(rd)
	return rd.Messages
}

func only(t *testing.T, ms []Message, typ MessageType) Message {
	t.Helper()
	if len(ms) != 1 || ms[0].Type != typ {
		t.Fatalf("got %d messages %+v, want exactly one %s", len(ms), ms, typ)
	}
	return ms[0]
}

func logString(n *Node, st *MemoryStorage) string {
	var b []byte
	b = fmt.Appendf(b, "snapshot (%d,%d) +", st.snapshot.Index, st.snapshot.Term)
	for i := n.log.firstIndex(); i <= n.LastIndex(); i++ {
		term, _ := n.TermAt(i)
		b = fmt.Appendf(b, " %d/%d", i, term)
	}
	return string(b)
}

// TestStaleTailRestartVoteAndMatchIndex (T3.11 test 3, required): a restarted
// voter with snapshot (5,2) and stale tail 6..10 at term 1, one fresh fixture
// per subtest.
func TestStaleTailRestartVoteAndMatchIndex(t *testing.T) {
	const leader, candidate = NodeID(1), NodeID(5)

	t.Run("3a vote", func(t *testing.T) {
		// RequestVote at term 3. The node compares candidates against (5,2), the
		// committed boundary, not against (10,1) from the stale tail.
		for _, tc := range []struct {
			name    string
			idx     Index
			term    Term
			granted bool
		}{
			{"(12,1) must be rejected", 12, 1, false},
			{"(7,1) must be rejected", 7, 1, false},
			{"(4,1) must be rejected", 4, 1, false},
			{"control (5,2) is granted", 5, 2, true},
			{"control (6,2) is granted", 6, 2, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				st := recovered().storage(t)
				n := seededNode(t, 4, st)
				if err := n.Step(Message{Type: MsgVoteRequest, From: candidate, Term: 3,
					LastLogIndex: tc.idx, LastLogTerm: tc.term}); err != nil {
					t.Fatalf("step: %v", err)
				}
				resp := only(t, messages(n), MsgVoteResponse)
				if resp.Term != 3 || resp.Granted != tc.granted {
					t.Errorf("response term %d granted %v, want term 3 granted %v (log: %s)",
						resp.Term, resp.Granted, tc.granted, logString(n, st))
				}
				wantVote := None
				if tc.granted {
					wantVote = candidate
				}
				if hs, _ := st.InitialState(); hs.Term != 3 || hs.VotedFor != wantVote {
					t.Errorf("persisted hard state %+v, want term 3 VotedFor %d", hs, wantVote)
				}
				if n.vote != wantVote {
					t.Errorf("VotedFor = %d, want %d", n.vote, wantVote)
				}
			})
		}
	})

	t.Run("3b heartbeat", func(t *testing.T) {
		// An empty AppendEntries (what the leader's periodic heartbeat is) with
		// prev=(5,2) and leader commit 10 acknowledges and commits only the boundary.
		n := seededNode(t, 4, recovered().storage(t))
		if err := n.Step(Message{Type: MsgAppendRequest, From: leader, Term: 2,
			PrevLogIndex: 5, PrevLogTerm: 2, CommitIndex: 10}); err != nil {
			t.Fatalf("step: %v", err)
		}
		resp := only(t, messages(n), MsgAppendResponse)
		if !resp.Success || resp.MatchIndex != 5 {
			t.Errorf("response success=%v match=%d, want success with match 5", resp.Success, resp.MatchIndex)
		}
		if n.CommitIndex() != 5 {
			t.Errorf("commit index %d, want 5", n.CommitIndex())
		}
	})

	t.Run("3c append", func(t *testing.T) {
		// New entries 6..7 at term 2 replace the stale tail; 8..10 are gone.
		st := recovered().storage(t)
		n := seededNode(t, 4, st)
		if err := n.Step(Message{Type: MsgAppendRequest, From: leader, Term: 2,
			PrevLogIndex: 5, PrevLogTerm: 2, Entries: mkEntries(2, 6, 7), CommitIndex: 7}); err != nil {
			t.Fatalf("step: %v", err)
		}
		resp := only(t, messages(n), MsgAppendResponse)
		if !resp.Success || resp.MatchIndex != 7 {
			t.Errorf("response success=%v match=%d, want success with match 7", resp.Success, resp.MatchIndex)
		}
		if n.CommitIndex() != 7 {
			t.Errorf("commit index %d, want 7", n.CommitIndex())
		}
		if n.LastIndex() != 7 {
			t.Errorf("log: %s, want exactly 6..7 above the snapshot", logString(n, st))
		}
		for _, i := range []Index{6, 7} {
			if term, _ := n.TermAt(i); term != 2 {
				t.Errorf("term at %d is %d, want 2 (log: %s)", i, term, logString(n, st))
			}
		}
	})

	t.Run("3d rejection hints", func(t *testing.T) {
		reject := func(t *testing.T, n *Node, prevIdx Index, prevTerm Term) Message {
			t.Helper()
			if err := n.Step(Message{Type: MsgAppendRequest, From: leader, Term: 2,
				PrevLogIndex: prevIdx, PrevLogTerm: prevTerm}); err != nil {
				t.Fatalf("step: %v", err)
			}
			resp := only(t, messages(n), MsgAppendResponse)
			if resp.Success {
				t.Fatalf("prev=(%d,%d) was accepted, want a rejection", prevIdx, prevTerm)
			}
			return resp
		}

		for _, tc := range []struct {
			name         string
			prevIdx      Index
			prevTerm     Term
			wantIdx      Index
			wantTerm     Term
			replaceFirst bool
		}{
			{"prev=(8,2) on the untouched fixture", 8, 2, 6, 1, false},
			{"prev=(12,2) on the untouched fixture", 12, 2, 11, 0, false},
			{"prev=(8,2) after 6..10 was replaced by 6..7 at term 2", 8, 2, 8, 0, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				n := seededNode(t, 4, recovered().storage(t))
				if tc.replaceFirst {
					if err := n.Step(Message{Type: MsgAppendRequest, From: leader, Term: 2,
						PrevLogIndex: 5, PrevLogTerm: 2, Entries: mkEntries(2, 6, 7)}); err != nil {
						t.Fatalf("replacing the tail: %v", err)
					}
					messages(n)
				}
				resp := reject(t, n, tc.prevIdx, tc.prevTerm)
				if resp.ConflictIndex != tc.wantIdx || resp.ConflictTerm != tc.wantTerm {
					t.Errorf("hint (%d,%d), want (%d,%d)", resp.ConflictIndex, resp.ConflictTerm, tc.wantIdx, tc.wantTerm)
				}
			})
		}

		// A leader whose next for the peer is 9 never has it rise on any hint
		// (replication.go backoff only lowers next).
		for _, hint := range []struct {
			idx  Index
			term Term
		}{{6, 1}, {11, 0}, {8, 0}} {
			t.Run(fmt.Sprintf("leader next stays <= 9 on hint (%d,%d)", hint.idx, hint.term), func(t *testing.T) {
				c := newCluster(t, 3, clusterOpts{seed: 1})
				c.campaign(1)
				for i := range 12 {
					if err := c.propose(1, fmt.Sprintf("p%d", i)); err != nil {
						t.Fatalf("propose: %v", err)
					}
				}
				l := c.node(1)
				l.progress[2].next = 9
				if err := l.Step(Message{Type: MsgAppendResponse, From: 2, Term: l.Term(),
					ConflictIndex: hint.idx, ConflictTerm: hint.term}); err != nil {
					t.Fatalf("step: %v", err)
				}
				if got := l.progress[2].next; got > 9 {
					t.Errorf("next rose to %d on hint (%d,%d)", got, hint.idx, hint.term)
				}
			})
		}
	})
}

// TestStaleTailIsOverwrittenByTheLeader (T3.11 test 4; may pass today): the
// ordinary AppendEntries path repairs the stale tail.
func TestStaleTailIsOverwrittenByTheLeader(t *testing.T) {
	st := recovered().storage(t)
	n := seededNode(t, 4, st)

	// prev=(8,3) does not match the stale entry 8/1: rejected with a hint.
	if err := n.Step(Message{Type: MsgAppendRequest, From: 1, Term: 3, PrevLogIndex: 8, PrevLogTerm: 3}); err != nil {
		t.Fatalf("step: %v", err)
	}
	resp := only(t, messages(n), MsgAppendResponse)
	if resp.Success || resp.ConflictIndex != 6 || resp.ConflictTerm != 1 {
		t.Fatalf("prev=(8,3): success=%v hint (%d,%d), want rejection with hint (6,1)",
			resp.Success, resp.ConflictIndex, resp.ConflictTerm)
	}

	// The leader backs up to the boundary and sends its log 6..8 at term 3.
	if err := n.Step(Message{Type: MsgAppendRequest, From: 1, Term: 3, PrevLogIndex: 5, PrevLogTerm: 2,
		Entries: mkEntries(3, 6, 8), CommitIndex: 8}); err != nil {
		t.Fatalf("step: %v", err)
	}
	resp = only(t, messages(n), MsgAppendResponse)
	if !resp.Success || resp.MatchIndex != 8 {
		t.Fatalf("success=%v match=%d, want success with match 8", resp.Success, resp.MatchIndex)
	}
	if n.LastIndex() != 8 || n.CommitIndex() != 8 {
		t.Errorf("log: %s, commit %d; want the leader's 6..8 and commit 8", logString(n, st), n.CommitIndex())
	}
	for _, i := range []Index{6, 7, 8} {
		if term, _ := n.TermAt(i); term != 3 {
			t.Errorf("term at %d is %d, want the leader's 3", i, term)
		}
	}
}

// TestRestartedNodeWithStaleTailCannotWinElection (T3.11 test 5; may pass
// today): peers hold (5,2), so a majority refuses the stale node's RequestVote.
func TestRestartedNodeWithStaleTailCannotWinElection(t *testing.T) {
	peer := seed{snap: snapshot52(), hs: HardState{Term: 2}}
	c := seededCluster(t, map[NodeID]seed{1: peer, 2: peer, 3: peer, 4: recovered(), 5: peer})

	var responses []Message
	c.filter = func(m Message) bool {
		if m.Type == MsgVoteResponse && m.To == 4 {
			responses = append(responses, m)
		}
		return true
	}
	c.campaign(4)

	d := c.node(4)
	if d.State() == Leader {
		t.Fatalf("the stale node won the election at term %d", d.Term())
	}
	if len(responses) != 4 {
		t.Fatalf("got %d vote responses, want 4", len(responses))
	}
	for _, r := range responses {
		if r.Granted {
			t.Errorf("node %d granted its vote to the stale node", r.From)
		}
	}
}

// TestVoteRegressionAfterInstallSnapshotRestart (T3.11 test 7, required, red
// today): the reachable 5-node sequence. A, B, C (1-3) commit (5,2); D (4) keeps
// an uncommitted term-1 log through 10, E (5) through 12; A installs the snapshot
// on D. Before D restarts it rejects E's higher-term RequestVote advertising
// (12,1); after it restarts with (5,2)+(6..10,1) it must reject it again. It
// shows an invalid local vote only: no Leader Completeness violation is claimed.
func TestVoteRegressionAfterInstallSnapshotRestart(t *testing.T) {
	committed := seed{snap: snapshot52(), hs: HardState{Term: 3, VotedFor: 1}}
	c := seededCluster(t, map[NodeID]seed{
		1: committed, 2: committed, 3: committed,
		4: {entries: mkEntries(1, 1, 10), hs: HardState{Term: 3, VotedFor: 1}},
		5: {entries: mkEntries(1, 1, 12), hs: HardState{Term: 3, VotedFor: None}},
	})
	d := c.node(4)

	// A installs the snapshot on D; D's entry 5 has term 1, a mismatch.
	if term, _ := d.TermAt(5); term == snapshot52().Term {
		t.Fatalf("precondition: D's term at 5 is %d, the install must be a mismatch", term)
	}
	if err := d.Step(Message{Type: MsgInstallSnapshot, From: 1, Term: 3, Snapshot: snapshot52()}); err != nil {
		t.Fatalf("installing the snapshot on D: %v", err)
	}
	if got := only(t, messages(d), MsgInstallSnapshotResponse); !got.Success {
		t.Fatalf("D refused the snapshot")
	}

	// E, partitioned until now, campaigns at term 4 advertising (12,1).
	var votes = map[NodeID]bool{}
	c.filter = func(m Message) bool {
		if m.Type == MsgVoteResponse && m.To == 5 {
			votes[m.From] = m.Granted
		}
		return true
	}
	c.campaign(5)
	e := c.node(5)
	if e.State() == Leader {
		t.Fatalf("E won the election")
	}
	for _, id := range []NodeID{1, 2, 3, 4} {
		granted, seen := votes[id]
		if !seen || granted {
			t.Errorf("before the restart: node %d vote seen=%v granted=%v, want a rejection", id, seen, granted)
		}
	}

	// D restarts. Its disk holds the snapshot (5,2), the stale 6..10 at term 1 and
	// the hard state it persisted on seeing E's term 4.
	req := Message{Type: MsgVoteRequest, From: 5, Term: 4, LastLogIndex: 12, LastLogTerm: 1}
	st := seed{snap: snapshot52(), entries: mkEntries(1, 6, 10), hs: HardState{Term: 4}}.storage(t)
	restarted := seededNode(t, 4, st)
	if err := restarted.Step(req); err != nil {
		t.Fatalf("step: %v", err)
	}
	resp := only(t, messages(restarted), MsgVoteResponse)
	if resp.Term != 4 || resp.Granted {
		t.Errorf("after the restart: D answered term %d granted=%v to E's (12,1), want a rejection (log: %s)",
			resp.Term, resp.Granted, logString(restarted, st))
	}
}
