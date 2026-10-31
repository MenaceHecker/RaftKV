package raft_test

// T3.11 test 8: an attempt to refute the counting argument that a node with a
// stale tail cannot help build a stale leader. Nodes run raft.Node on a real
// DiskStorage over faultfs (SyncAlways), restarts are Crash(drop) plus reopen.
// A refuting result is a bug report: no test here claims, or assumes, that
// Leader Completeness holds; the checker looks for a violation on every election.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

const (
	dataDir = "/data"
	walDir  = "/data/wal"
	snapDir = "/data/snap"
)

type voteKey struct {
	candidate raft.NodeID
	term      raft.Term
}

type sim struct {
	t    *testing.T
	seed int64
	ids  []raft.NodeID

	fss    map[raft.NodeID]*faultfs.FS
	stores map[raft.NodeID]*storage.DiskStorage
	nodes  map[raft.NodeID]*raft.Node
	starts map[raft.NodeID]int

	// group[id] is the partition group; nodes talk only inside a group.
	group map[raft.NodeID]int

	// committed maps an index to the term of the entry committed there.
	committed map[raft.Index]raft.Term
	// leaderTerm is the last term at which each node was checked as a leader.
	leaderTerm map[raft.NodeID]raft.Term

	inflight []raft.Message

	// afterStep sees the messages a node produced right after handling one.
	afterStep func(id raft.NodeID, in raft.Message, out []raft.Message)

	// voteGrants counts granted votes by candidate and term.
	voteGrants map[voteKey]int
}

func newSim(t *testing.T, seed int64, n int) *sim {
	t.Helper()
	s := &sim{
		t: t, seed: seed,
		fss:        map[raft.NodeID]*faultfs.FS{},
		stores:     map[raft.NodeID]*storage.DiskStorage{},
		nodes:      map[raft.NodeID]*raft.Node{},
		starts:     map[raft.NodeID]int{},
		group:      map[raft.NodeID]int{},
		committed:  map[raft.Index]raft.Term{},
		leaderTerm: map[raft.NodeID]raft.Term{},
		voteGrants: map[voteKey]int{},
	}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		s.ids = append(s.ids, id)
		f := faultfs.New(rand.New(rand.NewSource(seed*100+int64(i))), faultfs.Options{}) //nolint:gosec // deterministic simulation
		for _, d := range []string{walDir, snapDir} {
			if err := f.MkdirAll(d, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s): %v", d, err)
			}
		}
		for _, d := range []string{dataDir, "/"} {
			if err := f.SyncDir(d); err != nil {
				t.Fatalf("SyncDir(%s): %v", d, err)
			}
		}
		s.fss[id] = f
	}
	return s
}

func (s *sim) open(id raft.NodeID) (*storage.DiskStorage, storage.Snapshot) {
	s.t.Helper()
	st, snap, err := storage.OpenDiskStorage(storage.DiskConfig{Dir: dataDir, Sync: storage.SyncAlways, FS: s.fss[id]})
	if err != nil {
		s.t.Fatalf("seed %d: opening node %d: %v", s.seed, id, err)
	}
	return st, snap
}

func entries(term raft.Term, from, to raft.Index) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Type: raft.EntryNormal, Data: []byte(fmt.Sprintf("e%d@%d", i, term))})
	}
	return out
}

// seedDisk writes a node's durable state before it starts: the entries, the hard
// state, then optionally a snapshot at snapAt (CreateSnapshot, so it keeps its suffix).
func (s *sim) seedDisk(id raft.NodeID, es []raft.Entry, hs raft.HardState, snapAt raft.Index) {
	s.t.Helper()
	st, _ := s.open(id)
	if err := st.Append(es); err != nil {
		s.t.Fatalf("seeding node %d log: %v", id, err)
	}
	if err := st.SetHardState(hs); err != nil {
		s.t.Fatalf("seeding node %d hard state: %v", id, err)
	}
	if snapAt > 0 {
		if err := st.CreateSnapshot(snapAt, []byte("state"), raft.ConfState{Voters: s.ids}); err != nil {
			s.t.Fatalf("seeding node %d snapshot: %v", id, err)
		}
	}
	if err := st.Close(); err != nil {
		s.t.Fatalf("closing node %d: %v", id, err)
	}
}

func (s *sim) start(id raft.NodeID) {
	s.t.Helper()
	st, snap := s.open(id)
	s.starts[id]++
	cfg := raft.Config{
		ID:            id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       st,
		Rand:          rand.New(rand.NewSource(s.seed*7919 + int64(id)*131 + int64(s.starts[id]))), //nolint:gosec // deterministic simulation
	}
	if !snap.Conf.IsEmpty() {
		conf := snap.Conf
		cfg.InitialConfState = &conf
	} else {
		cfg.Peers = s.ids
	}
	n, err := raft.NewNode(cfg)
	if err != nil {
		s.t.Fatalf("seed %d: node %d: %v", s.seed, id, err)
	}
	s.stores[id], s.nodes[id] = st, n
}

// restart is a power loss (unsynced bytes dropped) followed by a reopen.
func (s *sim) restart(id raft.NodeID) {
	s.t.Helper()
	s.fss[id].Crash(faultfs.CrashOptions{})
	s.start(id)
}

func (s *sim) reachable(a, b raft.NodeID) bool { return s.group[a] == s.group[b] }

func (s *sim) markCommitted(id raft.NodeID, es []raft.Entry) {
	for _, e := range es {
		if prev, ok := s.committed[e.Index]; ok && prev != e.Term {
			s.t.Fatalf("seed %d: State Machine Safety violated: node %d applied %d/%d but %d/%d was committed",
				s.seed, id, e.Index, e.Term, e.Index, prev)
		}
		s.committed[e.Index] = e.Term
	}
}

// drain takes node id's Ready and returns the messages to send.
func (s *sim) drain(id raft.NodeID) []raft.Message {
	n := s.nodes[id]
	rd := n.Ready()
	s.markCommitted(id, rd.CommittedEntries)
	n.Advance(rd)
	return rd.Messages
}

func (s *sim) enqueue(ms []raft.Message) {
	for _, m := range ms {
		if m.Type == raft.MsgVoteResponse && m.Granted {
			s.voteGrants[voteKey{m.To, m.Term}]++
		}
		s.inflight = append(s.inflight, m)
	}
}

// pump delivers messages until the network is quiet, checking every election.
func (s *sim) pump() {
	s.t.Helper()
	for round := 0; ; round++ {
		if round > 500 {
			s.t.Fatalf("seed %d: the network did not settle", s.seed)
		}
		for _, id := range s.ids {
			if s.nodes[id] != nil {
				s.enqueue(s.drain(id))
			}
		}
		if len(s.inflight) == 0 {
			s.checkLeaders()
			return
		}
		batch := s.inflight
		s.inflight = nil
		for _, m := range batch {
			dst := s.nodes[m.To]
			if dst == nil || !s.reachable(m.From, m.To) {
				continue
			}
			if err := dst.Step(m); err != nil {
				s.t.Fatalf("seed %d: node %d stepping %s from %d: %v", s.seed, m.To, m.Type, m.From, err)
			}
			out := s.drain(m.To)
			if s.afterStep != nil {
				s.afterStep(m.To, m, out)
			}
			s.enqueue(out)
		}
		s.checkLeaders()
	}
}

// checkLeaders is the Leader Completeness checker, run on every election: a node
// that has just become leader of a term must hold every entry committed so far.
func (s *sim) checkLeaders() {
	s.t.Helper()
	for _, id := range s.ids {
		n := s.nodes[id]
		if n == nil || n.State() != raft.Leader || s.leaderTerm[id] == n.Term() {
			continue
		}
		s.leaderTerm[id] = n.Term()
		for idx, term := range s.committed {
			got, err := n.TermAt(idx)
			if err != nil {
				continue // compacted: the snapshot covers it
			}
			if got != term {
				s.t.Fatalf("seed %d: Leader Completeness violated: node %d became leader of term %d holding %d/%d, but %d/%d was committed",
					s.seed, id, n.Term(), idx, got, idx, term)
			}
		}
		if n.LastIndex() < maxIndex(s.committed) {
			s.t.Fatalf("seed %d: Leader Completeness violated: node %d became leader of term %d with last index %d, below committed index %d",
				s.seed, id, n.Term(), n.LastIndex(), maxIndex(s.committed))
		}
	}
}

func maxIndex(m map[raft.Index]raft.Term) raft.Index {
	var hi raft.Index
	for i := range m {
		hi = max(hi, i)
	}
	return hi
}

func (s *sim) tick(rounds int) {
	s.t.Helper()
	for range rounds {
		for _, id := range s.ids {
			if err := s.nodes[id].Tick(); err != nil {
				s.t.Fatalf("seed %d: node %d tick: %v", s.seed, id, err)
			}
		}
		s.pump()
	}
}

func (s *sim) campaign(id raft.NodeID) {
	s.t.Helper()
	if err := s.nodes[id].Step(raft.Message{Type: raft.MsgCampaign}); err != nil {
		s.t.Fatalf("seed %d: node %d campaign: %v", s.seed, id, err)
	}
	s.pump()
}

func (s *sim) leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range s.ids {
		if s.nodes[id].State() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

func snap52(ids []raft.NodeID) *raft.Snapshot {
	return &raft.Snapshot{Index: 5, Term: 2, Conf: raft.ConfState{Voters: ids}, Data: []byte("state")}
}

// seedStaleScenario lays out the maximal regression. With n = 2f+1: the quorum Q
// is nodes 1..f+1, which committed (5,2) (entries 1..4 at term 1, 5 at term 2,
// snapshotted at 5); E is node n, which holds an uncommitted term-1 log through
// 12; every other node R holds an uncommitted term-1 log through 10.
func (s *sim) seedStaleScenario() (q, r []raft.NodeID, e raft.NodeID) {
	f := (len(s.ids) - 1) / 2
	for i, id := range s.ids {
		switch {
		case i <= f:
			q = append(q, id)
			s.seedDisk(id, append(entries(1, 1, 4), entries(2, 5, 5)...), raft.HardState{Term: 2}, 5)
		case i == len(s.ids)-1:
			e = id
			s.seedDisk(id, entries(1, 1, 12), raft.HardState{Term: 3}, 0)
		default:
			r = append(r, id)
			s.seedDisk(id, entries(1, 1, 10), raft.HardState{Term: 2}, 0)
		}
	}
	for idx, term := range map[raft.Index]raft.Term{1: 1, 2: 1, 3: 1, 4: 1, 5: 2} {
		s.committed[idx] = term
	}
	for _, id := range s.ids {
		s.start(id)
	}
	return q, r, e
}

// TestStaleTailRegressionCannotBuildAStaleLeader (T3.11 test 8, required) (i):
// for n = 3, 5, 7 every non-quorum node except E installs the snapshot (5,2) over
// its mismatching term-1 log and restarts; the quorum is partitioned away and E
// campaigns. E must never reach a majority: the expected count is f votes, one short.
func TestStaleTailRegressionCannotBuildAStaleLeader(t *testing.T) {
	for _, n := range []int{3, 5, 7} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			s := newSim(t, int64(n), n)
			q, r, e := s.seedStaleScenario()
			f := (n - 1) / 2

			for _, id := range r {
				if term, _ := s.nodes[id].TermAt(5); term == 2 {
					t.Fatalf("precondition: node %d term at 5 is 2, the install must mismatch", id)
				}
				if err := s.nodes[id].Step(raft.Message{Type: raft.MsgInstallSnapshot, From: q[0], Term: 3,
					Snapshot: snap52(s.ids)}); err != nil {
					t.Fatalf("installing on node %d: %v", id, err)
				}
				s.drain(id)
				s.restart(id)
				t.Logf("node %d after install and restart: last index %d, commit %d", id, s.nodes[id].LastIndex(), s.nodes[id].CommitIndex())
			}

			// Quorum away; E can reach only the regressed nodes.
			for _, id := range q {
				s.group[id] = 1
			}
			s.voteGrants = map[voteKey]int{}
			s.campaign(e)
			s.tick(60) // E keeps retrying at higher terms

			mostGrants := 0
			for k, g := range s.voteGrants {
				if k.candidate == e {
					mostGrants = max(mostGrants, g)
				}
			}
			t.Logf("n=%d f=%d: E reached term %d; at most %d vote(s) granted in any one term by the %d regressed nodes, +1 own = %d; a majority is %d",
				n, f, s.nodes[e].Term(), mostGrants, len(r), 1+mostGrants, f+1)
			// The partitioned quorum may elect its own leader; nobody else may. The
			// Leader Completeness checker already ran on every election, quorum's included.
			for _, id := range s.leaders() {
				if s.group[id] == 0 {
					t.Fatalf("n=%d: node %d, outside the quorum, is a leader of term %d", n, id, s.nodes[id].Term())
				}
			}
			if 1+mostGrants >= f+1 {
				t.Fatalf("n=%d: E collected %d votes, a majority of %d", n, 1+mostGrants, f+1)
			}
		})
	}
}

// TestRestartedNodeAcksSurviveCrashes (T3.11 test 8 (ii)): the leader counts a
// regressed node D's InstallSnapshot ack (match = 5) and then its AppendEntries
// ack in commit quorums; D crashes (drop) right after each ack. Under SyncAlways
// D must still hold everything its acks claimed. Nodes: leader 1, D = 3, node 2
// isolated, so every commit needs D.
func TestRestartedNodeAcksSurviveCrashes(t *testing.T) {
	s := newSim(t, 11, 3)
	const leader, dead, d = raft.NodeID(1), raft.NodeID(2), raft.NodeID(3)
	s.seedDisk(leader, append(entries(1, 1, 4), entries(2, 5, 5)...), raft.HardState{Term: 2}, 5)
	s.seedDisk(dead, append(entries(1, 1, 4), entries(2, 5, 5)...), raft.HardState{Term: 2}, 5)
	s.seedDisk(d, entries(1, 1, 10), raft.HardState{Term: 2}, 0)
	for idx, term := range map[raft.Index]raft.Term{1: 1, 2: 1, 3: 1, 4: 1, 5: 2} {
		s.committed[idx] = term
	}
	for _, id := range s.ids {
		s.start(id)
	}
	s.group[dead] = 1 // node 2 is unreachable

	type claim struct {
		match raft.Index
		term  raft.Term
	}
	var claims []claim
	var crashes int
	logged := map[string]bool{}
	s.afterStep = func(id raft.NodeID, _ raft.Message, out []raft.Message) {
		if id != d {
			return
		}
		for _, m := range out {
			if !m.Success || (m.Type != raft.MsgAppendResponse && m.Type != raft.MsgInstallSnapshotResponse) {
				continue
			}
			term, err := s.nodes[d].TermAt(m.MatchIndex)
			if err != nil {
				t.Fatalf("D acked match %d but cannot read its term: %v", m.MatchIndex, err)
			}
			c := claim{m.MatchIndex, term}
			claims = append(claims, c)

			// Crash D right after the ack, before anything else reaches it.
			s.restart(d)
			crashes++
			got, err := s.nodes[d].TermAt(c.match)
			if !logged[m.Type.String()+fmt.Sprint(c.match)] {
				logged[m.Type.String()+fmt.Sprint(c.match)] = true
				t.Logf("D acked %s match %d (term %d), crashed; recovered: last index %d, term at %d = %d (err %v)",
					m.Type, c.match, c.term, s.nodes[d].LastIndex(), c.match, got, err)
			}
			if s.nodes[d].LastIndex() < c.match || err != nil || got != c.term {
				t.Errorf("D lost what its ack claimed: acked match %d at term %d, recovered term %d (err %v), last index %d",
					c.match, c.term, got, err, s.nodes[d].LastIndex())
			}
		}
	}

	s.campaign(leader)
	s.tick(30)
	if len(claims) < 2 {
		t.Fatalf("D acked %d times, want at least the InstallSnapshot and one AppendEntries ack", len(claims))
	}
	if s.nodes[leader].State() != raft.Leader {
		t.Fatalf("node 1 is not the leader (state %s)", s.nodes[leader].State())
	}
	if s.nodes[leader].CommitIndex() < 6 {
		t.Fatalf("leader commit index %d: D's acks did not commit the leader's term-3 no-op at 6", s.nodes[leader].CommitIndex())
	}
	if err := s.nodes[leader].Propose([]byte("more")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	s.pump()
	t.Logf("leader commit %d, D last index %d after %d crashes", s.nodes[leader].CommitIndex(), s.nodes[d].LastIndex(), crashes)
	if s.nodes[leader].CommitIndex() < 7 {
		t.Errorf("leader commit index %d, want the proposal at 7 committed with D's ack", s.nodes[leader].CommitIndex())
	}
}

// TestRandomSchedulesKeepLeaderCompleteness (T3.11 test 8, 200 seeded random
// schedules): five nodes on faultfs under SyncAlways; random ticks, proposals,
// partitions, power-loss restarts, compactions and (through the leader) snapshot
// installs, with the Leader Completeness and State Machine Safety checkers on
// every election and apply. Failures print the seed.
func TestRandomSchedulesKeepLeaderCompleteness(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 20
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		runRandomSchedule(t, seed)
	}
}

func runRandomSchedule(t *testing.T, seed int64) {
	t.Helper()
	s := newSim(t, seed, 5)
	for _, id := range s.ids {
		s.start(id)
	}
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic simulation

	for step := 0; step < 150; step++ {
		switch op := rng.Intn(10); {
		case op < 4:
			s.tick(1 + rng.Intn(8))
		case op < 6:
			for _, id := range s.ids {
				if s.nodes[id].State() == raft.Leader {
					_ = s.nodes[id].Propose([]byte(fmt.Sprintf("p%d", step)))
				}
			}
			s.pump()
		case op == 6: // partition into two random groups
			for _, id := range s.ids {
				s.group[id] = rng.Intn(2)
			}
		case op == 7: // heal
			for _, id := range s.ids {
				s.group[id] = 0
			}
		case op == 8:
			s.restart(s.ids[rng.Intn(len(s.ids))])
			s.pump()
		default: // compaction at the commit index
			id := s.ids[rng.Intn(len(s.ids))]
			n, st := s.nodes[id], s.stores[id]
			if idx := n.CommitIndex(); idx > st.SnapshotMeta().Index && idx <= st.LastIndex() {
				if err := st.CreateSnapshot(idx, []byte("state"), n.ConfState()); err != nil {
					t.Fatalf("seed %d: compacting node %d at %d: %v", seed, id, idx, err)
				}
			}
		}
	}
}
