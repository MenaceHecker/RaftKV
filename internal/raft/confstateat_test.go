package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func mustAdd(t *testing.T, c *cluster, leader, target NodeID) {
	t.Helper()
	if err := c.node(leader).ProposeConfChange(ConfChange{
		Type: ConfChangeAddNode, NodeID: target, Addr: fmt.Sprintf("host:%d", target),
	}); err != nil {
		t.Fatalf("Add %d: %v", target, err)
	}
	c.deliverAll()
	c.tickN(3)
}

func confIndexes(t *testing.T, c *cluster, id NodeID) (add4, leave Index) {
	t.Helper()
	for _, e := range c.logEntries(id) {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			t.Fatalf("decoding entry %d: %v", e.Index, err)
		}
		if cc.Type == ConfChangeAddNode && cc.NodeID == 4 {
			add4 = e.Index
		}
		if cc.Type == ConfChangeLeaveJoint && add4 != 0 && leave == 0 {
			leave = e.Index
		}
	}
	if add4 == 0 || leave == 0 {
		t.Fatalf("node %d log has no Add 4 / Leave pair (add=%d leave=%d)", id, add4, leave)
	}
	return add4, leave
}

func wantState(t *testing.T, what string, got ConfState, err error, want ConfState) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: unexpected error %v", what, err)
		return
	}
	if !sameState(got, want) {
		t.Errorf("%s = %s, want %s", what, got, want)
	}
}

func sameState(a, b ConfState) bool {
	norm := func(cs ConfState) ConfState {
		if len(cs.Voters) == 0 {
			cs.Voters = nil
		}
		if len(cs.Incoming) == 0 {
			cs.Incoming = nil
		}
		if len(cs.Addrs) == 0 {
			cs.Addrs = nil
		}
		return cs
	}
	return reflect.DeepEqual(norm(a), norm(b))
}

func TestConfStateAtWithoutSnapshotUsesTheInitialConfiguration(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	settleAddNode4(t, c, 1)
	n := c.node(1)
	add4, leave := confIndexes(t, c, 1)

	initial := ConfState{Voters: []NodeID{1, 2, 3}}
	got, err := n.ConfStateAt(0)
	wantState(t, "ConfStateAt(0)", got, err, initial)
	got, err = n.ConfStateAt(add4 - 1)
	wantState(t, "ConfStateAt(before Add 4)", got, err, initial)

	got, err = n.ConfStateAt(add4)
	wantState(t, "ConfStateAt(Add 4)", got, err, ConfState{
		Voters: []NodeID{1, 2, 3}, Incoming: []NodeID{1, 2, 3, 4}, Joint: true,
		Addrs: map[NodeID]string{4: "host:4"},
	})
	got, err = n.ConfStateAt(leave)
	wantState(t, "ConfStateAt(Leave)", got, err, ConfState{
		Voters: []NodeID{1, 2, 3, 4}, Addrs: map[NodeID]string{4: "host:4"},
	})
}

func TestConfStateAtRejectsIndexesAboveTheLog(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	n := c.node(1)

	got, err := n.ConfStateAt(n.LastIndex() + 1)
	if err == nil {
		t.Fatalf("ConfStateAt(LastIndex+1) = %s, want an error", got)
	}
	if !got.IsEmpty() {
		t.Errorf("ConfStateAt(LastIndex+1) returned a partial result %s alongside its error", got)
	}
	if errors.Is(err, ErrCompacted) {
		t.Errorf("ConfStateAt(LastIndex+1) error %v is ErrCompacted, want a distinct rejection", err)
	}
}

func TestConfStateAtAtAndBelowTheSnapshot(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	settleAddNode4(t, c, 1)
	n := c.node(1)
	_, leave := confIndexes(t, c, 1)

	settled := ConfState{Voters: []NodeID{1, 2, 3, 4}, Addrs: map[NodeID]string{4: "host:4"}}
	if err := c.storages[1].CreateSnapshot(leave, nil, settled); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got, err := n.ConfStateAt(leave)
	wantState(t, "ConfStateAt(snapshot index)", got, err, settled)

	if _, err := n.ConfStateAt(leave - 1); !errors.Is(err, ErrCompacted) {
		t.Errorf("ConfStateAt(snapshot index - 1) error = %v, want ErrCompacted", err)
	}
	if _, err := n.ConfStateAt(0); !errors.Is(err, ErrCompacted) {
		t.Errorf("ConfStateAt(0) over a snapshot error = %v, want ErrCompacted", err)
	}
}

func TestConfStateAtReplaysAboveTheSnapshot(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 1})
	c.campaign(1)
	settleAddNode4(t, c, 1)
	n := c.node(1)
	_, leave := confIndexes(t, c, 1)

	settled := ConfState{Voters: []NodeID{1, 2, 3, 4}, Addrs: map[NodeID]string{4: "host:4"}}
	if err := c.storages[1].CreateSnapshot(leave, nil, settled); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	mustAdd(t, c, 1, 5)
	add5 := n.LastIndex()
	for _, e := range c.logEntries(1) {
		if e.Type == EntryConfChange {
			if cc, _ := DecodeConfChange(e.Data); cc.Type == ConfChangeAddNode && cc.NodeID == 5 {
				add5 = e.Index
			}
		}
	}

	got, err := n.ConfStateAt(add5)
	wantState(t, "ConfStateAt(Add 5)", got, err, ConfState{
		Voters: []NodeID{1, 2, 3, 4}, Incoming: []NodeID{1, 2, 3, 4, 5}, Joint: true,
		Addrs: map[NodeID]string{4: "host:4", 5: "host:5"},
	})
}

// failingSnapshots lets a test make Snapshot() fail, or report an older
// snapshot (and the first index that goes with it) than the one stored.
type failingSnapshots struct {
	*MemoryStorage
	err   error
	older *Snapshot
}

func (s *failingSnapshots) Snapshot() (Snapshot, error) {
	if s.err != nil {
		return Snapshot{}, s.err
	}
	if s.older != nil {
		return *s.older, nil
	}
	return s.MemoryStorage.Snapshot()
}

func (s *failingSnapshots) FirstIndex() Index {
	if s.older != nil {
		return s.older.Index + 1
	}
	return s.MemoryStorage.FirstIndex()
}

func newOverStorage(t *testing.T, st Storage) *Node {
	t.Helper()
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1,
		Storage: st, Rand: rand.New(rand.NewSource(1)), //nolint:gosec // deterministic test
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestConfStateAtPropagatesStorageErrors(t *testing.T) {
	boom := errors.New("disk on fire")
	st := &failingSnapshots{MemoryStorage: filledStorage(t, 6)}
	n := newOverStorage(t, st)

	// A compaction moves the base, so the next lookup has to reload it.
	st.err = boom
	if err := st.CreateSnapshot(4, nil, ConfState{Voters: []NodeID{1, 2, 3}}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got, err := n.ConfStateAt(5)
	if !errors.Is(err, boom) {
		t.Fatalf("ConfStateAt error = %v (state %s), want it to wrap the storage error", err, got)
	}
	if !errors.Is(err, ErrStorage) {
		t.Errorf("ConfStateAt error = %v, want it to wrap ErrStorage", err)
	}
	if errors.Is(err, ErrSnapshotUnavailable) {
		t.Errorf("a storage failure must not be mistaken for ErrSnapshotUnavailable: %v", err)
	}
}

func TestConfStateAtFallsBackWhenTheSnapshotConfIsEmpty(t *testing.T) {
	st := filledStorage(t, 6)
	n := newOverStorage(t, st)
	if err := st.CreateSnapshot(4, nil, ConfState{}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	got, err := n.ConfStateAt(4)
	wantState(t, "ConfStateAt(snapshot with empty Conf)", got, err, ConfState{Voters: []NodeID{1, 2, 3}})
	if _, err := n.ConfStateAt(3); !errors.Is(err, ErrCompacted) {
		t.Errorf("ConfStateAt(3) error = %v, want ErrCompacted", err)
	}
}

func TestConfStateAtNeverMovesTheBaseBackwards(t *testing.T) {
	st := &failingSnapshots{MemoryStorage: filledStorage(t, 10)}
	n := newOverStorage(t, st)
	if err := st.CreateSnapshot(6, nil, ConfState{Voters: []NodeID{1, 2, 3, 4}}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if _, err := n.ConfStateAt(7); err != nil {
		t.Fatalf("ConfStateAt(7): %v", err)
	}

	// Storage now reports an older snapshot (index 3) than the base held (6).
	st.older = &Snapshot{Index: 3, Term: 1, Conf: ConfState{Voters: []NodeID{1, 2, 3}}}
	if _, err := n.ConfStateAt(7); err == nil {
		t.Fatal("ConfStateAt accepted a snapshot older than the base it already holds")
	}
	st.older = nil
	got, err := n.ConfStateAt(6)
	wantState(t, "ConfStateAt(6) after the rejected move", got, err, ConfState{Voters: []NodeID{1, 2, 3, 4}})
}

// ---- property: stored snapshot Conf and ConfStateAt against a replay oracle ----

type confSim struct {
	t    *testing.T
	seed int64
	rng  *rand.Rand
	c    *cluster
	opts clusterOpts

	// committed holds every committed configuration entry seen on any node,
	// scanned before anything is compacted, so a snapshot's Conf has a history
	// to be checked against even though each log is compacted.
	committed map[Index]Entry

	trace []string
}

func newConfSim(t *testing.T, seed int64) *confSim {
	opts := clusterOpts{seed: seed}
	return &confSim{
		t: t, seed: seed, rng: rand.New(rand.NewSource(seed)), //nolint:gosec // deterministic simulation
		c: newCluster(t, 3, opts), opts: opts, committed: make(map[Index]Entry),
	}
}

func (s *confSim) failf(format string, args ...any) {
	s.t.Helper()
	s.t.Fatalf("seed %d: %s\nsteps so far:\n  %s\n%s", s.seed, fmt.Sprintf(format, args...),
		strings.Join(s.trace, "\n  "), s.c.dump())
}

func (s *confSim) step(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf(format, args...))
}

func (s *confSim) scan() {
	for _, id := range s.c.ids {
		st, n := s.c.storages[id], s.c.node(id)
		lo := max(st.FirstIndex(), 1)
		hi := min(n.CommitIndex(), st.LastIndex())
		if lo > hi {
			continue
		}
		entries, err := st.Entries(lo, hi+1)
		if err != nil {
			s.failf("scanning node %d: %v", id, err)
		}
		for _, e := range entries {
			if e.Type != EntryConfChange {
				continue
			}
			if prev, ok := s.committed[e.Index]; ok && prev.Term != e.Term {
				s.failf("committed entries disagree at index %d: terms %d and %d", e.Index, prev.Term, e.Term)
			}
			s.committed[e.Index] = e
		}
	}
}

func applyEntries(conf config, entries []Entry) config {
	for _, e := range entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			continue
		}
		if next, err := applyConfChange(conf, cc); err == nil {
			conf = next
		}
	}
	return conf
}

// expectedAt replays the committed history up to the node's snapshot and the
// node's own log above it, as of index i. This is the definition of "the
// configuration as of i", independent of the code under test.
func (s *confSim) expectedAt(id NodeID, i Index) ConfState {
	st := s.c.storages[id]
	snapIdx := st.FirstIndex() - 1

	var history []Entry
	for idx, e := range s.committed {
		if idx <= min(i, snapIdx) {
			history = append(history, e)
		}
	}
	sort.Slice(history, func(a, b int) bool { return history[a].Index < history[b].Index })
	conf := applyEntries(newConfig(s.c.ids), history)

	if i > snapIdx {
		own, err := st.Entries(snapIdx+1, i+1)
		if err != nil {
			s.failf("reading node %d log (%d,%d]: %v", id, snapIdx, i, err)
		}
		conf = applyEntries(conf, own)
	}
	return conf.toState()
}

func (s *confSim) check() {
	s.t.Helper()
	s.scan()
	for _, id := range s.c.ids {
		n, st := s.c.node(id), s.c.storages[id]
		snapIdx := st.FirstIndex() - 1

		if snap, err := st.Snapshot(); err == nil {
			if want := s.expectedAt(id, snap.Index); !sameState(snap.Conf, want) {
				s.failf("node %d stored snapshot at %d has Conf %s, want %s", id, snap.Index, snap.Conf, want)
			}
		}

		if want := s.expectedAt(id, n.LastIndex()); !sameState(n.ConfState(), want) {
			s.failf("node %d configuration is %s, want %s (replayed through index %d)",
				id, n.ConfState(), want, n.LastIndex())
		}

		for i := snapIdx; i <= n.LastIndex(); i++ {
			got, err := n.ConfStateAt(i)
			if err != nil {
				s.failf("node %d ConfStateAt(%d): %v", id, i, err)
			}
			if want := s.expectedAt(id, i); !sameState(got, want) {
				s.failf("node %d ConfStateAt(%d) = %s, want %s", id, i, got, want)
			}
		}
	}
}

func (s *confSim) leaderID() (NodeID, bool) { return s.c.leader() }

func (s *confSim) opTick(n int) { s.step("tick %d", n); s.c.tickN(n); s.check() }

func (s *confSim) opCampaign(id NodeID) { s.step("campaign %d", id); s.c.campaign(id); s.check() }

func (s *confSim) opConf(add bool, target NodeID) {
	id, ok := s.leaderID()
	if !ok {
		return
	}
	typ := ConfChangeRemoveNode
	if add {
		typ = ConfChangeAddNode
	}
	s.step("leader %d proposes %v node %d", id, typ, target)
	_ = s.c.node(id).ProposeConfChange(ConfChange{Type: typ, NodeID: target, Addr: fmt.Sprintf("host:%d", target)})
	s.c.deliverAll()
	s.check()
}

func (s *confSim) opNormal() {
	if id, ok := s.leaderID(); ok {
		s.step("leader %d proposes data", id)
		_ = s.c.propose(id, "x")
		s.check()
	}
}

// opCompact snapshots node id at index at, with the configuration ConfStateAt
// reports for it, as the production writer does.
func (s *confSim) opCompact(id NodeID, at Index) {
	s.scan()
	st := s.c.storages[id]
	if at <= st.FirstIndex()-1 || at > st.LastIndex() || at > s.c.node(id).CommitIndex() {
		return
	}
	s.step("compact node %d at %d", id, at)
	conf, err := s.c.node(id).ConfStateAt(at)
	if err != nil {
		s.failf("ConfStateAt(%d) on node %d: %v", at, id, err)
	}
	if err := st.CreateSnapshot(at, nil, conf); err != nil {
		s.failf("CreateSnapshot(%d) on node %d: %v", at, id, err)
	}
	s.check()
}

func (s *confSim) opIsolateLeader() {
	if id, ok := s.leaderID(); ok {
		s.step("isolate leader %d", id)
		var rest []NodeID
		for _, o := range s.c.ids {
			if o != id {
				rest = append(rest, o)
			}
		}
		s.c.partition([]NodeID{id}, rest)
		s.check()
	}
}

func (s *confSim) opHeal() { s.step("heal"); s.c.heal(); s.c.tickN(2); s.check() }

// opRestart restarts a node the way production does: the snapshot's Conf is the
// initial configuration when a snapshot exists.
func (s *confSim) opRestart(id NodeID) {
	s.step("restart node %d", id)
	cfg := Config{
		ID: id, Peers: s.c.ids, ElectionTick: defaultElectionTick, HeartbeatTick: defaultHeartbeatTick,
		Storage: s.c.storages[id], Rand: rand.New(rand.NewSource(s.seed + int64(id)*7919)), //nolint:gosec // deterministic simulation
	}
	if snap, err := s.c.storages[id].Snapshot(); err == nil && !snap.Conf.IsEmpty() {
		conf := snap.Conf
		cfg.InitialConfState = &conf
	}
	n, err := NewNode(cfg)
	if err != nil {
		s.failf("restarting node %d: %v", id, err)
	}
	s.c.nodes[id] = n
	s.check()
}

func (s *confSim) randomStep() {
	id := s.c.ids[s.rng.Intn(len(s.c.ids))]
	switch s.rng.Intn(9) {
	case 0, 1:
		s.opTick(1 + s.rng.Intn(15))
	case 2:
		s.opCampaign(id)
	case 3:
		target := NodeID(4 + s.rng.Intn(2)) //nolint:gosec // 4 or 5
		lead, ok := s.leaderID()
		member := ok && containsID(s.c.node(lead).Members(), target)
		s.opConf(!member, target)
	case 4:
		s.opNormal()
	case 5, 6:
		n := s.c.node(id)
		lo := s.c.storages[id].FirstIndex()
		if hi := n.CommitIndex(); hi >= lo {
			s.opCompact(id, lo+Index(s.rng.Intn(int(hi-lo+1)))) //nolint:gosec // small test indexes
		}
	case 7:
		if s.rng.Intn(2) == 0 {
			s.opIsolateLeader()
		} else {
			s.opHeal()
		}
	case 8:
		s.opRestart(id)
	}
}

func containsID(ids []NodeID, id NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func TestSnapshotConfProperty(t *testing.T) {
	for seed := int64(1); seed <= 150; seed++ {
		s := newConfSim(t, seed)
		s.opCampaign(1)
		for range 60 {
			s.randomStep()
		}
	}
}

// TestSnapshotConfTargetedSchedule drives the two cases that motivated the card
// through the same step function and checks, so it is red regardless of seeds:
// compacting through Add 4 and its Leave before Add 5, and compacting while a
// change is pending above the snapshot.
func TestSnapshotConfTargetedSchedule(t *testing.T) {
	s := newConfSim(t, 7)
	s.opCampaign(1)

	s.opConf(true, 4)
	s.opTick(5)
	for _, id := range s.c.ids {
		s.opCompact(id, s.c.node(id).CommitIndex())
	}
	s.opConf(true, 5)
	s.opTick(5)

	lead, ok := s.leaderID()
	if !ok {
		s.failf("no leader")
	}
	s.opIsolateLeader()
	s.opConf(false, 5)
	s.opCompact(lead, s.c.node(lead).CommitIndex())
	s.opHeal()
	s.opTick(20)
	for _, id := range s.c.ids {
		s.opRestart(id)
	}
}
