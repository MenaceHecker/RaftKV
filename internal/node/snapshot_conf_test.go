package node

import (
	"context"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// A snapshot's Conf must be the configuration as of the snapshot index, not
// the append-effective one that includes config entries above applied.

// slowElections gives an isolated leader time to take a proposal and compact
// before CheckQuorum makes it step down.
func slowElections(cfg *Config) { cfg.ElectionTick = 40 }

func (c *testCluster) block(from, to raft.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked[[2]raft.NodeID{from, to}] = true
}

func (c *testCluster) awaitAllApplied() {
	c.t.Helper()
	c.eventually("every node to apply what the leader applied", func() bool {
		var want raft.Index
		for _, n := range c.running() {
			want = max(want, n.Status().Applied)
		}
		for _, n := range c.running() {
			if n.Status().Applied < want {
				return false
			}
		}
		return true
	})
}

// proposeAddNode4 starts AddNode(4) in the background and waits until n holds it
// in its log (its configuration is joint). The call cannot commit while n is cut
// off, and is cancelled when the test ends.
func proposeAddNode4(t *testing.T, c *testCluster, n *Node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.AddNode(ctx, 4, "host:4") }()
	t.Cleanup(func() { cancel(); t.Logf("AddNode ended: %v", <-done) })

	c.eventually("the cut-off node to hold Add 4 above applied", func() bool {
		return n.Status().Members.Joint
	})
}

func storedConf(t *testing.T, n *Node) (raft.ConfState, raft.Index) {
	t.Helper()
	snap, err := n.storage.Snapshot()
	if err != nil {
		t.Fatalf("reading the stored snapshot: %v", err)
	}
	return snap.Conf, snap.Index
}

// confAcks is the number of acknowledgements a commit or an election needs
// from each side of cs: one majority outside a joint phase, two inside it.
func confAcks(cs raft.ConfState) (old, incoming int) {
	old = len(cs.Voters)/2 + 1
	if cs.Joint {
		incoming = len(cs.Incoming)/2 + 1
	}
	return old, incoming
}

func assertPlainThree(t *testing.T, what string, cs raft.ConfState) {
	t.Helper()
	if cs.Joint || !slices.Equal(cs.Voters, []raft.NodeID{1, 2, 3}) {
		t.Fatalf("%s: configuration is %s, want voters=[1 2 3]", what, cs)
	}
	// {1,2,3} settled means two acknowledgements suffice, so nodes {1,2} can
	// elect and commit. The joint {1,2,3}/{1,2,3,4} would need two of three and
	// three of four.
	if old, incoming := confAcks(cs); old != 2 || incoming != 0 {
		t.Fatalf("%s: needs %d/%d acknowledgements, want 2 from one majority", what, old, incoming)
	}
}

// Test 1.
func TestSnapshotDoesNotStoreAnUncommittedConfigChange(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	leader := c.awaitLeader()
	mustPut(t, leader, 1, 1, "k", "v")
	c.awaitAllApplied()

	c.isolate(leader.Status().ID)
	proposeAddNode4(t, c, leader)

	ctx, cancel := testContext(t)
	defer cancel()
	if err := leader.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	conf, at := storedConf(t, leader)
	assertPlainThree(t, "stored snapshot at index "+itoa(at)+" must not include the uncommitted Add 4", conf)
}

// Test 4a: a follower compacts while a config entry is pending above applied.
func TestFollowerSnapshotDoesNotStoreAnUncommittedConfigChange(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	leader := c.awaitLeader()
	mustPut(t, leader, 1, 1, "k", "v")
	c.awaitAllApplied()

	var follower, other *Node
	for _, n := range c.running() {
		switch {
		case n == leader:
		case follower == nil:
			follower = n
		default:
			other = n
		}
	}
	lid, fid, oid := leader.Status().ID, follower.Status().ID, other.Status().ID

	// The follower receives the entry but its acknowledgement never reaches the
	// leader, and the other follower is cut off, so the entry cannot commit.
	c.block(fid, lid)
	c.block(lid, oid)
	c.block(oid, lid)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- leader.AddNode(ctx, 4, "host:4") }()
	t.Cleanup(func() { cancel(); t.Logf("AddNode ended: %v", <-done) })

	c.eventually("the follower to hold Add 4 without committing it", func() bool {
		s := follower.Status()
		return s.Members.Joint && s.Commit == s.Applied
	})

	cctx, ccancel := testContext(t)
	defer ccancel()
	if err := follower.Compact(cctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	conf, at := storedConf(t, follower)
	assertPlainThree(t, "follower snapshot at index "+itoa(at)+" must not include the uncommitted Add 4", conf)
}

// badSnapshotScenario leaves the cluster in this state: cut-off leader `old`
// compacted while Add 4 was pending above applied, an intermediate leader
// overwrote that entry, and `old` has rejoined as a follower of it.
func badSnapshotScenario(t *testing.T, c *testCluster) (old, mid, third *Node) {
	t.Helper()

	old = c.awaitLeader()
	oldID := old.Status().ID
	for i := range 3 {
		mustPut(t, old, 1, uint64(i+1), "k", "v")
	}
	c.awaitAllApplied()

	c.isolate(oldID)
	proposeAddNode4(t, c, old)

	ctx, cancel := testContext(t)
	defer cancel()
	if err := old.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	mid = c.awaitLeaderOtherThan(oldID)
	mustPut(t, mid, 2, 1, "after", "overwrite")

	c.heal()
	midID := mid.Status().ID
	c.eventually("the old leader to follow the intermediate leader and drop Add 4", func() bool {
		s, m := old.Status(), mid.Status()
		return s.State == raft.Follower && s.Leader == midID && s.Applied >= m.Applied
	})

	for _, n := range c.running() {
		if id := n.Status().ID; id != oldID && id != midID {
			third = n
		}
	}
	return old, mid, third
}

// Test 2: the overwrite reaches the old leader, which then restarts.
//
// Only ConfState (and the acknowledgements it implies, see assertPlainThree) is
// asserted here. How a given configuration elects and commits is pinned at the
// raft level: TestCommitDuringJointNeedsBothMajorities,
// TestVoteDuringJointNeedsBothMajorities and TestConfigAfterCompactionAcrossRestart.
func TestRestartAfterTruncatedConfigChangeIsNotJoint(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	old, _, _ := badSnapshotScenario(t, c)
	oldID := old.Status().ID

	c.stop(oldID)
	c.start(oldID)

	assertPlainThree(t, "after restart the old leader", c.node(oldID).Status().Members)
}

// Test 4b: the node that wrote the snapshot becomes leader again and sends it
// to a follower that is behind the compaction point.
//
// Model: the lagging follower is a node whose data directory was emptied and
// which was then restarted (os.RemoveAll on its directory, then start). It has no
// log and no snapshot, so the leader can only catch it up with its snapshot. This
// is a test device, not a production scenario: the node forgets its vote.
//
// Only ConfState is asserted (see test 2 for where quorum behaviour is covered).
func TestSnapshotAdoptedFromTheWriterCarriesTheSettledConfig(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	old, mid, third := badSnapshotScenario(t, c)
	oldID, midID, thirdID := old.Status().ID, mid.Status().ID, third.Status().ID

	c.stop(thirdID)
	if err := os.RemoveAll(c.dirs[thirdID]); err != nil {
		t.Fatalf("emptying node %d: %v", thirdID, err)
	}
	c.isolate(midID)
	c.start(thirdID)

	leader := c.awaitLeaderOtherThan(midID)
	if got := leader.Status().ID; got != oldID {
		t.Fatalf("node %d became leader, want node %d (the only up-to-date node besides the isolated one)", got, oldID)
	}

	rejoined := c.node(thirdID)
	c.eventually("the emptied node to adopt the leader's snapshot", func() bool {
		return rejoined.Status().SnapshotsReceived > 0
	})
	assertPlainThree(t, "node "+itoa(thirdID)+" after adopting the snapshot written by node "+itoa(oldID),
		rejoined.Status().Members)

	mustPut(t, leader, 3, 1, "final", "ok")
}

func itoa[T ~uint64](v T) string { return strconv.FormatUint(uint64(v), 10) }

// ---- test 3: an entry above the snapshot survives and commits ----

// stripTransport rewrites each message before it is sent. It lets a test keep a
// Leave away from followers while they still learn the commit index.
type stripTransport struct {
	inner   Transport
	rewrite func(raft.Message) raft.Message
}

func (d *stripTransport) Send(msgs []raft.Message) {
	out := make([]raft.Message, len(msgs))
	for i, m := range msgs {
		out[i] = d.rewrite(m)
	}
	d.inner.Send(out)
}

func isLeaveEntry(e raft.Entry) bool {
	if e.Type != raft.EntryConfChange {
		return false
	}
	cc, err := raft.DecodeConfChange(e.Data)
	return err == nil && cc.Type == raft.ConfChangeLeaveJoint
}

// withoutLeave cuts an append just before its first Leave entry; the commit
// index it carries is kept, so the receiver still learns what committed.
func withoutLeave(m raft.Message) raft.Message {
	if m.Type != raft.MsgAppendRequest {
		return m
	}
	if i := slices.IndexFunc(m.Entries, isLeaveEntry); i >= 0 {
		m.Entries = m.Entries[:i]
	}
	return m
}

// leaves returns the automatic Leave entries in n's log above index after.
func leaves(t *testing.T, n *Node, after raft.Index) []raft.Entry {
	t.Helper()
	lo := max(after+1, n.storage.FirstIndex())
	hi := n.storage.LastIndex()
	if lo > hi {
		return nil
	}
	entries, err := n.storage.Entries(lo, hi+1)
	if err != nil {
		t.Fatalf("reading node %d log (%d,%d]: %v", n.Status().ID, lo-1, hi, err)
	}
	var out []raft.Entry
	for _, e := range entries {
		if isLeaveEntry(e) {
			out = append(out, e)
		}
	}
	return out
}

func confEntryIndex(t *testing.T, n *Node, typ raft.ConfChangeType) raft.Index {
	t.Helper()
	entries, err := n.storage.Entries(n.storage.FirstIndex(), n.storage.LastIndex()+1)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	for _, e := range entries {
		if e.Type != raft.EntryConfChange {
			continue
		}
		if cc, err := raft.DecodeConfChange(e.Data); err == nil && cc.Type == typ {
			return e.Index
		}
	}
	t.Fatalf("node %d has no configuration entry of type %v", n.Status().ID, typ)
	return 0
}

// addNode4 runs AddNode(4) in the background; wait returns its result.
func addNode4(t *testing.T, n *Node) (wait func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	var result error
	go func() {
		defer close(finished)
		result = n.AddNode(ctx, 4, "host:4")
	}()
	t.Cleanup(func() { cancel(); <-finished })
	return func() error {
		select {
		case <-finished:
			return result
		case <-time.After(settleTimeout):
			t.Fatalf("AddNode(4) did not return within %v", settleTimeout)
			return nil
		}
	}
}

func settledWithFour(cs raft.ConfState) bool {
	return !cs.Joint && slices.Equal(cs.Voters, []raft.NodeID{1, 2, 3, 4})
}

// 3a: the Leave is already in the log when the node restarts. Replaying the
// entry above a snapshot must not produce a second Leave.
func TestRestartWithLeaveAlreadyAppendedAddsNoSecondLeave(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	leader := c.awaitLeader()
	lid := leader.Status().ID
	mustPut(t, leader, 1, 1, "k", "v")
	c.awaitAllApplied()

	c.isolate(lid)
	wait := addNode4(t, leader)
	c.eventually("the cut-off leader to hold Add 4", func() bool { return leader.Status().Members.Joint })

	ctx, cancel := testContext(t)
	defer cancel()
	if err := leader.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	_, at := storedConf(t, leader)

	c.heal()
	if err := wait(); err != nil {
		t.Fatalf("AddNode(4) after the partition healed: %v", err)
	}
	c.eventually("the transition to settle on {1,2,3,4}", func() bool {
		return settledWithFour(leader.Status().Members) && len(leaves(t, leader, at)) == 1
	})
	before := leaves(t, leader, at)
	if len(before) != 1 {
		t.Fatalf("setup: %d Leave entries above the snapshot before the restart, want 1", len(before))
	}
	c.awaitAllApplied()

	c.stop(lid)
	c.start(lid)
	lead := c.awaitLeader()

	c.eventually("every node to settle on {1,2,3,4} after the restart", func() bool {
		for _, n := range c.running() {
			if !settledWithFour(n.Status().Members) {
				return false
			}
		}
		return true
	})
	c.awaitAllApplied()
	for _, n := range c.running() {
		got := leaves(t, n, at)
		if len(got) != 1 || got[0].Index != before[0].Index {
			t.Fatalf("node %d has Leave entries %v above the snapshot, want exactly the one at index %d (leader is node %d)",
				n.Status().ID, indexes(got), before[0].Index, lead.Status().ID)
		}
	}
}

func indexes(es []raft.Entry) []raft.Index {
	out := make([]raft.Index, len(es))
	for i, e := range es {
		out[i] = e.Index
	}
	return out
}

// 3b: a follower that compacted while a change was pending restarts holding the
// change committed but no Leave, and is the node that has to generate it.
//
// The change is the leader removing itself, not Add 4: the joint phase of an
// Add 4 needs three of {1,2,3,4}, and node 4 does not exist, so once the old
// leader is cut off nobody could be elected. Removing the leader leaves {F,A},
// which satisfies both majorities of {1,2,3}/{F,A} on its own.
func TestRestartedFollowerGeneratesTheOneMissingLeave(t *testing.T) {
	var holdLeave atomic.Bool
	var fastID, idleID atomic.Uint64
	c := newTunedTestCluster(t, 3, func(cfg *Config) {
		cfg.ElectionTick = 100
		// Once restarted, the follower under test times out first and the other
		// follower practically never, so the follower under test is the one that
		// campaigns and generates the Leave.
		switch uint64(cfg.ID) {
		case fastID.Load():
			cfg.ElectionTick = 10
		case idleID.Load():
			cfg.ElectionTick = 2000
		}
		cfg.Transport = &stripTransport{inner: cfg.Transport, rewrite: func(m raft.Message) raft.Message {
			if holdLeave.Load() {
				return withoutLeave(m)
			}
			return m
		}}
	})

	leader := c.awaitLeader()
	lid := leader.Status().ID
	mustPut(t, leader, 1, 1, "k", "v")
	c.awaitAllApplied()

	var follower, other *Node
	for _, n := range c.running() {
		switch {
		case n == leader:
		case follower == nil:
			follower = n
		default:
			other = n
		}
	}
	fid, oid := follower.Status().ID, other.Status().ID

	// As in 4a: the follower holds the change uncommitted and compacts.
	holdLeave.Store(true)
	c.block(fid, lid)
	c.block(lid, oid)
	c.block(oid, lid)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = leader.RemoveNode(ctx, lid)
	}()
	t.Cleanup(func() { cancel(); <-finished })

	c.eventually("the follower to hold the removal without committing it", func() bool {
		s := follower.Status()
		return s.Members.Joint && s.Commit == s.Applied
	})
	cctx, ccancel := testContext(t)
	defer ccancel()
	if err := follower.Compact(cctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	_, at := storedConf(t, follower)
	oldTerm := leader.Status().Term

	// The removal commits; the old leader's Leave never reaches a follower.
	c.heal()
	remove := confEntryIndex(t, leader, raft.ConfChangeRemoveNode)
	c.eventually("both followers to commit the removal", func() bool {
		return follower.Status().Commit >= remove && other.Status().Commit >= remove
	})
	if got := leaves(t, follower, at); len(got) != 0 {
		t.Fatalf("setup: the follower already holds Leave entries %v", indexes(got))
	}

	// Cut the old leader off for good and restart both followers (the other one
	// only to take it out of the race; it has no snapshot and a complete log).
	c.isolate(lid)
	fastID.Store(uint64(fid))
	idleID.Store(uint64(oid))
	c.stop(fid)
	c.stop(oid)
	c.start(fid)
	c.start(oid)
	holdLeave.Store(false)

	lead := c.awaitLeaderOtherThan(lid)
	t.Logf("restarted follower %d, other %d, new leader %d", fid, oid, lead.Status().ID)
	wantVoters := []raft.NodeID{min(fid, oid), max(fid, oid)}
	settled := func(cs raft.ConfState) bool { return !cs.Joint && slices.Equal(cs.Voters, wantVoters) }
	c.eventually("the transition to complete on the surviving nodes", func() bool {
		return settled(c.node(fid).Status().Members) && settled(c.node(oid).Status().Members) &&
			c.node(fid).Status().Applied == c.node(oid).Status().Applied
	})

	var seen raft.Index
	for _, id := range []raft.NodeID{fid, oid} {
		got := leaves(t, c.node(id), at)
		if len(got) != 1 {
			t.Fatalf("node %d has Leave entries %v above the snapshot, want exactly one (leader is node %d)",
				id, indexes(got), lead.Status().ID)
		}
		if got[0].Term <= oldTerm {
			t.Fatalf("node %d's Leave at index %d has term %d, want a term after the old leader's %d: it must be newly generated",
				id, got[0].Index, got[0].Term, oldTerm)
		}
		if seen != 0 && got[0].Index != seen {
			t.Fatalf("the surviving nodes disagree on the Leave: index %d vs %d", seen, got[0].Index)
		}
		seen = got[0].Index
	}
}
