package node

import (
	"context"
	"os"
	"slices"
	"strconv"
	"testing"

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

func assertPlainThree(t *testing.T, what string, cs raft.ConfState) {
	t.Helper()
	if cs.Joint || !slices.Equal(cs.Voters, []raft.NodeID{1, 2, 3}) {
		t.Fatalf("%s: configuration is %s, want voters=[1 2 3]", what, cs)
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
func TestRestartAfterTruncatedConfigChangeIsNotJoint(t *testing.T) {
	c := newTunedTestCluster(t, 3, slowElections)
	old, _, _ := badSnapshotScenario(t, c)
	oldID := old.Status().ID

	c.stop(oldID)
	c.start(oldID)

	assertPlainThree(t, "after restart the old leader", c.node(oldID).Status().Members)
}

// Test 4b: the node that wrote the snapshot becomes leader again and sends it
// to a follower that is behind the compaction point (modelled as an emptied
// node, which has no log at all).
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
