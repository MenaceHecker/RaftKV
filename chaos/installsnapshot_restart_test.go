package chaos

// T3.11 test 6: a follower whose log diverged takes a snapshot that mismatches its
// entry at the snapshot index, is restarted before any leader repairs its suffix,
// and the cluster must still converge and stay linearizable. Real DiskStorage,
// SyncAlways, seeds 1-20. The restart here is a quiesced Crash (storage closed,
// all traffic to the node held), not a crash at an arbitrary moment: the
// crash-timing tests are on faultfs in internal/storage and internal/raft.

import (
	"fmt"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

func TestFollowerRestartAfterInstallSnapshotConverges(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) { followerRestartAfterInstall(t, seed) })
	}
}

func followerRestartAfterInstall(t *testing.T, seed int64) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: seed, DataDir: t.TempDir(), Sync: storage.SyncAlways})

	// F is the leader that gets cut off and keeps an uncommitted tail of an old term.
	f := awaitLeader(t, c)
	if err := Workload(c, 2, "k", "base", 3, 400); err != nil {
		t.Fatal(err)
	}
	majority := MajorityWithout(c, f)
	c.Network().Partition([]raft.NodeID{f}, majority)

	baseIdx := c.nodes[f].LastIndex()
	for i := range 5 {
		if op := c.WriteTo(9, f, fmt.Sprintf("stale%d", i), "v"); op.Status != StatusPending {
			t.Fatalf("the isolated leader refused stale write %d (%s)", i, op.Status)
		}
	}
	tailLast := c.nodes[f].LastIndex()
	staleTerm := c.nodes[f].Term()

	// The majority elects a newer leader and commits something else, then compacts.
	var newer raft.NodeID
	for range 400 {
		tick(t, c, 1)
		for _, id := range majority {
			if n := c.nodes[id]; n.State() == raft.Leader && n.Term() > staleTerm {
				newer = id
			}
		}
		if newer != 0 {
			break
		}
	}
	if newer == 0 {
		t.Fatalf("the majority never elected a leader above term %d\n%s", staleTerm, c.Dump())
	}
	if op, err := WriteAndSettle(c, 3, "fresh", "v1", 400); err != nil || op.Status != StatusOK {
		t.Fatalf("write through the new leader: status %v err %v", op.Status, err)
	}
	for _, id := range majority {
		if err := c.Compact(id); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := c.storages[newer].Snapshot()
	if err != nil {
		t.Fatalf("the new leader has no snapshot: %v", err)
	}
	if snap.Index <= baseIdx || snap.Index > tailLast {
		t.Fatalf("snapshot at %d is outside F's stale tail (%d, %d]: the case would not mismatch", snap.Index, baseIdx, tailLast)
	}
	if term, err := c.nodes[f].TermAt(snap.Index); err != nil || term == snap.Term {
		t.Fatalf("precondition: F's term at %d is %d (err %v), snapshot term %d; the install must be a mismatch",
			snap.Index, term, err, snap.Term)
	}

	// Heal until F takes the snapshot, then hold all traffic and restart F before
	// anyone can repair its suffix.
	c.Network().Heal()
	for range 300 {
		if c.SnapshotsInstalled(f) > 0 {
			break
		}
		tick(t, c, 1)
	}
	if c.SnapshotsInstalled(f) == 0 {
		t.Fatalf("F never received a snapshot\n%s", c.Dump())
	}
	c.Crash(f)
	c.Network().Partition([]raft.NodeID{f}, majority)
	if err := c.Restart(f); err != nil {
		t.Fatalf("restarting F: %v", err)
	}

	n := c.nodes[f]
	switch last := n.LastIndex(); last {
	case tailLast:
		t.Logf("seed %d: F recovered snapshot (%d,%d) plus its stale tail through %d", seed, snap.Index, snap.Term, last)
		for i := snap.Index + 1; i <= last; i++ {
			if term, _ := n.TermAt(i); term != staleTerm {
				t.Fatalf("recovered tail entry %d has term %d, want the stale term %d", i, term, staleTerm)
			}
		}
	case snap.Index:
		t.Logf("seed %d: F recovered the snapshot (%d,%d) with an empty tail", seed, snap.Index, snap.Term)
	default:
		t.Fatalf("F recovered last index %d: neither the stale tail (%d) nor the snapshot (%d); a leader repaired it first",
			last, tailLast, snap.Index)
	}
	if n.CommitIndex() > snap.Index {
		t.Fatalf("F recovered commit index %d above the snapshot %d", n.CommitIndex(), snap.Index)
	}

	// Release traffic and run a workload.
	c.Network().Heal()
	tick(t, c, 100)
	if err := Workload(c, 3, "x", "after", 3, 500); err != nil {
		t.Fatalf("workload after the restart: %v\n%s", err, c.Dump())
	}
	tick(t, c, 100)
	c.FailPending()

	ok, err := c.Converged()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("replicas did not converge\n%s", c.Dump())
	}
	if res := Check(c.History()); !res.OK() {
		t.Fatalf("history is not linearizable: %s", res)
	}
}
