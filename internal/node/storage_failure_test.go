package node

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// T3.9: a failed WAL write or sync, and a failed snapshot or compaction, stop
// the node. The faults are one-shot (Rule.Nth): the next call would succeed,
// so only the node's own refusal to go on can stop it.

// faultNode starts a one-node cluster on a faultfs and returns its node once
// it leads. A snapshot is taken every threshold applied entries; sync is the
// WAL policy.
func faultNode(t *testing.T, threshold uint64, sync storage.SyncPolicy) (*Node, *faultfs.FS) {
	t.Helper()
	// A seeded generator is the point: the same seed must give the same run.
	f := faultfs.New(rand.New(rand.NewSource(1)), faultfs.Options{}) //nolint:gosec // deterministic simulation, not security
	if err := f.MkdirAll("/data", 0o755); err != nil {
		t.Fatalf("creating the data directory: %v", err)
	}
	for _, d := range []string{"/data", "/"} {
		if err := f.SyncDir(d); err != nil {
			t.Fatalf("syncing %s: %v", d, err)
		}
	}
	c := newTunedTestCluster(t, 1, func(cfg *Config) {
		cfg.FS = f
		cfg.DataDir = "/data"
		cfg.SnapshotThreshold = threshold
		cfg.Sync = sync
	})
	return c.awaitLeader(), f
}

// putUntilApplied proposes one entry at a time until the node has applied
// exactly target entries. The seq counts from the entries already applied.
func putUntilApplied(t *testing.T, n *Node, target raft.Index) {
	t.Helper()
	for {
		applied := n.Status().Applied
		if applied == target {
			return
		}
		if applied > target {
			t.Fatalf("applied %d entries, past the target %d", applied, target)
		}
		mustPut(t, n, 1, uint64(applied)+1, "k", "v")
	}
}

func awaitDone(t *testing.T, n *Node, what string) {
	t.Helper()
	select {
	case <-n.Done():
	case <-time.After(waitLimit):
		t.Fatalf("the node kept running after %s", what)
	}
}

// requireStopped checks what a stopped node promises: Stop returns instead of
// hanging, and no later proposal is acknowledged.
func requireStopped(t *testing.T, n *Node) {
	t.Helper()
	stopped := make(chan struct{})
	go func() { _ = n.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(waitLimit):
		t.Fatal("Stop hung on a failed node")
	}
	if err := put(t, n, 2, 1, "after", "failure"); err == nil {
		t.Fatal("a proposal was acknowledged after the node had stopped")
	}
}

// TestNodeStopsWhenTheWALFails is a control: a failed WAL append was already
// fatal through raft.ErrStorage and stays so.
func TestNodeStopsWhenTheWALFails(t *testing.T) {
	n, f := faultNode(t, 1<<40, storage.SyncAlways)
	mustPut(t, n, 1, 1, "before", "failure")

	f.Inject(faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrSync})
	if err := put(t, n, 1, 2, "k", "lost"); err == nil {
		t.Fatal("a proposal was acknowledged although its WAL fsync failed")
	}
	awaitDone(t, n, "a failed WAL fsync")
	requireStopped(t, n)
}

func TestFailedSnapshotWriteStopsTheNode(t *testing.T) {
	const threshold = 4
	n, f := faultNode(t, threshold, storage.SyncNever)
	putUntilApplied(t, n, threshold-1)
	if got := n.storage.SnapshotMeta().Index; got != 0 {
		t.Fatalf("a snapshot at %d was taken before the threshold", got)
	}

	// The next proposal is one WAL write (its entry); the snapshot it triggers
	// writes the snapshot record second.
	f.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 2, Err: faultfs.ErrNoSpace})
	if err := put(t, n, 1, threshold, "k", "v"); err != nil {
		t.Fatalf("the proposal itself was refused, so the fault did not hit the snapshot: %v", err)
	}
	awaitDone(t, n, "the snapshot's WAL write failed")
	requireStopped(t, n)
}

func TestFailedCompactStopsTheNode(t *testing.T) {
	n, f := faultNode(t, 1<<40, storage.SyncNever)
	mustPut(t, n, 1, 1, "k", "v")

	f.Inject(faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 1, Err: faultfs.ErrNoSpace})
	ctx, cancel := testContext(t)
	defer cancel()
	if err := n.Compact(ctx); !errors.Is(err, faultfs.ErrNoSpace) {
		t.Fatalf("Compact returned %v, want the injected ENOSPC", err)
	}
	awaitDone(t, n, "a failed Compact")
	requireStopped(t, n)
}

func TestSnapshotCleanupFailuresAreReportedAndStopTheNode(t *testing.T) {
	const threshold = 3 // snapshots at 3, 6, 9; the one at 12 purges the oldest
	cases := []struct {
		name string
		rule faultfs.Rule
	}{
		// One WAL write for the entry, one for the snapshot record, then
		// TruncateBefore's hard state record.
		{"TruncateBefore, hard state write", faultfs.Rule{Op: faultfs.OpWrite, PathGlob: "*.wal", Nth: 3, Err: faultfs.ErrNoSpace}},
		{"purge, removing an old snapshot", faultfs.Rule{Op: faultfs.OpRemove, PathGlob: "*.snap", Nth: 1, Err: faultfs.ErrRename}},
		// The first directory fsync of snap/ publishes the new snapshot.
		{"purge, directory fsync", faultfs.Rule{Op: faultfs.OpSyncDir, PathGlob: "snap", Nth: 2, Err: faultfs.ErrSyncDir}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, f := faultNode(t, threshold, storage.SyncNever)
			putUntilApplied(t, n, 4*threshold-1)
			if got := n.storage.SnapshotMeta().Index; got != 3*threshold {
				t.Fatalf("the newest snapshot is at %d, want %d", got, 3*threshold)
			}

			f.Inject(tc.rule)
			if err := put(t, n, 1, 4*threshold, "k", "v"); err != nil {
				t.Fatalf("the proposal itself was refused, so the fault did not hit the snapshot: %v", err)
			}
			awaitDone(t, n, tc.name)
			requireStopped(t, n)
		})
	}
}
