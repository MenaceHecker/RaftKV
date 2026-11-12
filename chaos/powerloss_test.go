package chaos

import (
	"errors"
	"math/rand"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

func powerCluster(t *testing.T, sync storage.SyncPolicy) *Cluster {
	t.Helper()
	return newTestCluster(t, Config{
		Nodes:   3,
		Seed:    4,
		DataDir: "/raft",
		Sync:    sync,
		FS:      faultfs.New(rand.New(rand.NewSource(4)), faultfs.Options{}), //nolint:gosec // deterministic simulation, not security
	})
}

func powerLossThenRead(t *testing.T, c *Cluster) *Op {
	t.Helper()

	awaitLeader(t, c)
	if op := writeAndSettle(t, c, 1, "k", "acked"); op.Status != StatusOK {
		t.Fatalf("the write was not acknowledged: %s\n%s", op.Status, c.Dump())
	}

	if err := c.PowerLoss(); err != nil {
		t.Fatalf("PowerLoss: %v", err)
	}
	for _, id := range c.IDs() {
		if !c.IsDown(id) {
			t.Fatalf("node %d is still up after a power loss", id)
		}
	}
	for _, id := range c.IDs() {
		if err := c.Restart(id); err != nil {
			t.Fatalf("restarting node %d: %v", id, err)
		}
	}

	awaitLeader(t, c)
	tick(t, c, 40)
	op, err := ReadAndSettle(c, 2, "k", 200)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func TestPowerLossKeepsWhatWasSynced(t *testing.T) {
	op := powerLossThenRead(t, powerCluster(t, storage.SyncAlways))

	if op.Status != StatusOK || !op.Found || op.Value != "acked" {
		t.Fatalf("an acknowledged write did not survive a power loss with fsync on: status=%s found=%v value=%q",
			op.Status, op.Found, op.Value)
	}
}

func TestPowerLossDropsWhatWasNeverSynced(t *testing.T) {
	op := powerLossThenRead(t, powerCluster(t, storage.SyncNever))

	if op.Status != StatusOK {
		t.Fatalf("the read after restart did not complete: %s", op.Status)
	}
	if op.Found {
		t.Fatalf("a write that was never synced survived a power loss (read %q), so PowerLoss lost nothing", op.Value)
	}
}

func TestPowerLossNeedsAFilesystem(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 1})
	err := c.PowerLoss()
	if err == nil {
		t.Fatal("PowerLoss succeeded on a cluster with no faultfs filesystem, so nothing was lost")
	}
	if !strings.Contains(err.Error(), "Config.FS") {
		t.Fatalf("PowerLoss failed, but not for the missing filesystem: %v", err)
	}
}

func TestPowerLossMakesPendingOperationsUnknown(t *testing.T) {
	c := powerCluster(t, storage.SyncAlways)
	awaitLeader(t, c)

	op := c.Write(1, "k", "in flight")
	if op.Status != StatusPending {
		t.Fatalf("the write was %s, want pending", op.Status)
	}
	if err := c.PowerLoss(); err != nil {
		t.Fatal(err)
	}
	if op.Status != StatusUnknown {
		t.Fatalf("a write pending at a power loss is %s, want unknown", op.Status)
	}
}

func TestAwaitReadableReturnsOnAHealthyCluster(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 3})
	awaitLeader(t, c)

	if err := c.AwaitReadable(200); err != nil {
		t.Fatalf("a healthy cluster was not readable: %v", err)
	}
}

func TestAwaitReadableFailsWithTheSentinelWhenNoReadIsServed(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 3})
	leader := awaitLeader(t, c)
	tick(t, c, 20)

	c.Network().Partition([]raft.NodeID{leader}, MajorityWithout(c, leader))
	c.Crash(OtherThan(c, leader))
	for _, id := range MajorityWithout(c, leader) {
		c.Crash(id)
	}

	err := c.AwaitReadable(60)
	if !errors.Is(err, ErrNotReadable) {
		t.Fatalf("AwaitReadable on a cluster that cannot serve reads returned %v, want ErrNotReadable", err)
	}
}

func TestScenarioTuneAdjustsTheClusterConfiguration(t *testing.T) {
	var seen Config
	s := Scenario{
		Name:  "tuned",
		Nodes: 3,
		Tune:  func(cfg *Config) { cfg.MaxAppendBytes = 1 },
		Run: func(c *Cluster) error {
			seen = c.cfg
			return nil
		},
	}
	RunScenario(s, []int64{9})

	if seen.MaxAppendBytes != 1 {
		t.Fatalf("the scenario ran with MaxAppendBytes %d, want the tuned 1", seen.MaxAppendBytes)
	}
	if seen.Seed != 9 || seen.Nodes != 3 {
		t.Fatalf("Tune lost the run's own settings: seed %d, nodes %d", seen.Seed, seen.Nodes)
	}
}

func TestDropInFlightFromDiscardsOnlyThatNodesMessages(t *testing.T) {
	n, err := NewNetwork(1, Faults{MinDelay: 5, MaxDelay: 5})
	if err != nil {
		t.Fatal(err)
	}
	n.Send([]raft.Message{
		{From: 1, To: 2}, {From: 1, To: 3}, {From: 2, To: 1}, {From: 3, To: 1},
	})
	if n.InFlight() != 4 {
		t.Fatalf("setup queued %d messages, want 4", n.InFlight())
	}

	n.DropInFlightFrom(1)
	if n.InFlight() != 2 {
		t.Fatalf("%d messages remain, want the 2 not sent by node 1", n.InFlight())
	}
	if n.Stats().Dropped != 2 {
		t.Fatalf("%d messages counted as dropped, want 2", n.Stats().Dropped)
	}
}

func TestPausedNodeKeepsItsStateAndMissesEverythingElse(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 3})
	leader := awaitLeader(t, c)
	tick(t, c, 20)

	follower := OtherThan(c, leader)
	term := c.nodes[follower].Term()
	c.Pause(follower)

	// Far longer than an election timeout: a running follower that heard nothing
	// would have campaigned, but a frozen one has no clock.
	c.Network().Partition([]raft.NodeID{follower}, MajorityWithout(c, follower))
	tick(t, c, 100)
	if got := c.nodes[follower].Term(); got != term {
		t.Fatalf("a paused node's term moved from %d to %d, so its clock ran", term, got)
	}
	if c.nodes[follower].State() != raft.Follower {
		t.Fatalf("a paused node changed state to %s", c.nodes[follower].State())
	}

	c.Resume(follower)
	c.Network().Heal()
	tick(t, c, 60)
	if got := c.nodes[follower].Leader(); got != leader {
		t.Fatalf("a resumed node follows %d, want the leader %d\n%s", got, leader, c.Dump())
	}
}

func TestCrashClearsAPause(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 3})
	leader := awaitLeader(t, c)
	follower := OtherThan(c, leader)

	c.Pause(follower)
	c.Crash(follower)
	if err := c.Restart(follower); err != nil {
		t.Fatal(err)
	}
	tick(t, c, 60)
	if c.paused[follower] {
		t.Fatal("a node that crashed and restarted is still paused")
	}
	if got := c.nodes[follower].Leader(); got != leader {
		t.Fatalf("the restarted node follows %d, want %d", got, leader)
	}
}
