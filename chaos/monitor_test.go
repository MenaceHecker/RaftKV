package chaos

import (
	"errors"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func healthyCommittedCluster(t *testing.T) (*Cluster, raft.NodeID) {
	t.Helper()
	c := newTestCluster(t, Config{Nodes: 3, Seed: 2})
	leader := awaitLeader(t, c)
	for i := range 3 {
		op := writeAndSettle(t, c, 1, "x", string(rune('a'+i)))
		if op.Status != StatusOK {
			t.Fatalf("write %d did not commit: %s\n%s", i, op.Status, c.Dump())
		}
	}
	tick(t, c, 30)
	return c, leader
}

func TestCommittedLogMonitorAcceptsHealthyRuns(t *testing.T) {
	c, _ := healthyCommittedCluster(t)

	if len(c.mon.terms) == 0 {
		t.Fatal("the monitor recorded no committed entry, so it checked nothing")
	}
	if err := c.checkCommittedLog(); err != nil {
		t.Fatalf("a healthy cluster was flagged: %v", err)
	}
}

func TestCommittedLogMonitorFlagsADifferentTermAtACommittedIndex(t *testing.T) {
	c, leader := healthyCommittedCluster(t)

	victim := OtherThan(c, leader)
	idx := c.nodes[victim].CommitIndex()
	term, err := c.nodes[victim].TermAt(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.storages[victim].Append([]raft.Entry{{Index: idx, Term: term + 40, Type: raft.EntryNoOp}}); err != nil {
		t.Fatalf("setting up the divergence: %v", err)
	}

	err = c.checkCommittedLog()
	if !errors.Is(err, ErrCommittedLog) {
		t.Fatalf("a different term at committed index %d was not flagged: %v", idx, err)
	}
	if !strings.Contains(err.Error(), "index") {
		t.Errorf("the error does not name the index: %v", err)
	}
}

func TestCommittedLogMonitorFlagsApplyingPastCommit(t *testing.T) {
	c, leader := healthyCommittedCluster(t)

	follower := OtherThan(c, leader)
	next := c.machines[follower].Applied() + 1
	if err := c.machines[follower].Apply(raft.Entry{Index: next, Term: 1, Type: raft.EntryNoOp}); err != nil {
		t.Fatalf("setting up the over-application: %v", err)
	}
	if c.machines[follower].Applied() <= c.nodes[follower].CommitIndex() {
		t.Fatal("the setup did not apply past the commit index")
	}

	err := c.checkCommittedLog()
	if !errors.Is(err, ErrCommittedLog) {
		t.Fatalf("applying past the commit index was not flagged: %v", err)
	}
	if !strings.Contains(err.Error(), "applied") {
		t.Errorf("the error does not say what happened: %v", err)
	}
}

func TestCommittedLogMonitorFlagsALeaderThatLacksACommittedEntry(t *testing.T) {
	c, leader := healthyCommittedCluster(t)

	// An entry some earlier leader's quorum committed, which this leader never saw.
	missing := c.nodes[leader].LastIndex() + 5
	c.mon.terms[missing] = c.nodes[leader].Term() - 1
	c.mon.top = missing

	err := c.checkCommittedLog()
	if !errors.Is(err, ErrCommittedLog) {
		t.Fatalf("a leader missing committed entry %d was not flagged: %v", missing, err)
	}
	if !strings.Contains(err.Error(), "leader") {
		t.Errorf("the error does not name the leader: %v", err)
	}
}

func TestCommittedLogMonitorSurvivesARestartThatRewindsACommitIndex(t *testing.T) {
	c, leader := healthyCommittedCluster(t)

	follower := OtherThan(c, leader)
	c.Crash(follower)
	if err := c.Restart(follower); err != nil {
		t.Fatal(err)
	}
	tick(t, c, 60)

	if err := c.checkCommittedLog(); err != nil {
		t.Fatalf("a restarted follower was flagged: %v", err)
	}
}
