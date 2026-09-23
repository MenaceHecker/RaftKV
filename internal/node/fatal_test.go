package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Which failures from the consensus core stop this node.
//
// The loop steps every message the transport delivers, and until now it
// dropped anything that came back as an error, on the reasoning that a
// message which makes no sense is one message. That reasoning is right for a
// message and wrong for a disk. A vote, an append and a snapshot all reach
// the core through the same call, and all three write before they answer, so
// a node whose storage had failed went on running: answering nothing,
// recording nothing, and reporting itself healthy the entire time. The same
// failure arriving on a tick already stopped it.

var errDiskGone = errors.New("the disk is gone")

// deadStorage is a Storage whose hard state writes fail once armed.
type deadStorage struct {
	raft.Storage
	armed bool
}

func (d *deadStorage) SetHardState(hs raft.HardState) error {
	if d.armed {
		return errDiskGone
	}
	return d.Storage.SetHardState(hs)
}

// realStorageFailure returns the error the core actually produces when a
// write fails, rather than one built by hand to match.
func realStorageFailure(t *testing.T) error {
	t.Helper()

	st := &deadStorage{Storage: raft.NewMemoryStorage()}
	n, err := raft.NewNode(raft.Config{
		ID:            1,
		Peers:         []raft.NodeID{1, 2, 3},
		Storage:       st,
		ElectionTick:  10,
		HeartbeatTick: 1,
	})
	if err != nil {
		t.Fatalf("creating a core node: %v", err)
	}

	// Reach term 1 with a working disk, so the vote request below is not
	// turned away by the term rules before it gets near the write.
	if err := n.Step(raft.Message{
		Type: raft.MsgAppendRequest, From: 2, To: 1, Term: 1,
	}); err != nil {
		t.Fatalf("moving to term 1: %v", err)
	}
	st.armed = true

	err = n.Step(raft.Message{Type: raft.MsgVoteRequest, From: 2, To: 1, Term: 1})
	if err == nil {
		t.Fatal("the core accepted a vote it could not persist")
	}
	return err
}

func TestAStorageFailureFromTheCoreStopsThisNode(t *testing.T) {
	err := realStorageFailure(t)

	if !fatal(err) {
		t.Fatalf("the loop would carry on after %v, which means this node "+
			"keeps running with no way to record anything", err)
	}
}

func TestAnOrdinaryMessageErrorDoesNotStopThisNode(t *testing.T) {
	// If everything were fatal, one malformed message from a hostile or
	// buggy peer would take the node down, which is a worse trade than the
	// one being fixed.
	for _, err := range []error{
		errors.New("a message that made no sense"),
		errors.New("raft: node 1 received an append from 2 in its own leader term 3"),
	} {
		if fatal(err) {
			t.Errorf("the node would stop over %v", err)
		}
	}
}

func TestNoErrorIsNotAFailure(t *testing.T) {
	if fatal(nil) {
		t.Error("a nil error was treated as fatal")
	}
}

// silentTransport drops everything, so nothing arrives except what a test
// steps in by hand.
type silentTransport struct{}

func (silentTransport) Send([]raft.Message) {}

func TestTheLoopStopsWhenTheDiskDies(t *testing.T) {
	// The wiring, not the predicate. Removing the check would leave the
	// tests above green, because they only ask how an error is classified
	// and never whether anything acts on the answer.
	//
	// The tick interval is an hour so that nothing else can stop this node
	// while the test runs. A node whose election timer fired would try to
	// persist a new term and stop down the path that already existed, and
	// the test would pass with the fix removed.
	n, err := Start(Config{
		ID:            1,
		Peers:         []raft.NodeID{1, 2, 3},
		DataDir:       t.TempDir(),
		Transport:     silentTransport{},
		TickInterval:  time.Hour,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	// The disk goes away under a running node. Every write from here on
	// fails, which is what a failed device looks like from inside a process
	// that is otherwise fine.
	if err := n.storage.Close(); err != nil {
		t.Fatalf("closing storage: %v", err)
	}

	// A vote request in a later term. Answering it means recording the term
	// and the vote first, so this is a message that cannot be handled
	// without a write.
	n.Step(raft.Message{Type: raft.MsgVoteRequest, From: 2, To: 1, Term: 1})

	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the node is still running although it can no longer write anything")
	}
	if !n.Stopped() {
		t.Error("the node does not report itself stopped, so nothing above it can tell")
	}

	// And callers find out, rather than waiting on a node that will never
	// answer them.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	})
	if !errors.Is(err, ErrStopped) {
		t.Errorf("a write after the node stopped returned %v, want ErrStopped", err)
	}
}

func TestARunningNodeIsNotDone(t *testing.T) {
	// The other half of Done. If it were closed from the start, the process
	// watching it would exit immediately and every check on it would pass
	// for the wrong reason.
	n, err := Start(Config{
		ID:            1,
		Peers:         []raft.NodeID{1},
		DataDir:       t.TempDir(),
		Transport:     silentTransport{},
		TickInterval:  5 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	if n.Stopped() {
		t.Fatal("a node reports itself stopped immediately after starting")
	}
	select {
	case <-n.Done():
		t.Fatal("Done is already closed on a running node")
	default:
	}

	if err := n.Stop(); err != nil {
		t.Fatalf("stopping: %v", err)
	}
	if !n.Stopped() {
		t.Error("a stopped node does not report itself stopped")
	}
}

// newLeaderNode returns a single-voter node that has elected itself.
//
// One voter is deliberate. A leader of one is its own quorum, so the
// check-quorum path never makes it stand down and never persists a term
// behind the test's back. The only write left is the one the test asks for,
// which is what stops these tests passing for a reason they did not intend.
func newLeaderNode(t *testing.T) *Node {
	t.Helper()

	n, err := Start(Config{
		ID:            1,
		Peers:         []raft.NodeID{1},
		DataDir:       t.TempDir(),
		Transport:     silentTransport{},
		TickInterval:  5 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().State == raft.Leader {
			return n
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("the node never became leader")
	return nil
}

func TestALeaderThatCannotAppendStandsDown(t *testing.T) {
	// The worst version of the failure, because a leader holds the cluster.
	// It keeps heartbeating, so no follower campaigns; it cannot append, so
	// nothing commits. Telling the client its write failed leaves that in
	// place indefinitely. Stopping is what lets somebody else take over.
	n := newLeaderNode(t)

	if err := n.storage.Close(); err != nil {
		t.Fatalf("closing storage: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	})
	if err == nil {
		t.Fatal("a write succeeded on a node that cannot write")
	}

	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the leader is still leading although it cannot append anything")
	}
}

func TestAMembershipChangeThatCannotBeAppendedStopsTheNode(t *testing.T) {
	// Membership changes go into the log like anything else, so the same
	// reasoning applies: a leader that cannot write one cannot write
	// anything.
	n := newLeaderNode(t)

	if err := n.storage.Close(); err != nil {
		t.Fatalf("closing storage: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := n.AddNode(ctx, 2, "127.0.0.1:9002"); err == nil {
		t.Fatal("a membership change succeeded on a node that cannot write")
	}

	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the node is still running although it cannot append anything")
	}
}
