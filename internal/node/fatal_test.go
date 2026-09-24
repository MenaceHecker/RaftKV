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

var errDiskGone = errors.New("the disk is gone")

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

type silentTransport struct{}

func (silentTransport) Send([]raft.Message) {}

func TestTheLoopStopsWhenTheDiskDies(t *testing.T) {
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

	if err := n.storage.Close(); err != nil {
		t.Fatalf("closing storage: %v", err)
	}

	n.Step(raft.Message{Type: raft.MsgVoteRequest, From: 2, To: 1, Term: 1})

	select {
	case <-n.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the node is still running although it can no longer write anything")
	}
	if !n.Stopped() {
		t.Error("the node does not report itself stopped, so nothing above it can tell")
	}

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
