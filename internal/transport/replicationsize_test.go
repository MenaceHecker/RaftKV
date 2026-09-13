package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

// Tests for replication traffic that grows with the data.
//
// A snapshot is the obvious message whose size follows the data, and it now
// has its own streamed path. It is not the only one. An AppendEntries carries
// however many entries a follower is missing, and a follower can be missing
// an unbounded number of them, so ordinary replication has exactly the same
// failure mode: it works in every test where the data is small and stops
// working once it is not.

// TestFollowerCatchesUpFromALargeBacklog stops a follower, accumulates far
// more than one message worth of entries without compacting, and brings it
// back.
//
// Not compacting is the whole point. With a snapshot available the leader
// would send one and the streamed path would carry it. This forces catch-up
// through the log, which is what a follower that was only briefly away
// actually does.
func TestFollowerCatchesUpFromALargeBacklog(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	var victim raft.NodeID
	for _, id := range c.ids {
		if id != leaderID {
			victim = id
			break
		}
	}

	c.servers[victim].Stop()
	if err := c.nodes[victim].Stop(); err != nil {
		t.Fatalf("stopping node %d: %v", victim, err)
	}
	delete(c.nodes, victim)

	// About 8 MiB of entries, comfortably past a default gRPC message, and
	// deliberately never compacted.
	const (
		writes    = 64
		valueSize = 128 << 10
	)
	value := bytes.Repeat([]byte("x"), valueSize)
	for i := range writes {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("key-%d", i), Value: value,
		})
		if err != nil {
			t.Fatalf("writing while node %d is down: %v", victim, err)
		}
	}

	// Bring it back with its data directory intact, so it resumes from where
	// it left off and the leader has to ship the whole backlog.
	l, err := net.Listen("tcp", c.addrs[victim])
	if err != nil {
		t.Fatalf("re-binding node %d: %v", victim, err)
	}
	tr, err := NewPeerTransport(PeerConfig{Self: victim, Addresses: c.addrs})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	restarted, err := node.Start(node.Config{
		ID:            victim,
		Peers:         c.ids,
		DataDir:       c.dataDirs[victim],
		Transport:     tr,
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("restarting node %d: %v", victim, err)
	}
	c.nodes[victim] = restarted
	c.transports[victim] = tr
	t.Cleanup(func() { restarted.Stop() })
	tr.SetLocal(restarted)

	srv, err := NewRaftServer(restarted)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	c.servers[victim] = gs
	go gs.Serve(l)
	t.Cleanup(gs.Stop)

	want := leader.Status().Applied
	c.eventually("the restarted node to catch up from the log", func() bool {
		return c.nodes[victim].Status().Applied >= want
	})

	// It must have caught up from the log, not from an image. If a snapshot
	// were involved this would be re-testing the streamed path instead of
	// ordinary replication.
	if got := c.nodes[victim].Status().SnapshotsReceived; got != 0 {
		t.Fatalf("node %d received %d snapshots, so this test is not exercising "+
			"log replication", victim, got)
	}
}

// TestLargeConcurrentWritesReplicate covers the other way a single append
// grows: group commit puts everything that was waiting into one batch, and
// the leader then replicates that batch as one message.
func TestLargeConcurrentWritesReplicate(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	const (
		writers   = 48
		valueSize = 128 << 10
	)
	value := bytes.Repeat([]byte("x"), valueSize)

	errs := make(chan error, writers)
	gate := make(chan struct{})
	for i := range writers {
		go func(i int) {
			<-gate
			ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
			defer cancel()
			errs <- leader.Propose(ctx, statemachine.Command{
				ClientID: uint64(i + 1), Seq: 1, Op: statemachine.OpPut,
				Key: fmt.Sprintf("key-%d", i), Value: value,
			})
		}(i)
	}
	close(gate)

	for range writers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent large write: %v", err)
		}
	}

	want := leader.Status().Applied
	c.eventually("every node to apply the batch", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Status().Applied < want {
				return false
			}
		}
		return true
	})
}
