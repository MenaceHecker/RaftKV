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

	if got := c.nodes[victim].Status().SnapshotsReceived; got != 0 {
		t.Fatalf("node %d received %d snapshots, so this test is not exercising "+
			"log replication", victim, got)
	}
}

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
