package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

func (c *grpcCluster) joinNode(id raft.NodeID) *node.Node {
	c.t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("listening for node %d: %v", id, err)
	}
	c.addrs[id] = l.Addr().String()

	members := append(append([]raft.NodeID{}, c.ids...), id)

	tr, err := NewPeerTransport(PeerConfig{Self: id, Addresses: c.addrs})
	if err != nil {
		c.t.Fatalf("creating transport for node %d: %v", id, err)
	}
	c.transports[id] = tr
	c.t.Cleanup(func() { tr.Close() })

	c.dataDirs[id] = filepath.Join(c.t.TempDir(), "joiner")
	n, err := node.Start(node.Config{
		ID:            id,
		Peers:         members,
		DataDir:       c.dataDirs[id],
		Transport:     tr,
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}
	c.nodes[id] = n
	c.t.Cleanup(func() { n.Stop() })
	tr.SetLocal(n)

	srv, err := NewRaftServer(n)
	if err != nil {
		c.t.Fatalf("creating server for node %d: %v", id, err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	c.servers[id] = gs
	go gs.Serve(l)
	c.t.Cleanup(gs.Stop)

	c.ids = append(c.ids, id)
	return n
}

func TestANodeAddedAtRuntimeReceivesTheLog(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "before", Value: []byte("joined"),
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the added node to catch up with the leader", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	c.eventually("the added node to agree about the membership", func() bool {
		return joiner.Status().Members.String() == leader.Status().Members.String()
	})

	var toJoiner uint64
	for _, st := range c.transports[leader.Status().ID].Stats() {
		if st.ID == 4 {
			toJoiner = st.Sent
		}
	}
	if toJoiner == 0 {
		t.Fatalf("the leader sent nothing to the added node\n%s", c.dump())
	}
}

func TestASingleNodeClusterCanGrow(t *testing.T) {
	c := newGRPCCluster(t, 1)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	joiner := c.joinNode(2)
	if err := leader.AddNode(ctx, 2, c.addrs[2]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the second node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "grown", Value: []byte("two"),
	}); err != nil {
		t.Fatalf("Propose after growing: %v", err)
	}
}

func TestARemovedNodeLosesItsLink(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tr := c.transports[leader.Status().ID]
	c.eventually("the leader to hold a link to the added node", func() bool {
		for _, st := range tr.Stats() {
			if st.ID == 4 {
				return true
			}
		}
		return false
	})

	if err := leader.RemoveNode(ctx, 4); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	c.eventually("the leader to drop the link to the removed node", func() bool {
		for _, st := range tr.Stats() {
			if st.ID == 4 {
				return false
			}
		}
		return true
	})
}

func TestAMemberAddedAfterCompactionCatchesUpBySnapshot(t *testing.T) {
	const (
		writes    = 32
		valueSize = 4 << 10
	)

	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	value := bytes.Repeat([]byte("x"), valueSize)
	for i := range writes {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("key-%d", i), Value: value,
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	for _, id := range c.ids {
		if err := c.nodes[id].Compact(ctx); err != nil {
			t.Fatalf("compacting node %d: %v", id, err)
		}
	}

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the added node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	if got := joiner.Status().SnapshotsReceived; got == 0 {
		t.Fatalf("the added node caught up without receiving a snapshot\n%s", c.dump())
	}
}

func (c *grpcCluster) restartStale(id raft.NodeID, staticPeers []raft.NodeID) *node.Node {
	c.t.Helper()

	c.servers[id].Stop()
	if err := c.nodes[id].Stop(); err != nil {
		c.t.Fatalf("stopping node %d: %v", id, err)
	}

	stale := make(map[raft.NodeID]string, len(staticPeers))
	for _, p := range staticPeers {
		stale[p] = c.addrs[p]
	}

	l, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		c.t.Fatalf("re-binding node %d: %v", id, err)
	}

	tr, err := NewPeerTransport(PeerConfig{Self: id, Addresses: stale})
	if err != nil {
		c.t.Fatalf("creating transport for node %d: %v", id, err)
	}
	c.transports[id] = tr
	c.t.Cleanup(func() { tr.Close() })

	n, err := node.Start(node.Config{
		ID:            id,
		Peers:         staticPeers,
		DataDir:       c.dataDirs[id],
		Transport:     tr,
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		c.t.Fatalf("restarting node %d: %v", id, err)
	}
	c.nodes[id] = n
	c.t.Cleanup(func() { n.Stop() })
	tr.SetLocal(n)

	srv, err := NewRaftServer(n)
	if err != nil {
		c.t.Fatalf("creating server for node %d: %v", id, err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	c.servers[id] = gs
	go gs.Serve(l)
	c.t.Cleanup(gs.Stop)

	return n
}

func linked(tr *PeerTransport, id raft.NodeID) bool {
	for _, st := range tr.Stats() {
		if st.ID == id {
			return true
		}
	}
	return false
}

func TestARestartedNodeRediscoversARuntimeMember(t *testing.T) {
	original := []raft.NodeID{1, 2, 3}

	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	c.eventually("the added node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	victim := raft.NodeID(0)
	for _, id := range original {
		if id != leaderID {
			victim = id
			break
		}
	}

	restarted := c.restartStale(victim, original)

	c.eventually("the restarted node to recover the membership", func() bool {
		for _, id := range restarted.Status().Members.Members() {
			if id == 4 {
				return true
			}
		}
		return false
	})

	c.eventually("the restarted node to link to the runtime member", func() bool {
		return linked(c.transports[victim], 4)
	})

	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 2, Seq: 1, Op: statemachine.OpPut, Key: "after", Value: []byte("restart"),
	}); err != nil {
		t.Fatalf("Propose after the restart: %v", err)
	}
}

func TestARestartedNodeRediscoversARuntimeMemberFromItsSnapshot(t *testing.T) {
	original := []raft.NodeID{1, 2, 3}

	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	c.eventually("the added node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	for i := range 8 {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 3, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("post-%d", i), Value: []byte("v"),
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	for _, id := range c.ids {
		if err := c.nodes[id].Compact(ctx); err != nil {
			t.Fatalf("compacting node %d: %v", id, err)
		}
	}

	victim := raft.NodeID(0)
	for _, id := range original {
		if id != leaderID {
			victim = id
			break
		}
	}

	restarted := c.restartStale(victim, original)

	c.eventually("the restarted node to recover the membership", func() bool {
		for _, id := range restarted.Status().Members.Members() {
			if id == 4 {
				return true
			}
		}
		return false
	})
	c.eventually("the restarted node to link to the runtime member", func() bool {
		return linked(c.transports[victim], 4)
	})
}
