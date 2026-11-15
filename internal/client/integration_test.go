package client_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MenaceHecker/raftkv/internal/client"
	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/transport"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// liveCluster is three real nodes on loopback, built from the production
// pieces (shape of newAdminCluster in internal/transport/admin_test.go, which
// is unexported there).
type liveCluster struct {
	t     *testing.T
	ids   []raft.NodeID
	addrs map[raft.NodeID]string
	nodes map[raft.NodeID]*node.Node
	srvs  map[raft.NodeID]*grpc.Server
}

func newLiveCluster(t *testing.T, size int) *liveCluster {
	t.Helper()
	c := &liveCluster{
		t:     t,
		addrs: map[raft.NodeID]string{},
		nodes: map[raft.NodeID]*node.Node{},
		srvs:  map[raft.NodeID]*grpc.Server{},
	}
	lis := map[raft.NodeID]net.Listener{}
	for i := range size {
		id := raft.NodeID(i + 1)
		c.ids = append(c.ids, id)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lis[id] = l
		c.addrs[id] = l.Addr().String()
	}

	root := t.TempDir()
	for _, id := range c.ids {
		peer, err := transport.NewPeerTransport(transport.PeerConfig{Self: id, Addresses: c.addrs})
		if err != nil {
			t.Fatalf("PeerTransport %d: %v", id, err)
		}
		t.Cleanup(func() { _ = peer.Close() })

		n, err := node.Start(node.Config{
			ID: id, Peers: c.ids, DataDir: filepath.Join(root, fmt.Sprint("n", id)),
			Transport: peer, TickInterval: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 1,
			Sync: storage.SyncNever,
		})
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		c.nodes[id] = n
		t.Cleanup(func() { _ = n.Stop() })
		peer.SetLocal(n)

		raftSrv, err := transport.NewRaftServer(n)
		if err != nil {
			t.Fatal(err)
		}
		kv, err := transport.NewKVServer(n, c.addrs)
		if err != nil {
			t.Fatal(err)
		}
		gs := grpc.NewServer()
		raftkvv1.RegisterRaftServiceServer(gs, raftSrv)
		kv.Register(gs)
		c.srvs[id] = gs
		go func() { _ = gs.Serve(lis[id]) }()
		t.Cleanup(gs.Stop)
	}
	return c
}

func (c *liveCluster) addrList() []string {
	var out []string
	for _, id := range c.ids {
		out = append(out, c.addrs[id])
	}
	return out
}

// leader polls until some node leads.
func (c *liveCluster) leader() raft.NodeID {
	c.t.Helper()
	var found raft.NodeID
	settle(c.t, "a leader", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Status().State == raft.Leader {
				found = id
				return true
			}
		}
		return false
	})
	return found
}

// kill stops the node and its server, as a crash would from the client's side.
func (c *liveCluster) kill(id raft.NodeID) {
	c.srvs[id].Stop()
	_ = c.nodes[id].Stop()
}

func (c *liveCluster) newClient(cfg client.Config) *client.Client {
	c.t.Helper()
	cfg.Addrs = c.addrList()
	cfg.DialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cl, err := client.New(cfg)
	if err != nil {
		c.t.Fatalf("client.New: %v", err)
	}
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func TestRetryAppliesOnce(t *testing.T) {
	c := newLiveCluster(t, 3)
	cl := c.newClient(client.Config{ClientID: 77, StartSeq: 5})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := cl.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	// Replay the same (id, seq) with a different value straight at the wire,
	// as a retry whose first reply was lost would.
	var accepted int
	for _, id := range c.ids {
		cc, err := grpc.NewClient(c.addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		_, err = raftkvv1.NewKVServiceClient(cc).Put(ctx, &raftkvv1.PutRequest{
			Client: &raftkvv1.ClientRequest{ClientId: 77, Sequence: 5},
			Key:    "k", Value: []byte("v2"),
		})
		_ = cc.Close()
		if err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("the leader accepted %d replays, want exactly 1 (followers redirect)", accepted)
	}

	v, ok, err := cl.Get(ctx, "k")
	if err != nil || !ok || string(v) != "v1" {
		t.Errorf("Get = %q, %v, %v; want v1: the replayed seq must be dropped, not applied", v, ok, err)
	}
}

func TestKillLeaderMidWorkloadClientCompletes(t *testing.T) {
	const total, killAfter = 200, 50

	c := newLiveCluster(t, 3)
	cl := c.newClient(client.Config{})
	c.leader() // a cluster with no leader yet is also fine for the client; this just gives the kill a target

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	key := func(i int) string { return fmt.Sprintf("key%03d", i) }
	val := func(i int) []byte { return []byte(fmt.Sprintf("val%03d", i)) }

	reached := make(chan struct{})
	result := make(chan error, 1)
	var indeterminate int
	go func() {
		for i := range total {
			opCtx, opCancel := context.WithTimeout(ctx, 15*time.Second)
			err := cl.Put(opCtx, key(i), val(i))
			if client.IsIndeterminate(err) {
				indeterminate++
				// Unknown outcome: read back, and write again only if absent.
				v, found, gerr := cl.Get(opCtx, key(i))
				switch {
				case gerr != nil:
					err = fmt.Errorf("verifying read after unknown outcome: %w", gerr)
				case found && string(v) == string(val(i)):
					err = nil
				default:
					err = cl.Put(opCtx, key(i), val(i))
				}
			}
			opCancel()
			if err != nil {
				result <- fmt.Errorf("key %d: %w", i, err)
				return
			}
			if i+1 == killAfter {
				close(reached)
			}
		}
		result <- nil
	}()

	select {
	case <-reached:
	case err := <-result:
		t.Fatalf("workload ended before the kill: %v", err)
	case <-ctx.Done():
		t.Fatal("workload never reached the kill point")
	}
	victim := c.leader()
	c.kill(victim)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("workload did not complete after the leader was killed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("workload did not finish after the leader was killed")
	}
	t.Logf("killed leader %d after %d acked writes; %d writes had an unknown outcome", victim, killAfter, indeterminate)

	for i := range total {
		v, found, err := cl.Get(ctx, key(i))
		if err != nil {
			t.Fatalf("final Get %s: %v", key(i), err)
		}
		if !found || string(v) != string(val(i)) {
			t.Errorf("acked key %s = %q (found=%v), want %q", key(i), v, found, val(i))
		}
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("context ended: %v", err)
	}
}
