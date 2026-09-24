package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	grpcSettleTimeout = 20 * time.Second

	unroutableAddr = "192.0.2.1:1"
)

type grpcCluster struct {
	t *testing.T

	ids        []raft.NodeID
	addrs      map[raft.NodeID]string
	dataDirs   map[raft.NodeID]string
	nodes      map[raft.NodeID]*node.Node
	transports map[raft.NodeID]*PeerTransport
	servers    map[raft.NodeID]*grpc.Server

	conns map[raft.NodeID]*grpc.ClientConn
}

func newGRPCCluster(t *testing.T, size int) *grpcCluster {
	t.Helper()

	c := &grpcCluster{
		t:          t,
		addrs:      make(map[raft.NodeID]string, size),
		dataDirs:   make(map[raft.NodeID]string, size),
		nodes:      make(map[raft.NodeID]*node.Node, size),
		transports: make(map[raft.NodeID]*PeerTransport, size),
		servers:    make(map[raft.NodeID]*grpc.Server, size),
		conns:      make(map[raft.NodeID]*grpc.ClientConn, size),
	}
	for i := range size {
		c.ids = append(c.ids, raft.NodeID(i+1))
	}

	listeners := make(map[raft.NodeID]net.Listener, size)
	for _, id := range c.ids {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listening for node %d: %v", id, err)
		}
		listeners[id] = l
		c.addrs[id] = l.Addr().String()
	}

	root := t.TempDir()
	for _, id := range c.ids {
		c.dataDirs[id] = filepath.Join(root, fmt.Sprintf("node-%d", id))

		tr, err := NewPeerTransport(PeerConfig{Self: id, Addresses: c.addrs})
		if err != nil {
			t.Fatalf("creating transport for node %d: %v", id, err)
		}
		c.transports[id] = tr
		t.Cleanup(func() { tr.Close() })

		n, err := node.Start(node.Config{
			ID:            id,
			Peers:         c.ids,
			DataDir:       c.dataDirs[id],
			Transport:     tr,
			TickInterval:  10 * time.Millisecond,
			ElectionTick:  10,
			HeartbeatTick: 1,
			Sync:          storage.SyncNever,
		})
		if err != nil {
			t.Fatalf("starting node %d: %v", id, err)
		}
		c.nodes[id] = n
		t.Cleanup(func() { n.Stop() })

		tr.SetLocal(n)

		srv, err := NewRaftServer(n)
		if err != nil {
			t.Fatalf("creating server for node %d: %v", id, err)
		}
		gs := grpc.NewServer()
		srv.Register(gs)

		kv, err := NewKVServer(n, c.addrs)
		if err != nil {
			t.Fatalf("creating client server for node %d: %v", id, err)
		}
		kv.Register(gs)

		c.servers[id] = gs
		go gs.Serve(listeners[id])
		t.Cleanup(gs.Stop)
	}

	for _, id := range c.ids {
		conn, err := grpc.NewClient(c.addrs[id],
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dialling node %d: %v", id, err)
		}
		c.conns[id] = conn
		t.Cleanup(func() { conn.Close() })
	}

	return c
}

func (c *grpcCluster) kv(id raft.NodeID) raftkvv1.KVServiceClient {
	c.t.Helper()
	conn, ok := c.conns[id]
	if !ok {
		c.t.Fatalf("no connection to node %d", id)
	}
	return raftkvv1.NewKVServiceClient(conn)
}

func (c *grpcCluster) cluster(id raft.NodeID) raftkvv1.ClusterServiceClient {
	c.t.Helper()
	conn, ok := c.conns[id]
	if !ok {
		c.t.Fatalf("no connection to node %d", id)
	}
	return raftkvv1.NewClusterServiceClient(conn)
}

func (c *grpcCluster) followerOf(leader raft.NodeID) raft.NodeID {
	c.t.Helper()
	for _, id := range c.ids {
		if id != leader {
			return id
		}
	}
	c.t.Fatal("the cluster has only one node")
	return 0
}

func (c *grpcCluster) eventually(what string, cond func() bool) {
	c.t.Helper()

	deadline := time.Now().Add(grpcSettleTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s\n%s", what, c.dump())
}

func (c *grpcCluster) awaitLeader() *node.Node {
	c.t.Helper()

	var found *node.Node
	c.eventually("a leader to be elected over gRPC", func() bool {
		var leaders []*node.Node
		for _, id := range c.ids {
			if n, ok := c.nodes[id]; ok && n.Status().State == raft.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) != 1 {
			return false
		}
		found = leaders[0]
		return true
	})
	return found
}

func (c *grpcCluster) dump() string {
	out := "cluster state:\n"
	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok {
			out += fmt.Sprintf("  node %d: stopped\n", id)
			continue
		}
		s := n.Status()
		out += fmt.Sprintf("  node %d (%s): state=%-9s term=%d leader=%d commit=%d applied=%d\n",
			s.ID, c.addrs[id], s.State, s.Term, s.Leader, s.Commit, s.Applied)
	}
	for _, id := range c.ids {
		if tr, ok := c.transports[id]; ok {
			for _, st := range tr.Stats() {
				out += fmt.Sprintf("  link %d->%d: sent=%d dropped=%d failed=%d\n",
					id, st.ID, st.Sent, st.Dropped, st.Failed)
			}
		}
	}
	return out
}

func TestClusterFormsOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	c.eventually("every follower to recognize the leader", func() bool {
		want := leader.Status()
		for _, id := range c.ids {
			s := c.nodes[id].Status()
			if s.Leader != want.ID || s.Term != want.Term {
				return false
			}
		}
		return true
	})
}

func TestWriteAndReadOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "x", Value: []byte("over-the-wire"),
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	value, ok, err := leader.Get(ctx, "x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(value) != "over-the-wire" {
		t.Fatalf("x = %q (found %v), want over-the-wire", value, ok)
	}
}

func TestReplicationReachesEveryNodeOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	for i := range 20 {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("key-%d", i), Value: []byte("value"),
		})
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	want := leader.Status().Applied
	c.eventually("every node to apply the same entries", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Status().Applied != want {
				return false
			}
		}
		return true
	})

	total := uint64(0)
	for _, id := range c.ids {
		for _, st := range c.transports[id].Stats() {
			total += st.Sent
		}
	}
	if total == 0 {
		t.Fatalf("no messages were sent over gRPC\n%s", c.dump())
	}
}

func TestClusterSurvivesOneNodeDying(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

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

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	for i := range 5 {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("after-death-%d", i), Value: []byte("v"),
		})
		if err != nil {
			t.Fatalf("writing with one node down: %v\n%s", err, c.dump())
		}
	}
}

func TestSendDoesNotBlockOnAnUnreachablePeer(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr},
		QueueSize: 4,
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	msgs := make([]raft.Message, 100)
	for i := range msgs {
		msgs[i] = raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1}
	}

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		tr.Send(msgs)
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		if elapsed > 2*time.Second {
			t.Fatalf("Send took %v against an unreachable peer; it must not block "+
				"the Raft loop", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Send blocked indefinitely against an unreachable peer; one dead " +
			"node would stall the whole cluster")
	}
}

func TestFullQueueDropsRatherThanBlocking(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr},
		QueueSize: 1,
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	for range 500 {
		tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1}})
	}

	stats := tr.Stats()
	if len(stats) != 1 {
		t.Fatalf("got %d peers in stats, want 1", len(stats))
	}
	if stats[0].Dropped == 0 {
		t.Fatalf("no messages were dropped with a queue of 1 and an unreachable peer; "+
			"they are being buffered without bound\n%+v", stats[0])
	}
}

func TestMessagesToUnknownPeersAreIgnored(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr},
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 99, Term: 1}})
}

type recordingStepper struct {
	mu   sync.Mutex
	msgs []raft.Message
}

func (r *recordingStepper) Step(m raft.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
}

func (r *recordingStepper) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func TestSelfAddressedMessagesBypassTheNetwork(t *testing.T) {
	local := &recordingStepper{}

	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{1: unroutableAddr, 2: unroutableAddr},
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()
	tr.SetLocal(local)

	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 1, Term: 1}})

	if got := local.count(); got != 1 {
		t.Fatalf("local stepper received %d messages, want 1", got)
	}
	for _, st := range tr.Stats() {
		if st.ID == 1 {
			t.Fatal("a connection was created to this node's own address")
		}
	}
}

func TestSelfAddressedMessagesAreDroppedWithoutALocalStepper(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{1: unroutableAddr},
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 1, Term: 1}})
}

func TestLocalSignalsAreNotSent(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr},
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	tr.Send([]raft.Message{{Type: raft.MsgCampaign, From: 1, To: 2}})

	stats := tr.Stats()
	if stats[0].Failed == 0 {
		t.Fatal("a node-local signal was accepted for sending")
	}
	if stats[0].Sent != 0 {
		t.Fatal("a node-local signal was sent over the network")
	}
}

func TestServerRejectsUndecodableMessages(t *testing.T) {
	local := &recordingStepper{}
	srv, err := NewRaftServer(local)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	cases := map[string]*raftkvv1.DeliverRequest{
		"no message":       {},
		"unspecified type": {Message: &raftkvv1.Message{From: 1, To: 2, Term: 1}},
		"unknown type": {Message: &raftkvv1.Message{
			Type: raftkvv1.MessageType(9999), From: 1, To: 2, Term: 1,
		}},
		"bad entry type": {Message: &raftkvv1.Message{
			Type: raftkvv1.MessageType_MESSAGE_TYPE_APPEND_REQUEST,
			From: 1, To: 2, Term: 1,
			Entries: []*raftkvv1.Entry{{Term: 1, Index: 1, Type: raftkvv1.EntryType(9999)}},
		}},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := srv.Deliver(context.Background(), req); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}

	if got := local.count(); got != 0 {
		t.Fatalf("%d undecodable messages reached the node", got)
	}
}

func TestServerDeliversValidMessages(t *testing.T) {
	local := &recordingStepper{}
	srv, err := NewRaftServer(local)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	want := raft.Message{
		Type: raft.MsgAppendRequest, From: 1, To: 2, Term: 7,
		PrevLogIndex: 3, PrevLogTerm: 6, CommitIndex: 3,
		Entries: []raft.Entry{
			{Term: 7, Index: 4, Type: raft.EntryNormal, Data: []byte("cmd")},
		},
	}

	wire, err := MessageToWire(want)
	if err != nil {
		t.Fatalf("MessageToWire: %v", err)
	}
	if _, err := srv.Deliver(context.Background(), &raftkvv1.DeliverRequest{Message: wire}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	local.mu.Lock()
	defer local.mu.Unlock()
	if len(local.msgs) != 1 {
		t.Fatalf("node received %d messages, want 1", len(local.msgs))
	}
	assertMessageEqual(t, local.msgs[0], want)
}

func TestNewRaftServerRequiresANode(t *testing.T) {
	if _, err := NewRaftServer(nil); err == nil {
		t.Fatal("a server was created with no node to deliver to")
	}
}

func TestTransportRequiresANonZeroSelfID(t *testing.T) {
	if _, err := NewPeerTransport(PeerConfig{}); err == nil {
		t.Fatal("a transport was created without a node ID")
	}
}

func TestPeerWithNoAddressIsRejected(t *testing.T) {
	_, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: ""},
	})
	if err == nil {
		t.Fatal("a peer with no address was accepted")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr},
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConcurrentSendsAreSafe(t *testing.T) {
	tr, err := NewPeerTransport(PeerConfig{
		Self:      1,
		Addresses: map[raft.NodeID]string{2: unroutableAddr, 3: unroutableAddr},
		QueueSize: 8,
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	defer tr.Close()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 50 {
				tr.Send([]raft.Message{{
					Type: raft.MsgHeartbeat, From: 1,
					To: raft.NodeID(2 + i%2), Term: 1,
				}})
			}
		}(i)
	}
	wg.Wait()
}

func TestSnapshotTransferOverGRPC(t *testing.T) {
	runSnapshotTransfer(t, 40, 5)
}

func TestLargeSnapshotTransferOverGRPC(t *testing.T) {
	runSnapshotTransfer(t, 64, 128<<10)
}

func runSnapshotTransfer(t *testing.T, writes, valueSize int) {
	t.Helper()

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

	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok {
			continue
		}
		if err := n.Compact(ctx); err != nil {
			t.Fatalf("compacting node %d: %v", id, err)
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
	c.eventually("the restarted node to be caught up", func() bool {
		return c.nodes[victim].Status().Applied >= want
	})

	if got := c.nodes[victim].Status().SnapshotsReceived; got == 0 {
		t.Fatalf("node %d caught up without receiving a snapshot, so this test is "+
			"not exercising snapshot transfer\n%s", victim, c.dump())
	}
}
