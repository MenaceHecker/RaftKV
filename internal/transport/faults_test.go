package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Tests for the fault wrapper (T4.7, tests 1-9). They use a fake clock and a
// scripted Rand, so nothing here sleeps or depends on wall time.

// fakeClock is a manually advanced Clock. Timer callbacks run synchronously
// inside Advance, without the clock's lock held, so they may call back into
// the code under test.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer

	// advisoryStop makes Stop report success without preventing the callback,
	// which models a timer that has already fired when Stop is called.
	advisoryStop bool
}

type fakeTimer struct {
	c       *fakeClock
	when    time.Time
	f       func()
	stopped bool
	fired   bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, when: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	if !t.c.advisoryStop {
		t.stopped = true
	}
	return true
}

// Advance moves time forward by d, firing due timers in order.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	for {
		var next *fakeTimer
		for _, t := range c.timers {
			if t.fired || t.stopped || t.when.After(target) {
				continue
			}
			if next == nil || t.when.Before(next.when) {
				next = t
			}
		}
		if next == nil {
			break
		}
		next.fired = true
		if next.when.After(c.now) {
			c.now = next.when
		}
		c.mu.Unlock()
		next.f()
		c.mu.Lock()
	}
	c.now = target
	c.mu.Unlock()
}

// scriptedRand returns a fixed sequence of draws, cycling at the end.
type scriptedRand struct {
	t     *testing.T
	mu    sync.Mutex
	draws []int
	next  int
	calls int
}

func (r *scriptedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n != 100 {
		r.t.Errorf("Intn(%d): loss is a percentage, so every draw must be Intn(100)", n)
	}
	r.calls++
	v := r.draws[r.next%len(r.draws)]
	r.next++
	return v
}

// recordingTransport is the inner transport: it records what passes.
type recordingTransport struct {
	mu      sync.Mutex
	sent    []raft.Message
	added   map[raft.NodeID]string
	removed []raft.NodeID
}

func (r *recordingTransport) Send(msgs []raft.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, msgs...)
}

func (r *recordingTransport) AddPeer(id raft.NodeID, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.added == nil {
		r.added = make(map[raft.NodeID]string)
	}
	r.added[id] = addr
	return nil
}

func (r *recordingTransport) RemovePeer(id raft.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, id)
}

// terms returns the Term of every message passed to peer, in order. Tests tag
// messages by Term so order and identity are visible.
func (r *recordingTransport) terms(to raft.NodeID) []raft.Term {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []raft.Term
	for _, m := range r.sent {
		if m.To == to {
			out = append(out, m.Term)
		}
	}
	return out
}

const self = raft.NodeID(1)

func msg(to raft.NodeID, tag raft.Term) raft.Message {
	return raft.Message{Type: raft.MsgHeartbeat, From: self, To: to, Term: tag}
}

func newTestFaults(t *testing.T) (*FaultTransport, *recordingTransport, *fakeClock, *scriptedRand) {
	t.Helper()
	inner := &recordingTransport{}
	clock := newFakeClock()
	rnd := &scriptedRand{t: t, draws: []int{50}}
	f := NewFaultTransport(FaultConfig{Self: self, Inner: inner, Clock: clock, Rand: rnd})
	return f, inner, clock, rnd
}

func sameTerms(got, want []raft.Term) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func expectTerms(t *testing.T, what string, got, want []raft.Term) {
	t.Helper()
	if !sameTerms(got, want) {
		t.Fatalf("%s: delivered terms %v, want %v", what, got, want)
	}
}

// 1
func TestBlockedPeerReceivesNothing(t *testing.T) {
	f, inner, _, _ := newTestFaults(t)

	f.SetPartition([]raft.NodeID{2})
	f.Send([]raft.Message{msg(self, 1), msg(2, 2), msg(3, 3)})

	expectTerms(t, "blocked peer 2", inner.terms(2), nil)
	expectTerms(t, "self (never dropped)", inner.terms(self), []raft.Term{1})
	expectTerms(t, "unblocked peer 3", inner.terms(3), []raft.Term{3})

	if f.AllowIngress(2) {
		t.Fatal("ingress from blocked peer 2 was allowed")
	}
	if !f.AllowIngress(3) {
		t.Fatal("ingress from unblocked peer 3 was refused")
	}
	if !f.AllowIngress(self) {
		t.Fatal("ingress from self was refused")
	}
}

// spyRaftServer records what reaches the inner RaftService.
type spyRaftServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	mu        sync.Mutex
	delivered int

	// For DeliverSnapshot: the frames the inner handler managed to receive and
	// the error that ended the first Recv after the header, if any.
	framesSeen   int
	headerSeen   chan struct{}
	proceed      chan struct{}
	recvErr      chan error
	holdAfterHdr bool
}

func (s *spyRaftServer) Deliver(_ context.Context, _ *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered++
	return &raftkvv1.DeliverResponse{}, nil
}

func (s *spyRaftServer) DeliverSnapshot(stream raftkvv1.RaftService_DeliverSnapshotServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			select {
			case s.recvErr <- err:
			default:
			}
			return err
		}
		s.mu.Lock()
		s.framesSeen++
		first := s.framesSeen == 1
		s.mu.Unlock()
		if first && s.holdAfterHdr {
			close(s.headerSeen)
			<-s.proceed
		}
	}
}

func (s *spyRaftServer) frames() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.framesSeen
}

func (s *spyRaftServer) deliveries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered
}

func serveIngress(t *testing.T, f *FaultTransport, inner raftkvv1.RaftServiceServer) raftkvv1.RaftServiceClient {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftkvv1.RegisterRaftServiceServer(gs, f.WrapRaftServer(inner))
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(l.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftkvv1.NewRaftServiceClient(conn)
}

func wireFrom(from raft.NodeID) *raftkvv1.Message {
	return &raftkvv1.Message{
		Type: raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT,
		From: uint64(from),
		To:   uint64(self),
		Snapshot: &raftkvv1.Snapshot{
			Index: 1, Term: 1, Conf: &raftkvv1.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
}

func requireUnavailable(t *testing.T, what string, err error) {
	t.Helper()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("%s: got %v, want codes.Unavailable", what, err)
	}
}

// 2
func TestDeliverAndDeliverSnapshotDropBlockedPeer(t *testing.T) {
	t.Run("Deliver", func(t *testing.T) {
		f, _, _, _ := newTestFaults(t)
		spy := &spyRaftServer{}
		client := serveIngress(t, f, spy)

		f.SetPartition([]raft.NodeID{2})

		_, err := client.Deliver(context.Background(), &raftkvv1.DeliverRequest{Message: wireFrom(2)})
		requireUnavailable(t, "Deliver from blocked peer 2", err)
		if got := spy.deliveries(); got != 0 {
			t.Fatalf("blocked Deliver reached the node %d time(s)", got)
		}

		if _, err := client.Deliver(context.Background(), &raftkvv1.DeliverRequest{Message: wireFrom(3)}); err != nil {
			t.Fatalf("Deliver from unblocked peer 3: %v", err)
		}
		if got := spy.deliveries(); got != 1 {
			t.Fatalf("Deliver from peer 3 reached the node %d time(s), want 1", got)
		}
	})

	t.Run("DeliverSnapshot is rejected on the header frame", func(t *testing.T) {
		f, _, _, _ := newTestFaults(t)
		spy := &spyRaftServer{recvErr: make(chan error, 1)}
		client := serveIngress(t, f, spy)

		f.SetPartition([]raft.NodeID{2})

		stream, err := client.DeliverSnapshot(context.Background())
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}
		hdr := wireFrom(2)
		_ = stream.Send(&raftkvv1.SnapshotChunk{Frame: &raftkvv1.SnapshotChunk_Header{Header: hdr}})
		_, err = stream.CloseAndRecv()
		requireUnavailable(t, "snapshot stream from blocked peer 2", err)

		if got := spy.frames(); got != 0 {
			t.Fatalf("the node received %d frame(s) of a blocked stream; it must be rejected before any is buffered", got)
		}
	})

	t.Run("a stream opened before the block stops at the next chunk", func(t *testing.T) {
		f, _, _, _ := newTestFaults(t)
		spy := &spyRaftServer{
			recvErr:      make(chan error, 1),
			headerSeen:   make(chan struct{}),
			proceed:      make(chan struct{}),
			holdAfterHdr: true,
		}
		client := serveIngress(t, f, spy)

		stream, err := client.DeliverSnapshot(context.Background())
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}
		if err := stream.Send(&raftkvv1.SnapshotChunk{
			Frame: &raftkvv1.SnapshotChunk_Header{Header: wireFrom(2)},
		}); err != nil {
			t.Fatalf("sending header: %v", err)
		}
		<-spy.headerSeen // the header was accepted while peer 2 was not blocked

		f.SetPartition([]raft.NodeID{2})
		_ = stream.Send(&raftkvv1.SnapshotChunk{Frame: &raftkvv1.SnapshotChunk_Data{Data: []byte("chunk")}})
		close(spy.proceed)

		select {
		case err := <-spy.recvErr:
			requireUnavailable(t, "next chunk after the partition", err)
		case <-time.After(10 * time.Second):
			t.Fatal("the stream kept flowing after its sender was partitioned")
		}
		if got := spy.frames(); got != 1 {
			t.Fatalf("the node received %d frames, want only the header", got)
		}
	})
}

// 3
func TestDropDecisionPerMessage(t *testing.T) {
	t.Run("scripted draws decide each message", func(t *testing.T) {
		f, inner, _, rnd := newTestFaults(t)
		// A message is dropped when its draw is below the percentage.
		rnd.draws = []int{0, 29, 30, 99, 15, 30}
		if err := f.SetDropPercent(2, 30); err != nil {
			t.Fatalf("SetDropPercent: %v", err)
		}

		for tag := raft.Term(1); tag <= 6; tag++ {
			f.Send([]raft.Message{msg(2, tag)})
		}
		// draws 0, 29 and 15 are < 30; 30, 99 and 30 are not.
		expectTerms(t, "30% loss", inner.terms(2), []raft.Term{3, 4, 6})
	})

	t.Run("one draw per message in a batch", func(t *testing.T) {
		f, inner, _, rnd := newTestFaults(t)
		rnd.draws = []int{10, 90}
		if err := f.SetDropPercent(2, 50); err != nil {
			t.Fatalf("SetDropPercent: %v", err)
		}
		f.Send([]raft.Message{msg(2, 1), msg(2, 2)})
		expectTerms(t, "batch", inner.terms(2), []raft.Term{2})
	})

	t.Run("100 percent drops every message", func(t *testing.T) {
		f, inner, _, rnd := newTestFaults(t)
		rnd.draws = []int{0, 99, 50}
		if err := f.SetDropPercent(2, 100); err != nil {
			t.Fatalf("SetDropPercent: %v", err)
		}
		for tag := raft.Term(1); tag <= 6; tag++ {
			f.Send([]raft.Message{msg(2, tag)})
		}
		expectTerms(t, "100% loss", inner.terms(2), nil)
	})

	// Negative control: green against the pass-through stub by design. It
	// guards the implementation against dropping when no loss is set.
	t.Run("0 percent drops nothing", func(t *testing.T) {
		f, inner, _, rnd := newTestFaults(t)
		rnd.draws = []int{0, 99, 50}
		if err := f.SetDropPercent(2, 0); err != nil {
			t.Fatalf("SetDropPercent: %v", err)
		}
		for tag := raft.Term(1); tag <= 6; tag++ {
			f.Send([]raft.Message{msg(2, tag)})
		}
		expectTerms(t, "0% loss", inner.terms(2), []raft.Term{1, 2, 3, 4, 5, 6})
	})

	t.Run("out of range is refused", func(t *testing.T) {
		f, _, _, _ := newTestFaults(t)
		if err := f.SetDropPercent(2, 101); err == nil {
			t.Fatal("SetDropPercent(101) succeeded")
		}
		if err := f.SetDropPercent(2, -1); err == nil {
			t.Fatal("SetDropPercent(-1) succeeded")
		}
	})

	// Negative control: green against the pass-through stub by design. It
	// guards the implementation against dropping or delaying self messages.
	t.Run("self messages are never dropped", func(t *testing.T) {
		f, inner, _, _ := newTestFaults(t)
		if err := f.SetDropPercent(0, 100); err != nil {
			t.Fatalf("SetDropPercent: %v", err)
		}
		f.Send([]raft.Message{msg(self, 1)})
		expectTerms(t, "self under 100% loss", inner.terms(self), []raft.Term{1})
	})
}

// 4
func TestDelayKeepsPerPeerOrderAndUsesInjectedClock(t *testing.T) {
	f, inner, clock, _ := newTestFaults(t)

	f.SetDelay(2, 50*time.Millisecond)

	f.Send([]raft.Message{msg(2, 1)}) // t=0, due 50ms
	clock.Advance(10 * time.Millisecond)
	f.Send([]raft.Message{msg(2, 2), msg(3, 30)}) // t=10ms, due 60ms; peer 3 has no delay

	expectTerms(t, "peer 3 is not delayed", inner.terms(3), []raft.Term{30})
	expectTerms(t, "nothing yet at 10ms", inner.terms(2), nil)

	clock.Advance(39 * time.Millisecond) // 49ms
	expectTerms(t, "nothing yet at 49ms", inner.terms(2), nil)

	clock.Advance(1 * time.Millisecond) // 50ms
	expectTerms(t, "first message at 50ms", inner.terms(2), []raft.Term{1})

	clock.Advance(9 * time.Millisecond) // 59ms
	expectTerms(t, "second not yet at 59ms", inner.terms(2), []raft.Term{1})

	clock.Advance(1 * time.Millisecond) // 60ms
	expectTerms(t, "second at 60ms", inner.terms(2), []raft.Term{1, 2})

	// A later, shorter delay must not let a newer message overtake an older one.
	f.SetDelay(2, 100*time.Millisecond)
	f.Send([]raft.Message{msg(2, 3)}) // due +100ms
	f.SetDelay(2, 10*time.Millisecond)
	f.Send([]raft.Message{msg(2, 4)}) // due +10ms, but queued behind 3

	clock.Advance(10 * time.Millisecond)
	expectTerms(t, "4 waits behind 3", inner.terms(2), []raft.Term{1, 2})
	clock.Advance(90 * time.Millisecond)
	expectTerms(t, "3 then 4", inner.terms(2), []raft.Term{1, 2, 3, 4})
}

// 5
func TestClearingFaultsRestoresPassThrough(t *testing.T) {
	f, inner, clock, rnd := newTestFaults(t)
	rnd.draws = []int{0}

	f.SetPartition([]raft.NodeID{2})
	f.SetDelay(3, 50*time.Millisecond)
	if err := f.SetDropPercent(4, 100); err != nil {
		t.Fatalf("SetDropPercent: %v", err)
	}

	f.Send([]raft.Message{msg(2, 1), msg(3, 1), msg(4, 1)})
	expectTerms(t, "partitioned peer 2", inner.terms(2), nil)
	expectTerms(t, "delayed peer 3", inner.terms(3), nil)
	expectTerms(t, "lossy peer 4", inner.terms(4), nil)

	f.ClearFaults()

	f.Send([]raft.Message{msg(2, 2), msg(3, 2), msg(4, 2)})
	expectTerms(t, "peer 2 after clear", inner.terms(2), []raft.Term{2})
	expectTerms(t, "peer 3 after clear", inner.terms(3), []raft.Term{2})
	expectTerms(t, "peer 4 after clear", inner.terms(4), []raft.Term{2})
	if !f.AllowIngress(2) {
		t.Fatal("ingress from peer 2 still refused after clear")
	}

	clock.Advance(time.Second)
	expectTerms(t, "the message held before the clear stays discarded", inner.terms(3), []raft.Term{2})
}

// 6
func TestWrapperForwardsAddPeerAndRemovePeer(t *testing.T) {
	t.Run("calls reach the inner transport", func(t *testing.T) {
		f, inner, _, _ := newTestFaults(t)

		if err := f.AddPeer(7, "127.0.0.1:7007"); err != nil {
			t.Fatalf("AddPeer: %v", err)
		}
		f.RemovePeer(7)

		inner.mu.Lock()
		defer inner.mu.Unlock()
		if got := inner.added[7]; got != "127.0.0.1:7007" {
			t.Fatalf("inner transport saw AddPeer address %q, want 127.0.0.1:7007", got)
		}
		if len(inner.removed) != 1 || inner.removed[0] != 7 {
			t.Fatalf("inner transport saw RemovePeer %v, want [7]", inner.removed)
		}
	})

	t.Run("a membership change with the wrapper and no faults reaches the new node", func(t *testing.T) {
		c := newFaultCluster(t, 3)
		leader := c.awaitLeader()

		ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
		defer cancel()

		joiner := c.join(4)
		c.eventually("the leader to accept AddNode", func() bool {
			err := leader.AddNode(ctx, 4, c.addrs[4])
			if errors.Is(err, raft.ErrConfChangePending) {
				return false
			}
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			return true
		})

		c.eventually("the added node to learn the membership", func() bool {
			return joiner.Status().Members.String() == leader.Status().Members.String()
		})
		c.eventually("the added node to catch up", func() bool {
			return joiner.Status().Applied >= leader.Status().Applied
		})
	})
}

// faultCluster is a small gRPC cluster whose every node sends through a
// FaultTransport and receives through the wrapped RaftService, with no faults
// set.
type faultCluster struct {
	t     *testing.T
	ids   []raft.NodeID
	addrs map[raft.NodeID]string
	nodes map[raft.NodeID]*node.Node
}

func newFaultCluster(t *testing.T, size int) *faultCluster {
	t.Helper()
	c := &faultCluster{
		t:     t,
		addrs: make(map[raft.NodeID]string),
		nodes: make(map[raft.NodeID]*node.Node),
	}
	listeners := make(map[raft.NodeID]net.Listener)
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[id] = l
		c.addrs[id] = l.Addr().String()
	}
	root := t.TempDir()
	static := append([]raft.NodeID{}, c.ids...)
	for _, id := range static {
		c.start(id, static, listeners[id], filepath.Join(root, fmt.Sprintf("node-%d", id)))
	}
	return c
}

func (c *faultCluster) start(id raft.NodeID, members []raft.NodeID, l net.Listener, dir string) *node.Node {
	c.t.Helper()
	tr, err := NewPeerTransport(PeerConfig{Self: id, Addresses: c.addrs})
	if err != nil {
		c.t.Fatalf("transport for %d: %v", id, err)
	}
	c.t.Cleanup(func() { _ = tr.Close() })

	faults := NewFaultTransport(FaultConfig{
		Self: id, Inner: tr, Clock: RealClock{}, Rand: RealRand{},
	})
	n, err := node.Start(node.Config{
		ID:            id,
		Peers:         members,
		DataDir:       dir,
		Transport:     faults,
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		c.t.Fatalf("starting %d: %v", id, err)
	}
	c.t.Cleanup(func() { _ = n.Stop() })
	tr.SetLocal(n)
	c.nodes[id] = n

	raftServer, err := NewRaftServer(n)
	if err != nil {
		c.t.Fatalf("raft server for %d: %v", id, err)
	}
	gs := grpc.NewServer()
	raftkvv1.RegisterRaftServiceServer(gs, faults.WrapRaftServer(raftServer))
	go func() { _ = gs.Serve(l) }()
	c.t.Cleanup(gs.Stop)
	return n
}

func (c *faultCluster) join(id raft.NodeID) *node.Node {
	c.t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("listen: %v", err)
	}
	c.addrs[id] = l.Addr().String()
	members := append(append([]raft.NodeID{}, c.ids...), id)
	n := c.start(id, members, l, filepath.Join(c.t.TempDir(), "joiner"))
	c.ids = append(c.ids, id)
	return n
}

func (c *faultCluster) eventually(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(grpcSettleTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

func (c *faultCluster) awaitLeader() *node.Node {
	c.t.Helper()
	var found *node.Node
	c.eventually("a single leader", func() bool {
		var leaders []*node.Node
		for _, id := range c.ids {
			if n := c.nodes[id]; n.Status().State == raft.Leader {
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

// 7
func TestEachFaultExpiresIndependentlyAt120s(t *testing.T) {
	f, inner, clock, rnd := newTestFaults(t)
	rnd.draws = []int{0}

	const eps = time.Nanosecond

	f.SetPartition([]raft.NodeID{2}) // set at 0, expires at 120s
	clock.Advance(30 * time.Second)
	f.SetDelay(3, 5*time.Second) // set at 30s, expires at 150s
	clock.Advance(30 * time.Second)
	if err := f.SetDropPercent(4, 100); err != nil { // set at 60s, expires at 180s
		t.Fatalf("SetDropPercent: %v", err)
	}

	probe := func(tag raft.Term) {
		f.Send([]raft.Message{msg(2, tag), msg(3, tag), msg(4, tag)})
	}

	clock.Advance(60*time.Second - eps) // 119.999999999s
	probe(1)
	expectTerms(t, "partition just before expiry", inner.terms(2), nil)
	if f.AllowIngress(2) {
		t.Fatal("ingress from peer 2 allowed just before the partition expires")
	}

	clock.Advance(eps) // 120s: only the partition expires
	probe(2)
	expectTerms(t, "partition after expiry", inner.terms(2), []raft.Term{2})
	if !f.AllowIngress(2) {
		t.Fatal("ingress from peer 2 refused after the partition expired")
	}
	expectTerms(t, "delay still active at 120s", inner.terms(3), nil)
	expectTerms(t, "loss still active at 120s", inner.terms(4), nil)

	clock.Advance(30*time.Second - eps) // 149.999999999s
	probe(3)
	expectTerms(t, "loss still active before 180s", inner.terms(4), nil)

	clock.Advance(eps) // 150s: the delay expires
	inner.mu.Lock()
	inner.sent = nil
	inner.mu.Unlock()
	probe(4)
	expectTerms(t, "delay after expiry passes at once", inner.terms(3), []raft.Term{4})
	expectTerms(t, "loss still active at 150s", inner.terms(4), nil)

	clock.Advance(30*time.Second - eps) // 179.999999999s
	probe(5)
	expectTerms(t, "loss just before expiry", inner.terms(4), nil)

	clock.Advance(eps) // 180s
	probe(6)
	expectTerms(t, "loss after expiry", inner.terms(4), []raft.Term{6})
}

// 8
func TestResetAndClearRaceWithExpiry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		advisory bool
	}{
		{"stopped timers do not fire", false},
		{"a stale timer fires anyway", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("re-set at 100s expires at 220s", func(t *testing.T) {
				f, inner, clock, _ := newTestFaults(t)
				clock.advisoryStop = tc.advisory

				f.SetPartition([]raft.NodeID{2}) // expires 120s
				clock.Advance(100 * time.Second)
				f.SetPartition([]raft.NodeID{2}) // re-set: expires 220s

				clock.Advance(20 * time.Second) // 120s: the first deadline passes
				f.Send([]raft.Message{msg(2, 1)})
				expectTerms(t, "at 120s the re-set partition holds", inner.terms(2), nil)

				clock.Advance(99*time.Second + 999*time.Millisecond) // 219.999s
				f.Send([]raft.Message{msg(2, 2)})
				expectTerms(t, "just before 220s", inner.terms(2), nil)

				clock.Advance(time.Millisecond) // 220s
				f.Send([]raft.Message{msg(2, 3)})
				expectTerms(t, "at 220s", inner.terms(2), []raft.Term{3})
			})

			t.Run("clear at 119s then set at 120s is not expired early", func(t *testing.T) {
				f, inner, clock, _ := newTestFaults(t)
				clock.advisoryStop = tc.advisory

				f.SetPartition([]raft.NodeID{2}) // would expire at 120s
				clock.Advance(119 * time.Second)
				f.ClearFaults()
				clock.Advance(time.Second) // 120s: the cleared fault's deadline
				f.SetPartition([]raft.NodeID{2})

				clock.Advance(time.Second) // 121s
				f.Send([]raft.Message{msg(2, 1)})
				expectTerms(t, "a new fault survives the old deadline", inner.terms(2), nil)

				clock.Advance(118*time.Second + 999*time.Millisecond) // 121s -> 239.999s
				f.Send([]raft.Message{msg(2, 2)})
				expectTerms(t, "just before the new deadline at 240s", inner.terms(2), nil)

				clock.Advance(time.Millisecond) // 240s
				f.Send([]raft.Message{msg(2, 3)})
				expectTerms(t, "at the new deadline", inner.terms(2), []raft.Term{3})
			})
		})
	}
}

// 9
func TestDelayQueueIsBoundedAndDroppedOnExpiryOrClear(t *testing.T) {
	t.Run("overflow is dropped and counted", func(t *testing.T) {
		f, inner, clock, _ := newTestFaults(t)
		f.SetDelay(2, 50*time.Millisecond)

		const extra = 10
		for i := range MaxDelayQueue + extra {
			f.Send([]raft.Message{msg(2, raft.Term(i+1))})
		}
		if got := f.Stats().QueueOverflow; got != extra {
			t.Fatalf("QueueOverflow = %d, want %d", got, extra)
		}

		clock.Advance(50 * time.Millisecond)
		got := inner.terms(2)
		if len(got) != MaxDelayQueue {
			t.Fatalf("delivered %d messages, want the %d that fit", len(got), MaxDelayQueue)
		}
		for i, tag := range got {
			if tag != raft.Term(i+1) {
				t.Fatalf("message %d has tag %d: the queue kept order but lost the head", i, tag)
			}
		}
	})

	t.Run("expiry discards queued messages and later ones pass at once", func(t *testing.T) {
		f, inner, clock, _ := newTestFaults(t)
		f.SetDelay(2, 119*time.Second) // expires at 120s

		clock.Advance(10 * time.Second)
		f.Send([]raft.Message{msg(2, 1)}) // due at 129s, after the fault is gone

		clock.Advance(110 * time.Second) // 120s: the delay expires with no admin call
		if got := f.Stats().Discarded; got != 1 {
			t.Fatalf("Discarded = %d after expiry, want 1", got)
		}

		clock.Advance(time.Second) // 121s
		f.Send([]raft.Message{msg(2, 2)})
		expectTerms(t, "a message after expiry passes at once", inner.terms(2), []raft.Term{2})

		clock.Advance(100 * time.Second) // past the discarded message's old due time
		expectTerms(t, "the discarded message never arrives", inner.terms(2), []raft.Term{2})
	})

	t.Run("clear discards queued messages", func(t *testing.T) {
		f, inner, clock, _ := newTestFaults(t)
		f.SetDelay(3, 50*time.Second)
		f.Send([]raft.Message{msg(3, 1)})

		f.ClearFaults()
		if got := f.Stats().Discarded; got != 1 {
			t.Fatalf("Discarded = %d after clear, want 1", got)
		}

		clock.Advance(time.Minute)
		expectTerms(t, "the discarded message never arrives", inner.terms(3), nil)
	})
}
