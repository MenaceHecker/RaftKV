package transport

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	adminToken = "t0ken-for-admin-tests"

	// adminWait is a failure ceiling for things that must happen; passing runs
	// return as soon as they do.
	adminWait = 10 * time.Second
)

func withToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func adminCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), adminWait)
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return l
}

// discardSender drops everything a node sends.
type discardSender struct{}

func (discardSender) Send([]raft.Message) {}

// spyAdminNode records the calls that would change the node.
type spyAdminNode struct {
	*node.Node

	mu     sync.Mutex
	pauses []time.Duration
	subs   int
}

func (s *spyAdminNode) Pause(d time.Duration) time.Duration {
	s.mu.Lock()
	s.pauses = append(s.pauses, d)
	s.mu.Unlock()
	return s.Node.Pause(d)
}

func (s *spyAdminNode) Subscribe() (*node.Subscription, error) {
	s.mu.Lock()
	s.subs++
	s.mu.Unlock()
	return s.Node.Subscribe()
}

func (s *spyAdminNode) counts() (pauses []time.Duration, subs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.pauses...), s.subs
}

// spyFaults records fault mutations.
type spyFaults struct {
	mu    sync.Mutex
	calls []string
}

func (s *spyFaults) record(format string, a ...any) {
	s.mu.Lock()
	s.calls = append(s.calls, fmt.Sprintf(format, a...))
	s.mu.Unlock()
}

func (s *spyFaults) SetPartition(b []raft.NodeID) { s.record("partition %v", b) }
func (s *spyFaults) SetDelay(p raft.NodeID, d time.Duration) {
	s.record("delay %d %v", p, d)
}
func (s *spyFaults) SetDropPercent(p raft.NodeID, pct int) error {
	s.record("drop %d %d", p, pct)
	return nil
}
func (s *spyFaults) ClearFaults() { s.record("clear") }

func (s *spyFaults) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// exitSpy records process exits instead of performing them.
type exitSpy struct {
	mu    sync.Mutex
	codes []int
}

func (e *exitSpy) exit(code int) {
	e.mu.Lock()
	e.codes = append(e.codes, code)
	e.mu.Unlock()
}

func (e *exitSpy) got() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.codes...)
}

// pending counts timers that have neither fired nor been stopped.
func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.fired && !t.stopped {
			n++
		}
	}
	return n
}

// adminRig is one node (ID 1 of 1,2,3, never elected because its peers do not
// exist) served over real gRPC on a shared listener and an admin listener.
type adminRig struct {
	t      *testing.T
	node   *spyAdminNode
	faults *spyFaults
	clock  *fakeClock
	exits  *exitSpy

	adminAddr, sharedAddr string
	admin                 raftkvv1.AdminServiceClient
	sharedConn            *grpc.ClientConn
}

func newAdminRig(t *testing.T, chaos bool) *adminRig {
	t.Helper()
	n, err := node.Start(node.Config{
		ID:            1,
		Peers:         []raft.NodeID{1, 2, 3},
		DataDir:       t.TempDir(),
		Transport:     discardSender{},
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { _ = n.Stop() })

	r := &adminRig{
		t:      t,
		node:   &spyAdminNode{Node: n},
		faults: &spyFaults{},
		clock:  newFakeClock(),
		exits:  &exitSpy{},
	}

	srv, err := NewAdminServer(AdminConfig{
		Node: r.node, Faults: r.faults, Chaos: chaos, Clock: r.clock, Exit: r.exits.exit,
	})
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}
	unary, stream := NewAdminInterceptors(adminToken, chaos)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	srv.Register(gs)
	l := listen(t)
	r.adminAddr = l.Addr().String()
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)

	// The shared listener carries the client API and, deliberately, no admin.
	kv, err := NewKVServer(n, map[raft.NodeID]string{1: "x"})
	if err != nil {
		t.Fatalf("NewKVServer: %v", err)
	}
	pu, ps := NewPauseGate(n.Paused)
	shared := grpc.NewServer(grpc.ChainUnaryInterceptor(pu), grpc.ChainStreamInterceptor(ps))
	kv.Register(shared)
	sl := listen(t)
	r.sharedAddr = sl.Addr().String()
	go func() { _ = shared.Serve(sl) }()
	t.Cleanup(shared.Stop)

	r.admin = raftkvv1.NewAdminServiceClient(dial(t, r.adminAddr))
	r.sharedConn = dial(t, r.sharedAddr)
	return r
}

type adminCall struct {
	name string
	call func(ctx context.Context, c raftkvv1.AdminServiceClient) error
}

func adminCalls() []adminCall {
	return []adminCall{
		{"Status", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			_, err := c.Status(ctx, &raftkvv1.AdminStatusRequest{})
			return err
		}},
		{"Crash", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			_, err := c.Crash(ctx, &raftkvv1.CrashRequest{})
			return err
		}},
		{"Pause", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			_, err := c.Pause(ctx, &raftkvv1.PauseRequest{DurationMs: 5000})
			return err
		}},
		{"SetPartition", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			_, err := c.SetPartition(ctx, &raftkvv1.SetPartitionRequest{Blocked: []uint64{2}})
			return err
		}},
		{"SetNetem", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			_, err := c.SetNetem(ctx, &raftkvv1.SetNetemRequest{Peer: 2, DelayMs: 50, DropPercent: 10})
			return err
		}},
		{"Events", func(ctx context.Context, c raftkvv1.AdminServiceClient) error {
			stream, err := c.Events(ctx, &raftkvv1.EventsRequest{})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		}},
	}
}

func wantCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Errorf("%s: code %v (%v), want %v", what, got, err, want)
	}
}

// 24: with no token or a wrong one, every admin method and the Events stream
// fail with the same code and message, and nothing is changed or scheduled.
func TestAdminRPCsRejectMissingAndWrongTokenAndChangeNothing(t *testing.T) {
	r := newAdminRig(t, true)

	messages := map[string]bool{}
	for _, c := range adminCalls() {
		for _, creds := range []struct{ name, token string }{{"no token", ""}, {"wrong token", "not-" + adminToken}} {
			ctx, cancel := adminCtx(t)
			if creds.token != "" {
				ctx = withToken(ctx, creds.token)
			}
			err := c.call(ctx, r.admin)
			cancel()
			wantCode(t, c.name+" with "+creds.name, err, codes.Unauthenticated)
			messages[status.Convert(err).Message()] = true
		}
	}
	if len(messages) != 1 {
		t.Errorf("missing and wrong tokens must give one identical message, got %d distinct: %v", len(messages), messages)
	}
	for msg := range messages {
		if msg != "" && strings.Contains(msg, adminToken) {
			t.Errorf("the rejection message leaks the token: %q", msg)
		}
	}

	pauses, subs := r.node.counts()
	if len(pauses) != 0 || subs != 0 {
		t.Errorf("rejected calls reached the node: pauses %v, subscriptions %d", pauses, subs)
	}
	if calls := r.faults.got(); len(calls) != 0 {
		t.Errorf("rejected calls set faults: %v", calls)
	}
	if n := r.clock.pending(); n != 0 {
		t.Errorf("rejected calls scheduled %d timers", n)
	}
	r.clock.Advance(time.Minute)
	if got := r.exits.got(); len(got) != 0 {
		t.Errorf("a rejected Crash exited: %v", got)
	}
}

// 25: the right token works, and each call reaches the node or the wrapper
// with the arguments it was given.
func TestTokenOKWorks(t *testing.T) {
	r := newAdminRig(t, true)
	ctx, cancel := adminCtx(t)
	defer cancel()
	ctx = withToken(ctx, adminToken)

	st, err := r.admin.Status(ctx, &raftkvv1.AdminStatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetNodeId() != 1 {
		t.Errorf("Status node_id = %d, want 1", st.GetNodeId())
	}

	const pauseMs = 2500 // the wire unit; compared below as a duration, not waited on
	if _, err := r.admin.Pause(ctx, &raftkvv1.PauseRequest{DurationMs: pauseMs}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if _, err := r.admin.SetPartition(ctx, &raftkvv1.SetPartitionRequest{Blocked: []uint64{2, 3}}); err != nil {
		t.Fatalf("SetPartition: %v", err)
	}
	if _, err := r.admin.SetNetem(ctx, &raftkvv1.SetNetemRequest{Peer: 2, DelayMs: 40, DropPercent: 25}); err != nil {
		t.Fatalf("SetNetem: %v", err)
	}

	pauses, _ := r.node.counts()
	if len(pauses) != 1 || pauses[0] != time.Duration(pauseMs)*time.Millisecond {
		t.Errorf("node saw pauses %v, want [2.5s]", pauses)
	}
	got := r.faults.got()
	want := []string{"partition [2 3]", "delay 2 40ms", "drop 2 25"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("wrapper saw %v, want %v", got, want)
	}

	// The node is never elected, so it times out into a pre-candidate: an event.
	stream, err := r.admin.Events(ctx, &raftkvv1.EventsRequest{})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Events Recv: %v", err)
	}
	if ev.GetKind() != raftkvv1.EventKind_EVENT_KIND_STATE || ev.GetNode() != 1 || ev.GetSeq() == 0 {
		t.Errorf("first event = %+v, want a STATE event from node 1 with a seq", ev)
	}
}

// 26: without --admin-chaos the fault RPCs are refused before anything happens;
// the read-only ones still work.
func TestChaosRPCsNeedAdminChaos(t *testing.T) {
	r := newAdminRig(t, false)
	ctx, cancel := adminCtx(t)
	defer cancel()
	ctx = withToken(ctx, adminToken)

	for _, c := range adminCalls() {
		err := c.call(ctx, r.admin)
		switch c.name {
		case "Status", "Events":
			if err != nil {
				t.Errorf("%s without chaos: %v, want success", c.name, err)
			}
		default:
			wantCode(t, c.name+" without chaos", err, codes.PermissionDenied)
		}
	}

	pauses, _ := r.node.counts()
	if len(pauses) != 0 {
		t.Errorf("a refused Pause reached the node: %v", pauses)
	}
	if calls := r.faults.got(); len(calls) != 0 {
		t.Errorf("refused calls set faults: %v", calls)
	}
	if n := r.clock.pending(); n != 0 {
		t.Errorf("a refused Crash scheduled %d timers", n)
	}
	r.clock.Advance(time.Minute)
	if got := r.exits.got(); len(got) != 0 {
		t.Errorf("a refused Crash exited: %v", got)
	}
}

// 27: the server will not start the admin listener without a usable token.
func TestServerRefusesAdminListenWithoutToken(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	blank := filepath.Join(dir, "blank")
	good := filepath.Join(dir, "good")
	for path, body := range map[string]string{empty: "", blank: " \n\t\n", good: "  " + adminToken + "\n"} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct{ name, path string }{
		{"unset", ""},
		{"missing", filepath.Join(dir, "does-not-exist")},
		{"empty", empty},
		{"only whitespace", blank},
	} {
		if tok, err := LoadAdminToken(tc.path); err == nil {
			t.Errorf("%s: LoadAdminToken accepted it and returned %q, want an error", tc.name, tok)
		}
	}

	tok, err := LoadAdminToken(good)
	if err != nil || tok != adminToken {
		t.Errorf("a good token file: got %q, %v; want %q, nil (surrounding whitespace trimmed)", tok, err, adminToken)
	}
}

// 28: Crash replies, then exits with 137 after 50ms of injected-clock time, with
// no clean shutdown. A rejected Crash schedules nothing.
func TestCrashExitsAfter50msWith137(t *testing.T) {
	t.Run("authorised", func(t *testing.T) {
		r := newAdminRig(t, true)
		ctx, cancel := adminCtx(t)
		defer cancel()
		if _, err := r.admin.Crash(withToken(ctx, adminToken), &raftkvv1.CrashRequest{}); err != nil {
			t.Fatalf("Crash: %v", err)
		}
		if got := r.exits.got(); len(got) != 0 {
			t.Fatalf("exited %v before any time passed", got)
		}
		r.clock.Advance(CrashDelay - time.Nanosecond)
		if got := r.exits.got(); len(got) != 0 {
			t.Fatalf("exited %v 1ns before %v", got, CrashDelay)
		}
		r.clock.Advance(time.Nanosecond)
		if got := r.exits.got(); len(got) != 1 || got[0] != CrashExitCode {
			t.Fatalf("exits = %v at %v, want exactly [%d]", got, CrashDelay, CrashExitCode)
		}
		r.clock.Advance(time.Minute)
		if got := r.exits.got(); len(got) != 1 {
			t.Fatalf("exits = %v, want one", got)
		}
		if r.node.Stopped() {
			t.Error("Crash stopped the node cleanly; it must just exit")
		}
	})

	t.Run("unauthenticated", func(t *testing.T) {
		r := newAdminRig(t, true)
		ctx, cancel := adminCtx(t)
		defer cancel()
		_, err := r.admin.Crash(ctx, &raftkvv1.CrashRequest{})
		wantCode(t, "Crash with no token", err, codes.Unauthenticated)
		if n := r.clock.pending(); n != 0 {
			t.Errorf("scheduled %d timers", n)
		}
		r.clock.Advance(time.Minute)
		if got := r.exits.got(); len(got) != 0 {
			t.Errorf("exited %v", got)
		}
	})

	t.Run("chaos disabled", func(t *testing.T) {
		r := newAdminRig(t, false)
		ctx, cancel := adminCtx(t)
		defer cancel()
		_, err := r.admin.Crash(withToken(ctx, adminToken), &raftkvv1.CrashRequest{})
		wantCode(t, "Crash without chaos", err, codes.PermissionDenied)
		if n := r.clock.pending(); n != 0 {
			t.Errorf("scheduled %d timers", n)
		}
		r.clock.Advance(time.Minute)
		if got := r.exits.got(); len(got) != 0 {
			t.Errorf("exited %v", got)
		}
	})
}

// 29: admin is served only on the admin endpoint. The positive control: the
// authenticated admin endpoint answers Status, so a failure on the shared
// endpoint means "not served there", not "admin is broken".
func TestAdminOnlyOnAdminEndpoint(t *testing.T) {
	r := newAdminRig(t, true)
	ctx, cancel := adminCtx(t)
	defer cancel()
	ctx = withToken(ctx, adminToken)

	if _, err := r.admin.Status(ctx, &raftkvv1.AdminStatusRequest{}); err != nil {
		t.Fatalf("positive control: Status on the admin endpoint: %v", err)
	}

	shared := raftkvv1.NewAdminServiceClient(r.sharedConn)
	for _, c := range adminCalls() {
		err := c.call(ctx, shared)
		if err == nil {
			t.Errorf("%s succeeded on the shared endpoint", c.name)
			continue
		}
		wantCode(t, c.name+" on the shared endpoint", err, codes.Unimplemented)
	}
	if pauses, subs := r.node.counts(); len(pauses) != 0 || subs != 0 {
		t.Errorf("calls on the shared endpoint reached the node: pauses %v, subs %d", pauses, subs)
	}
}

// 21: Status and Events keep working while the node is paused.
func TestAdminStatusAndEventsWorkWhilePaused(t *testing.T) {
	r := newAdminRig(t, true)
	ctx, cancel := adminCtx(t)
	defer cancel()
	ctx = withToken(ctx, adminToken)

	r.node.Node.Pause(time.Minute) // set up directly: this test is about what still works
	if !r.node.Paused() {
		t.Fatal("the node is not paused")
	}

	st, err := r.admin.Status(ctx, &raftkvv1.AdminStatusRequest{})
	if err != nil {
		t.Fatalf("Status while paused: %v", err)
	}
	if !st.GetPaused() {
		t.Error("Status.paused = false while paused")
	}

	stream, err := r.admin.Events(ctx, &raftkvv1.EventsRequest{})
	if err != nil {
		t.Fatalf("Events while paused: %v", err)
	}
	// A paused node does not tick, so make a transition happen from outside.
	r.node.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: 50})
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Events Recv while paused: %v", err)
	}
	if ev.GetTerm() != 50 {
		t.Errorf("event term = %d, want 50", ev.GetTerm())
	}
}

// adminCluster is a gRPC cluster built from the production pieces: PeerTransport
// wrapped in FaultTransport, a Raft endpoint filtered by the same wrapper, the
// pause gate on the shared listener, and a separate authenticated admin listener.
type adminCluster struct {
	t *testing.T

	ids    []raft.NodeID
	addrs  map[raft.NodeID]string // shared listeners
	admins map[raft.NodeID]string
	nodes  map[raft.NodeID]*node.Node
	faults map[raft.NodeID]*FaultTransport
}

func newAdminCluster(t *testing.T, size int) *adminCluster {
	t.Helper()
	c := &adminCluster{
		t:      t,
		addrs:  map[raft.NodeID]string{},
		admins: map[raft.NodeID]string{},
		nodes:  map[raft.NodeID]*node.Node{},
		faults: map[raft.NodeID]*FaultTransport{},
	}
	shared := map[raft.NodeID]net.Listener{}
	adminL := map[raft.NodeID]net.Listener{}
	for i := range size {
		id := raft.NodeID(i + 1)
		c.ids = append(c.ids, id)
		shared[id], adminL[id] = listen(t), listen(t)
		c.addrs[id], c.admins[id] = shared[id].Addr().String(), adminL[id].Addr().String()
	}

	root := t.TempDir()
	for _, id := range c.ids {
		peer, err := NewPeerTransport(PeerConfig{Self: id, Addresses: c.addrs})
		if err != nil {
			t.Fatalf("PeerTransport %d: %v", id, err)
		}
		t.Cleanup(func() { _ = peer.Close() })

		var self *node.Node
		ft := NewFaultTransport(FaultConfig{
			Self: id, Inner: peer, Clock: RealClock{}, Rand: RealRand{},
			Paused: func() bool { return self != nil && self.Paused() },
		})
		c.faults[id] = ft

		n, err := node.Start(node.Config{
			ID: id, Peers: c.ids, DataDir: filepath.Join(root, fmt.Sprint("n", id)),
			Transport: ft, TickInterval: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 1,
			Sync: storage.SyncNever,
		})
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		self = n
		c.nodes[id] = n
		t.Cleanup(func() { _ = n.Stop() })
		peer.SetLocal(n)

		raftSrv, err := NewRaftServer(n)
		if err != nil {
			t.Fatal(err)
		}
		kv, err := NewKVServer(n, c.addrs)
		if err != nil {
			t.Fatal(err)
		}
		pu, ps := NewPauseGate(n.Paused)
		gs := grpc.NewServer(grpc.ChainUnaryInterceptor(pu), grpc.ChainStreamInterceptor(ps))
		raftkvv1.RegisterRaftServiceServer(gs, ft.WrapRaftServer(raftSrv))
		kv.Register(gs)
		go func() { _ = gs.Serve(shared[id]) }()
		t.Cleanup(gs.Stop)

		adm, err := NewAdminServer(AdminConfig{
			Node: n, Faults: ft, Chaos: true, Clock: RealClock{}, Exit: func(int) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		au, as := NewAdminInterceptors(adminToken, true)
		ags := grpc.NewServer(grpc.ChainUnaryInterceptor(au), grpc.ChainStreamInterceptor(as))
		adm.Register(ags)
		go func() { _ = ags.Serve(adminL[id]) }()
		t.Cleanup(ags.Stop)
	}
	return c
}

func (c *adminCluster) adminClient(id raft.NodeID) raftkvv1.AdminServiceClient {
	return raftkvv1.NewAdminServiceClient(dial(c.t, c.admins[id]))
}

func (c *adminCluster) eventually(what string, cond func() bool) {
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

// leaders returns the nodes among ids that currently lead.
func (c *adminCluster) leaders(ids ...raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range ids {
		if c.nodes[id].Status().State == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

func (c *adminCluster) awaitLeader() raft.NodeID {
	c.t.Helper()
	var id raft.NodeID
	c.eventually("a leader", func() bool {
		l := c.leaders(c.ids...)
		if len(l) != 1 {
			return false
		}
		id = l[0]
		return true
	})
	return id
}

func (c *adminCluster) others(id raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, o := range c.ids {
		if o != id {
			out = append(out, o)
		}
	}
	return out
}

// 17: a paused node drops peer messages in both directions: inbound Deliver and
// DeliverSnapshot (including a stream opened before the pause) and outbound
// sends. Resuming restores both.
func TestPauseStopsTicksAndDropsBothDirections(t *testing.T) {
	c := newAdminCluster(t, 3)
	leader := c.awaitLeader()
	victim := c.others(leader)[0]
	peer := c.others(victim)[0]

	raftClient := raftkvv1.NewRaftServiceClient(dial(t, c.addrs[victim]))
	hb := func() *raftkvv1.DeliverRequest {
		w, err := MessageToWire(raft.Message{Type: raft.MsgHeartbeat, From: peer, To: victim, Term: 1})
		if err != nil {
			t.Fatal(err)
		}
		return &raftkvv1.DeliverRequest{Message: w}
	}
	header := func() *raftkvv1.SnapshotChunk {
		w, err := MessageToWire(raft.Message{Type: raft.MsgInstallSnapshot, From: peer, To: victim, Term: 1,
			Snapshot: &raft.Snapshot{Index: 1, Term: 1}})
		if err != nil {
			t.Fatal(err)
		}
		return &raftkvv1.SnapshotChunk{Frame: &raftkvv1.SnapshotChunk_Header{Header: w}}
	}

	ctx, cancel := adminCtx(t)
	defer cancel()

	// A snapshot stream opened before the pause: its header gets through.
	early, err := raftClient.DeliverSnapshot(ctx)
	if err != nil {
		t.Fatalf("opening the early snapshot stream: %v", err)
	}
	if err := early.Send(header()); err != nil {
		t.Fatalf("early header: %v", err)
	}

	if _, err := c.adminClient(victim).Pause(withToken(ctx, adminToken), &raftkvv1.PauseRequest{DurationMs: 30000}); err != nil {
		t.Fatalf("Pause over the admin API: %v", err)
	}
	if !c.nodes[victim].Paused() {
		t.Fatal("the node is not paused after the Pause RPC")
	}

	// Inbound.
	_, err = raftClient.Deliver(ctx, hb())
	wantCode(t, "Deliver to a paused node", err, codes.Unavailable)

	fresh, err := raftClient.DeliverSnapshot(ctx)
	if err != nil {
		t.Fatalf("opening a snapshot stream: %v", err)
	}
	_ = fresh.Send(header())
	_, err = fresh.CloseAndRecv()
	wantCode(t, "DeliverSnapshot to a paused node", err, codes.Unavailable)

	_ = early.Send(&raftkvv1.SnapshotChunk{Frame: &raftkvv1.SnapshotChunk_Data{Data: []byte("more")}})
	_, err = early.CloseAndRecv()
	wantCode(t, "a stream opened before the pause, at its next chunk", err, codes.Unavailable)

	// Outbound.
	before := c.faults[victim].Stats().Dropped
	c.faults[victim].Send([]raft.Message{{Type: raft.MsgHeartbeat, From: victim, To: peer, Term: 1}})
	if got := c.faults[victim].Stats().Dropped - before; got != 1 {
		t.Errorf("a paused node dropped %d outbound messages, want 1", got)
	}

	// Resume restores inbound.
	if _, err := c.adminClient(victim).Pause(withToken(ctx, adminToken), &raftkvv1.PauseRequest{DurationMs: 0}); err != nil {
		t.Fatalf("resuming: %v", err)
	}
	if _, err := raftClient.Deliver(ctx, hb()); err != nil {
		t.Errorf("Deliver after Resume: %v", err)
	}
}

// 20: every KVService and ClusterService method, enumerated from the service
// descriptors so a new method cannot be forgotten, fails Unavailable while paused.
func TestEveryKVAndClusterMethodIsUnavailableWhilePaused(t *testing.T) {
	c := newAdminCluster(t, 3)
	id := c.ids[0]
	conn := dial(t, c.addrs[id])

	var methods []string
	for _, d := range []grpc.ServiceDesc{raftkvv1.KVService_ServiceDesc, raftkvv1.ClusterService_ServiceDesc} {
		for _, m := range d.Methods {
			methods = append(methods, "/"+d.ServiceName+"/"+m.MethodName)
		}
		for _, s := range d.Streams {
			methods = append(methods, "/"+d.ServiceName+"/"+s.StreamName)
		}
	}
	if len(methods) < 7 {
		t.Fatalf("enumerated only %v from the descriptors", methods)
	}

	c.nodes[id].Pause(time.Minute) // set directly: this test is about the gate
	if !c.nodes[id].Paused() {
		t.Fatal("the node is not paused")
	}

	ctx, cancel := adminCtx(t)
	defer cancel()
	for _, m := range methods {
		err := conn.Invoke(ctx, m, &emptypb.Empty{}, &emptypb.Empty{})
		if status.Code(err) != codes.Unavailable {
			t.Errorf("%s while paused: %v, want Unavailable", m, err)
		}
	}

	// Positive control: after Resume the gate opens and a real call works.
	c.nodes[id].Resume()
	if _, err := raftkvv1.NewKVServiceClient(conn).Status(ctx, &raftkvv1.StatusRequest{}); err != nil {
		t.Errorf("KVService.Status after Resume: %v", err)
	}
}

// 30: a leader cut off through the authenticated admin API and the production
// wrapper loses leadership; the rest elect a new one; after the heal there is
// one leader and reads and writes work again.
func TestPartitionedLeaderLosesLeadership(t *testing.T) {
	c := newAdminCluster(t, 3)
	old := c.awaitLeader()
	rest := c.others(old)

	ctx, cancel := context.WithTimeout(context.Background(), 2*grpcSettleTimeout)
	defer cancel()
	admin := c.adminClient(old)

	blocked := make([]uint64, 0, len(rest))
	for _, id := range rest {
		blocked = append(blocked, uint64(id))
	}
	if _, err := admin.SetPartition(withToken(ctx, adminToken), &raftkvv1.SetPartitionRequest{Blocked: blocked}); err != nil {
		t.Fatalf("SetPartition through the admin API: %v", err)
	}

	c.eventually("the cut-off leader to step down", func() bool {
		return c.nodes[old].Status().State != raft.Leader
	})
	var fresh raft.NodeID
	c.eventually("the other nodes to elect a leader", func() bool {
		l := c.leaders(rest...)
		if len(l) != 1 {
			return false
		}
		fresh = l[0]
		return true
	})
	if c.nodes[fresh].Status().Term <= 1 {
		t.Errorf("the new leader's term is %d, want a later term", c.nodes[fresh].Status().Term)
	}

	if _, err := admin.SetPartition(withToken(ctx, adminToken), &raftkvv1.SetPartitionRequest{}); err != nil {
		t.Fatalf("healing through the admin API: %v", err)
	}
	var healed raft.NodeID
	c.eventually("one leader after the heal", func() bool {
		l := c.leaders(c.ids...)
		if len(l) != 1 {
			return false
		}
		healed = l[0]
		return true
	})

	kv := raftkvv1.NewKVServiceClient(dial(t, c.addrs[healed]))
	c.eventually("a write through the healed cluster", func() bool {
		_, err := kv.Put(ctx, &raftkvv1.PutRequest{
			Client: &raftkvv1.ClientRequest{ClientId: 9, Sequence: 1}, Key: "after-heal", Value: []byte("ok"),
		})
		return err == nil
	})
	got, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: "after-heal"})
	if err != nil || string(got.GetValue()) != "ok" {
		t.Errorf("read after the heal: %q, %v; want ok", got.GetValue(), err)
	}
}
