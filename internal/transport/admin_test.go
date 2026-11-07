package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

	mu      sync.Mutex
	pauses  []time.Duration
	subs    int // Subscribe attempts
	refused int // attempts that failed
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
	sub, err := s.Node.Subscribe()
	if err != nil {
		s.mu.Lock()
		s.refused++
		s.mu.Unlock()
	}
	return sub, err
}

func (s *spyAdminNode) subCounts() (attempts, refused int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs, s.refused
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
	pokes                 atomic.Uint64
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
		ElectionTick:  100000, // no spontaneous elections: tests trigger the transitions they need
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

// poke makes the node observe one new (term, role, leader), so a subscriber gets
// an event. The terms rise on every call.
func (r *adminRig) poke() {
	r.t.Helper()
	term := raft.Term(5000 + r.pokes.Add(1))
	r.node.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: term})
}

// waitSubscribed waits until the node has seen n Subscribe attempts. The Events
// handler subscribes after the RPC call returns, so a test that triggers a
// transition must wait, or the event could precede the subscription and be missed.
func (r *adminRig) waitSubscribed(n int) {
	r.t.Helper()
	for deadline := time.Now().Add(adminWait); ; {
		if attempts, _ := r.node.subCounts(); attempts >= n {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the Events handler never subscribed (wanted %d attempts)", n)
		}
		time.Sleep(time.Millisecond)
	}
}

// firstEvent opens an Events stream, waits for the subscription, triggers a
// transition, and returns the first event.
func (r *adminRig) firstEvent(ctx context.Context) (*raftkvv1.Event, error) {
	r.t.Helper()
	attempts, _ := r.node.subCounts()
	stream, err := r.admin.Events(ctx, &raftkvv1.EventsRequest{})
	if err != nil {
		return nil, err
	}
	r.waitSubscribed(attempts + 1)
	r.poke()
	return stream.Recv()
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

	ev, err := r.firstEvent(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
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
		if c.name == "Events" {
			if _, err := r.firstEvent(ctx); err != nil {
				t.Errorf("Events without chaos: %v, want success", err)
			}
			continue
		}
		err := c.call(ctx, r.admin)
		switch c.name {
		case "Status":
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

	ev, err := r.firstEvent(ctx) // a paused node does not tick; firstEvent triggers the transition from outside
	if err != nil {
		t.Fatalf("Events while paused: %v", err)
	}
	if ev.GetTerm() < 5000 {
		t.Errorf("event term = %d, want the poked term", ev.GetTerm())
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

// fixedNode serves a canned Status so the mapping to the wire can be checked
// field by field, including states a live node is hard to hold in.
type fixedNode struct {
	st     node.Status
	paused bool
}

func (f fixedNode) Status() node.Status { return f.st }
func (f fixedNode) Subscribe() (*node.Subscription, error) {
	return nil, errors.New("fixedNode: no events")
}
func (f fixedNode) Pause(time.Duration) time.Duration { return 0 }
func (f fixedNode) Paused() bool                      { return f.paused }

func statusViaGRPC(t *testing.T, f fixedNode) *raftkvv1.AdminStatusResponse {
	t.Helper()
	srv, err := NewAdminServer(AdminConfig{Node: f, Faults: &spyFaults{}, Clock: newFakeClock(), Exit: func(int) {}})
	if err != nil {
		t.Fatalf("NewAdminServer: %v", err)
	}
	u, st := NewAdminInterceptors(adminToken, false)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(u), grpc.ChainStreamInterceptor(st))
	srv.Register(gs)
	l := listen(t)
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)

	ctx, cancel := adminCtx(t)
	defer cancel()
	resp, err := raftkvv1.NewAdminServiceClient(dial(t, l.Addr().String())).
		Status(withToken(ctx, adminToken), &raftkvv1.AdminStatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return resp
}

// Decision 3: Status carries the KVService fields plus members, joint and the
// first, last and snapshot indexes, and says whether the node is paused.
func TestAdminStatusMapsEveryField(t *testing.T) {
	joint := statusViaGRPC(t, fixedNode{paused: true, st: node.Status{
		ID: 1, Leader: 2, Term: 7, State: raft.Follower, Commit: 9, Applied: 8,
		Members: raft.ConfState{
			Voters: []raft.NodeID{1, 2, 3}, Incoming: []raft.NodeID{1, 2, 4}, Joint: true,
			Addrs: map[raft.NodeID]string{2: "b:2", 4: "d:4"},
		},
		FirstIndex: 4, LastIndex: 12, SnapshotIndex: 3,
	}})
	if joint.GetNodeId() != 1 || joint.GetLeaderId() != 2 || joint.GetLeaderAddress() != "b:2" ||
		joint.GetTerm() != 7 || joint.GetState() != raftkvv1.NodeState_NODE_STATE_FOLLOWER ||
		joint.GetCommitIndex() != 9 || joint.GetAppliedIndex() != 8 {
		t.Errorf("KVService fields: %+v", joint)
	}
	if !joint.GetJoint() {
		t.Error("joint = false for a joint configuration")
	}
	if joint.GetFirstIndex() != 4 || joint.GetLastIndex() != 12 || joint.GetSnapshotIndex() != 3 {
		t.Errorf("indexes first %d last %d snapshot %d, want 4, 12, 3",
			joint.GetFirstIndex(), joint.GetLastIndex(), joint.GetSnapshotIndex())
	}
	if !joint.GetPaused() {
		t.Error("paused = false for a paused node")
	}
	got := map[uint64]string{}
	for _, m := range joint.GetMembers() {
		got[m.GetId()] = m.GetAddress()
	}
	if want := (map[uint64]string{1: "", 2: "b:2", 4: "d:4"}); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("members %v, want the incoming set %v (as ListMembers reports during a joint configuration)", got, want)
	}

	plain := statusViaGRPC(t, fixedNode{st: node.Status{
		ID: 3, Leader: 3, State: raft.Leader, Members: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}},
	}})
	if plain.GetJoint() || plain.GetPaused() || len(plain.GetMembers()) != 3 ||
		plain.GetState() != raftkvv1.NodeState_NODE_STATE_LEADER {
		t.Errorf("plain configuration: %+v", plain)
	}
	if plain.GetFirstIndex() != 0 || plain.GetLastIndex() != 0 || plain.GetSnapshotIndex() != 0 {
		t.Errorf("an empty log must report zero indexes, got %+v", plain)
	}
}

// The token file must not be readable by group or others, and the error must
// say so without ever showing what the file holds.
func TestTokenFileMustNotBeGroupOrWorldReadable(t *testing.T) {
	dir := t.TempDir()
	for _, mode := range []os.FileMode{0o640, 0o604, 0o644, 0o660, 0o666, 0o607} {
		path := filepath.Join(dir, fmt.Sprintf("tok-%o", mode))
		if err := os.WriteFile(path, []byte(adminToken+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		tok, err := LoadAdminToken(path)
		if err == nil {
			t.Errorf("mode %o: accepted, want a refusal", mode)
			continue
		}
		if tok != "" || strings.Contains(err.Error(), adminToken) {
			t.Errorf("mode %o: token %q returned or leaked in %q", mode, tok, err)
		}
	}
	for _, mode := range []os.FileMode{0o600, 0o400} {
		path := filepath.Join(dir, fmt.Sprintf("ok-%o", mode))
		if err := os.WriteFile(path, []byte(adminToken), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if tok, err := LoadAdminToken(path); err != nil || tok != adminToken {
			t.Errorf("mode %o: got %q, %v; want the token", mode, tok, err)
		}
	}
}

// tryEvents opens an Events stream and reports whether the server admitted it.
// The stream stays open until its returned cancel is called.
func (r *adminRig) tryEvents() (admitted bool, cancel context.CancelFunc) {
	r.t.Helper()
	return r.tryEventsOn(r.admin)
}

func (r *adminRig) tryEventsOn(client raftkvv1.AdminServiceClient) (admitted bool, cancel context.CancelFunc) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(withToken(context.Background(), adminToken))
	r.t.Cleanup(cancel)
	beforeAttempts, beforeRefused := r.node.subCounts()
	stream, err := client.Events(ctx, &raftkvv1.EventsRequest{})
	if err != nil {
		r.t.Fatalf("opening Events: %v", err)
	}
	deadline := time.Now().Add(adminWait)
	for {
		attempts, refused := r.node.subCounts()
		if attempts > beforeAttempts {
			if refused > beforeRefused {
				_, err := stream.Recv()
				wantCode(r.t, "a refused Events stream", err, codes.ResourceExhausted)
				return false, cancel
			}
			return true, cancel
		}
		if time.Now().After(deadline) {
			r.t.Fatal("the Events call never reached the node")
		}
		time.Sleep(time.Millisecond)
	}
}

// Events is bounded: node.MaxSubscribers streams are admitted and the next is
// refused with ResourceExhausted. A stream that goes away gives its slot back.
func TestEventsAreBoundedOverGRPC(t *testing.T) {
	r := newAdminRig(t, false)

	var cancels []context.CancelFunc
	for i := 0; i < node.MaxSubscribers; i++ {
		ok, cancel := r.tryEvents()
		if !ok {
			t.Fatalf("stream %d of %d refused", i+1, node.MaxSubscribers)
		}
		cancels = append(cancels, cancel)
	}
	if ok, _ := r.tryEvents(); ok {
		t.Fatalf("stream %d admitted, want ResourceExhausted at the cap", node.MaxSubscribers+1)
	}

	cancels[0]() // a client disconnects
	deadline := time.Now().Add(adminWait)
	for {
		if ok, _ := r.tryEvents(); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the slot of a disconnected stream was never released")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A subscriber that stops reading is dropped, not waited for, and its slot is
// released: with every slot held by a client that never reads, enough events
// overflow the buffers and a new stream is admitted again.
func TestSlowEventsClientIsDroppedAndItsSlotReleasedOverGRPC(t *testing.T) {
	r := newAdminRig(t, false)

	// A tiny static flow-control window, so a client that never reads backs the
	// server's sends up after a few events instead of after megabytes.
	conn, err := grpc.NewClient(r.adminAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStaticStreamWindowSize(1024), grpc.WithStaticConnWindowSize(1024))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	slow := raftkvv1.NewAdminServiceClient(conn)

	for i := 0; i < node.MaxSubscribers; i++ {
		if ok, _ := r.tryEventsOn(slow); !ok { // never read from these streams
			t.Fatalf("stream %d refused", i+1)
		}
	}
	if ok, _ := r.tryEvents(); ok {
		t.Fatal("admitted past the cap")
	}

	// Each higher term is one event. gRPC queues up to about 64 KiB of unsent
	// data per stream (some 1,500 events) before the server's send blocks; after
	// that the per-subscriber buffer (node.SubscriberBuffer) fills and overflows.
	term := raft.Term(1000)
	for batch := 0; batch < 60; batch++ {
		for i := 0; i < 100; i++ {
			term++
			r.node.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: term})
			for r.node.Status().Term < term {
				time.Sleep(50 * time.Microsecond)
			}
		}
		if ok, _ := r.tryEvents(); ok {
			return
		}
	}
	t.Fatal("after 6000 events no slow subscriber was dropped, so no slot was released")
}
