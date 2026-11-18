package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/client"
	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// ---- KV over a real cluster ----

func TestLiveKVRoundTrip(t *testing.T) {
	r := newRig(t, true)
	r.leader()
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()

	kv := l.NewKV()
	if _, found, err := kv.Get(ctx, "k"); err != nil || found {
		t.Fatalf("Get of a missing key = found %v, err %v", found, err)
	}
	if err := kv.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := kv.Get(ctx, "k")
	if err != nil || !found || string(v) != "hello" {
		t.Fatalf("Get = %q, %v, %v; want hello", v, found, err)
	}
	if err := kv.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, err := kv.Get(ctx, "k"); err != nil || found {
		t.Fatalf("Get after Delete = found %v, err %v", found, err)
	}
}

func TestLiveSessionsHaveRandomDistinctClientIDs(t *testing.T) {
	r := newRig(t, true)
	type withID interface{ ClientID() uint64 }
	seen := map[uint64]bool{}
	for range 2 { // two adapters: ids must not repeat across processes either
		l := r.live(t)
		for range gateway.DefaultPoolSize {
			kv, ok := l.NewKV().(withID)
			if !ok {
				t.Fatal("the live KV does not expose its client id (ClientID() uint64)")
			}
			id := kv.ClientID()
			if id == 0 || id == 1 {
				t.Fatalf("client id %d is zero or a counter, want a random non-zero id", id)
			}
			if seen[id] {
				t.Fatalf("client id %d handed out twice", id)
			}
			seen[id] = true
		}
	}
}

func TestLiveLeaderKilledMidUseStillCompletes(t *testing.T) {
	r := newRig(t, true)
	old := r.leader()
	l := r.live(t)
	kv := l.NewKV()

	const total, killAt = 60, 20
	for i := range total {
		if i == killAt {
			r.kill(old)
		}
		key, val := fmt.Sprintf("k%02d", i), []byte(fmt.Sprintf("v%02d", i))
		ctx, cancel := opCtx(t)
		var err error
		for try := 0; try < 5; try++ {
			err = kv.Put(ctx, key, val)
			if !errors.Is(err, gateway.ErrIndeterminate) && !errors.Is(err, gateway.ErrNoLeader) {
				break // done, or an error that is not part of failover
			}
		}
		cancel()
		if err != nil {
			t.Fatalf("Put %s (leader %d killed at %d): %v", key, old, killAt, err)
		}
	}
	ctx, cancel := opCtx(t)
	defer cancel()
	for i := range total {
		v, found, err := kv.Get(ctx, fmt.Sprintf("k%02d", i))
		if err != nil || !found || string(v) != fmt.Sprintf("v%02d", i) {
			t.Fatalf("k%02d = %q, %v, %v after failover", i, v, found, err)
		}
	}
}

// ---- Status and faults over real admin RPCs ----

func TestLiveStatusMapsFields(t *testing.T) {
	r := newRig(t, true)
	lead := r.leader()
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	if err := l.NewKV().Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	for _, id := range r.ids {
		eventually(t, fmt.Sprintf("node %d status to match the node", id), func() bool {
			got, err := l.Status(ctx, uint64(id))
			if err != nil {
				t.Fatalf("Status(%d): %v", id, err)
			}
			st := r.nodes[id].n.Status()
			role := map[raft.State]string{raft.Leader: "leader", raft.Follower: "follower", raft.Candidate: "candidate"}[st.State]
			want := gateway.NodeStatus{
				ID: uint64(id), LeaderID: uint64(st.Leader), Term: uint64(st.Term), Role: role,
				CommitIndex: uint64(st.Commit), AppliedIndex: uint64(st.Applied),
				FirstIndex: uint64(st.FirstIndex), LastIndex: uint64(st.LastIndex), SnapshotIndex: uint64(st.SnapshotIndex),
			}
			return got == want && got.CommitIndex > 0 && got.LastIndex > 0
		})
	}
	st, err := l.Status(ctx, uint64(lead))
	if err != nil || st.Role != "leader" || st.LeaderID != uint64(lead) || st.Paused {
		t.Fatalf("leader status = %+v, %v", st, err)
	}
	if _, err := l.Status(ctx, 99); !errors.Is(err, gateway.ErrUnknownNode) {
		t.Fatalf("Status of an unconfigured node = %v, want ErrUnknownNode", err)
	}
}

func TestLiveTopologyFromConfig(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	got := l.Topology()
	if len(got) != 3 {
		t.Fatalf("Topology = %v", got)
	}
	for i, n := range got {
		if n.ID != uint64(i+1) || n.AZ != fmt.Sprintf("az-%d", i+1) {
			t.Errorf("Topology[%d] = %+v", i, n)
		}
	}
}

func TestLivePauseMakesKVUnavailableButStatusStillWorks(t *testing.T) {
	r := newRig(t, true)
	r.leader()
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	kv := l.NewKV()
	if err := kv.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	for _, id := range r.ids {
		if err := l.Pause(ctx, uint64(id), 30*time.Second); err != nil {
			t.Fatalf("Pause(%d): %v", id, err)
		}
	}
	short, stop := context.WithTimeout(ctx, time.Second)
	_, _, err := kv.Get(short, "k")
	stop()
	if !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("Get with every node paused = %v, want ErrNoLeader", err)
	}
	short, stop = context.WithTimeout(ctx, time.Second)
	err = kv.Put(short, "k2", []byte("v"))
	stop()
	if !errors.Is(err, gateway.ErrIndeterminate) && !errors.Is(err, gateway.ErrNoLeader) {
		t.Fatalf("Put with every node paused = %v, want ErrIndeterminate or ErrNoLeader", err)
	}
	for _, id := range r.ids {
		st, err := l.Status(ctx, uint64(id))
		if err != nil || !st.Paused {
			t.Fatalf("Status(%d) while paused = %+v, %v; want it to answer with Paused", id, st, err)
		}
	}

	for _, id := range r.ids {
		if err := l.Pause(ctx, uint64(id), 0); err != nil {
			t.Fatalf("resume %d: %v", id, err)
		}
	}
	eventually(t, "KV to work after the pause is lifted", func() bool {
		c, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		return kv.Put(c, "k3", []byte("v")) == nil
	})
}

func TestLivePartitionThenClearFaultsRestores(t *testing.T) {
	r := newRig(t, true)
	old := r.leader()
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	var others []uint64
	for _, id := range r.ids {
		if id != old {
			others = append(others, uint64(id))
		}
	}

	if err := l.Partition(ctx, uint64(old), others); err != nil {
		t.Fatalf("Partition: %v", err)
	}
	// The two connected nodes elect a leader of their own.
	var newTerm uint64
	eventually(t, "the majority side to elect a new leader", func() bool {
		for _, id := range others {
			st, err := l.Status(ctx, id)
			if err == nil && st.Role == "leader" {
				newTerm = st.Term
				return true
			}
		}
		return false
	})
	// The isolated node has not seen the new term. Its role is not asserted:
	// nodes run with CheckQuorum, so an isolated leader steps down (to follower,
	// then pre-candidate, at its old term) within one election timeout, which is
	// the same timescale as the majority electing a new leader. Whether it is
	// still leader when this runs depends on scheduling (CI: Role:follower,
	// Term:1, new term 4). A stale term is the stable proof of the partition.
	st, err := l.Status(ctx, uint64(old))
	if err != nil {
		t.Fatalf("Status of the isolated node: %v", err)
	}
	if st.Term >= newTerm {
		t.Fatalf("the isolated node %+v has seen the new term %d: the partition did not isolate it", st, newTerm)
	}

	if err := l.ClearFaults(ctx, uint64(old)); err != nil {
		t.Fatalf("ClearFaults: %v", err)
	}
	eventually(t, "the isolated node to rejoin as a follower", func() bool {
		st, err := l.Status(ctx, uint64(old))
		return err == nil && st.Role == "follower" && st.Term >= newTerm
	})
	// ClearFaults is exactly SetPartition([]) then SetNetem(0,0,0): there is no clear RPC.
	if got := r.nodes[old].faults.got(); fmt.Sprint(got) != "[partition [2 3] partition [] delay 0 0s drop 0 0]" &&
		fmt.Sprint(got) != "[partition [1 3] partition [] delay 0 0s drop 0 0]" &&
		fmt.Sprint(got) != "[partition [1 2] partition [] delay 0 0s drop 0 0]" {
		t.Fatalf("node-side fault calls = %v", got)
	}
	if err := l.NewKV().Put(ctx, "after", []byte("v")); err != nil {
		t.Fatalf("Put after healing: %v", err)
	}
}

func TestLiveNetemReachesTheNode(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	if err := l.Netem(ctx, 2, 3, 40*time.Millisecond, 15); err != nil {
		t.Fatalf("Netem: %v", err)
	}
	if got := fmt.Sprint(r.nodes[2].faults.got()); got != "[delay 3 40ms drop 3 15]" {
		t.Fatalf("node-side calls = %s, want delay 3 40ms then drop 3 15", got)
	}
}

func TestLiveCrashGoesThroughTheNodesExit(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	if err := l.Crash(ctx, 3); err != nil {
		t.Fatalf("Crash: %v", err)
	}
	eventually(t, "the node to ask for its exit", func() bool { return r.nodes[3].exits.Load() == 1 })
	if r.nodes[1].exits.Load() != 0 || r.nodes[2].exits.Load() != 0 {
		t.Fatal("a Crash of node 3 reached another node")
	}
}

func TestLiveWrongMachineTokenIsAClearErrorAndAppliesNoFault(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t, func(c *gateway.LiveConfig) { c.NodeToken = "not-the-machine-token-xyz" })
	ctx, cancel := opCtx(t)
	defer cancel()

	calls := map[string]error{
		"Status":    func() error { _, err := l.Status(ctx, 1); return err }(),
		"Crash":     l.Crash(ctx, 1),
		"Pause":     l.Pause(ctx, 1, time.Second),
		"Partition": l.Partition(ctx, 1, []uint64{2}),
		"Netem":     l.Netem(ctx, 1, 0, time.Millisecond, 1),
		"Clear":     l.ClearFaults(ctx, 1),
	}
	for name, err := range calls {
		if !errors.Is(err, gateway.ErrNodeUnauthenticated) {
			t.Errorf("%s with a wrong token = %v, want ErrNodeUnauthenticated", name, err)
		}
		if err != nil && strings.Contains(err.Error(), "not-the-machine-token-xyz") {
			t.Errorf("%s: the error leaks the token: %v", name, err)
		}
	}
	rn := r.nodes[1]
	if rn.exits.Load() != 0 || rn.n.Paused() || len(rn.faults.got()) != 0 {
		t.Fatalf("a refused call changed the node: exits %d paused %v faults %v", rn.exits.Load(), rn.n.Paused(), rn.faults.got())
	}
}

func TestLiveChaosDisabledNodeIsAClearError(t *testing.T) {
	r := newRig(t, false)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	if _, err := l.Status(ctx, 1); err != nil {
		t.Fatalf("Status works without chaos: %v", err)
	}
	if err := l.Pause(ctx, 1, time.Second); !errors.Is(err, gateway.ErrChaosDisabled) {
		t.Fatalf("Pause on a node without --admin-chaos = %v, want ErrChaosDisabled", err)
	}
}

// ---- Events relay ----

func TestLiveEventsRelayDeliversTermAndRoleChange(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	ch, err := l.Events(ctx, 1)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	r.waitSubscribed(1, 1)
	term := r.poke(1)
	ev := nextEvent(t, ch, func(e gateway.NodeEvent) bool { return e.Term >= term })
	if ev.Node != 1 || ev.Seq == 0 || ev.UnixNano == 0 || (ev.Role != "follower" && ev.Role != "candidate") {
		t.Fatalf("event = %+v, want node 1 as follower or candidate in term >= %d, with seq and time", ev, term)
	}
	if _, err := l.Events(ctx, 99); !errors.Is(err, gateway.ErrUnknownNode) {
		t.Fatalf("Events of an unconfigured node = %v, want ErrUnknownNode", err)
	}
}

func nextEvent(t *testing.T, ch <-chan gateway.NodeEvent, ok func(gateway.NodeEvent) bool) gateway.NodeEvent {
	t.Helper()
	timeout := time.After(rigWait)
	for {
		select {
		case e, open := <-ch:
			if !open {
				t.Fatal("the events channel closed")
			}
			if ok(e) {
				return e
			}
		case <-timeout:
			t.Fatal("timed out waiting for an event")
		}
	}
}

func TestLiveEventsSurviveAnUpstreamReconnect(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()
	ch, err := l.Events(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	r.waitSubscribed(1, 1)
	first := r.poke(1)
	nextEvent(t, ch, func(e gateway.NodeEvent) bool { return e.Term >= first })

	r.restartAdmin(1) // ends the upstream stream
	r.waitSubscribed(1, 2)
	second := r.poke(1)
	nextEvent(t, ch, func(e gateway.NodeEvent) bool { return e.Term >= second })
	if got := r.nodes[1].n.refused.Load(); got != 0 {
		t.Fatalf("the node refused %d subscriptions", got)
	}
}

func TestLiveOneUpstreamEventsStreamPerNodeRegardlessOfSubscribers(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ctx, cancel := opCtx(t)
	defer cancel()

	const subscribers = 40 // far above the node's cap of 16
	var chans []<-chan gateway.NodeEvent
	for i := range subscribers {
		ch, err := l.Events(ctx, uint64(1+i%2))
		if err != nil {
			t.Fatalf("Events #%d: %v", i, err)
		}
		chans = append(chans, ch)
	}
	r.waitSubscribed(1, 1)
	r.waitSubscribed(2, 1)
	// Node 3 has no local subscriber, but the relay still opens its upstream
	// stream, asynchronously; the final check below expects exactly one, so wait
	// for it too (seen in CI: "node 3 saw 0 Events subscriptions").
	r.waitSubscribed(3, 1)
	t1, t2 := r.poke(1), r.poke(2)
	for i, ch := range chans {
		want := t1
		if i%2 == 1 {
			want = t2
		}
		nextEvent(t, ch, func(e gateway.NodeEvent) bool { return e.Term >= want })
	}
	for _, id := range []raft.NodeID{1, 2, 3} {
		n := r.nodes[id].n
		const want = 1 // node 3 too: the relay holds its stream even with no local subscriber
		if got := n.subs.Load(); got != want || n.refused.Load() != 0 {
			t.Errorf("node %d saw %d Events subscriptions (%d refused) for %d local subscribers, want exactly %d", id, got, n.refused.Load(), subscribers, want)
		}
	}
}

func TestLiveCloseEndsRelaysAndClosesSubscriberChannels(t *testing.T) {
	r := newRig(t, true)
	l := r.live(t)
	ch, err := l.Events(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	r.waitSubscribed(1, 1)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	eventually(t, "the subscriber channel to close", func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	})
}

// ---- Events relay against a scripted upstream, on a fake clock ----

type stubAdmin struct {
	raftkvv1.AdminServiceClient
	mu       sync.Mutex
	statusEr error
	crashEr  error
	opens    int
	events   func(n int) (grpc.ServerStreamingClient[raftkvv1.Event], error)
}

func (s *stubAdmin) Status(context.Context, *raftkvv1.AdminStatusRequest, ...grpc.CallOption) (*raftkvv1.AdminStatusResponse, error) {
	return &raftkvv1.AdminStatusResponse{}, s.statusEr
}
func (s *stubAdmin) Crash(context.Context, *raftkvv1.CrashRequest, ...grpc.CallOption) (*raftkvv1.CrashResponse, error) {
	return &raftkvv1.CrashResponse{}, s.crashEr
}
func (s *stubAdmin) Events(ctx context.Context, _ *raftkvv1.EventsRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[raftkvv1.Event], error) {
	if s.events == nil { // a quiet node
		return &scripted{ctx: ctx}, nil
	}
	s.mu.Lock()
	s.opens++
	n := s.opens
	s.mu.Unlock()
	return s.events(n)
}
func (s *stubAdmin) opened() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens
}

// scripted is a stream that returns its items in order, then blocks until ctx ends.
type scripted struct {
	grpc.ClientStream
	items []any // *raftkvv1.Event or error
	ctx   context.Context

	// gate, when set, holds the first Recv until it is closed, so a test can
	// attach its subscribers before any event is published.
	gate <-chan struct{}
	// exhausted, when set, is closed once every item has been returned and the
	// relay asks for the next one: proof that it read the whole script.
	exhausted     chan struct{}
	exhaustedOnce sync.Once
}

func (s *scripted) Recv() (*raftkvv1.Event, error) {
	if s.gate != nil {
		select {
		case <-s.gate:
			s.gate = nil
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
	if len(s.items) == 0 {
		if s.exhausted != nil {
			s.exhaustedOnce.Do(func() { close(s.exhausted) })
		}
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	it := s.items[0]
	s.items = s.items[1:]
	if err, ok := it.(error); ok {
		return nil, err
	}
	return it.(*raftkvv1.Event), nil
}

// recordingClock wraps the fake clock and remembers each wait the adapter asked for.
type recordingClock struct {
	*fakecluster.Clock
	mu    sync.Mutex
	waits []time.Duration
}

func (c *recordingClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.waits = append(c.waits, d)
	c.mu.Unlock()
	return c.Clock.After(d)
}

func (c *recordingClock) got() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
}

func stubLive(t *testing.T, admin raftkvv1.AdminServiceClient, clk gateway.Clock) *gateway.Live {
	t.Helper()
	l, err := gateway.NewLive(gateway.LiveConfig{
		Nodes:     []gateway.LiveNode{{ID: 1, AZ: "a", Addr: "127.0.0.1:1", AdminAddr: "127.0.0.1:2"}},
		NodeToken: "tok", Clock: clk, Rand: fakecluster.NewRand(1),
		AdminDial: func(gateway.LiveNode) (raftkvv1.AdminServiceClient, io.Closer, error) {
			return admin, nopCloser{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func TestLiveEventsReconnectBacksOffAndHandlesResourceExhausted(t *testing.T) {
	ev := &raftkvv1.Event{Node: 1, Term: 9, Role: raftkvv1.NodeState_NODE_STATE_LEADER, Leader: 1, Seq: 4, UnixNano: 5}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	full := status.Error(codes.ResourceExhausted, "too many event subscribers")
	admin := &stubAdmin{events: func(n int) (grpc.ServerStreamingClient[raftkvv1.Event], error) {
		switch {
		case n <= 6: // the node's 16 slots are taken: refused on Recv, as a real node does
			return &scripted{ctx: ctx, items: []any{full}}, nil
		default:
			return &scripted{ctx: ctx, items: []any{ev}}, nil
		}
	}}
	clk := &recordingClock{Clock: fakecluster.NewClock()}
	l := stubLive(t, admin, clk)
	ch, err := l.Events(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 6; i++ {
		clk.BlockUntil(1) // the relay is waiting to reconnect
		clk.Advance(time.Hour)
		eventually(t, fmt.Sprintf("reconnect #%d", i+1), func() bool { return admin.opened() >= i+1 })
	}
	got := nextEvent(t, ch, func(gateway.NodeEvent) bool { return true })
	if got.Node != 1 || got.Term != 9 || got.Role != "leader" || got.LeaderID != 1 || got.Seq != 4 || got.UnixNano != 5 {
		t.Fatalf("relayed event = %+v", got)
	}

	waits := clk.got()
	if len(waits) < 6 {
		t.Fatalf("only %d backoff waits for 6 refusals: %v", len(waits), waits)
	}
	// Each wait is within [d/2, d] of an exponential envelope that is capped.
	var capSeen time.Duration
	for i, w := range waits[:6] {
		upper := min(gateway.EventsBackoffMax, gateway.EventsBackoffInitial<<i)
		if w < upper/2 || w > upper {
			t.Errorf("wait #%d = %v, want within [%v, %v]", i+1, w, upper/2, upper)
		}
		capSeen = max(capSeen, w)
	}
	if capSeen > gateway.EventsBackoffMax {
		t.Errorf("a wait of %v exceeds the cap %v", capSeen, gateway.EventsBackoffMax)
	}
	if waits[5] <= waits[0] {
		t.Errorf("backoff did not grow: first %v, sixth %v", waits[0], waits[5])
	}
}

func TestLiveEventsDoNotBlockOnASlowSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var items []any
	for i := uint64(1); i <= 500; i++ {
		items = append(items, &raftkvv1.Event{Node: 1, Term: i, Seq: i})
	}
	// Hold the stream until both subscribers exist, so no event is published
	// before they are attached, and note when the relay has read the whole script.
	gate := make(chan struct{})
	exhausted := make(chan struct{})
	admin := &stubAdmin{events: func(int) (grpc.ServerStreamingClient[raftkvv1.Event], error) {
		return &scripted{ctx: ctx, items: items, gate: gate, exhausted: exhausted}, nil
	}}
	l := stubLive(t, admin, fakecluster.NewClock())
	slow, err := l.Events(ctx, 1) // never read until the end
	if err != nil {
		t.Fatal(err)
	}
	fast, err := l.Events(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	// The fast subscriber is drained concurrently. Subscribers have a bounded
	// buffer and a full one drops, so which events it sees depends on scheduling;
	// only its order is asserted.
	var mu sync.Mutex
	var fastSeqs []uint64
	go func() {
		for e := range fast {
			mu.Lock()
			fastSeqs = append(fastSeqs, e.Seq)
			mu.Unlock()
		}
	}()

	close(gate)
	// A subscriber that is never read must not stall the relay: it reads all 500
	// events and asks for more.
	select {
	case <-exhausted:
	case <-time.After(rigWait):
		t.Fatal("the relay did not read the whole stream: a slow subscriber blocked it")
	}
	eventually(t, "the fast subscriber to receive events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(fastSeqs) > 0
	})
	mu.Lock()
	for i := 1; i < len(fastSeqs); i++ {
		if fastSeqs[i] <= fastSeqs[i-1] {
			t.Fatalf("the fast subscriber's seq went backwards: %d after %d", fastSeqs[i], fastSeqs[i-1])
		}
	}
	mu.Unlock()
	// The slow subscriber lost events but kept their seq numbers, so a gap is detectable.
	var last uint64
	for drained := false; !drained; {
		select {
		case e := <-slow:
			if e.Seq <= last {
				t.Fatalf("seq went backwards: %d after %d", e.Seq, last)
			}
			last = e.Seq
		default:
			drained = true
		}
	}
	if last == 0 {
		t.Fatal("the slow subscriber received nothing")
	}
}

// ---- error mapping ----

func TestMapKVErrorTable(t *testing.T) {
	deadline := &client.DefinitiveError{Code: codes.DeadlineExceeded, Err: context.DeadlineExceeded}
	cases := []struct {
		name string
		err  error
		is   error   // the mapped error must satisfy errors.Is
		not  []error // and none of these
	}{
		{"indeterminate write", &client.IndeterminateError{Attempts: 2, Seq: 7, Cause: errors.New("conn reset")}, gateway.ErrIndeterminate, []error{gateway.ErrNoLeader, gateway.ErrRejected}},
		{"wrapped indeterminate", fmt.Errorf("op: %w", &client.IndeterminateError{Attempts: 1, Cause: context.DeadlineExceeded}), gateway.ErrIndeterminate, []error{gateway.ErrNoLeader}},
		{"read unavailable", fmt.Errorf("%w: boom", client.ErrUnavailable), gateway.ErrNoLeader, []error{gateway.ErrIndeterminate}},
		{"read unavailable after deadline", fmt.Errorf("%w: %w", client.ErrUnavailable, context.DeadlineExceeded), gateway.ErrNoLeader, []error{gateway.ErrIndeterminate}},
		{"write never sent, deadline", deadline, gateway.ErrNoLeader, []error{gateway.ErrIndeterminate}},
		{"write never sent, unavailable", &client.DefinitiveError{Code: codes.Unavailable, Err: errors.New("dial")}, gateway.ErrNoLeader, []error{gateway.ErrIndeterminate}},
		{"refused: invalid argument", &client.DefinitiveError{Code: codes.InvalidArgument, Err: errors.New("bad")}, gateway.ErrRejected, []error{gateway.ErrIndeterminate, gateway.ErrNoLeader}},
		{"refused: resource exhausted", &client.DefinitiveError{Code: codes.ResourceExhausted, Err: errors.New("big")}, gateway.ErrRejected, nil},
		{"refused: unauthenticated", &client.DefinitiveError{Code: codes.Unauthenticated, Err: errors.New("x")}, gateway.ErrRejected, nil},
		{"refused: permission denied", &client.DefinitiveError{Code: codes.PermissionDenied, Err: errors.New("x")}, gateway.ErrRejected, nil},
		{"refused: unimplemented", &client.DefinitiveError{Code: codes.Unimplemented, Err: errors.New("x")}, gateway.ErrRejected, nil},
		{"caller cancelled", &client.DefinitiveError{Code: codes.Canceled, Err: context.Canceled}, context.Canceled, []error{gateway.ErrIndeterminate, gateway.ErrNoLeader}},
		{"bare context deadline", context.DeadlineExceeded, context.DeadlineExceeded, []error{gateway.ErrIndeterminate}},
	}
	for _, c := range cases {
		got := gateway.MapKVError(c.err)
		if !errors.Is(got, c.is) {
			t.Errorf("%s: MapKVError(%v) = %v, want errors.Is %v", c.name, c.err, got, c.is)
		}
		for _, n := range c.not {
			if errors.Is(got, n) {
				t.Errorf("%s: MapKVError(%v) = %v must not be %v", c.name, c.err, got, n)
			}
		}
	}
	if gateway.MapKVError(nil) != nil {
		t.Error("MapKVError(nil) != nil")
	}
	other := errors.New("something else")
	if got := gateway.MapKVError(other); errors.Is(got, gateway.ErrIndeterminate) || errors.Is(got, gateway.ErrNoLeader) || errors.Is(got, gateway.ErrRejected) {
		t.Errorf("an unclassified error was given a meaning: %v", got)
	}
}

func TestLiveAdminErrorMappingTable(t *testing.T) {
	cases := []struct {
		code codes.Code
		want error
	}{
		{codes.Unauthenticated, gateway.ErrNodeUnauthenticated},
		{codes.PermissionDenied, gateway.ErrChaosDisabled},
		{codes.Unavailable, gateway.ErrNodeUnavailable},
		{codes.DeadlineExceeded, gateway.ErrNodeUnavailable},
	}
	for _, c := range cases {
		admin := &stubAdmin{statusEr: status.Error(c.code, "x"), crashEr: status.Error(c.code, "x")}
		l := stubLive(t, admin, fakecluster.NewClock())
		if _, err := l.Status(context.Background(), 1); !errors.Is(err, c.want) {
			t.Errorf("Status with %v = %v, want %v", c.code, err, c.want)
		}
		if err := l.Crash(context.Background(), 1); !errors.Is(err, c.want) {
			t.Errorf("Crash with %v = %v, want %v", c.code, err, c.want)
		}
	}
	// An error that is not a gRPC status is a transport failure.
	admin := &stubAdmin{crashEr: io.EOF}
	l := stubLive(t, admin, fakecluster.NewClock())
	if err := l.Crash(context.Background(), 1); !errors.Is(err, gateway.ErrNodeUnavailable) {
		t.Errorf("Crash with EOF = %v, want ErrNodeUnavailable", err)
	}
}

// A refusal by the cluster is a 502 that says the write did not happen, and it
// does not leave the key counted against the cap.
func TestRejectedWriteIsDefinitiveAtTheHTTPLayer(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) { c.Cluster = failingCluster{fakecluster.New(), gateway.ErrRejected} })
	rec := e.put("fresh", "v")
	wantStatus(t, rec, http.StatusBadGateway)
	var body struct{ Error, Outcome string }
	decode(t, rec, &body)
	if body.Outcome == "unknown" || body.Outcome == "" {
		t.Fatalf("body %q: a refused write is not unknown", rec.Body.String())
	}
	if len(e.g.Keys()) != 0 {
		t.Fatalf("a refused write left %v in the registry", e.g.Keys())
	}
}
