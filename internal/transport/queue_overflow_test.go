package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

type delivery struct {
	typ  raft.MessageType
	term raft.Term
	idx  raft.Index
}

// gatedServer is a RaftService whose Deliver blocks until the gate is closed,
// then reports each message in arrival order.
type gatedServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	entered   chan struct{}
	gate      chan struct{}
	delivered chan delivery
}

func (s *gatedServer) Deliver(ctx context.Context, req *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	m, err := MessageFromWire(req.GetMessage())
	if err != nil {
		return nil, err
	}
	s.delivered <- delivery{typ: m.Type, term: m.Term, idx: m.PrevLogIndex}
	return &raftkvv1.DeliverResponse{}, nil
}

const overflowQueue = 8

// gatedPeer starts a transport to a gated stub and parks one message (an
// append with PrevLogIndex 0) in flight, so the queue behind it is empty.
func gatedPeer(t *testing.T) (*PeerTransport, *gatedServer) {
	t.Helper()

	srv := &gatedServer{
		entered:   make(chan struct{}, 1),
		gate:      make(chan struct{}),
		delivered: make(chan delivery, 8*overflowQueue),
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftkvv1.RegisterRaftServiceServer(gs, srv)
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)

	tr, err := NewPeerTransport(PeerConfig{
		Self:        1,
		Addresses:   map[raft.NodeID]string{2: l.Addr().String()},
		QueueSize:   overflowQueue,
		SendTimeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("creating transport: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	// Runs before the transport's Close (cleanups are last in, first out) and
	// releases a blocked Deliver so the peer goroutine can exit.
	t.Cleanup(func() {
		select {
		case <-srv.gate:
		default:
			close(srv.gate)
		}
	})

	sendAppend(tr, 1, 0)
	<-srv.entered
	return tr, srv
}

func sendAppend(tr *PeerTransport, term raft.Term, idx raft.Index) {
	tr.Send([]raft.Message{{
		Type: raft.MsgAppendRequest, From: 1, To: 2, Term: term,
		PrevLogIndex: idx, PrevLogTerm: term,
	}})
}

func link(t *testing.T, tr *PeerTransport) PeerStats {
	t.Helper()
	st := tr.Stats()
	if len(st) != 1 {
		t.Fatalf("got %d peers in stats, want 1", len(st))
	}
	return st[0]
}

// drain reads deliveries until `stop` returns true for one or `limit` have
// arrived, and returns everything read including the last.
func drain(t *testing.T, srv *gatedServer, limit int, stop func(delivery) bool) []delivery {
	t.Helper()
	var got []delivery
	guard := time.After(30 * time.Second)
	for len(got) < limit {
		select {
		case d := <-srv.delivered:
			got = append(got, d)
			if stop(d) {
				return got
			}
		case <-guard:
			t.Fatalf("timed out; delivered so far: %v", got)
		}
	}
	return got
}

// T1: a flood of appends to a stalled peer must not build a queue of stale
// duplicates (T3.20, H2). The leader resends its current append on every tick;
// only the newest one matters, and it must not wait behind the others.
func TestFreshAppendReachesPeerAfterQueueOverflow(t *testing.T) {
	const flood = 5 * overflowQueue
	tr, srv := gatedPeer(t)
	for i := 1; i <= flood; i++ {
		sendAppend(tr, 1, raft.Index(i))
	}

	st := link(t, tr)
	if st.Queued > 1 {
		t.Fatalf("%d messages queued after %d appends of one term, want at most 1: "+
			"stale appends are accumulating behind the fresh one", st.Queued, flood)
	}
	if st.Dropped != 0 {
		t.Fatalf("%d messages were dropped; superseded appends should be replaced, not dropped", st.Dropped)
	}

	close(srv.gate)
	got := drain(t, srv, overflowQueue+1, func(d delivery) bool { return d.idx == flood })
	if len(got) == 0 || got[len(got)-1].idx != flood {
		t.Fatalf("newest append (PrevLogIndex %d) not among the first %d deliveries: %v",
			flood, overflowQueue+1, got)
	}
	if len(got) > 2 {
		t.Fatalf("newest append arrived after %d deliveries, want at most 2 (the in-flight one and itself): %v",
			len(got), got)
	}
}

// T2: a heartbeat enqueued behind queued appends waits for at most one of
// them, not for a whole queue. A leader's heartbeats suppress elections, so
// head-of-line delay here can cost a follower its patience.
func TestHeartbeatWaitsForAtMostOneQueuedAppend(t *testing.T) {
	tr, srv := gatedPeer(t)
	for i := 1; i <= 3*overflowQueue; i++ {
		sendAppend(tr, 1, raft.Index(i))
	}
	tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1}})

	close(srv.gate)
	got := drain(t, srv, 4*overflowQueue, func(d delivery) bool { return d.typ == raft.MsgHeartbeat })
	if len(got) == 0 || got[len(got)-1].typ != raft.MsgHeartbeat {
		t.Fatalf("heartbeat was never delivered: %v", got)
	}

	// got[0] is the append that was already in flight when the heartbeat was
	// sent, which nothing can overtake.
	queuedAhead := 0
	for _, d := range got[1 : len(got)-1] {
		if d.typ == raft.MsgAppendRequest {
			queuedAhead++
		}
	}
	if queuedAhead > 1 {
		t.Fatalf("heartbeat waited behind %d queued appends, want at most 1: %v", queuedAhead, got)
	}
}

// T3: coalescing is limited to appends of one term, and never reorders or
// replaces anything else.
func TestCoalescingNeverMergesAcrossTermsOrTypes(t *testing.T) {
	tr, srv := gatedPeer(t)

	want := []delivery{
		{raft.MsgVoteRequest, 2, 0},
		{raft.MsgAppendRequest, 2, 10}, // term 2 append, replaced by the next one
		{raft.MsgHeartbeat, 2, 0},
		{raft.MsgAppendRequest, 3, 20}, // a different term: kept
		{raft.MsgAppendRequest, 2, 11}, // replaces the term-2 append at 10
		{raft.MsgVoteRequest, 3, 0},
	}
	for _, d := range want {
		tr.Send([]raft.Message{{
			Type: d.typ, From: 1, To: 2, Term: d.term,
			PrevLogIndex: d.idx, PrevLogTerm: d.term,
		}})
	}

	close(srv.gate)
	got := drain(t, srv, 1+5, func(delivery) bool { return false })

	// The in-flight message, then everything except the replaced append, in
	// order, with the replacement at the tail of what it was queued with.
	expect := []delivery{
		{raft.MsgAppendRequest, 1, 0},
		{raft.MsgVoteRequest, 2, 0},
		{raft.MsgHeartbeat, 2, 0},
		{raft.MsgAppendRequest, 3, 20},
		{raft.MsgAppendRequest, 2, 11},
		{raft.MsgVoteRequest, 3, 0},
	}
	if len(got) != len(expect) {
		t.Fatalf("delivered %d messages, want %d:\n got %v\nwant %v", len(got), len(expect), got, expect)
	}
	for i := range expect {
		if got[i] != expect[i] {
			t.Fatalf("delivery %d = %v, want %v\n got %v\nwant %v", i, got[i], expect[i], got, expect)
		}
	}
	if c := link(t, tr).Coalesced; c != 1 {
		t.Fatalf("coalesced %d appends, want exactly 1", c)
	}
}
