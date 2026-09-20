package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the entrypoint's handling of what an operator types.
//
// This is the only code in the system whose input is a human under time
// pressure, and it had no tests. Misconfiguration is not an exotic failure
// here: the peer list is how every node learns who the cluster is, and the
// ways it can be wrong mostly fail late and quietly rather than at startup.
// The parser's job is to turn those into a refusal with a reason.

func TestParsePeersAcceptsAWellFormedList(t *testing.T) {
	peers, err := parsePeers("1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003")
	if err != nil {
		t.Fatalf("a well-formed list was rejected: %v", err)
	}
	if len(peers) != 3 {
		t.Fatalf("parsed %d peers, want 3", len(peers))
	}
	if got := peers[raft.NodeID(2)]; got != "127.0.0.1:9002" {
		t.Errorf("peer 2 is at %q, want 127.0.0.1:9002", got)
	}
}

func TestParsePeersToleratesWhitespaceAndStrayCommas(t *testing.T) {
	// An operator pasting a list across lines should not have to think about
	// spacing.
	peers, err := parsePeers(" 1 = host-a:9001 , 2=host-b:9002 , ")
	if err != nil {
		t.Fatalf("a spaced list was rejected: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("parsed %d peers, want 2", len(peers))
	}
	if got := peers[raft.NodeID(1)]; got != "host-a:9001" {
		t.Errorf("peer 1 is at %q; whitespace was not trimmed", got)
	}
}

func TestParsePeersRejectsMalformedLists(t *testing.T) {
	// Each of these has a specific reason, and the message has to name it:
	// an operator reading it at three in the morning should not have to
	// diff the string by eye.
	for _, tc := range []struct {
		name  string
		spec  string
		wants string
	}{
		{"empty", "", "required"},
		{"only whitespace", "   ", "required"},
		{"only commas", ",,,", "no members"},
		{"no equals sign", "1:127.0.0.1:9001", "id=host:port"},
		{"unreadable id", "abc=127.0.0.1:9001", "unreadable ID"},
		{"reserved id zero", "0=127.0.0.1:9001", "reserved ID 0"},
		{"missing address", "1=", "no address"},
		{"whitespace address", "1=   ", "no address"},
		{"repeated id", "1=host-a:9001,1=host-b:9002", "more than once"},
		{"repeated address", "1=host-a:9001,2=host-a:9001", "both at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePeers(tc.spec)
			if err == nil {
				t.Fatalf("parsePeers(%q) was accepted", tc.spec)
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Errorf("parsePeers(%q) said %q, which does not mention %q",
					tc.spec, err, tc.wants)
			}
		})
	}
}

func TestParsePeersNamesBothSidesOfAnAddressCollision(t *testing.T) {
	// The whole value of catching this is being told which two lines to
	// look at.
	_, err := parsePeers("1=host-a:9001,2=host-b:9002,3=host-a:9001")
	if err == nil {
		t.Fatal("two peers sharing an address were accepted")
	}
	for _, want := range []string{"1", "3", "host-a:9001"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not mention %q", err, want)
		}
	}
}

func TestSortedIDsAreAscendingRegardlessOfInputOrder(t *testing.T) {
	// Every node derives its initial configuration from this order, so two
	// nodes given the same members in a different order must still agree.
	a, err := parsePeers("3=c:3,1=a:1,2=b:2")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}
	b, err := parsePeers("2=b:2,3=c:3,1=a:1")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	first, second := sortedIDs(a), sortedIDs(b)
	if len(first) != 3 {
		t.Fatalf("got %d ids, want 3", len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("the same members in a different order produced %v and %v", first, second)
		}
	}
	for i := 1; i < len(first); i++ {
		if first[i] <= first[i-1] {
			t.Errorf("ids are not ascending: %v", first)
		}
	}
}

func TestNewLoggerRejectsAnUnknownLevel(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		if _, err := newLogger(level); err != nil {
			t.Errorf("newLogger(%q) failed: %v", level, err)
		}
	}
	if _, err := newLogger("chatty"); err == nil {
		t.Error("an unknown log level was accepted")
	}
}

// blockingRaft is a RaftService whose Deliver never returns until released.
// It stands in for the situation that made shutdown unbounded: a request the
// node can no longer finish, held open by a client that has not given up.
type blockingRaft struct {
	raftkvv1.UnimplementedRaftServiceServer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRaft) Deliver(ctx context.Context, _ *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return &raftkvv1.DeliverResponse{}, nil
}

func TestStopServerDoesNotWaitForeverOnAStuckRequest(t *testing.T) {
	// GracefulStop refuses new calls on every service at once, Raft's
	// included, so a leader stops being able to commit and the client writes
	// already in its hands cannot finish. Before this was bounded, shutdown
	// lasted exactly as long as the client was willing to wait: measured at
	// 3, 10 and 20 seconds for clients configured with those timeouts.
	blocker := &blockingRaft{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(blocker.release)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	srv := grpc.NewServer()
	raftkvv1.RegisterRaftServiceServer(srv, blocker)
	go srv.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer conn.Close()

	// A call that will still be in flight when the shutdown begins. Its
	// context outlives the grace period, which is the case that used to
	// hold the process open.
	ctx, cancel := context.WithTimeout(context.Background(), 5*shutdownGrace)
	defer cancel()
	go raftkvv1.NewRaftServiceClient(conn).Deliver(ctx, &raftkvv1.DeliverRequest{
		Message: &raftkvv1.Message{
			Type: raftkvv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 1, To: 2, Term: 1,
		},
	})

	select {
	case <-blocker.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the blocking call never reached the server, so nothing was held open")
	}

	start := time.Now()
	stopServer(srv)
	took := time.Since(start)

	// Bounded by the grace period rather than by the caller's patience. The
	// slack absorbs scheduling on a loaded machine without admitting the
	// failure this guards against, which was an order of magnitude larger.
	if limit := shutdownGrace + 3*time.Second; took > limit {
		t.Errorf("stopping took %v with one request stuck; it should give up after about %v",
			took, shutdownGrace)
	}
	if took < shutdownGrace/2 {
		t.Errorf("stopping took only %v, so the in-flight request was cut off immediately "+
			"rather than being given the grace period", took)
	}
}

func TestStopServerReturnsImmediatelyWhenIdle(t *testing.T) {
	// The common case must not pay the grace period.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	srv := grpc.NewServer()
	raftkvv1.RegisterRaftServiceServer(srv, &blockingRaft{
		entered: make(chan struct{}), release: make(chan struct{}),
	})
	go srv.Serve(lis)

	start := time.Now()
	stopServer(srv)
	if took := time.Since(start); took > time.Second {
		t.Errorf("stopping an idle server took %v", took)
	}
}
