package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/transport"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

type spyStore struct {
	mu   sync.Mutex
	adds int
}

func (s *spyStore) Get(context.Context, string) ([]byte, bool, error)   { return nil, false, nil }
func (s *spyStore) Propose(context.Context, statemachine.Command) error { return nil }
func (s *spyStore) AddNode(context.Context, raft.NodeID, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adds++
	return nil
}
func (s *spyStore) RemoveNode(context.Context, raft.NodeID) error { return nil }
func (s *spyStore) Status() node.Status                           { return node.Status{ID: 1} }

type stepNop struct{}

func (stepNop) Step(raft.Message) {}

// sharedClient serves the shared listener as run() builds it, with a spy store.
func sharedClient(t *testing.T, clusterToken string) (raftkvv1.ClusterServiceClient, *spyStore) {
	t.Helper()
	store := &spyStore{}
	kv, err := transport.NewKVServer(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := transport.NewRaftServer(stepNop{})
	if err != nil {
		t.Fatal(err)
	}
	srv := newSharedServerWithClusterToken(nil, rs, kv, nil, clusterToken)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return raftkvv1.NewClusterServiceClient(cc), store
}

func TestClusterTokenFlagOffKeepsTodaysBehaviour(t *testing.T) {
	opt := parseOptions(t)
	if opt.clusterTokenFile != "" {
		t.Fatalf("-cluster-token-file defaults to %q, want off", opt.clusterTokenFile)
	}
	if tok, err := validateClusterToken(opt); err != nil || tok != "" {
		t.Fatalf("validateClusterToken with the flag unset: %q, %v; want off and no error", tok, err)
	}

	cluster, store := sharedClient(t, "")
	if _, err := cluster.AddNode(context.Background(), &raftkvv1.AddNodeRequest{NodeId: 9, Address: "h:1"}); err != nil {
		t.Fatalf("AddNode with the flag off: %v", err)
	}
	if store.adds != 1 {
		t.Errorf("store saw %d adds, want 1", store.adds)
	}
}

func TestSharedServerWithClusterTokenProtectsAddNode(t *testing.T) {
	cluster, store := sharedClient(t, "tok-123")
	req := &raftkvv1.AddNodeRequest{NodeId: 9, Address: "h:1"}

	if _, err := cluster.AddNode(context.Background(), req); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("AddNode without a token: %v, want Unauthenticated", err)
	}
	if store.adds != 0 {
		t.Fatalf("store saw %d adds for a refused call", store.adds)
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer tok-123")
	if _, err := cluster.AddNode(ctx, req); err != nil {
		t.Fatalf("AddNode with the token: %v", err)
	}
	if _, err := cluster.ListMembers(context.Background(), &raftkvv1.ListMembersRequest{}); err != nil {
		t.Fatalf("ListMembers must stay open: %v", err)
	}
}

func TestServerRefusesClusterTokenFlagWithUnusableFile(t *testing.T) {
	const secret = "s3cret-cluster-token"
	dir := t.TempDir()
	write := func(name, body string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, path := range map[string]string{
		"missing":    filepath.Join(dir, "absent"),
		"empty":      write("empty", "", 0o600),
		"unreadable": write("unreadable", secret, 0o000),
		"group":      write("grp", secret, 0o640),
	} {
		_, err := validateClusterToken(options{clusterTokenFile: path})
		if err == nil {
			t.Errorf("%s: -cluster-token-file accepted", name)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: error leaks the token: %v", name, err)
		}
	}
	good := write("good", secret+"\n", 0o600)
	tok, err := validateClusterToken(options{clusterTokenFile: good})
	if err != nil || tok != secret {
		t.Fatalf("good file: %q, %v", tok, err)
	}
}
