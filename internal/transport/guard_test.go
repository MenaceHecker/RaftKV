package transport

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
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
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const clusterSecret = "cluster-secret-token"

// spyStore records every membership call that reaches the store.
type spyStore struct {
	mu      sync.Mutex
	adds    []raft.NodeID
	removes []raft.NodeID
}

func (s *spyStore) Get(context.Context, string) ([]byte, bool, error)   { return nil, false, nil }
func (s *spyStore) Propose(context.Context, statemachine.Command) error { return nil }
func (s *spyStore) AddNode(_ context.Context, id raft.NodeID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adds = append(s.adds, id)
	return nil
}
func (s *spyStore) RemoveNode(_ context.Context, id raft.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removes = append(s.removes, id)
	return nil
}
func (s *spyStore) Status() node.Status {
	return node.Status{ID: 1, Members: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}
}
func (s *spyStore) calls() (adds, removes []raft.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.adds), slices.Clone(s.removes)
}

type guardedServer struct {
	cluster raftkvv1.ClusterServiceClient
	kv      raftkvv1.KVServiceClient
	store   *spyStore
}

// startGuarded serves KVServer over a spy store with the cluster-token
// interceptors installed, as cmd/raftkv-server installs them for the flag.
func startGuarded(t *testing.T, token string) guardedServer {
	t.Helper()
	store := &spyStore{}
	kvs, err := NewKVServer(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	unary, stream := NewClusterTokenInterceptors(token)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
	kvs.Register(gs)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(l) }()
	t.Cleanup(gs.Stop)
	cc, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return guardedServer{raftkvv1.NewClusterServiceClient(cc), raftkvv1.NewKVServiceClient(cc), store}
}

func withBearer(ctx context.Context, tok string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func TestAddRemoveNodeWithoutTokenIsUnauthenticated(t *testing.T) {
	g := startGuarded(t, clusterSecret)
	ctx, cancel := clientCtx(t)
	defer cancel()

	type call func(ctx context.Context) error
	add := func(ctx context.Context) error {
		_, err := g.cluster.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 9, Address: "h:1"})
		return err
	}
	remove := func(ctx context.Context) error {
		_, err := g.cluster.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 2})
		return err
	}
	for name, do := range map[string]call{"AddNode": add, "RemoveNode": remove} {
		missing := do(ctx)
		wrong := do(withBearer(ctx, "not-the-token"))
		for what, err := range map[string]error{"missing": missing, "wrong": wrong} {
			if status.Code(err) != codes.Unauthenticated {
				t.Errorf("%s with a %s token: code %s (%v), want Unauthenticated", name, what, status.Code(err), err)
			}
		}
		if status.Convert(missing).Message() != status.Convert(wrong).Message() {
			t.Errorf("%s: missing and wrong tokens give different messages: %q vs %q",
				name, status.Convert(missing).Message(), status.Convert(wrong).Message())
		}
	}

	adds, removes := g.store.calls()
	if len(adds) != 0 || len(removes) != 0 {
		t.Errorf("membership changed without a token: adds %v, removes %v", adds, removes)
	}
	ml, err := g.cluster.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
	if err != nil || len(ml.GetMembers()) != 3 {
		t.Errorf("ListMembers = %v, %v; want the three unchanged members", ml, err)
	}
}

func TestAddRemoveNodeWithTokenWorks(t *testing.T) {
	g := startGuarded(t, clusterSecret)
	ctx, cancel := clientCtx(t)
	defer cancel()
	ctx = withBearer(ctx, clusterSecret)

	if _, err := g.cluster.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 9, Address: "h:1"}); err != nil {
		t.Fatalf("AddNode with the token: %v", err)
	}
	if _, err := g.cluster.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 2}); err != nil {
		t.Fatalf("RemoveNode with the token: %v", err)
	}
	adds, removes := g.store.calls()
	if !slices.Equal(adds, []raft.NodeID{9}) || !slices.Equal(removes, []raft.NodeID{2}) {
		t.Errorf("store saw adds %v, removes %v; want [9] and [2]", adds, removes)
	}
}

func TestListMembersAndKVStayOpen(t *testing.T) {
	g := startGuarded(t, clusterSecret)
	ctx, cancel := clientCtx(t)
	defer cancel()

	if _, err := g.cluster.ListMembers(ctx, &raftkvv1.ListMembersRequest{}); err != nil {
		t.Errorf("ListMembers without a token: %v", err)
	}
	if _, err := g.kv.Get(ctx, &raftkvv1.GetRequest{Key: "k"}); err != nil {
		t.Errorf("KV Get without a token: %v", err)
	}
	if _, err := g.kv.Status(ctx, &raftkvv1.StatusRequest{}); err != nil {
		t.Errorf("KV Status without a token: %v", err)
	}
}

func TestLoadClusterTokenRefusesUnusableFiles(t *testing.T) {
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
		"empty path": "",
		"missing":    filepath.Join(dir, "absent"),
		"empty":      write("empty", "  \n", 0o600),
		"group":      write("grp", clusterSecret, 0o640),
	} {
		if tok, err := LoadClusterToken(path); err == nil {
			t.Errorf("%s: accepted (token len %d)", name, len(tok))
		}
	}
	tok, err := LoadClusterToken(write("good", clusterSecret+"\n", 0o600))
	if err != nil || tok != clusterSecret {
		t.Errorf("good file: %v", err)
	}
}
