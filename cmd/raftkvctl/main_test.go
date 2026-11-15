package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/client"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// fakeKV is one scripted node.
type fakeKV struct {
	mu       sync.Mutex
	store    map[string][]byte
	putErr   error
	delErr   error
	statusFn func() (*raftkvv1.StatusResponse, error)
	puts     []*raftkvv1.PutRequest
	dels     []*raftkvv1.DeleteRequest
}

func (f *fakeKV) Get(_ context.Context, in *raftkvv1.GetRequest, _ ...grpc.CallOption) (*raftkvv1.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[in.GetKey()]
	return &raftkvv1.GetResponse{Value: v, Found: ok}, nil
}

func (f *fakeKV) Put(_ context.Context, in *raftkvv1.PutRequest, _ ...grpc.CallOption) (*raftkvv1.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, in)
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.store[in.GetKey()] = in.GetValue()
	return &raftkvv1.PutResponse{}, nil
}

func (f *fakeKV) Delete(_ context.Context, in *raftkvv1.DeleteRequest, _ ...grpc.CallOption) (*raftkvv1.DeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dels = append(f.dels, in)
	if f.delErr != nil {
		return nil, f.delErr
	}
	delete(f.store, in.GetKey())
	return &raftkvv1.DeleteResponse{}, nil
}

func (f *fakeKV) Status(context.Context, *raftkvv1.StatusRequest, ...grpc.CallOption) (*raftkvv1.StatusResponse, error) {
	if f.statusFn != nil {
		return f.statusFn()
	}
	return &raftkvv1.StatusResponse{}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func runCtl(t *testing.T, kv map[string]*fakeKV, stdin string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	d := deps{connect: func(addr string) (client.KV, io.Closer, error) {
		k, ok := kv[addr]
		if !ok {
			t.Fatalf("connect to unexpected address %q", addr)
		}
		return k, nopCloser{}, nil
	}}
	code = run(args, strings.NewReader(stdin), &out, &errb, d)
	return code, out.String(), errb.String()
}

func oneNode() (map[string]*fakeKV, *fakeKV) {
	k := &fakeKV{store: map[string][]byte{}}
	return map[string]*fakeKV{"a:1": k}, k
}

func TestCtlExitCodesAndOutput(t *testing.T) {
	t.Run("put then get then delete", func(t *testing.T) {
		nodes, k := oneNode()
		if code, _, se := runCtl(t, nodes, "", "--nodes", "a:1", "--client-id", "9", "--start-seq", "4", "put", "k", "hello"); code != 0 {
			t.Fatalf("put exit = %d (%s), want 0", code, se)
		}
		if len(k.puts) != 1 || k.puts[0].GetClient().GetClientId() != 9 || k.puts[0].GetClient().GetSequence() != 4 {
			t.Errorf("put request = %v, want client id 9 seq 4", k.puts)
		}
		code, so, _ := runCtl(t, nodes, "", "--nodes", "a:1", "get", "k")
		if code != 0 || so != "hello\n" {
			t.Errorf("get = exit %d stdout %q, want 0 and \"hello\\n\"", code, so)
		}
		if code, _, se := runCtl(t, nodes, "", "--nodes", "a:1", "delete", "k"); code != 0 {
			t.Errorf("delete exit = %d (%s), want 0", code, se)
		}
		if _, still := k.store["k"]; still {
			t.Error("delete left the key in place")
		}
	})

	t.Run("get miss exits 1", func(t *testing.T) {
		nodes, _ := oneNode()
		code, so, se := runCtl(t, nodes, "", "--nodes", "a:1", "get", "nope")
		if code != 1 || so != "" || !strings.Contains(se, "not found") {
			t.Errorf("get miss = exit %d stdout %q stderr %q; want 1, empty, \"not found\"", code, so, se)
		}
	})

	t.Run("value from stdin", func(t *testing.T) {
		nodes, k := oneNode()
		if code, _, se := runCtl(t, nodes, "from stdin", "--nodes", "a:1", "put", "k", "-"); code != 0 {
			t.Fatalf("put - exit = %d (%s)", code, se)
		}
		if got := string(k.puts[0].GetValue()); got != "from stdin" {
			t.Errorf("value = %q, want %q", got, "from stdin")
		}
	})

	t.Run("indeterminate exits 3", func(t *testing.T) {
		nodes, k := oneNode()
		k.putErr = status.Error(codes.Internal, "boom")
		code, _, se := runCtl(t, nodes, "", "--nodes", "a:1", "put", "k", "v")
		if code != 3 || !strings.Contains(se, "outcome unknown") {
			t.Errorf("put = exit %d stderr %q; want 3 and \"outcome unknown\"", code, se)
		}
	})

	t.Run("definite failure exits 2", func(t *testing.T) {
		nodes, k := oneNode()
		k.delErr = status.Error(codes.InvalidArgument, "bad key")
		code, _, se := runCtl(t, nodes, "", "--nodes", "a:1", "delete", "k")
		if code != 2 || strings.Contains(se, "outcome unknown") {
			t.Errorf("delete = exit %d stderr %q; want 2 and no \"outcome unknown\"", code, se)
		}
	})

	t.Run("usage errors exit 2", func(t *testing.T) {
		nodes, _ := oneNode()
		for _, args := range [][]string{
			{},
			{"--nodes", "a:1"},
			{"--nodes", "a:1", "put", "only-key"},
			{"--nodes", "a:1", "frobnicate"},
			{"put", "k", "v"},
		} {
			if code, _, _ := runCtl(t, nodes, "", args...); code != 2 {
				t.Errorf("args %v: exit = %d, want 2", args, code)
			}
		}
	})

	t.Run("status asks every node", func(t *testing.T) {
		mk := func(id, leader uint64, st raftkvv1.NodeState) *fakeKV {
			k := &fakeKV{store: map[string][]byte{}}
			k.statusFn = func() (*raftkvv1.StatusResponse, error) {
				return &raftkvv1.StatusResponse{NodeId: id, LeaderId: leader, Term: 4, State: st}, nil
			}
			return k
		}
		down := &fakeKV{store: map[string][]byte{}}
		down.statusFn = func() (*raftkvv1.StatusResponse, error) { return nil, status.Error(codes.Unavailable, "down") }
		nodes := map[string]*fakeKV{
			"a:1": mk(1, 2, raftkvv1.NodeState_NODE_STATE_FOLLOWER),
			"b:1": mk(2, 2, raftkvv1.NodeState_NODE_STATE_LEADER),
			"c:1": down,
		}
		code, so, _ := runCtl(t, nodes, "", "--nodes", "a:1,b:1,c:1", "status")
		if code != 0 {
			t.Errorf("status exit = %d, want 0 while some node answers", code)
		}
		for _, want := range []string{"a:1", "b:1", "c:1", "follower", "leader", "term=4", "unreachable"} {
			if !strings.Contains(so, want) {
				t.Errorf("status output lacks %q:\n%s", want, so)
			}
		}
	})
}
