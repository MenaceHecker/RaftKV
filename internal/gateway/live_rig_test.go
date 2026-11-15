package gateway_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/transport"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	rigToken = "machine-token-for-live-tests"

	// rigWait is a failure ceiling: passing runs return as soon as the
	// condition holds.
	rigWait = 20 * time.Second
)

// eventually polls cond until it holds or rigWait passes. It is the only
// waiting these tests do on real nodes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(rigWait); ; {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// spyNode counts Events subscriptions on the node side, so a test can prove
// how many upstream streams the gateway holds.
type spyNode struct {
	*node.Node
	subs    atomic.Int64
	refused atomic.Int64
}

func (s *spyNode) Subscribe() (*node.Subscription, error) {
	s.subs.Add(1)
	sub, err := s.Node.Subscribe()
	if err != nil {
		s.refused.Add(1)
	}
	return sub, err
}

// spyFaults records the fault changes that reach the node and forwards them.
type spyFaults struct {
	inner *transport.FaultTransport
	mu    sync.Mutex
	calls []string
}

func (s *spyFaults) rec(f string, a ...any) {
	s.mu.Lock()
	s.calls = append(s.calls, fmt.Sprintf(f, a...))
	s.mu.Unlock()
}
func (s *spyFaults) got() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}
func (s *spyFaults) SetPartition(b []raft.NodeID) { s.rec("partition %v", b); s.inner.SetPartition(b) }
func (s *spyFaults) SetDelay(p raft.NodeID, d time.Duration) {
	s.rec("delay %d %v", p, d)
	s.inner.SetDelay(p, d)
}
func (s *spyFaults) SetDropPercent(p raft.NodeID, pct int) error {
	s.rec("drop %d %d", p, pct)
	return s.inner.SetDropPercent(p, pct)
}
func (s *spyFaults) ClearFaults() { s.rec("clear"); s.inner.ClearFaults() }

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type rigNode struct {
	id        raft.NodeID
	n         *spyNode
	faults    *spyFaults
	ft        *transport.FaultTransport
	shared    *grpc.Server
	admin     *grpc.Server
	adminAddr string
	exits     atomic.Int64 // process exits requested through the injected exit func
}

// rig is three real nodes on loopback, built from the production pieces
// (peer transport, fault wrapper, pause gate, KV and admin servers), with the
// admin API on its own listener. A Crash calls an injected exit func, so the
// test process survives it.
type rig struct {
	t     *testing.T
	ids   []raft.NodeID
	kv    map[raft.NodeID]string
	nodes map[raft.NodeID]*rigNode
	token string
	chaos bool
}

func newRig(t *testing.T, chaos bool) *rig {
	t.Helper()
	r := &rig{t: t, kv: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*rigNode{}, token: rigToken, chaos: chaos}
	lis := map[raft.NodeID]net.Listener{}
	adm := map[raft.NodeID]net.Listener{}
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		r.ids = append(r.ids, id)
		lis[id] = loopback(t)
		adm[id] = loopback(t)
		r.kv[id] = lis[id].Addr().String()
	}
	root := t.TempDir()
	for _, id := range r.ids {
		rn := &rigNode{id: id, adminAddr: adm[id].Addr().String()}
		peer, err := transport.NewPeerTransport(transport.PeerConfig{Self: id, Addresses: r.kv})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })

		var ref atomic.Pointer[node.Node]
		rn.ft = transport.NewFaultTransport(transport.FaultConfig{
			Self: id, Inner: peer, Clock: transport.RealClock{}, Rand: transport.RealRand{},
			Paused: func() bool { n := ref.Load(); return n != nil && n.Paused() },
		})
		n, err := node.Start(node.Config{
			ID: id, Peers: r.ids, DataDir: filepath.Join(root, fmt.Sprint("n", id)),
			Transport: rn.ft, TickInterval: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 1,
			Sync: storage.SyncNever,
		})
		if err != nil {
			t.Fatal(err)
		}
		ref.Store(n)
		t.Cleanup(func() { _ = n.Stop() })
		peer.SetLocal(n)
		rn.n = &spyNode{Node: n}
		rn.faults = &spyFaults{inner: rn.ft}

		raftSrv, err := transport.NewRaftServer(n)
		if err != nil {
			t.Fatal(err)
		}
		kv, err := transport.NewKVServer(n, r.kv)
		if err != nil {
			t.Fatal(err)
		}
		pu, ps := transport.NewPauseGate(n.Paused)
		rn.shared = grpc.NewServer(grpc.ChainUnaryInterceptor(pu), grpc.ChainStreamInterceptor(ps))
		raftkvv1.RegisterRaftServiceServer(rn.shared, rn.ft.WrapRaftServer(raftSrv))
		kv.Register(rn.shared)
		go func() { _ = rn.shared.Serve(lis[id]) }()
		t.Cleanup(rn.shared.Stop)

		rn.admin = r.serveAdmin(rn, adm[id])
		t.Cleanup(func() { rn.admin.Stop() })
		r.nodes[id] = rn
	}
	return r
}

func loopback(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func (r *rig) serveAdmin(rn *rigNode, l net.Listener) *grpc.Server {
	r.t.Helper()
	svc, err := transport.NewAdminServer(transport.AdminConfig{
		Node: rn.n, Faults: rn.faults, Chaos: r.chaos,
		Exit: func(int) { rn.exits.Add(1) },
	})
	if err != nil {
		r.t.Fatal(err)
	}
	u, s := transport.NewAdminInterceptors(r.token, r.chaos)
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(u), grpc.ChainStreamInterceptor(s))
	svc.Register(gs)
	go func() { _ = gs.Serve(l) }()
	return gs
}

// restartAdmin stops node id's admin server (ending its Events streams) and
// serves a new one on the same address.
func (r *rig) restartAdmin(id raft.NodeID) {
	r.t.Helper()
	rn := r.nodes[id]
	rn.admin.Stop()
	var l net.Listener
	eventually(r.t, "the admin port to be free again", func() bool {
		var err error
		l, err = net.Listen("tcp", rn.adminAddr)
		return err == nil
	})
	rn.admin = r.serveAdmin(rn, l)
}

func (r *rig) liveNodes() []gateway.LiveNode {
	var out []gateway.LiveNode
	for _, id := range r.ids {
		out = append(out, gateway.LiveNode{
			ID: uint64(id), AZ: fmt.Sprintf("az-%d", id), Addr: r.kv[id], AdminAddr: r.nodes[id].adminAddr,
		})
	}
	return out
}

func (r *rig) live(t *testing.T, mod ...func(*gateway.LiveConfig)) *gateway.Live {
	t.Helper()
	cfg := gateway.LiveConfig{Nodes: r.liveNodes(), NodeToken: r.token, Clock: realClock{}, Rand: lcg{}}
	for _, m := range mod {
		m(&cfg)
	}
	l, err := gateway.NewLive(cfg)
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// lcg is a trivial gateway.Rand; the jitter does not need quality.
type lcg struct{}

func (lcg) Uint64() uint64 { return uint64(time.Now().UnixNano()) * 6364136223846793005 }

func (r *rig) leader() raft.NodeID {
	r.t.Helper()
	var found raft.NodeID
	eventually(r.t, "a leader", func() bool {
		for _, id := range r.ids {
			if r.nodes[id].n.Status().State == raft.Leader {
				found = id
				return true
			}
		}
		return false
	})
	return found
}

// kill stops a node and both of its servers, as a crashed process would.
func (r *rig) kill(id raft.NodeID) {
	rn := r.nodes[id]
	rn.shared.Stop()
	rn.admin.Stop()
	_ = rn.n.Stop()
}

// poke makes node id observe a new (term, role, leader): a heartbeat from a
// peer in a term 1000 above the node's own, which it returns. The node may
// campaign and rise further on its own, so callers match "term >= returned".
func (r *rig) poke(id raft.NodeID) uint64 {
	peer := r.ids[0]
	if peer == id {
		peer = r.ids[1]
	}
	term := r.nodes[id].n.Status().Term + 1000
	r.nodes[id].n.Step(raft.Message{Type: raft.MsgHeartbeat, From: peer, To: id, Term: term})
	return uint64(term)
}

func (r *rig) waitSubscribed(id raft.NodeID, n int64) {
	r.t.Helper()
	eventually(r.t, fmt.Sprintf("node %d to see %d Events subscription(s)", id, n), func() bool {
		return r.nodes[id].n.subs.Load() >= n
	})
}

func opCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 15*time.Second)
}
