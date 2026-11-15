package client_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/client"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	perAttempt   = 2 * time.Second
	defaultTotal = 30 * time.Second
	backoffInit  = 20 * time.Millisecond
	backoffMax   = time.Second
)

// rec is one request that reached a fake node.
type rec struct {
	ctx   context.Context
	n     int // 1-based arrival number across the whole fake network
	addr  string
	op    string // get, put, delete, status
	id    uint64
	seq   uint64
	key   string
	value []byte
}

type script func(r rec) (any, error)

// fakeNet is a scripted set of nodes behind client.Config.Connect.
type fakeNet struct {
	mu     sync.Mutex
	log    []rec
	dialed []string
	closed map[string]int
	script script
}

func newFakeNet(s script) *fakeNet { return &fakeNet{closed: map[string]int{}, script: s} }

func (f *fakeNet) connect(addr string) (client.KV, io.Closer, error) {
	f.mu.Lock()
	f.dialed = append(f.dialed, addr)
	f.mu.Unlock()
	return &fakeConn{net: f, addr: addr}, closerFunc(func() error {
		f.mu.Lock()
		f.closed[addr]++
		f.mu.Unlock()
		return nil
	}), nil
}

func (f *fakeNet) calls() []rec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rec(nil), f.log...)
}

func (f *fakeNet) puts() []rec {
	var out []rec
	for _, r := range f.calls() {
		if r.op == "put" || r.op == "delete" {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeNet) addrs() []string {
	var out []string
	for _, r := range f.calls() {
		out = append(out, r.addr)
	}
	return out
}

func (f *fakeNet) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.closed {
		total += n
	}
	return total
}

func (f *fakeNet) do(r rec) (any, error) {
	f.mu.Lock()
	r.n = len(f.log) + 1
	f.log = append(f.log, r)
	s := f.script
	f.mu.Unlock()
	return s(r)
}

type closerFunc func() error

func (c closerFunc) Close() error { return c() }

type fakeConn struct {
	net  *fakeNet
	addr string
}

func (c *fakeConn) Get(ctx context.Context, in *raftkvv1.GetRequest, _ ...grpc.CallOption) (*raftkvv1.GetResponse, error) {
	resp, err := c.net.do(rec{ctx: ctx, addr: c.addr, op: "get", key: in.GetKey()})
	if err != nil {
		return nil, err
	}
	if r, ok := resp.(*raftkvv1.GetResponse); ok {
		return r, nil
	}
	return &raftkvv1.GetResponse{}, nil
}

func (c *fakeConn) Put(ctx context.Context, in *raftkvv1.PutRequest, _ ...grpc.CallOption) (*raftkvv1.PutResponse, error) {
	_, err := c.net.do(rec{ctx: ctx, addr: c.addr, op: "put", key: in.GetKey(), value: in.GetValue(),
		id: in.GetClient().GetClientId(), seq: in.GetClient().GetSequence()})
	if err != nil {
		return nil, err
	}
	return &raftkvv1.PutResponse{}, nil
}

func (c *fakeConn) Delete(ctx context.Context, in *raftkvv1.DeleteRequest, _ ...grpc.CallOption) (*raftkvv1.DeleteResponse, error) {
	_, err := c.net.do(rec{ctx: ctx, addr: c.addr, op: "delete", key: in.GetKey(),
		id: in.GetClient().GetClientId(), seq: in.GetClient().GetSequence()})
	if err != nil {
		return nil, err
	}
	return &raftkvv1.DeleteResponse{}, nil
}

func (c *fakeConn) Status(ctx context.Context, _ *raftkvv1.StatusRequest, _ ...grpc.CallOption) (*raftkvv1.StatusResponse, error) {
	resp, err := c.net.do(rec{ctx: ctx, addr: c.addr, op: "status"})
	if err != nil {
		return nil, err
	}
	if r, ok := resp.(*raftkvv1.StatusResponse); ok {
		return r, nil
	}
	return &raftkvv1.StatusResponse{}, nil
}

// notLeader builds the error the real KVServer returns.
func notLeader(leaderID uint64, leaderAddr string) error {
	st, err := status.New(codes.FailedPrecondition, "not the leader").
		WithDetails(&raftkvv1.NotLeader{LeaderId: leaderID, LeaderAddress: leaderAddr})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

func code(c codes.Code) error { return status.Error(c, c.String()) }

// fakeClock is a hand-driven Clock. Timers fire only when the test says so,
// except those for which fireNow returns true.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []*fakeTimer
	delays  []time.Duration
	fireNow func(d time.Duration) bool
	created chan time.Duration
}

type fakeTimer struct {
	clk     *fakeClock
	c       chan time.Time
	d       time.Duration
	stopped bool
	fired   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_000_000, 0), created: make(chan time.Duration, 1024)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) client.Timer {
	c.mu.Lock()
	t := &fakeTimer{clk: c, c: make(chan time.Time, 1), d: d}
	c.delays = append(c.delays, d)
	fire := c.fireNow != nil && c.fireNow(d)
	if fire {
		t.fired = true
		t.c <- c.now
	} else {
		c.pending = append(c.pending, t)
	}
	c.mu.Unlock()
	c.created <- d
	return t
}

func (t *fakeTimer) Stop() bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	was := !t.stopped && !t.fired
	t.stopped = true
	return was
}

// fireDuration fires every pending timer created with duration d.
func (c *fakeClock) fireDuration(d time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.pending {
		if t.d == d && !t.stopped && !t.fired {
			t.fired = true
			t.c <- c.now
			n++
		}
	}
	return n
}

// backoffs returns the durations of every timer that is neither the per-attempt
// nor the total timer.
func (c *fakeClock) backoffs() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, d := range c.delays {
		if d != perAttempt && d != defaultTotal {
			out = append(out, d)
		}
	}
	return out
}

// waitTimer blocks until a timer of duration d has been created.
func (c *fakeClock) waitTimer(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case got := <-c.created:
			if got == d {
				return
			}
		case <-deadline:
			t.Fatalf("no timer of %v was created", d)
		}
	}
}

// autoBackoff fires backoff timers at once, leaving attempt and total timers
// under the test's control.
func autoBackoff(d time.Duration) bool { return d != perAttempt && d != defaultTotal }

// fakeRand returns a fixed fraction of the range and records every request.
type fakeRand struct {
	mu   sync.Mutex
	pick func(n int64) int64
	args []int64
}

func maxRand() *fakeRand { return &fakeRand{pick: func(n int64) int64 { return n - 1 }} }

func (r *fakeRand) Int63n(n int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.args = append(r.args, n)
	return r.pick(n)
}

func (r *fakeRand) requested() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.args...)
}

func testConfig(f *fakeNet, clk *fakeClock, rnd client.Rand, addrs ...string) client.Config {
	if len(addrs) == 0 {
		addrs = []string{"a", "b", "c"}
	}
	return client.Config{
		Addrs:        addrs,
		ClientID:     42,
		Connect:      f.connect,
		PerAttempt:   perAttempt,
		DefaultTotal: defaultTotal,
		Backoff:      client.Backoff{Initial: backoffInit, Max: backoffMax, Multiplier: 2},
		Rand:         rnd,
		Clock:        clk,
	}
}

func newClient(t *testing.T, cfg client.Config) *client.Client {
	t.Helper()
	c, err := client.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
