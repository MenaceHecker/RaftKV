package client_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/MenaceHecker/raftkv/internal/client"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func TestPutSendsSameSeqOnEveryRetry(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.op == "put" && r.n <= 3 {
			return nil, code(codes.Unavailable)
		}
		return nil, nil
	})
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	c := newClient(t, testConfig(f, clk, maxRand()))

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	puts := f.puts()
	if len(puts) != 4 {
		t.Fatalf("sent %d requests, want 4", len(puts))
	}
	for i, p := range puts {
		if p.id != 42 || p.seq != 1 {
			t.Errorf("attempt %d carried (id=%d, seq=%d), want (42, 1)", i+1, p.id, p.seq)
		}
	}
	if err := c.Put(context.Background(), "k2", []byte("v")); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if got := f.puts()[4].seq; got != 2 {
		t.Errorf("next Put carried seq %d, want 2", got)
	}
}

func TestNotLeaderHintFollowed(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.addr == "a" {
			return nil, notLeader(2, "b")
		}
		return nil, nil
	})
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	c := newClient(t, testConfig(f, clk, maxRand()))

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := f.addrs(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("nodes contacted = %v, want [a b]", got)
	}
	if got := clk.backoffs(); len(got) != 0 {
		t.Errorf("followed a hint but slept %v, want no backoff", got)
	}
}

func TestNotLeaderHintLoopGuard(t *testing.T) {
	var callsAtFirstBackoff atomic.Int64
	f := newFakeNet(func(r rec) (any, error) {
		switch {
		case r.n >= 10:
			return nil, nil
		case r.addr == "a":
			return nil, notLeader(2, "b")
		default:
			return nil, notLeader(1, "a")
		}
	})
	clk := newFakeClock()
	clk.fireNow = func(d time.Duration) bool {
		if autoBackoff(d) {
			callsAtFirstBackoff.CompareAndSwap(0, int64(len(f.calls())))
			return true
		}
		return false
	}
	c := newClient(t, testConfig(f, clk, maxRand()))

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// a hints b, b hints a, a hints b again: that is the third call, and b was
	// already followed once, so the client must back off instead of cycling.
	if got := callsAtFirstBackoff.Load(); got != 3 {
		t.Errorf("backed off after %d calls, want 3 (a, b, a)", got)
	}
}

func TestEmptyHintRotatesAndBacksOff(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.n <= 9 {
			return nil, notLeader(0, "")
		}
		return nil, nil
	})
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	rnd := maxRand()
	c := newClient(t, testConfig(f, clk, rnd))

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	want := []time.Duration{20, 40, 80, 160, 320, 640, 1000, 1000, 1000}
	got := clk.backoffs()
	if len(got) != len(want) {
		t.Fatalf("backoffs = %v, want %d of them", got, len(want))
	}
	for i, w := range want {
		w *= time.Millisecond
		if got[i] != w {
			t.Errorf("backoff %d = %v, want %v (cap doubles to Max, never above)", i, got[i], w)
		}
		if got[i] <= 0 || got[i] > backoffMax {
			t.Errorf("backoff %d = %v is outside (0, Max]", i, got[i])
		}
	}
	// Full jitter: the range asked of Rand is the cap itself.
	args := rnd.requested()
	for i, w := range want {
		if i < len(args) && args[i] != int64(w*time.Millisecond) {
			t.Errorf("jitter range %d = %d, want %d", i, args[i], int64(w*time.Millisecond))
		}
	}
	// Each failed attempt moved to the next configured node.
	addrs := f.addrs()
	for i := 1; i < 4; i++ {
		if addrs[i] == addrs[i-1] {
			t.Errorf("attempt %d reused node %s; want rotation (%v)", i, addrs[i], addrs[:4])
		}
	}
}

func TestBackoffJitterIsNeverZero(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.n <= 3 {
			return nil, code(codes.Unavailable)
		}
		return nil, nil
	})
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	zero := &fakeRand{pick: func(int64) int64 { return 0 }}
	c := newClient(t, testConfig(f, clk, zero))

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := clk.backoffs()
	if len(got) != 3 {
		t.Fatalf("backoffs = %v, want 3", got)
	}
	for i, d := range got {
		if d <= 0 {
			t.Errorf("backoff %d = %v, want > 0 even when Rand returns 0", i, d)
		}
	}
}

func TestStatusDiscoversLeader(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.op == "status" {
			return &raftkvv1.StatusResponse{NodeId: 1, LeaderId: 3, LeaderAddress: "c", Term: 7,
				State: raftkvv1.NodeState_NODE_STATE_FOLLOWER}, nil
		}
		return nil, nil
	})
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	c := newClient(t, testConfig(f, clk, maxRand()))

	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.LeaderAddr != "c" || st.LeaderID != 3 || st.Term != 7 || st.State != "follower" || st.ID != 1 {
		t.Errorf("Status = %+v, want leader c (3), term 7, follower, id 1", st)
	}
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := f.puts()[0].addr; got != "c" {
		t.Errorf("first Put went to %s, want the discovered leader c", got)
	}
}

func TestStatusFromAsksOnlyThatNode(t *testing.T) {
	f := newFakeNet(func(rec) (any, error) { return &raftkvv1.StatusResponse{NodeId: 2}, nil })
	c := newClient(t, testConfig(f, newFakeClock(), maxRand()))

	st, err := c.StatusFrom(context.Background(), "b")
	if err != nil {
		t.Fatalf("StatusFrom: %v", err)
	}
	if st.Addr != "b" || st.ID != 2 {
		t.Errorf("StatusFrom = %+v, want node 2 at b", st)
	}
	if got := f.addrs(); len(got) != 1 || got[0] != "b" {
		t.Errorf("contacted %v, want only b", got)
	}
}

// failUntilTotalFires answers every request with err and, on the second one,
// fires the total-deadline timer so the operation must end.
func failUntilTotalFires(clk *fakeClock, err error) script {
	return func(r rec) (any, error) {
		if r.n == 2 {
			clk.fireDuration(defaultTotal)
		}
		return nil, err
	}
}

func TestIndeterminateClassification(t *testing.T) {
	retriable := []struct {
		name string
		err  error
	}{
		{"aborted", code(codes.Aborted)},
		{"unavailable", code(codes.Unavailable)},
		{"deadline exceeded", code(codes.DeadlineExceeded)},
		{"not leader with hint", notLeader(2, "b")},
		{"not leader without hint", notLeader(0, "")},
		{"connection reset", errors.New("read tcp: connection reset by peer")},
	}
	for _, tc := range retriable {
		t.Run("retriable/"+tc.name, func(t *testing.T) {
			clk := newFakeClock()
			clk.fireNow = autoBackoff
			f := newFakeNet(failUntilTotalFires(clk, tc.err))
			c := newClient(t, testConfig(f, clk, maxRand()))

			err := c.Put(context.Background(), "k", []byte("v"))
			var ie *client.IndeterminateError
			if !errors.As(err, &ie) {
				t.Fatalf("Put error = %T %v, want *IndeterminateError", err, err)
			}
			if !client.IsIndeterminate(err) {
				t.Error("IsIndeterminate = false")
			}
			if ie.Attempts < 1 || ie.Seq != 1 {
				t.Errorf("Attempts=%d Seq=%d, want >=1 and 1", ie.Attempts, ie.Seq)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("error does not wrap context.DeadlineExceeded: %v", err)
			}
		})
	}

	t.Run("internal ends at once as indeterminate", func(t *testing.T) {
		clk := newFakeClock()
		f := newFakeNet(func(rec) (any, error) { return nil, code(codes.Internal) })
		c := newClient(t, testConfig(f, clk, maxRand()))
		err := c.Put(context.Background(), "k", []byte("v"))
		if !client.IsIndeterminate(err) {
			t.Fatalf("Put error = %v, want indeterminate", err)
		}
		if n := len(f.puts()); n != 1 {
			t.Errorf("sent %d attempts, want 1 (not retriable)", n)
		}
	})

	for _, c := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated,
		codes.Unimplemented, codes.ResourceExhausted} {
		t.Run("definitive/"+c.String(), func(t *testing.T) {
			f := newFakeNet(func(rec) (any, error) { return nil, code(c) })
			cl := newClient(t, testConfig(f, newFakeClock(), maxRand()))
			err := cl.Put(context.Background(), "k", []byte("v"))
			var de *client.DefinitiveError
			if !errors.As(err, &de) {
				t.Fatalf("Put error = %T %v, want *DefinitiveError", err, err)
			}
			if de.Code != c {
				t.Errorf("Code = %v, want %v", de.Code, c)
			}
			if client.IsIndeterminate(err) {
				t.Error("IsIndeterminate = true for a definitive refusal")
			}
			if n := len(f.puts()); n != 1 {
				t.Errorf("sent %d attempts, want 1 (not retriable)", n)
			}
		})
	}

	t.Run("cancelled before the first send", func(t *testing.T) {
		f := newFakeNet(func(rec) (any, error) { return nil, nil })
		c := newClient(t, testConfig(f, newFakeClock(), maxRand()))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := c.Put(ctx, "k", []byte("v"))
		var de *client.DefinitiveError
		if !errors.As(err, &de) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Put error = %v, want a DefinitiveError wrapping context.Canceled", err)
		}
		if n := len(f.calls()); n != 0 {
			t.Errorf("%d requests reached the network, want 0", n)
		}
	})

	t.Run("get is never indeterminate", func(t *testing.T) {
		clk := newFakeClock()
		clk.fireNow = autoBackoff
		f := newFakeNet(failUntilTotalFires(clk, code(codes.Unavailable)))
		c := newClient(t, testConfig(f, clk, maxRand()))
		_, _, err := c.Get(context.Background(), "k")
		if err == nil {
			t.Fatal("Get succeeded against a dead cluster")
		}
		if client.IsIndeterminate(err) {
			t.Errorf("Get returned an indeterminate error: %v", err)
		}
		if !errors.Is(err, client.ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Get error = %v, want ErrUnavailable wrapping DeadlineExceeded", err)
		}
	})
}

func TestGetReturnsValueAndMiss(t *testing.T) {
	f := newFakeNet(func(r rec) (any, error) {
		if r.key == "hit" {
			return &raftkvv1.GetResponse{Value: []byte("x"), Found: true}, nil
		}
		return &raftkvv1.GetResponse{}, nil
	})
	c := newClient(t, testConfig(f, newFakeClock(), maxRand()))
	v, ok, err := c.Get(context.Background(), "hit")
	if err != nil || !ok || string(v) != "x" {
		t.Errorf("Get(hit) = %q, %v, %v; want x, true, nil", v, ok, err)
	}
	_, ok, err = c.Get(context.Background(), "miss")
	if err != nil || ok {
		t.Errorf("Get(miss) found=%v err=%v; want false, nil", ok, err)
	}
}

func TestContextDeadlineBoundsTheOperation(t *testing.T) {
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	f := newFakeNet(func(r rec) (any, error) {
		if r.n == 5 {
			clk.fireDuration(defaultTotal)
		}
		return nil, notLeader(0, "")
	})
	c := newClient(t, testConfig(f, clk, maxRand()))

	err := c.Put(context.Background(), "k", []byte("v")) // no deadline: DefaultTotal applies
	var ie *client.IndeterminateError
	if !errors.As(err, &ie) {
		t.Fatalf("Put error = %T %v, want *IndeterminateError", err, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error does not wrap context.DeadlineExceeded: %v", err)
	}
	if ie.Attempts != 5 {
		t.Errorf("Attempts = %d, want 5 (the deadline fired during the fifth)", ie.Attempts)
	}
	if n := len(f.puts()); n != 5 {
		t.Errorf("sent %d requests, want 5: nothing may be sent after the deadline", n)
	}
}

func TestCancelDuringBackoffEndsTheOperation(t *testing.T) {
	clk := newFakeClock() // backoff timers never fire
	f := newFakeNet(func(rec) (any, error) { return nil, code(codes.Unavailable) })
	c := newClient(t, testConfig(f, clk, maxRand()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Put(ctx, "k", []byte("v")) }()
	clk.waitTimer(t, backoffInit)
	cancel()

	err := await(t, done)
	if !client.IsIndeterminate(err) || !errors.Is(err, context.Canceled) {
		t.Errorf("Put error = %v, want indeterminate wrapping context.Canceled", err)
	}
}

func TestPerAttemptTimeoutRetriesSameSeq(t *testing.T) {
	clk := newFakeClock()
	clk.fireNow = autoBackoff
	f := newFakeNet(func(r rec) (any, error) {
		if r.n == 1 {
			<-r.ctx.Done() // a hung node: only the attempt timeout frees us
			return nil, code(codes.Canceled)
		}
		return nil, nil
	})
	c := newClient(t, testConfig(f, clk, maxRand()))

	done := make(chan error, 1)
	go func() { done <- c.Put(context.Background(), "k", []byte("v")) }()
	clk.waitTimer(t, perAttempt)
	if n := clk.fireDuration(perAttempt); n != 1 {
		t.Fatalf("fired %d attempt timers, want 1", n)
	}
	if err := await(t, done); err != nil {
		t.Fatalf("Put: %v", err)
	}
	puts := f.puts()
	if len(puts) != 2 {
		t.Fatalf("sent %d attempts, want 2", len(puts))
	}
	if puts[0].id != puts[1].id || puts[0].seq != puts[1].seq {
		t.Errorf("retry changed (id, seq): %v -> %v", puts[0], puts[1])
	}
}

func TestConcurrentWritesAreSerialized(t *testing.T) {
	var inflight, maxInflight atomic.Int64
	var mu sync.Mutex
	var seqs []uint64
	f := newFakeNet(func(r rec) (any, error) {
		cur := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if cur <= m || maxInflight.CompareAndSwap(m, cur) {
				break
			}
		}
		mu.Lock()
		seqs = append(seqs, r.seq)
		mu.Unlock()
		for range 20 {
			runtime.Gosched()
		}
		inflight.Add(-1)
		return nil, nil
	})
	c := newClient(t, testConfig(f, newFakeClock(), maxRand()))

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Put(context.Background(), "k", []byte(fmt.Sprint(i))); err != nil {
				t.Errorf("Put %d: %v", i, err)
			}
		}()
	}
	wg.Wait()

	if m := maxInflight.Load(); m != 1 {
		t.Errorf("max writes in flight = %d, want 1", m)
	}
	if len(seqs) != 16 {
		t.Fatalf("server saw %d writes, want 16", len(seqs))
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("seq %d arrived after %d: not strictly increasing (%v)", seqs[i], seqs[i-1], seqs)
			break
		}
	}
}

func TestWriteWaiterWithEndedContextIsDefinitive(t *testing.T) {
	gate := make(chan struct{})
	started := make(chan struct{})
	f := newFakeNet(func(r rec) (any, error) {
		if r.n == 1 {
			close(started)
			<-gate
		}
		return nil, nil
	})
	c := newClient(t, testConfig(f, newFakeClock(), maxRand()))

	first := make(chan error, 1)
	go func() { first <- c.Put(context.Background(), "k", []byte("1")) }()
	await(t, started)

	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { second <- c.Put(ctx, "k", []byte("2")) }()
	cancel()
	err := await(t, second)
	var de *client.DefinitiveError
	if !errors.As(err, &de) || !errors.Is(err, context.Canceled) {
		t.Errorf("waiter error = %v, want a DefinitiveError wrapping context.Canceled", err)
	}
	close(gate)
	if err := await(t, first); err != nil {
		t.Errorf("first Put: %v", err)
	}
	if n := len(f.puts()); n != 1 {
		t.Errorf("%d writes were sent, want 1: the waiter never sent", n)
	}
}

func TestRandomClientIDNonZeroAndDistinct(t *testing.T) {
	f := newFakeNet(func(rec) (any, error) { return nil, nil })

	cfg := testConfig(f, newFakeClock(), &fakeRand{pick: func(int64) int64 { return 0 }})
	cfg.ClientID = 0
	c := newClient(t, cfg)
	if c.ClientID() == 0 {
		t.Error("ClientID is 0 (no dedupe) even though Rand returned 0")
	}
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got := f.puts()[0].id; got == 0 || got != c.ClientID() {
		t.Errorf("request carried id %d, want %d (non-zero)", got, c.ClientID())
	}

	seen := map[uint64]bool{}
	for range 64 {
		cfg := testConfig(f, newFakeClock(), client.RealRand{})
		cfg.ClientID = 0
		cl := newClient(t, cfg)
		id := cl.ClientID()
		if id == 0 || seen[id] {
			t.Fatalf("client id %d is zero or repeated", id)
		}
		seen[id] = true
	}
}

func TestExplicitIDAndStartSeqHonoured(t *testing.T) {
	f := newFakeNet(func(rec) (any, error) { return nil, nil })
	cfg := testConfig(f, newFakeClock(), maxRand())
	cfg.ClientID, cfg.StartSeq = 7, 100
	c := newClient(t, cfg)

	if c.ClientID() != 7 {
		t.Errorf("ClientID() = %d, want 7", c.ClientID())
	}
	_ = c.Put(context.Background(), "a", []byte("1"))
	_ = c.Delete(context.Background(), "a")
	puts := f.puts()
	if len(puts) != 2 || puts[0].id != 7 || puts[0].seq != 100 || puts[1].seq != 101 {
		t.Errorf("requests = %+v, want (7,100) then (7,101)", puts)
	}
}

func TestRealRandStaysInRange(t *testing.T) {
	var r client.RealRand
	if got := r.Int63n(1); got != 0 {
		t.Errorf("Int63n(1) = %d, want 0", got)
	}
	for range 1000 {
		if got := r.Int63n(10); got < 0 || got >= 10 {
			t.Fatalf("Int63n(10) = %d, out of [0, 10)", got)
		}
	}
}

// await receives from ch, failing the test instead of hanging forever.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the operation to return")
		panic("unreachable")
	}
}

func settle(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

func TestCloseClosesConnsAndLeaksNoGoroutines(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	clk := newFakeClock() // backoff timers never fire
	f := newFakeNet(func(r rec) (any, error) {
		if r.key == "ok" {
			return nil, nil
		}
		return nil, code(codes.Unavailable)
	})
	c, err := client.New(testConfig(f, clk, maxRand()))
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Put(context.Background(), "ok", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Cancel mid-backoff.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Put(ctx, "stuck", []byte("v")) }()
	clk.waitTimer(t, backoffInit)
	cancel()
	if err := await(t, done); err == nil {
		t.Fatal("cancelled Put succeeded")
	}

	// Close unblocks a write that is waiting in backoff.
	go func() { done <- c.Put(context.Background(), "stuck2", []byte("v")) }()
	clk.waitTimer(t, backoffInit)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err = await(t, done)
	if !errors.Is(err, client.ErrClosed) {
		t.Errorf("Put unblocked by Close returned %v, want ErrClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	if dialed, closed := len(f.dialed), f.closeCount(); dialed == 0 || closed != dialed {
		t.Errorf("dialed %d connections, closed %d; want all closed exactly once", dialed, closed)
	}

	before := len(f.calls())
	err = c.Put(context.Background(), "k", []byte("v"))
	var de *client.DefinitiveError
	if !errors.As(err, &de) || !errors.Is(err, client.ErrClosed) {
		t.Errorf("Put after Close = %v, want a DefinitiveError wrapping ErrClosed", err)
	}
	if _, _, err := c.Get(context.Background(), "k"); !errors.Is(err, client.ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	if len(f.calls()) != before {
		t.Error("a request was sent after Close")
	}

	settle(t, "goroutines to drain after Close", func() bool { return runtime.NumGoroutine() <= baseline })
}
