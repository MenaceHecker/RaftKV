package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

// spyCluster records every fault RPC the gateway sends, and can make Crash
// report a lost connection or wrap the KV sessions.
type spyCluster struct {
	gateway.Cluster
	mu        sync.Mutex
	calls     []string
	statuses  atomic.Int64
	crashErr  error
	crashNoop bool // report crashErr without crashing the node
	kvWrap    func(gateway.KV) gateway.KV
}

func (s *spyCluster) rec(format string, a ...any) {
	s.mu.Lock()
	s.calls = append(s.calls, fmt.Sprintf(format, a...))
	s.mu.Unlock()
}

func (s *spyCluster) faults() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *spyCluster) count(prefix string) int {
	n := 0
	for _, c := range s.faults() {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func (s *spyCluster) Status(ctx context.Context, id uint64) (gateway.NodeStatus, error) {
	s.statuses.Add(1)
	return s.Cluster.Status(ctx, id)
}

func (s *spyCluster) Crash(ctx context.Context, id uint64) error {
	s.rec("crash %d", id)
	if s.crashNoop && s.crashErr != nil {
		return s.crashErr
	}
	err := s.Cluster.Crash(ctx, id)
	if s.crashErr != nil {
		return s.crashErr
	}
	return err
}

func (s *spyCluster) Pause(ctx context.Context, id uint64, d time.Duration) error {
	s.rec("pause %d %s", id, d)
	return s.Cluster.Pause(ctx, id, d)
}

func (s *spyCluster) Partition(ctx context.Context, id uint64, blocked []uint64) error {
	b := append([]uint64(nil), blocked...)
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	s.rec("partition %d %v", id, b)
	return s.Cluster.Partition(ctx, id, blocked)
}

func (s *spyCluster) Netem(ctx context.Context, id, peer uint64, d time.Duration, drop uint32) error {
	s.rec("netem %d peer=%d delay=%s drop=%d", id, peer, d, drop)
	return s.Cluster.Netem(ctx, id, peer, d, drop)
}

func (s *spyCluster) ClearFaults(ctx context.Context, id uint64) error {
	s.rec("clear %d", id)
	return s.Cluster.ClearFaults(ctx, id)
}

func (s *spyCluster) NewKV() gateway.KV {
	kv := s.Cluster.NewKV()
	if s.kvWrap != nil {
		return s.kvWrap(kv)
	}
	return kv
}

// newSpyEnv builds an env whose cluster is a spy around the fake. pre runs on
// the fake before the gateway exists.
func newSpyEnv(t *testing.T, pre func(*fakecluster.Cluster, *spyCluster), mods ...func(*gateway.Config)) (*env, *spyCluster) {
	t.Helper()
	spy := &spyCluster{}
	all := append([]func(*gateway.Config){func(c *gateway.Config) {
		spy.Cluster = c.Cluster
		if pre != nil {
			pre(c.Cluster.(*fakecluster.Cluster), spy)
		}
		c.Cluster = spy
	}}, mods...)
	return newEnv(t, all...), spy
}

func (e *env) post(path, body string, hdr ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.from(defaultIP, http.MethodPost, path, body, hdr...)
}

func (e *env) postFrom(addr, path, body string, hdr ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.from(addr, http.MethodPost, path, body, hdr...)
}

func bearer() []string { return []string{"Authorization", "Bearer " + adminToken} }

func (e *env) bpost(path, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.post(path, body, bearer()...)
}

func (e *env) pollAll() { e.g.PollOnce(context.Background()) }

func (e *env) start() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	e.g.Start(ctx)
}

// chaosState reads chaos.state from /v1/cluster.
func (e *env) chaosState() string {
	e.t.Helper()
	rec := e.do(http.MethodGet, "/v1/cluster", "")
	var v struct {
		Chaos struct {
			State string `json:"state"`
		} `json:"chaos"`
	}
	decode(e.t, rec, &v)
	return v.Chaos.State
}

// eventually runs cond until it holds. It yields instead of sleeping and fails
// instead of hanging.
func eventuallyYield(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// advanceUntil moves the fake clock in steps until cond holds.
func advanceUntil(t *testing.T, e *env, step time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		e.clk.Advance(step)
		runtime.Gosched()
	}
}

// sseEvent is one parsed server-sent event.
type sseEvent struct {
	ID, Event, Data string
}

func (ev sseEvent) json() map[string]any {
	var m map[string]any
	_ = json.Unmarshal([]byte(ev.Data), &m)
	return m
}

// sseConn is a ResponseWriter that parses what the gateway streams.
type sseConn struct {
	mu       sync.Mutex
	cond     *sync.Cond
	hdr      http.Header
	code     int
	buf      string
	events   []sseEvent
	comments int
	gate     chan struct{}
	ready    chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	once     sync.Once
}

func newSSEConn() *sseConn {
	c := &sseConn{hdr: http.Header{}, ready: make(chan struct{}), done: make(chan struct{})}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *sseConn) Header() http.Header { return c.hdr }

func (c *sseConn) WriteHeader(code int) {
	c.mu.Lock()
	if c.code == 0 {
		c.code = code
	}
	c.mu.Unlock()
}

func (c *sseConn) Flush() {}

func (c *sseConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	gate := c.gate
	c.mu.Unlock()
	if gate != nil {
		<-gate
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.code == 0 {
		c.code = http.StatusOK
	}
	c.buf += string(b)
	for {
		i := strings.Index(c.buf, "\n\n")
		if i < 0 {
			break
		}
		block := c.buf[:i]
		c.buf = c.buf[i+2:]
		var ev sseEvent
		isComment := false
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, ":"):
				isComment = true
			case strings.HasPrefix(line, "id: "):
				ev.ID = line[4:]
			case strings.HasPrefix(line, "event: "):
				ev.Event = line[7:]
			case strings.HasPrefix(line, "data: "):
				ev.Data += line[6:]
			}
		}
		if isComment && ev.Event == "" {
			c.comments++
			continue
		}
		c.events = append(c.events, ev)
	}
	c.once.Do(func() { close(c.ready) })
	c.cond.Broadcast()
	return len(b), nil
}

func (c *sseConn) status() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.code
}

func (c *sseConn) blockWrites() {
	c.mu.Lock()
	c.gate = make(chan struct{})
	c.mu.Unlock()
}

func (c *sseConn) unblockWrites() {
	c.mu.Lock()
	g := c.gate
	c.gate = nil
	c.mu.Unlock()
	if g != nil {
		close(g)
	}
}

func (c *sseConn) all() []sseEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sseEvent(nil), c.events...)
}

func (c *sseConn) seen(pred func(sseEvent) bool) bool {
	for _, ev := range c.all() {
		if pred(ev) {
			return true
		}
	}
	return false
}

// wait blocks until an event satisfying pred has arrived.
func (c *sseConn) wait(t *testing.T, what string, pred func(sseEvent) bool) {
	t.Helper()
	timer := time.AfterFunc(10*time.Second, func() { c.mu.Lock(); c.cond.Broadcast(); c.mu.Unlock() })
	defer timer.Stop()
	deadline := time.Now().Add(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		for _, ev := range c.events {
			if pred(ev) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		c.cond.Wait()
	}
}

// connect opens /v1/events from addr and returns once the handler has either
// started streaming or refused.
func (e *env) connect(addr string, hdr ...string) *sseConn {
	e.t.Helper()
	c := newSSEConn()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	e.t.Cleanup(func() { cancel(); c.unblockWrites() })
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
	req.RemoteAddr = addr
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	go func() {
		defer close(c.done)
		e.g.Handler().ServeHTTP(c, req)
	}()
	select {
	case <-c.ready:
	case <-c.done:
	case <-time.After(10 * time.Second):
		e.t.Fatal("timed out waiting for /v1/events to start")
	}
	return c
}

func ip(i int) string { return fmt.Sprintf("198.51.%d.%d:4000", i/250, i%250+1) }

// verification helpers

type vreport struct {
	ID             string    `json:"id"`
	Seed           string    `json:"seed"`
	Status         string    `json:"status"`
	Verdict        string    `json:"verdict"`
	Counterexample string    `json:"counterexample"`
	Ops            int       `json:"ops"`
	Reads          int       `json:"reads"`
	Writes         int       `json:"writes"`
	OK             int       `json:"ok"`
	Failed         int       `json:"failed"`
	Unknown        int       `json:"unknown"`
	Explored       int       `json:"explored"`
	ElapsedMs      int64     `json:"elapsedMs"`
	EndedAt        time.Time `json:"endedAt"`
	Faults         []struct {
		Action string `json:"action"`
		Target uint64 `json:"target"`
		OK     bool   `json:"ok"`
		Error  string `json:"error"`
	} `json:"faults"`
}

// drive runs the fake clock until the verification run ends. Restarting the
// nodes each step stands in for systemd.
//
// The run does real work after its workload ends (healing, waiting for the
// nodes, checking the history, an fsync) while the driver keeps advancing fake
// time. Unpaced, the driver can run the fake clock tens of minutes past the end
// of the workload before the run reports done (seen in CI), which silently
// consumes the 15-minute cooldown the tests then assert on. A short real pause
// per step bounds that: each real millisecond of post-run work costs the fake
// clock only a few seconds.
func (e *env) drive() {
	e.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for e.g.VerifyRunning() {
		if time.Now().After(deadline) {
			e.t.Fatal("the verification run did not finish")
		}
		for id := uint64(1); id <= 3; id++ {
			e.fc.Restart(id)
		}
		e.clk.Advance(50 * time.Millisecond)
		runtime.Gosched()
		time.Sleep(200 * time.Microsecond)
	}
}

func (e *env) last() vreport {
	e.t.Helper()
	rec := e.do(http.MethodGet, "/v1/verify/last", "")
	wantStatus(e.t, rec, http.StatusOK)
	var r vreport
	decode(e.t, rec, &r)
	return r
}

// runVerify starts a run with the admin token and drives it to the end.
func (e *env) runVerify() vreport {
	e.t.Helper()
	wantStatus(e.t, e.bpost("/v1/verify/run", ""), http.StatusAccepted)
	e.drive()
	return e.last()
}

func countLogs(log, sub string) int { return strings.Count(log, sub) }
