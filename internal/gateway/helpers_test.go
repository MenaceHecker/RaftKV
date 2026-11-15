package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

const (
	adminToken = "s3cret-admin-TOKEN-4711"
	dashOrigin = "https://dash.example.test"
	defaultIP  = "198.51.100.1:4000"
)

type env struct {
	t   *testing.T
	g   *gateway.Gateway
	fc  *fakecluster.Cluster
	clk *fakecluster.Clock
	dir string
	log *bytes.Buffer
	cfg gateway.Config
}

// newEnv builds a gateway on a fake cluster and a fake clock. The rate limit
// is raised unless a test opts into the defaults, so tests that make many
// requests do not trip it.
func newEnv(t *testing.T, mods ...func(*gateway.Config)) *env {
	t.Helper()
	e := &env{t: t, fc: fakecluster.New(), clk: fakecluster.NewClock(), dir: t.TempDir(), log: &bytes.Buffer{}}
	e.cfg = gateway.Config{
		Cluster:         e.fc,
		Clock:           e.clk,
		Rand:            fakecluster.NewRand(1),
		Logger:          slog.New(slog.NewJSONHandler(e.log, nil)),
		StateDir:        e.dir,
		DashboardOrigin: dashOrigin,
		AdminToken:      adminToken,
		PollTimeout:     2 * time.Second,
		RatePerSecond:   1e6,
		Burst:           1000000,
	}
	for _, m := range mods {
		m(&e.cfg)
	}
	e.open()
	return e
}

func (e *env) open() {
	e.t.Helper()
	g, err := gateway.New(e.cfg)
	if err != nil {
		e.t.Fatalf("gateway.New: %v", err)
	}
	e.g = g
	e.t.Cleanup(func() { _ = g.Close() })
}

// reopen closes the gateway and builds a new one on the same state dir and a
// fresh cluster, as a restart would.
func (e *env) reopen() {
	e.t.Helper()
	if err := e.g.Close(); err != nil {
		e.t.Fatalf("Close: %v", err)
	}
	e.fc = fakecluster.New()
	e.cfg.Cluster = e.fc
	e.open()
}

func (e *env) do(method, path, body string, hdr ...string) *httptest.ResponseRecorder {
	return e.from(defaultIP, method, path, body, hdr...)
}

func (e *env) from(addr, method, path, body string, hdr ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.doReader(addr, method, path, strings.NewReader(body), body != "", hdr...)
}

func (e *env) doReader(addr, method, path string, body io.Reader, json bool, hdr ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, path, body)
	req.RemoteAddr = addr
	if json {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	e.g.Handler().ServeHTTP(rec, req)
	return rec
}

func (e *env) put(key, value string) *httptest.ResponseRecorder {
	e.t.Helper()
	b, _ := json.Marshal(map[string]string{"value": value})
	return e.do(http.MethodPut, "/v1/kv/"+key, string(b))
}

func (e *env) get(key string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.do(http.MethodGet, "/v1/kv/"+key, "")
}

func (e *env) del(key string) *httptest.ResponseRecorder {
	e.t.Helper()
	return e.do(http.MethodDelete, "/v1/kv/"+key, "")
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body %q", rec.Code, want, rec.Body.String())
	}
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("body is not JSON (%v): %q", err, rec.Body.String())
	}
}

// guard runs a blocking wait and fails the test, rather than hanging it, if
// the wait does not finish. It is a safety net, not a way to pass time.
func guard(t *testing.T, what string, wait func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitBefore waits for wait to finish, but fails at once if the request
// being watched has already returned: the handler must still be working.
func awaitBefore(t *testing.T, what string, wait func(), early <-chan *httptest.ResponseRecorder) {
	t.Helper()
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case rec := <-early:
		t.Fatalf("request returned %d before %s", rec.Code, what)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func background() context.Context { return context.Background() }
