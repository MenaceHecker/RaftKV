package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

func TestKeyTooLongRejected(t *testing.T) {
	e := newEnv(t)

	wantStatus(t, e.put(strings.Repeat("a", 65), "v"), http.StatusBadRequest)
	wantStatus(t, e.get(strings.Repeat("a", 65)), http.StatusBadRequest)
	wantStatus(t, e.del(strings.Repeat("a", 65)), http.StatusBadRequest)
	wantStatus(t, e.put(strings.Repeat("a", 64), "v"), http.StatusOK)

	wantStatus(t, e.do(http.MethodPut, "/v1/kv/", `{"value":"v"}`), http.StatusBadRequest) // empty key
	wantStatus(t, e.do(http.MethodGet, "/v1/kv/", ""), http.StatusBadRequest)
	wantStatus(t, e.put("a%00b", "v"), http.StatusBadRequest) // NUL
	wantStatus(t, e.put("a%0Ab", "v"), http.StatusBadRequest) // control character
	wantStatus(t, e.put("a%2Fb", "v"), http.StatusBadRequest) // slash inside a key
	wantStatus(t, e.do(http.MethodPut, "/v1/kv/a/b", `{"value":"v"}`), http.StatusNotFound)
	wantStatus(t, e.put("__verify%2Fx", "v"), http.StatusForbidden) // reserved prefix
	wantStatus(t, e.get("__verify%2Fx"), http.StatusForbidden)
	wantStatus(t, e.del("__verify%2Fx"), http.StatusForbidden)

	if got := e.fc.Keys(); len(got) != 1 {
		t.Fatalf("rejected keys must not reach the cluster; cluster holds %v", got)
	}
}

// endless is a request body that never ends.
type endless struct{ read int }

func (b *endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	b.read += len(p)
	return len(p), nil
}

func TestValueTooLargeRejected(t *testing.T) {
	e := newEnv(t)

	wantStatus(t, e.put("k", strings.Repeat("v", gateway.MaxValueBytes)), http.StatusOK)
	wantStatus(t, e.put("k2", strings.Repeat("v", gateway.MaxValueBytes+1)), http.StatusRequestEntityTooLarge)
	if _, ok := e.fc.Value("k2"); ok {
		t.Fatal("an oversized value reached the cluster")
	}

	// The body is capped by http.MaxBytesReader: a body that never ends is cut
	// off, and only about the cap is ever read from it.
	body := &endless{}
	rec := e.doReader(defaultIP, http.MethodPut, "/v1/kv/k3", body, true)
	wantStatus(t, rec, http.StatusRequestEntityTooLarge)
	if body.read > gateway.MaxBodyBytes+1 {
		t.Fatalf("read %d bytes of an endless body; the cap is %d", body.read, gateway.MaxBodyBytes)
	}
	if gateway.MaxBodyBytes != 2048 {
		t.Fatalf("MaxBodyBytes = %d, want 2048 (a 1024 byte value must fit inside its JSON body)", gateway.MaxBodyBytes)
	}
}

func TestKeyCapAt10000(t *testing.T) {
	if gateway.DefaultMaxKeys != 10000 {
		t.Fatalf("DefaultMaxKeys = %d, want 10000", gateway.DefaultMaxKeys)
	}
	e := newEnv(t, func(c *gateway.Config) { c.MaxKeys = 3 })

	for _, k := range []string{"k1", "k2", "k3"} {
		wantStatus(t, e.put(k, "v"), http.StatusOK)
	}
	rec := e.put("k4", "v")
	wantStatus(t, rec, http.StatusInsufficientStorage)
	if _, ok := e.fc.Value("k4"); ok {
		t.Fatal("a write over the key cap reached the cluster")
	}
	wantStatus(t, e.put("k1", "again"), http.StatusOK) // overwrite is not a new key
	wantStatus(t, e.del("k2"), http.StatusOK)          // delete frees a slot
	wantStatus(t, e.put("k4", "v"), http.StatusOK)
	wantStatus(t, e.put("k5", "v"), http.StatusInsufficientStorage)
	if got := len(e.g.Keys()); got != 3 {
		t.Fatalf("registry holds %d keys, want 3", got)
	}
}

func TestKeyCapSurvivesRestart(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) { c.MaxKeys = 3 })
	for _, k := range []string{"a", "b", "c"} {
		wantStatus(t, e.put(k, "v"), http.StatusOK)
	}
	wantStatus(t, e.del("a"), http.StatusOK)
	wantStatus(t, e.put("d", "v"), http.StatusOK)

	// A crash mid-append leaves a last line with no newline.
	if err := e.g.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(e.dir, "keys.log"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("the registry file keys.log is not in the state dir: %v", err)
	}
	if _, err := f.WriteString("put tor"); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	e.reopen()
	want := []string{"b", "c", "d"}
	if got := e.g.Keys(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("keys after restart = %v, want %v", got, want)
	}
	wantStatus(t, e.put("e", "v"), http.StatusInsufficientStorage) // the cap is still full
	wantStatus(t, e.put("b", "v2"), http.StatusOK)

	e.reopen() // the torn line was repaired, not carried forward
	if got := e.g.Keys(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("keys after a second restart = %v, want %v", got, want)
	}
}

func TestRegistryFileStaysBounded(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) { c.MaxKeys = 3 })
	for i := 0; i < 300; i++ {
		wantStatus(t, e.put("churn", "v"), http.StatusOK)
		wantStatus(t, e.del("churn"), http.StatusOK)
	}
	b, err := os.ReadFile(filepath.Join(e.dir, "keys.log"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(b), "\n"); lines > 70 {
		t.Fatalf("keys.log has %d lines after 300 put/delete pairs of one key; it must be compacted", lines)
	}
	e.reopen()
	if got := e.g.Keys(); len(got) != 0 {
		t.Fatalf("keys after compaction and restart = %v, want none", got)
	}
}

func TestKeyspaceResetDeletesRegistryKeysAndEmptiesRegistry(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.put("a", "1"), http.StatusOK)
	wantStatus(t, e.put("b", "2"), http.StatusOK)

	e.clk.Advance(5 * time.Hour)
	if ran, err := e.g.ResetIfDue(background()); err != nil || ran {
		t.Fatalf("ResetIfDue before the interval = (%v, %v), want (false, nil)", ran, err)
	}

	e.clk.Advance(61 * time.Minute)
	err := e.g.WithVerifyLock(func() error {
		ran, err := e.g.ResetIfDue(background())
		if err != nil || ran {
			t.Errorf("ResetIfDue while verification runs = (%v, %v), want (false, nil)", ran, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(e.fc.Keys()) != 2 {
		t.Fatalf("the reset ran during verification: cluster holds %v", e.fc.Keys())
	}

	ran, err := e.g.ResetIfDue(background())
	if err != nil || !ran {
		t.Fatalf("ResetIfDue after verification = (%v, %v), want (true, nil)", ran, err)
	}
	if got := e.fc.Keys(); len(got) != 0 {
		t.Fatalf("cluster still holds %v after the reset", got)
	}
	if got := e.g.Keys(); len(got) != 0 {
		t.Fatalf("registry still holds %v after the reset", got)
	}
	wantStatus(t, e.get("a"), http.StatusNotFound)
	if ran, _ := e.g.ResetIfDue(background()); ran {
		t.Fatal("the reset ran twice without the interval elapsing again")
	}
	e.reopen()
	if got := e.g.Keys(); len(got) != 0 {
		t.Fatalf("registry after restart = %v, want empty", got)
	}
}

func TestPublicWritesLockedDuringVerify(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.put("k", "v"), http.StatusOK)

	err := e.g.WithVerifyLock(func() error {
		rec := e.put("k", "changed")
		wantStatus(t, rec, http.StatusServiceUnavailable)
		if !strings.Contains(rec.Body.String(), "verification in progress") {
			t.Errorf("body %q does not say verification in progress", rec.Body.String())
		}
		wantStatus(t, e.del("k"), http.StatusServiceUnavailable)
		wantStatus(t, e.get("k"), http.StatusOK) // reads stay open
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := e.fc.Value("k"); string(v) != "v" {
		t.Fatalf("a locked write changed the value to %q", v)
	}
	wantStatus(t, e.put("k", "after"), http.StatusOK)

	boom := errors.New("verify failed")
	if got := e.g.WithVerifyLock(func() error { return boom }); !errors.Is(got, boom) {
		t.Fatalf("WithVerifyLock returned %v, want the function's error", got)
	}
	wantStatus(t, e.put("k", "after error"), http.StatusOK)

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic did not propagate")
			}
		}()
		_ = e.g.WithVerifyLock(func() error { panic("verify crashed") })
	}()
	wantStatus(t, e.put("k", "after panic"), http.StatusOK)
}

type failingCluster struct {
	gateway.Cluster
	err error
}

func (f failingCluster) NewKV() gateway.KV { return failingKV{f.Cluster.NewKV(), f.err} }

type failingKV struct {
	gateway.KV
	err error
}

func (f failingKV) Put(_ context.Context, _ string, _ []byte) error { return f.err }
func (f failingKV) Delete(_ context.Context, _ string) error        { return f.err }

func TestKVErrorsMapToHTTP(t *testing.T) {
	// Round trip first.
	e := newEnv(t)
	wantStatus(t, e.get("k"), http.StatusNotFound)
	wantStatus(t, e.put("k", "hello"), http.StatusOK)
	rec := e.get("k")
	wantStatus(t, rec, http.StatusOK)
	var got struct{ Key, Value string }
	decode(t, rec, &got)
	if got.Key != "k" || got.Value != "hello" {
		t.Fatalf("GET = %+v", got)
	}
	wantStatus(t, e.del("k"), http.StatusOK)
	wantStatus(t, e.del("k"), http.StatusOK) // deleting a missing key succeeds
	wantStatus(t, e.get("k"), http.StatusNotFound)

	// No leader: the write was never sent, so it did not happen.
	nl := newEnv(t, func(c *gateway.Config) { c.Cluster = failingCluster{fakecluster.New(), gateway.ErrNoLeader} })
	rec = nl.put("fresh", "v")
	wantStatus(t, rec, http.StatusServiceUnavailable)
	if rec.Header().Get("Retry-After") == "" {
		t.Error("503 without Retry-After")
	}
	if len(nl.g.Keys()) != 0 {
		t.Fatalf("a write that was never sent left %v in the registry", nl.g.Keys())
	}

	// Indeterminate: the write may have committed, so the key stays counted.
	ind := newEnv(t, func(c *gateway.Config) { c.Cluster = failingCluster{fakecluster.New(), gateway.ErrIndeterminate} })
	rec = ind.put("maybe", "v")
	wantStatus(t, rec, http.StatusGatewayTimeout)
	var body struct{ Error, Outcome string }
	decode(t, rec, &body)
	if body.Outcome != "unknown" {
		t.Fatalf("body %q must carry outcome unknown", rec.Body.String())
	}
	if fmt.Sprint(ind.g.Keys()) != "[maybe]" {
		t.Fatalf("an indeterminate write must stay registered, registry = %v", ind.g.Keys())
	}

	// Anything else is a bad gateway and leaks no detail.
	bad := newEnv(t, func(c *gateway.Config) {
		c.Cluster = failingCluster{fakecluster.New(), errors.New("dial tcp 10.20.1.10:7001: refused")}
	})
	rec = bad.put("k", "v")
	wantStatus(t, rec, http.StatusBadGateway)
	if strings.Contains(rec.Body.String(), "10.20.1.10") {
		t.Fatalf("an internal address leaked into the response: %q", rec.Body.String())
	}
}

func TestPublicWritesUseSerializedSessions(t *testing.T) {
	e := newEnv(t)
	release := e.fc.HoldWrites()
	defer release()

	results := make(chan *httptest.ResponseRecorder, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- e.put(fmt.Sprintf("k%02d", i), "v")
		}()
	}
	awaitBefore(t, "8 writes were inside the cluster", func() { e.fc.WaitBlocked(8) }, results)
	release()
	wg.Wait()
	close(results)
	for rec := range results {
		wantStatus(t, rec, http.StatusOK)
	}
	if v := e.fc.Violations(); len(v) != 0 {
		t.Fatalf("session misuse: %v", v)
	}
	if n := e.fc.Clients(); n < 1 || n > gateway.DefaultPoolSize {
		t.Fatalf("%d client sessions were used, want between 1 and %d", n, gateway.DefaultPoolSize)
	}
	if len(e.fc.Keys()) != 16 {
		t.Fatalf("cluster holds %d keys, want 16", len(e.fc.Keys()))
	}
}

func TestRequestDeadlineBoundsKV(t *testing.T) {
	e := newEnv(t)
	e.fc.HangKV(true)

	// More timeouts than there are pooled sessions: if a timed out request
	// kept its session, the pool would run dry and the next one would hang.
	for i := 0; i < gateway.DefaultPoolSize+1; i++ {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- e.put(fmt.Sprintf("k%d", i), "v") }()
		awaitBefore(t, "the request reached the cluster", func() { e.fc.WaitActive(1) }, done)
		e.clk.Advance(gateway.DefaultTimeout)
		select {
		case rec := <-done:
			wantStatus(t, rec, http.StatusGatewayTimeout)
			if !strings.Contains(rec.Body.String(), `"unknown"`) {
				t.Fatalf("a timed out write must say its outcome is unknown: %q", rec.Body.String())
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the handler did not return after the 3 s deadline")
		}
		guard(t, "the abandoned cluster call to unwind", e.fc.WaitIdle)
	}

	e.fc.HangKV(false)
	wantStatus(t, e.put("ok", "v"), http.StatusOK)

	e.fc.HangKV(true)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- e.get("ok") }()
	awaitBefore(t, "the read reached the cluster", func() { e.fc.WaitActive(1) }, done)
	e.clk.Advance(gateway.DefaultTimeout)
	wantStatus(t, <-done, http.StatusGatewayTimeout)
	guard(t, "the abandoned read to unwind", e.fc.WaitIdle)
}

func TestCasEndpointIsNotImplemented(t *testing.T) {
	e := newEnv(t)
	rec := e.do(http.MethodPost, "/v1/kv/k/cas", `{"expected":"a","value":"b"}`)
	wantStatus(t, rec, http.StatusNotImplemented)
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}
	var body struct{ Error, Detail string }
	decode(t, rec, &body)
	if body.Error != "not implemented" || !strings.Contains(body.Detail, "compare-and-swap") {
		t.Fatalf("body = %+v, want a documented not-implemented answer naming compare-and-swap", body)
	}
	if len(e.g.Keys()) != 0 || len(e.fc.Keys()) != 0 {
		t.Fatal("the cas endpoint must have no backing")
	}
}

func TestInvalidJSONAndMethods(t *testing.T) {
	e := newEnv(t)

	rec := e.do(http.MethodPatch, "/v1/kv/k", `{"value":"v"}`)
	wantStatus(t, rec, http.StatusMethodNotAllowed)
	if rec.Header().Get("Allow") != "GET, PUT, DELETE" {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}
	wantStatus(t, e.do(http.MethodPost, "/v1/kv/k", `{"value":"v"}`), http.StatusMethodNotAllowed)
	wantStatus(t, e.do(http.MethodDelete, "/healthz", ""), http.StatusMethodNotAllowed)
	wantStatus(t, e.do(http.MethodPost, "/v1/cluster", ""), http.StatusMethodNotAllowed)

	for name, body := range map[string]string{
		"not json":       `{"value":`,
		"wrong type":     `{"value":123}`,
		"missing value":  `{}`,
		"unknown field":  `{"value":"v","extra":1}`,
		"trailing bytes": `{"value":"v"} {"value":"w"}`,
		"array":          `["v"]`,
	} {
		wantStatus(t, e.do(http.MethodPut, "/v1/kv/k", body), http.StatusBadRequest)
		_ = name
	}

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/k", strings.NewReader(`{"value":"v"}`))
	req.RemoteAddr = defaultIP
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	e.g.Handler().ServeHTTP(rec, req)
	wantStatus(t, rec, http.StatusUnsupportedMediaType)

	req = httptest.NewRequest(http.MethodPut, "/v1/kv/k", strings.NewReader(`{"value":"v"}`))
	req.RemoteAddr = defaultIP
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec = httptest.NewRecorder()
	e.g.Handler().ServeHTTP(rec, req)
	wantStatus(t, rec, http.StatusOK)

	for _, rec := range []*httptest.ResponseRecorder{
		e.do(http.MethodPatch, "/v1/kv/k", ""), e.get("missing"), e.do(http.MethodGet, "/nope", ""),
	} {
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("error response (%d) has Content-Type %q, want JSON", rec.Code, ct)
		}
	}
	wantStatus(t, e.do(http.MethodGet, "/nope", ""), http.StatusNotFound)
}
