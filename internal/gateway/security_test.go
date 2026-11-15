package gateway_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
)

func TestPerIPRateLimit(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) {
		c.RatePerSecond, c.Burst = 0, 0 // the defaults: 20 per second
		c.MaxLimiterIPs = 50
	})
	const ip1, ip2 = "203.0.113.7:1111", "203.0.113.8:2222"

	for i := 0; i < gateway.DefaultBurst; i++ {
		if rec := e.from(ip1, http.MethodGet, "/v1/kv/x", ""); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d was limited; the burst is %d", i+1, gateway.DefaultBurst)
		}
	}
	rec := e.from(ip1, http.MethodGet, "/v1/kv/x", "")
	wantStatus(t, rec, http.StatusTooManyRequests)
	if ra := rec.Header().Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("Retry-After = %q, want a positive number of seconds", ra)
	}
	wantStatus(t, e.from(ip2, http.MethodGet, "/v1/kv/x", ""), http.StatusNotFound) // another address is unaffected

	// X-Forwarded-For is ignored: it can neither dodge the limit nor push the
	// limit onto somebody else.
	wantStatus(t, e.from(ip1, http.MethodGet, "/v1/kv/x", "", "X-Forwarded-For", "192.0.2.99"), http.StatusTooManyRequests)
	wantStatus(t, e.from("203.0.113.9:3333", http.MethodGet, "/v1/kv/x", "", "X-Forwarded-For", "203.0.113.7"), http.StatusNotFound)

	e.clk.Advance(time.Second) // 20 tokens come back
	wantStatus(t, e.from(ip1, http.MethodGet, "/v1/kv/x", ""), http.StatusNotFound)

	// IPv6 is limited per /64: a whole prefix is one client.
	for i := 0; i < gateway.DefaultBurst; i++ {
		e.from("[2001:db8:1:2::1]:4000", http.MethodGet, "/v1/kv/x", "")
	}
	wantStatus(t, e.from("[2001:db8:1:2:ffff::9]:4000", http.MethodGet, "/v1/kv/x", ""), http.StatusTooManyRequests)
	wantStatus(t, e.from("[2001:db8:1:3::1]:4000", http.MethodGet, "/v1/kv/x", ""), http.StatusNotFound)

	// The table is bounded, and idle entries are evicted.
	for i := 0; i < 200; i++ {
		e.from(fmt.Sprintf("10.1.%d.%d:5000", i/250, i%250), http.MethodGet, "/v1/kv/x", "")
	}
	if n := e.g.LimiterSize(); n > 50 {
		t.Fatalf("limiter remembers %d addresses, the bound is 50", n)
	}
	e.clk.Advance(time.Hour)
	e.from("10.9.9.9:5000", http.MethodGet, "/v1/kv/x", "")
	if n := e.g.LimiterSize(); n != 1 {
		t.Fatalf("limiter remembers %d addresses after an hour idle, want 1", n)
	}
}

func TestAdminBearerConstantTime(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.put("k", "v"), http.StatusOK)

	missing := e.do(http.MethodPost, "/v1/admin/reset", "")
	wrong := e.do(http.MethodPost, "/v1/admin/reset", "", "Authorization", "Bearer "+strings.Repeat("x", len(adminToken)))
	short := e.do(http.MethodPost, "/v1/admin/reset", "", "Authorization", "Bearer x")
	basic := e.do(http.MethodPost, "/v1/admin/reset", "", "Authorization", "Basic "+adminToken)
	for name, rec := range map[string]*struct {
		code int
		body string
		www  string
	}{
		"wrong":  {wrong.Code, wrong.Body.String(), wrong.Header().Get("WWW-Authenticate")},
		"short":  {short.Code, short.Body.String(), short.Header().Get("WWW-Authenticate")},
		"basic":  {basic.Code, basic.Body.String(), basic.Header().Get("WWW-Authenticate")},
		"absent": {missing.Code, missing.Body.String(), missing.Header().Get("WWW-Authenticate")},
	} {
		if rec.code != http.StatusUnauthorized {
			t.Fatalf("%s token: status %d, want 401", name, rec.code)
		}
		if rec.body != missing.Body.String() || rec.www != missing.Header().Get("WWW-Authenticate") {
			t.Fatalf("%s token is distinguishable from a missing one: %q vs %q", name, rec.body, missing.Body.String())
		}
	}
	if len(e.g.Keys()) != 1 {
		t.Fatal("an unauthenticated call changed state")
	}

	ok := e.do(http.MethodPost, "/v1/admin/reset", "", "Authorization", "Bearer "+adminToken)
	wantStatus(t, ok, http.StatusOK)

	for _, out := range []string{e.log.String(), missing.Body.String(), wrong.Body.String(), ok.Body.String()} {
		if strings.Contains(out, adminToken) {
			t.Fatal("the admin token appeared in a log line or a response")
		}
	}

	// The comparison goes through crypto/subtle on fixed-size digests, so
	// neither the content nor the length of the token shows in the timing.
	src, err := os.ReadFile("auth.go") //nolint:gosec // a fixed file name in this package
	if err != nil {
		t.Fatalf("auth.go: %v", err)
	}
	for _, want := range []string{"subtle.ConstantTimeCompare", "sha256."} {
		if !bytes.Contains(src, []byte(want)) {
			t.Errorf("auth.go does not use %s", want)
		}
	}
}

func TestCORSOnlyDashboardOrigin(t *testing.T) {
	e := newEnv(t)
	acao := func(rec interface{ Header() http.Header }) string {
		return rec.Header().Get("Access-Control-Allow-Origin")
	}

	rec := e.do(http.MethodGet, "/healthz", "", "Origin", dashOrigin)
	if acao(rec) != dashOrigin || !strings.Contains(rec.Header().Get("Vary"), "Origin") {
		t.Fatalf("dashboard origin: ACAO %q Vary %q", acao(rec), rec.Header().Get("Vary"))
	}
	for _, origin := range []string{"https://evil.example.test", "http://dash.example.test", "https://dash.example.test.evil.test", "null", "*"} {
		rec := e.do(http.MethodGet, "/healthz", "", "Origin", origin)
		if h := rec.Header().Get("Access-Control-Allow-Origin"); h != "" {
			t.Errorf("origin %q got ACAO %q", origin, h)
		}
	}
	if acao(e.do(http.MethodGet, "/healthz", "")) != "" {
		t.Error("a request with no Origin got an ACAO header")
	}

	pre := e.do(http.MethodOptions, "/v1/kv/k", "", "Origin", dashOrigin, "Access-Control-Request-Method", "PUT",
		"Access-Control-Request-Headers", "content-type, authorization")
	wantStatus(t, pre, http.StatusNoContent)
	if acao(pre) != dashOrigin {
		t.Fatalf("preflight ACAO = %q", acao(pre))
	}
	if m := pre.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(m, "PUT") || !strings.Contains(m, "DELETE") {
		t.Fatalf("preflight methods = %q", m)
	}
	if h := strings.ToLower(pre.Header().Get("Access-Control-Allow-Headers")); !strings.Contains(h, "authorization") || !strings.Contains(h, "content-type") {
		t.Fatalf("preflight headers = %q", h)
	}
	other := e.do(http.MethodOptions, "/v1/kv/k", "", "Origin", "https://evil.example.test", "Access-Control-Request-Method", "PUT")
	if acao(other) != "" || other.Code < 400 {
		t.Fatalf("preflight from another origin: status %d ACAO %q", other.Code, acao(other))
	}
}

func TestRequestLogsHaveNoIPOrSecrets(t *testing.T) {
	e := newEnv(t)
	const (
		ip, xff = "203.0.113.77", "198.51.100.200"
		token   = "distinctive-bearer-TOKEN-9931"
	)
	addr := ip + ":5555"
	b, _ := json.Marshal(map[string]string{"value": "very-secret-value"})
	e.from(addr, http.MethodPut, "/v1/kv/very-secret-key", string(b), "Authorization", "Bearer "+token, "X-Forwarded-For", xff)
	e.from(addr, http.MethodGet, "/v1/kv/very-secret-key", "", "Authorization", "Bearer "+token, "X-Forwarded-For", xff)
	e.from(addr, http.MethodGet, "/v1/kv/"+strings.Repeat("z", 80), "")
	e.from(addr, http.MethodPost, "/v1/admin/reset", "", "Authorization", "Bearer "+token)
	e.from(addr, http.MethodPost, "/v1/admin/reset", "", "Authorization", "Bearer "+adminToken)

	logs := e.log.String()
	if strings.TrimSpace(logs) == "" {
		t.Fatal("no request was logged")
	}
	for _, secret := range []string{ip, xff, token, adminToken, "very-secret-key", "very-secret-value", strings.Repeat("z", 80)} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log contains %q", secret)
		}
	}
	var line map[string]any
	first := strings.SplitN(strings.TrimSpace(logs), "\n", 2)[0]
	if err := json.Unmarshal([]byte(first), &line); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	for _, field := range []string{"method", "route", "status", "latency_ms", "request_id"} {
		if _, ok := line[field]; !ok {
			t.Errorf("log line %v has no %q", line, field)
		}
	}
	if line["route"] != "/v1/kv/{key}" {
		t.Errorf("route = %v, want the route template", line["route"])
	}

	// No address is persisted anywhere.
	err := filepath.WalkDir(e.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, rerr := os.ReadFile(path) //nolint:gosec // walking our own temp dir
		if rerr != nil {
			return rerr
		}
		if bytes.Contains(data, []byte(ip)) || bytes.Contains(data, []byte(xff)) {
			t.Errorf("%s contains a client address", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGatewayCannotCallMembership(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf((*gateway.Cluster)(nil)).Elem(), reflect.TypeOf((*gateway.KV)(nil)).Elem()} {
		for i := 0; i < typ.NumMethod(); i++ {
			name := strings.ToLower(typ.Method(i).Name)
			if strings.Contains(name, "node") || strings.Contains(name, "member") {
				t.Errorf("%s has a membership-looking method %s", typ.Name(), typ.Method(i).Name)
			}
		}
	}

	// No non-test source in the package may name the membership client or its
	// calls. Parsed rather than grepped, so comments do not count. A live
	// adapter file, if present, is covered; its absence is fine.
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{
		"ClusterServiceClient": true, "NewClusterServiceClient": true, "ClusterServiceServer": true,
		"AddNode": true, "RemoveNode": true, "ListMembers": true,
		"AddNodeRequest": true, "RemoveNodeRequest": true, "ListMembersRequest": true,
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".go") || strings.HasSuffix(ent.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, ent.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && forbidden[id.Name] {
				t.Errorf("%s: references %s", fset.Position(id.Pos()), id.Name)
			}
			return true
		})
	}
}

func TestClusterEndpointShapeAndCache(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.put("k", "v"), http.StatusOK)
	e.g.PollOnce(background())

	for i := 0; i < 50; i++ {
		wantStatus(t, e.do(http.MethodGet, "/v1/cluster", ""), http.StatusOK)
	}
	for id := uint64(1); id <= 3; id++ {
		if n := e.fc.StatusCalls(id); n != 1 {
			t.Fatalf("node %d was polled %d times for 50 requests, want 1", id, n)
		}
	}

	type node struct {
		ID            uint64 `json:"id"`
		AZ            string `json:"az"`
		Up            bool   `json:"up"`
		Role          string `json:"role"`
		Term          uint64 `json:"term"`
		CommitIndex   uint64 `json:"commitIndex"`
		LastApplied   uint64 `json:"lastApplied"`
		LogLen        uint64 `json:"logLen"`
		SnapshotIndex uint64 `json:"snapshotIndex"`
		LastContactMs *int64 `json:"lastContactMs"`
		Paused        bool   `json:"paused"`
	}
	var view struct {
		Nodes []node `json:"nodes"`
	}
	fetch := func() map[uint64]node {
		t.Helper()
		rec := e.do(http.MethodGet, "/v1/cluster", "")
		wantStatus(t, rec, http.StatusOK)
		view.Nodes = nil
		decode(t, rec, &view)
		out := map[uint64]node{}
		for _, n := range view.Nodes {
			out[n.ID] = n
		}
		return out
	}
	nodes := fetch()
	if len(nodes) != 3 {
		t.Fatalf("%d nodes in the view, want 3", len(nodes))
	}
	n1 := nodes[1]
	if !n1.Up || n1.Role != "leader" || n1.AZ != "us-east-1a" || n1.Term == 0 || n1.CommitIndex == 0 ||
		n1.LastApplied != n1.CommitIndex || n1.LogLen != n1.CommitIndex+1 || n1.LastContactMs == nil || *n1.LastContactMs != 0 {
		t.Fatalf("leader entry = %+v", n1)
	}
	if nodes[2].Role != "follower" || nodes[3].AZ != "us-east-1c" {
		t.Fatalf("followers = %+v %+v", nodes[2], nodes[3])
	}

	e.clk.Advance(5 * time.Second)
	_ = e.fc.Crash(background(), 3)
	_ = e.fc.Pause(background(), 2, time.Second)
	e.g.PollOnce(background())
	nodes = fetch()
	if nodes[3].Up || nodes[3].Role != "unknown" || nodes[3].LastContactMs == nil || *nodes[3].LastContactMs != 5000 {
		t.Fatalf("crashed node entry = %+v (lastContactMs is ms since the last good poll)", nodes[3])
	}
	if !nodes[2].Up || !nodes[2].Paused {
		t.Fatalf("paused node entry = %+v", nodes[2])
	}

	// A node that never answers never blocks a request.
	e.fc.HangStatus(2, true)
	before := e.fc.StatusCalls(2)
	e.clk.Advance(time.Second)
	polled := make(chan struct{})
	go func() { e.g.PollOnce(background()); close(polled) }()
	guard(t, "the poll to reach the hanging node", func() { e.fc.WaitStatusCalls(2, before+1) })
	wantStatus(t, e.do(http.MethodGet, "/v1/cluster", ""), http.StatusOK)
	e.clk.Advance(2 * time.Second)
	guard(t, "the poll to give up on the hanging node", func() { <-polled })
	nodes = fetch()
	if nodes[2].Up {
		t.Fatalf("a node that timed out is still up: %+v", nodes[2])
	}
}

func TestHealthzNeverTouchesCluster(t *testing.T) {
	e := newEnv(t)
	e.fc.HangKV(true)
	for id := uint64(1); id <= 3; id++ {
		e.fc.HangStatus(id, true)
	}
	clients := e.fc.Clients()

	rec := e.do(http.MethodGet, "/healthz", "")
	wantStatus(t, rec, http.StatusOK)
	var body struct{ Status string }
	decode(t, rec, &body)
	if body.Status != "ok" {
		t.Fatalf("body = %q", rec.Body.String())
	}
	for id := uint64(1); id <= 3; id++ {
		if n := e.fc.StatusCalls(id); n != 0 {
			t.Fatalf("/healthz polled node %d", id)
		}
	}
	if e.fc.Clients() != clients {
		t.Fatal("/healthz opened a cluster session")
	}
}

func TestGatewayConfigFailsClosed(t *testing.T) {
	e := newEnv(t)
	for name, mod := range map[string]func(*gateway.Config){
		"no cluster":      func(c *gateway.Config) { c.Cluster = nil },
		"no clock":        func(c *gateway.Config) { c.Clock = nil },
		"no rand":         func(c *gateway.Config) { c.Rand = nil },
		"no state dir":    func(c *gateway.Config) { c.StateDir = "" },
		"no admin token":  func(c *gateway.Config) { c.AdminToken = "" },
		"no origin":       func(c *gateway.Config) { c.DashboardOrigin = "" },
		"wildcard origin": func(c *gateway.Config) { c.DashboardOrigin = "*" },
	} {
		cfg := e.cfg
		cfg.StateDir = t.TempDir()
		mod(&cfg)
		if g, err := gateway.New(cfg); err == nil {
			_ = g.Close()
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func TestLoadTokenFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("good", "  hunter2-secret \n", 0o600)
	if tok, err := gateway.LoadTokenFile(good); err != nil || tok != "hunter2-secret" {
		t.Fatalf("LoadTokenFile(good) = %q, %v", tok, err)
	}
	for name, p := range map[string]string{
		"missing":        filepath.Join(dir, "absent"),
		"empty":          write("empty", "", 0o600),
		"whitespace":     write("ws", " \n\t", 0o600),
		"group readable": write("loose", "hunter2-secret", 0o640),
		"directory":      dir,
		"no path":        "",
	} {
		tok, err := gateway.LoadTokenFile(p)
		if err == nil {
			t.Errorf("%s: no error", name)
			continue
		}
		if tok != "" || strings.Contains(err.Error(), "hunter2-secret") {
			t.Errorf("%s: token %q or error %q leaks the secret", name, tok, err)
		}
	}
}
