package gateway_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

func TestChaosTokenBucketOnePerTwentySeconds(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":100}`), http.StatusOK)

	rec := e.post("/v1/chaos/delay", `{"delayMs":100}`)
	wantStatus(t, rec, http.StatusTooManyRequests)
	if ra := rec.Header().Get("Retry-After"); ra != "20" {
		t.Fatalf("Retry-After = %q, want 20", ra)
	}
	// Another address does not get its own bucket: it is one bucket for everybody.
	wantStatus(t, e.postFrom("203.0.113.5:1", "/v1/chaos/delay", `{"delayMs":100}`), http.StatusTooManyRequests)

	wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK)
	e.clk.Advance(19 * time.Second)
	rec = e.post("/v1/chaos/delay", `{"delayMs":100}`)
	wantStatus(t, rec, http.StatusTooManyRequests)
	if ra := rec.Header().Get("Retry-After"); ra != "1" {
		t.Fatalf("Retry-After = %q, want 1", ra)
	}
	e.clk.Advance(time.Second)
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":100}`), http.StatusOK)
	if n := spy.count("netem"); n != 2 {
		t.Fatalf("%d netem calls, want 2 (the throttled ones must not reach a node): %v", n, spy.faults())
	}
}

func TestOnlyOneActiveFault(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	const n = 8
	results := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = e.bpost("/v1/chaos/delay", `{"delayMs":50}`).Code
		}()
	}
	wg.Wait()
	won, lost := 0, 0
	for _, c := range results {
		switch c {
		case http.StatusOK:
			won++
		case http.StatusConflict:
			lost++
		default:
			t.Fatalf("unexpected status %d in %v", c, results)
		}
	}
	if won != 1 || lost != n-1 {
		t.Fatalf("won %d lost %d, want 1 and %d: %v", won, lost, n-1, results)
	}
	if got := spy.faults(); len(got) != 1 {
		t.Fatalf("losers reached a node: %v", got)
	}
	wantStatus(t, e.bpost("/v1/chaos/partition", ""), http.StatusConflict)
	wantStatus(t, e.bpost("/v1/chaos/pause/2", ""), http.StatusConflict)
}

func TestAutoHealAt45s(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusOK)
	if got := spy.faults(); !reflect.DeepEqual(got, []string{"crash 1"}) {
		t.Fatalf("calls = %v", got)
	}
	e.clk.Advance(44 * time.Second)
	if n := spy.count("clear"); n != 0 {
		t.Fatalf("healed after 44 s: %v", spy.faults())
	}
	e.clk.Advance(time.Second)
	eventuallyYield(t, "heal on all nodes", func() bool { return spy.count("clear") == 3 })

	// Node 1 is still down, so the slot stays held and the cluster says so.
	e.pollAll()
	eventuallyYield(t, "state recovering", func() bool { return e.chaosState() == "recovering" })
	wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusConflict)
	e.clk.Advance(time.Hour) // the bucket is not what holds it back
	wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusConflict)

	e.fc.Restart(1)
	e.pollAll()
	if st := e.chaosState(); st != "idle" {
		t.Fatalf("state = %q after the node came back, want idle", st)
	}
	wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
}

func TestAutoHealClearsPausedNode(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	wantStatus(t, e.post("/v1/chaos/pause/2", `{"seconds":30}`), http.StatusOK)
	e.clk.Advance(45 * time.Second)
	eventuallyYield(t, "heal", func() bool { return spy.count("clear") == 3 })
	e.pollAll()
	eventuallyYield(t, "idle", func() bool { return e.chaosState() == "idle" })
}

func TestNeverTwoNodesDown(t *testing.T) {
	crashFirst := func(t *testing.T) (*env, *spyCluster) {
		e, spy := newSpyEnv(t, nil)
		wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusOK)
		return e, spy
	}
	t.Run("crash then pause", func(t *testing.T) {
		e, spy := crashFirst(t)
		wantStatus(t, e.bpost("/v1/chaos/pause/2", ""), http.StatusConflict)
		if spy.count("pause") != 0 {
			t.Fatalf("pause reached a node: %v", spy.faults())
		}
	})
	t.Run("crash then isolate", func(t *testing.T) {
		e, spy := crashFirst(t)
		wantStatus(t, e.bpost("/v1/chaos/partition", `{"node":2}`), http.StatusConflict)
		if spy.count("partition") != 0 {
			t.Fatalf("partition reached a node: %v", spy.faults())
		}
	})
	t.Run("crashed node not yet back after the auto-heal", func(t *testing.T) {
		e, spy := crashFirst(t)
		e.clk.Advance(45 * time.Second)
		eventuallyYield(t, "heal", func() bool { return spy.count("clear") == 3 })
		wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusConflict)
		wantStatus(t, e.bpost("/v1/chaos/pause/3", ""), http.StatusConflict)
	})
	t.Run("two nodes unreachable", func(t *testing.T) {
		e, spy := newSpyEnv(t, func(fc *fakecluster.Cluster, _ *spyCluster) {
			_ = fc.Crash(context.Background(), 2)
			_ = fc.Crash(context.Background(), 3)
		})
		wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusServiceUnavailable)
		if got := spy.faults(); len(got) != 0 {
			t.Fatalf("a fault was sent: %v", got)
		}
	})
	t.Run("one node already down, slot free", func(t *testing.T) {
		e, spy := newSpyEnv(t, func(fc *fakecluster.Cluster, _ *spyCluster) {
			_ = fc.Crash(context.Background(), 3)
		})
		wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusServiceUnavailable)
		wantStatus(t, e.bpost("/v1/chaos/pause/2", ""), http.StatusServiceUnavailable)
		if got := spy.faults(); len(got) != 0 {
			t.Fatalf("a fault was sent: %v", got)
		}
	})
	t.Run("no leader known", func(t *testing.T) {
		e, spy := newSpyEnv(t, func(fc *fakecluster.Cluster, _ *spyCluster) { fc.SetLeader(0) })
		wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusServiceUnavailable)
		if got := spy.faults(); len(got) != 0 {
			t.Fatalf("a fault was sent: %v", got)
		}
	})
}

func TestKillLeaderMapsToCrashOnLeader(t *testing.T) {
	cases := []struct {
		name   string
		leader uint64
		path   string
		body   string
		hdr    []string
		want   []string
	}{
		{"kill-leader", 1, "/v1/chaos/kill-leader", "", nil, []string{"crash 1"}},
		{"kill-leader after a leader change", 2, "/v1/chaos/kill-leader", "", nil, []string{"crash 2"}},
		{"kill the leader by id, public", 1, "/v1/chaos/kill/1", "", nil, []string{"crash 1"}},
		{"kill a follower, bearer", 1, "/v1/chaos/kill/3", "", bearer(), []string{"crash 3"}},
		{"pause with seconds", 1, "/v1/chaos/pause/2", `{"seconds":10}`, nil, []string{"pause 2 10s"}},
		{"pause default 20 s", 1, "/v1/chaos/pause/2", "", nil, []string{"pause 2 20s"}},
		{"partition defaults to the leader", 1, "/v1/chaos/partition", "", nil, []string{"partition 1 [2 3]"}},
		{"partition of a chosen node", 1, "/v1/chaos/partition", `{"node":3}`, nil, []string{"partition 3 [1 2]"}},
		{"delay", 1, "/v1/chaos/delay", `{"delayMs":200,"dropPercent":10}`, nil, []string{"netem 1 peer=0 delay=200ms drop=10"}},
		{"delay on a chosen node", 1, "/v1/chaos/delay", `{"delayMs":5,"node":2}`, nil, []string{"netem 2 peer=0 delay=5ms drop=0"}},
		{"heal", 1, "/v1/chaos/heal", "", nil, []string{"clear 1", "clear 2", "clear 3"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, spy := newSpyEnv(t, nil)
			e.fc.SetLeader(tc.leader)
			e.pollAll() // the 1 Hz poller's cache, which says who leads
			wantStatus(t, e.post(tc.path, tc.body, tc.hdr...), http.StatusOK)
			got := spy.faults()
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("RPCs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPublicKillOfAFollowerNeedsTheToken(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	e.pollAll()
	wantStatus(t, e.post("/v1/chaos/kill/2", ""), http.StatusUnauthorized)
	if got := spy.faults(); len(got) != 0 {
		t.Fatalf("a fault was sent: %v", got)
	}
	// A refusal does not use up the public bucket.
	wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusOK)
}

func TestPauseIsCappedBelowNodeCap(t *testing.T) {
	for _, body := range []string{`{"seconds":600}`, `{"seconds":46}`, `{"seconds":45}`, `{"seconds":3600000000}`} {
		e, spy := newSpyEnv(t, nil)
		wantStatus(t, e.post("/v1/chaos/pause/2", body), http.StatusOK)
		if got := spy.faults(); !reflect.DeepEqual(got, []string{"pause 2 45s"}) {
			t.Fatalf("body %s: RPCs = %v, want one pause of 45s", body, got)
		}
	}
	for _, body := range []string{`{"seconds":0}`, `{"seconds":-5}`, `{"seconds":"x"}`} {
		e, spy := newSpyEnv(t, nil)
		wantStatus(t, e.post("/v1/chaos/pause/2", body), http.StatusBadRequest)
		if got := spy.faults(); len(got) != 0 {
			t.Fatalf("body %s reached a node: %v", body, got)
		}
	}
}

func TestCrashConnectionLossIsSuccess(t *testing.T) {
	for name, err := range map[string]error{
		"EOF":         io.EOF,
		"unexpected":  io.ErrUnexpectedEOF,
		"unavailable": status.Error(codes.Unavailable, "transport is closing"),
	} {
		t.Run(name, func(t *testing.T) {
			e, spy := newSpyEnv(t, nil)
			spy.crashErr = err
			wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusOK)
			if st := e.chaosState(); st != "active" {
				t.Fatalf("state = %q, want active", st)
			}
			wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusConflict)
		})
	}
	t.Run("an adapter's own sentinel counts when the node is then gone", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil)
		spy.crashErr = errors.New("node unavailable")
		wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusOK)
	})
	t.Run("an unexplained error with the node still up is a failure", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil)
		spy.crashErr, spy.crashNoop = errors.New("weird"), true
		wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusBadGateway)
	})
	t.Run("a real failure is not success and frees the slot", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil)
		spy.crashErr, spy.crashNoop = status.Error(codes.PermissionDenied, "nope"), true
		wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusBadGateway)
		spy.crashErr = nil
		wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
	})
}

func TestNetemCaps(t *testing.T) {
	bad := []string{
		`{"delayMs":501}`, `{"dropPercent":51}`, `{"delayMs":-1}`, `{"dropPercent":-1}`,
		`{"delayMs":0,"dropPercent":0}`, `{"delayMs":"x"}`, `{"delayMs":1.5}`, `{"surprise":1,"delayMs":1}`,
	}
	e, spy := newSpyEnv(t, nil)
	for _, b := range bad {
		wantStatus(t, e.post("/v1/chaos/delay", b), http.StatusBadRequest)
	}
	if got := spy.faults(); len(got) != 0 {
		t.Fatalf("a refused request reached a node: %v", got)
	}
	// Refused input did not use up the bucket, and the caps themselves are allowed.
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":500,"dropPercent":50}`), http.StatusOK)
	if got := spy.faults(); !reflect.DeepEqual(got, []string{"netem 1 peer=0 delay=500ms drop=50"}) {
		t.Fatalf("RPCs = %v", got)
	}
}

func TestHealAlwaysAllowedAndUnthrottled(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	for i := 0; i < 5; i++ {
		wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK)
	}
	if n := spy.count("clear"); n != 15 {
		t.Fatalf("%d clear calls, want 15", n)
	}
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":100}`), http.StatusOK)
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":100}`), http.StatusTooManyRequests)
	wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK) // throttled for faults, never for heal
	if st := e.chaosState(); st != "idle" {
		t.Fatalf("state = %q after heal, want idle", st)
	}
	if rec := e.do(http.MethodGet, "/v1/chaos/heal", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET heal = %d, want 405", rec.Code)
	}
}

func TestRejectedChaosCallsNoAdminRPC(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
	faultsBefore, statusBefore := len(spy.faults()), spy.statuses.Load()

	rejected := []struct {
		name, path, body string
		hdr              []string
		want             int
	}{
		{"bucket", "/v1/chaos/delay", `{"delayMs":10}`, nil, http.StatusTooManyRequests},
		{"slot busy", "/v1/chaos/pause/2", "", bearer(), http.StatusConflict},
		{"public follower kill", "/v1/chaos/kill/2", "", nil, http.StatusUnauthorized},
		{"wrong token", "/v1/chaos/kill/2", "", []string{"Authorization", "Bearer wrong"}, http.StatusUnauthorized},
		{"unknown node", "/v1/chaos/pause/9", "", bearer(), http.StatusBadRequest},
		{"bad node id", "/v1/chaos/pause/x", "", bearer(), http.StatusBadRequest},
		{"bad json", "/v1/chaos/delay", `{`, bearer(), http.StatusBadRequest},
		{"big body", "/v1/chaos/delay", `{"delayMs":1,"pad":"` + strings.Repeat("a", 2000) + `"}`, bearer(), http.StatusRequestEntityTooLarge},
		{"unknown action", "/v1/chaos/explode", "", bearer(), http.StatusNotFound},
	}
	for _, tc := range rejected {
		wantStatus(t, e.post(tc.path, tc.body, tc.hdr...), tc.want)
		if got := len(spy.faults()); got != faultsBefore {
			t.Fatalf("%s: a rejected request sent a fault RPC: %v", tc.name, spy.faults())
		}
		if got := spy.statuses.Load(); got != statusBefore {
			t.Fatalf("%s: a rejected request called Status %d times", tc.name, got-statusBefore)
		}
	}
}

func TestChaosNeverLogsTokenOrAddress(t *testing.T) {
	e, _ := newSpyEnv(t, nil)
	wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
	wantStatus(t, e.post("/v1/chaos/kill/2", ""), http.StatusUnauthorized) // not the leader
	wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK)
	rec := e.post("/v1/chaos/kill/2", "", "Authorization", "Bearer "+adminToken)
	for _, secret := range []string{adminToken, "198.51.100.1"} {
		if strings.Contains(e.log.String(), secret) {
			t.Fatalf("the log contains %q:\n%s", secret, e.log.String())
		}
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("a response contains %q", secret)
		}
	}
	if !strings.Contains(e.log.String(), "/v1/chaos/delay") {
		t.Fatalf("the chaos route is not logged by template:\n%s", e.log.String())
	}
}

func TestBearerBypassesBucketNotSafetyRules(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	for i := 0; i < 3; i++ { // no clock movement: the bucket would refuse every second one
		wantStatus(t, e.bpost("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
		wantStatus(t, e.bpost("/v1/chaos/heal", ""), http.StatusOK)
	}
	// The bearer token does not use up the public bucket either.
	wantStatus(t, e.post("/v1/chaos/delay", `{"delayMs":10}`), http.StatusOK)
	wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK)

	// One fault at a time still applies.
	wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusOK)
	n := len(spy.faults())
	wantStatus(t, e.bpost("/v1/chaos/pause/2", ""), http.StatusConflict)
	// A wrong token is a refusal, not a public call.
	wantStatus(t, e.post("/v1/chaos/delay", "", "Authorization", "Bearer nope"), http.StatusUnauthorized)
	if len(spy.faults()) != n {
		t.Fatalf("refused requests reached a node: %v", spy.faults())
	}
}

func TestStartupHealsAndBlocksChaosUntilNodesUp(t *testing.T) {
	e, spy := newSpyEnv(t, func(fc *fakecluster.Cluster, _ *spyCluster) {
		_ = fc.Crash(context.Background(), 3)
	})
	e.start()
	eventuallyYield(t, "heal-all at start", func() bool { return spy.count("clear") == 3 })

	// The bucket counts as just used, so a restart cannot be used to dodge it.
	rec := e.post("/v1/chaos/kill-leader", "")
	wantStatus(t, rec, http.StatusTooManyRequests)
	if ra := rec.Header().Get("Retry-After"); ra != "20" {
		t.Fatalf("Retry-After = %q, want 20", ra)
	}
	e.clk.Advance(20 * time.Second)
	// Node 3 is not back: the start-up hold on the slot stays until it is.
	wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusConflict)
	wantStatus(t, e.bpost("/v1/chaos/kill-leader", ""), http.StatusConflict)
	if n := spy.count("crash"); n != 0 {
		t.Fatalf("a crash was sent while a node was down: %v", spy.faults())
	}
	e.fc.Restart(3)
	wantStatus(t, e.post("/v1/chaos/kill-leader", ""), http.StatusOK)
}
