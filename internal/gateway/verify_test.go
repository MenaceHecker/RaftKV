package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

func TestVerifySingleFlight(t *testing.T) {
	e := newEnv(t)
	rec := e.post("/v1/verify/run", "")
	wantStatus(t, rec, http.StatusAccepted)
	var first struct {
		ID, Status string
		Seed       string
	}
	decode(t, rec, &first)
	if first.ID == "" || first.Status != "running" || first.Seed == "" {
		t.Fatalf("start response = %s", rec.Body.String())
	}

	// A second run, public or with the token, joins nothing: it is refused and
	// told which run is going.
	for _, hdr := range [][]string{nil, bearer()} {
		rec = e.post("/v1/verify/run", "", hdr...)
		wantStatus(t, rec, http.StatusConflict)
		var second struct{ ID string }
		decode(t, rec, &second)
		if second.ID != first.ID {
			t.Fatalf("409 names run %q, want %q", second.ID, first.ID)
		}
	}
	// While it runs, public writes are locked and the report is not there yet.
	wantStatus(t, e.put("k", "v"), http.StatusServiceUnavailable)
	if !strings.Contains(e.put("k", "v").Body.String(), "verification in progress") {
		t.Fatal("the 503 does not say why")
	}
	if !e.g.VerifyRunning() {
		t.Fatal("VerifyRunning is false during a run")
	}
	e.drive()
	wantStatus(t, e.put("k", "v"), http.StatusOK)
}

func TestVerifyBlocksPublicChaosButNotHeal(t *testing.T) {
	e, spy := newSpyEnv(t, nil)
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusAccepted)
	for p, body := range map[string]string{
		"/v1/chaos/kill-leader": "", "/v1/chaos/delay": `{"delayMs":5}`,
		"/v1/chaos/pause/2": "", "/v1/chaos/partition": "",
	} {
		wantStatus(t, e.post(p, body), http.StatusConflict)
		wantStatus(t, e.bpost(p, body), http.StatusConflict)
	}
	if got := spy.faults(); len(got) != 0 {
		t.Fatalf("a fault reached a node during the run: %v", got)
	}
	wantStatus(t, e.post("/v1/chaos/heal", ""), http.StatusOK)
	e.drive()
}

func TestVerifyPublicCooldownPersistsAcrossRestart(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusAccepted)
	e.drive()

	// The cooldown runs 15 minutes from the end of the run.
	end := e.last().EndedAt
	left := end.Add(15 * time.Minute).Sub(e.clk.Now())
	rec := e.post("/v1/verify/run", "")
	wantStatus(t, rec, http.StatusTooManyRequests)
	if ra := rec.Header().Get("Retry-After"); ra != strconv.Itoa(int((left+time.Second-1)/time.Second)) {
		t.Fatalf("Retry-After = %q, want %d", ra, int((left+time.Second-1)/time.Second))
	}
	e.reopen() // a restart does not reset it
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusTooManyRequests)
	e.clk.Advance(left - time.Second)
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusTooManyRequests)
	e.clk.Advance(time.Second)
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusAccepted)
	e.drive()
}

func TestVerifyTokenSkipsCooldown(t *testing.T) {
	e := newEnv(t)
	e.runVerify()
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusTooManyRequests)
	wantStatus(t, e.post("/v1/verify/run", "", "Authorization", "Bearer wrong"), http.StatusUnauthorized)
	wantStatus(t, e.bpost("/v1/verify/run", ""), http.StatusAccepted)
	e.drive()
}

func TestVerifyAbortedMarkerOnRestart(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) { c.Cluster.(*fakecluster.Cluster).HangKV(true) })
	wantStatus(t, e.post("/v1/verify/run", ""), http.StatusAccepted)
	// The process dies now: only what is on disk survives.
	data, err := os.ReadFile(filepath.Join(e.dir, "verify.json"))
	if err != nil {
		t.Fatalf("no verify.json while the run is going: %v", err)
	}
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "verify.json"), data, 0o600); err != nil { //nolint:gosec // test temp dir
		t.Fatal(err)
	}

	e2 := newEnv(t, func(c *gateway.Config) { c.StateDir = dir2; c.Clock = e.clk })
	r := e2.last()
	if r.Status != "aborted" || r.Verdict != "inconclusive" {
		t.Fatalf("leftover run is %q / %q, want aborted / inconclusive", r.Status, r.Verdict)
	}
	// A crash loop cannot be used to dodge the cooldown.
	wantStatus(t, e2.post("/v1/verify/run", ""), http.StatusTooManyRequests)
	e.clk.Advance(15 * time.Minute)
	wantStatus(t, e2.post("/v1/verify/run", ""), http.StatusAccepted)
	_ = e.g.Close()
	_ = e2.g.Close()
}

func TestVerifyWallLimit60sAndOpCap(t *testing.T) {
	t.Run("wall limit", func(t *testing.T) {
		e := newEnv(t, func(c *gateway.Config) { c.Cluster.(*fakecluster.Cluster).HangKV(true) })
		r := e.runVerify()
		if r.ElapsedMs < 60000 || r.ElapsedMs > 62500 {
			t.Fatalf("elapsed = %d ms, want the 60 s wall limit", r.ElapsedMs)
		}
		if r.Ops == 0 || r.OK != 0 {
			t.Fatalf("ops %d ok %d", r.Ops, r.OK)
		}
		if r.Verdict != "inconclusive" {
			t.Fatalf("a run with no successful operation proves nothing, got %q", r.Verdict)
		}
	})
	t.Run("op cap", func(t *testing.T) {
		e := newEnv(t, func(c *gateway.Config) { c.VerifyMaxOps = 50 })
		r := e.runVerify()
		if r.Ops != 50 || r.OK+r.Failed+r.Unknown != 50 {
			t.Fatalf("ops = %d (ok %d failed %d unknown %d), want exactly 50", r.Ops, r.OK, r.Failed, r.Unknown)
		}
	})
}

func TestVerifyLinearizableFakePasses(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.bpost("/v1/verify/run", ""), http.StatusAccepted)
	e.drive()
	r := e.last()
	if r.Status != "done" || r.Verdict != "linearizable" {
		t.Fatalf("report = %+v", r)
	}
	if r.Ops < 500 || r.Ops > 5000 || r.Reads == 0 || r.Writes == 0 || r.Reads+r.Writes != r.Ops {
		t.Fatalf("ops %d reads %d writes %d, want hundreds split into both", r.Ops, r.Reads, r.Writes)
	}
	if r.Seed == "" || r.ID == "" || r.Explored == 0 {
		t.Fatalf("report = %+v", r)
	}
	var kinds []string
	for _, f := range r.Faults {
		kinds = append(kinds, f.Action)
		if !f.OK {
			t.Fatalf("nemesis step %+v failed", f)
		}
	}
	if !reflect.DeepEqual(kinds, []string{"partition", "heal", "kill"}) {
		t.Fatalf("faults = %v, want isolate the leader, heal, kill the leader", kinds)
	}
	if r.Faults[0].Target != 1 || r.Faults[2].Target != 1 {
		t.Fatalf("the nemesis did not target the leader: %+v", r.Faults)
	}
	if e.fc.Violations() != nil {
		t.Fatalf("session misuse: %v", e.fc.Violations())
	}
}

func TestVerifyDetectsLyingCluster(t *testing.T) {
	e := newEnv(t)
	e.fc.SetLyingReads(true)
	r := e.runVerify()
	if r.Verdict != "not_linearizable" {
		t.Fatalf("verdict = %q on a cluster that returns stale reads, want not_linearizable (%+v)", r.Verdict, r)
	}
	if !strings.Contains(r.Counterexample, "not linearizable") || !strings.Contains(r.Counterexample, "__verify/k") {
		t.Fatalf("counterexample = %q", r.Counterexample)
	}
}

func TestVerifyWriteErrorsAreUnknownNeverFailed(t *testing.T) {
	for name, err := range map[string]error{
		"not leader": gateway.ErrNoLeader,
		"aborted":    errors.New("aborted"),
		"plain":      errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, func(c *gateway.Config) { c.VerifyMaxOps = 400 })
			e.fc.FailWrites(err)
			r := e.runVerify()
			if r.Writes == 0 || r.Unknown != r.Writes {
				t.Fatalf("writes %d, unknown %d: every failed write is unknown", r.Writes, r.Unknown)
			}
			if r.Failed != 0 {
				t.Fatalf("%d ops failed; a write error must never be failed, and reads here succeed", r.Failed)
			}
			if r.OK != r.Reads {
				t.Fatalf("ok %d, reads %d", r.OK, r.Reads)
			}
		})
	}
	t.Run("read errors fail the read only", func(t *testing.T) {
		e := newEnv(t, func(c *gateway.Config) { c.VerifyMaxOps = 400 })
		e.fc.FailReads(errors.New("boom"))
		r := e.runVerify()
		if r.Failed != r.Reads || r.Unknown != 0 || r.OK != r.Writes {
			t.Fatalf("%+v", r)
		}
	})
}

func TestVerifyUndecidedIsInconclusive(t *testing.T) {
	e := newEnv(t, func(c *gateway.Config) { c.VerifyCheckBudget = 1; c.VerifyMaxOps = 200 })
	r := e.runVerify()
	if r.Verdict != "inconclusive" || !strings.Contains(r.Counterexample, "undecided") {
		t.Fatalf("verdict %q, detail %q: an undecided check must not pass", r.Verdict, r.Counterexample)
	}
}

type panicKV struct{ gateway.KV }

func (panicKV) Put(context.Context, string, []byte) error { panic("boom") }

func TestVerifyHealsEverythingAfterRun(t *testing.T) {
	lastClears := func(spy *spyCluster) []string {
		calls := spy.faults()
		if len(calls) < 3 {
			return calls
		}
		tail := append([]string(nil), calls[len(calls)-3:]...)
		sort.Strings(tail)
		return tail
	}
	want := []string{"clear 1", "clear 2", "clear 3"}

	t.Run("normal end", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil, func(c *gateway.Config) { c.VerifyMaxOps = 800 })
		e.runVerify()
		if got := lastClears(spy); !reflect.DeepEqual(got, want) {
			t.Fatalf("the last RPCs were %v, want heal on every node", spy.faults())
		}
	})
	t.Run("panic in a worker", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil)
		spy.kvWrap = func(kv gateway.KV) gateway.KV { return panicKV{kv} }
		wantStatus(t, e.bpost("/v1/verify/run", ""), http.StatusAccepted)
		e.drive()
		r := e.last()
		if r.Status != "aborted" || r.Verdict != "inconclusive" {
			t.Fatalf("report = %+v", r)
		}
		if got := lastClears(spy); !reflect.DeepEqual(got, want) {
			t.Fatalf("after a panic the last RPCs were %v, want heal on every node", spy.faults())
		}
		wantStatus(t, e.put("k", "v"), http.StatusOK) // the write lock is gone
	})
	t.Run("cancelled context", func(t *testing.T) {
		e, spy := newSpyEnv(t, nil)
		wantStatus(t, e.bpost("/v1/verify/run", ""), http.StatusAccepted)
		if err := e.g.Close(); err != nil {
			t.Fatal(err)
		}
		if got := lastClears(spy); !reflect.DeepEqual(got, want) {
			t.Fatalf("after shutdown the last RPCs were %v, want heal on every node", spy.faults())
		}
		data, err := os.ReadFile(filepath.Join(e.dir, "verify.json"))
		if err != nil || !strings.Contains(string(data), "aborted") {
			t.Fatalf("verify.json = %q (%v), want the run recorded as aborted", data, err)
		}
	})
}

func TestVerifyKeysAreDisjointFromPublic(t *testing.T) {
	e := newEnv(t)
	e.runVerify()
	keys := e.fc.Keys()
	if len(keys) == 0 {
		t.Fatal("the run wrote nothing")
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, gateway.ReservedPrefix+"k") {
			t.Fatalf("the run touched %q, outside %sk0..k4", k, gateway.ReservedPrefix)
		}
	}
	if len(e.g.Keys()) != 0 {
		t.Fatalf("verify keys leaked into the public registry: %v", e.g.Keys())
	}
	wantStatus(t, e.put("__verify%2Fk0", "x"), http.StatusForbidden)
	wantStatus(t, e.get("__verify%2Fk0"), http.StatusForbidden)
}

func TestVerifyLastReturnsPersistedReport(t *testing.T) {
	e := newEnv(t)
	wantStatus(t, e.do(http.MethodGet, "/v1/verify/last", ""), http.StatusNotFound)
	wantStatus(t, e.do(http.MethodGet, "/v1/verify/run", ""), http.StatusMethodNotAllowed)
	e.runVerify()
	before := e.do(http.MethodGet, "/v1/verify/last", "").Body.Bytes()
	var m map[string]any
	if err := json.Unmarshal(before, &m); err != nil || m["id"] == nil {
		t.Fatalf("bad report %s (%v)", before, err)
	}
	if strings.Contains(string(before), adminToken) {
		t.Fatal("the report contains the token")
	}
	e.reopen()
	after := e.do(http.MethodGet, "/v1/verify/last", "").Body.Bytes()
	var m2 map[string]any
	_ = json.Unmarshal(after, &m2)
	if !reflect.DeepEqual(m, m2) {
		t.Fatalf("report changed across a restart:\n%s\n%s", before, after)
	}
}
