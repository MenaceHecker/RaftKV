package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MenaceHecker/raftkv/chaos"
)

// A verification run drives a bounded workload through the real client path,
// records every operation, breaks the cluster on a fixed schedule, and has
// chaos.Check decide whether the recorded history is linearizable. The nemesis
// goes through the same controller as the public buttons, so its one-fault and
// one-node-down rules hold, but it skips the public bucket.
const (
	verifyFile        = "verify.json"
	verifyWorkers     = 5
	verifyKeys        = 5
	verifyPace        = 50 * time.Millisecond
	DefaultVerifyWall = 60 * time.Second
	DefaultVerifyOps  = 5000
	DefaultCooldown   = 15 * time.Minute
	DefaultOpTimeout  = 2 * time.Second
	verifyRecoverWait = 60 * time.Second
)

// Report is the stored result of one run. The history itself is not stored.
type Report struct {
	ID             string        `json:"id"`
	Seed           string        `json:"seed"`
	Status         string        `json:"status"`  // running, done or aborted
	Verdict        string        `json:"verdict"` // linearizable, not_linearizable or inconclusive
	StartedAt      time.Time     `json:"startedAt"`
	EndedAt        time.Time     `json:"endedAt"`
	ElapsedMs      int64         `json:"elapsedMs"`
	Ops            int           `json:"ops"`
	Reads          int           `json:"reads"`
	Writes         int           `json:"writes"`
	OK             int           `json:"ok"`
	Failed         int           `json:"failed"`
	Unknown        int           `json:"unknown"`
	Explored       int           `json:"explored"`
	Counterexample string        `json:"counterexample,omitempty"`
	Recovered      bool          `json:"recovered"`
	Faults         []verifyFault `json:"faults"`
}

type verifyFault struct {
	Action string `json:"action"` // partition, heal or kill
	Target uint64 `json:"target,omitempty"`
	AtMs   int64  `json:"atMs"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

type verifyRunning struct {
	ID        string `json:"id"`
	StartedAt int64  `json:"startedAtUnixNano"`
}

// verifyDisk is verify.json: the last report, when the last run ended (for
// the public cooldown), and a marker while a run is going.
type verifyDisk struct {
	LastEnd int64          `json:"lastEndUnixNano,omitempty"`
	Running *verifyRunning `json:"running,omitempty"`
	Report  *Report        `json:"report,omitempty"`
}

type verifier struct {
	mu       sync.Mutex
	running  bool
	id       string
	started  time.Time
	disk     verifyDisk
	sessions []KV
}

// loadVerify reads verify.json. A marker left by a dead process becomes an
// aborted report, and the cooldown restarts now, so a crash loop cannot be
// used to dodge it.
func (g *Gateway) loadVerify() error {
	path := filepath.Join(g.cfg.StateDir, verifyFile)
	data, err := os.ReadFile(path) //nolint:gosec // inside the operator's state dir
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", verifyFile, err)
	}
	var d verifyDisk
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("%s: %w", verifyFile, err)
	}
	if d.Running != nil {
		now := g.clk.Now()
		d.Report = &Report{
			ID: d.Running.ID, Status: "aborted", Verdict: "inconclusive",
			StartedAt: time.Unix(0, d.Running.StartedAt).UTC(), EndedAt: now.UTC(),
			Counterexample: "the gateway stopped while the run was going",
			Faults:         []verifyFault{},
		}
		d.LastEnd = now.UnixNano()
		d.Running = nil
		if err := g.saveVerify(d); err != nil {
			return err
		}
	}
	g.ver.disk = d
	return nil
}

// saveVerify writes verify.json atomically: temp file, fsync, rename, fsync
// of the directory.
func (g *Gateway) saveVerify(d verifyDisk) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	path := filepath.Join(g.cfg.StateDir, verifyFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // inside the operator's state dir
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(g.cfg.StateDir)
}

// VerifyRunning reports whether a verification run is in progress.
func (g *Gateway) VerifyRunning() bool {
	g.ver.mu.Lock()
	defer g.ver.mu.Unlock()
	return g.ver.running
}

func (g *Gateway) verifySettings() (wall time.Duration, maxOps int, opTimeout time.Duration, budget int) {
	wall, maxOps, opTimeout, budget = g.cfg.VerifyWall, g.cfg.VerifyMaxOps, g.cfg.VerifyOpTimeout, g.cfg.VerifyCheckBudget
	if wall <= 0 {
		wall = DefaultVerifyWall
	}
	if maxOps <= 0 {
		maxOps = DefaultVerifyOps
	}
	if opTimeout <= 0 {
		opTimeout = DefaultOpTimeout
	}
	if budget <= 0 {
		budget = chaos.DefaultBudget
	}
	return
}

func (g *Gateway) cooldown() time.Duration {
	if g.cfg.VerifyCooldown > 0 {
		return g.cfg.VerifyCooldown
	}
	return DefaultCooldown
}

func (g *Gateway) handleVerifyRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	admin := false
	if r.Header.Get("Authorization") != "" {
		if !g.authenticated(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		admin = true
	}

	v := &g.ver
	v.mu.Lock()
	if v.running {
		id := v.id
		v.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a verification run is already going", "id": id})
		return
	}
	now := g.clk.Now()
	if !admin && v.disk.LastEnd != 0 {
		wait := time.Unix(0, v.disk.LastEnd).Add(g.cooldown()).Sub(now)
		if wait > g.cooldown() { // the clock went backwards
			wait = g.cooldown()
		}
		if wait > 0 {
			v.mu.Unlock()
			w.Header().Set("Retry-After", strconv.Itoa(int((wait+time.Second-1)/time.Second)))
			writeError(w, http.StatusTooManyRequests, "a verification run was done recently")
			return
		}
	}
	id := "v" + strconv.FormatUint(g.rnd.Uint64(), 16)
	seed := g.rnd.Uint64()
	d := v.disk
	d.Running = &verifyRunning{ID: id, StartedAt: now.UnixNano()}
	if err := g.saveVerify(d); err != nil {
		v.mu.Unlock()
		g.log.Error("verify state write failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	v.running, v.id, v.started = true, id, now
	v.disk = d
	v.mu.Unlock()

	// Public writes are locked from now until the run ends.
	g.mu.Lock()
	g.verifying++
	g.mu.Unlock()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.runVerify(id, seed, now)
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "seed": strconv.FormatUint(seed, 10), "status": "running"})
}

func (g *Gateway) handleVerifyLast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	v := &g.ver
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case v.running:
		writeJSON(w, http.StatusOK, map[string]any{"id": v.id, "status": "running", "startedAt": v.started})
	case v.disk.Report == nil:
		writeError(w, http.StatusNotFound, "no verification run yet")
	default:
		writeJSON(w, http.StatusOK, v.disk.Report)
	}
}

// --- the run ---

type splitmix struct{ s uint64 }

func (r *splitmix) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

type runState struct {
	g     *Gateway
	id    string
	seed  uint64
	start time.Time
	ctx   context.Context

	cancel context.CancelFunc

	ts      atomic.Int64 // monotonic stamp for Invoked and Returned
	claimed atomic.Int64

	mu         sync.Mutex
	history    []chaos.Op
	reads      int
	writes     int
	okN        int
	failedN    int
	unknownN   int
	seedFailed bool
	faults     []verifyFault
	abort      string
	wallAt     time.Time
}

func (rs *runState) abortRun(reason string) {
	rs.mu.Lock()
	if rs.abort == "" {
		rs.abort = reason
	}
	rs.mu.Unlock()
	rs.cancel()
}

// guard turns a panic in any goroutine of the run into an aborted run, so the
// process survives and the heal below still happens.
func (rs *runState) guard() {
	if p := recover(); p != nil {
		rs.abortRun("internal error during the run")
	}
}

func (g *Gateway) verifySessions() []KV {
	g.ver.mu.Lock()
	defer g.ver.mu.Unlock()
	for len(g.ver.sessions) < verifyWorkers {
		g.ver.sessions = append(g.ver.sessions, g.cl.NewKV())
	}
	return g.ver.sessions
}

// timed runs one cluster call under the per-operation deadline. The call is
// always waited for, so a session never has two calls in flight.
func (rs *runState) timed(opTimeout time.Duration, fn func(context.Context) error) error {
	octx, ocancel := context.WithCancel(rs.ctx)
	defer ocancel()
	timer := rs.g.clk.After(opTimeout)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-timer:
			ocancel()
		case <-stop:
		}
	}()
	return fn(octx)
}

func (g *Gateway) runVerify(id string, seed uint64, start time.Time) {
	wall, maxOps, opTimeout, budget := g.verifySettings()
	ctx, cancel := context.WithCancel(g.base)
	defer cancel()
	rs := &runState{g: g, id: id, seed: seed, start: start, ctx: ctx, cancel: cancel}
	g.hub.publish("verify", map[string]string{"phase": "start", "id": id})

	func() {
		defer rs.guard()
		rs.exec(wall, maxOps, opTimeout)
	}()

	// Whatever happened, leave the cluster whole.
	hctx := context.WithoutCancel(g.base)
	g.chaosHeal(hctx, nil)
	recovered := rs.waitRecovered(hctx)

	rep := rs.report(budget, recovered)
	d := verifyDisk{LastEnd: rep.EndedAt.UnixNano(), Report: &rep}
	if err := g.saveVerify(d); err != nil {
		g.log.Error("verify state write failed", "err", err)
	}
	g.mu.Lock()
	g.verifying--
	g.mu.Unlock()
	g.ver.mu.Lock()
	g.ver.disk = d
	g.ver.running = false
	g.ver.mu.Unlock()
	g.hub.publish("verify", map[string]any{"phase": "done", "id": id, "verdict": rep.Verdict, "ops": rep.Ops})
}

func (rs *runState) exec(wall time.Duration, maxOps int, opTimeout time.Duration) {
	g := rs.g
	sessions := g.verifySessions()
	var bg sync.WaitGroup

	wallDone := g.clk.After(wall)
	bg.Add(2)
	go func() {
		defer bg.Done()
		select {
		case at := <-wallDone:
			rs.mu.Lock()
			rs.wallAt = at
			rs.mu.Unlock()
			rs.cancel()
		case <-rs.ctx.Done():
		}
	}()
	go func() {
		defer bg.Done()
		defer rs.guard()
		rs.nemesis()
	}()

	rs.seedKeys(sessions[0], opTimeout)

	var workers sync.WaitGroup
	for w := 0; w < verifyWorkers; w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer rs.guard()
			rs.worker(w, sessions[w], maxOps, opTimeout)
		}()
	}
	workers.Wait()
	rs.cancel()
	bg.Wait()
}

// seedKeys writes a known first value to each key, so a value left behind by
// an earlier run can never be mistaken for a lie.
func (rs *runState) seedKeys(kv KV, opTimeout time.Duration) {
	for i := 0; i < verifyKeys && rs.ctx.Err() == nil; i++ {
		key := fmt.Sprintf("%sk%d", ReservedPrefix, i)
		val := fmt.Sprintf("seed-%s-%d", rs.id, i)
		inv := rs.ts.Add(1)
		err := rs.timed(opTimeout, func(c context.Context) error { return kv.Put(c, key, []byte(val)) })
		ret := rs.ts.Add(1)
		op := chaos.Op{Kind: chaos.OpWrite, Client: 100 + i, Key: key, Value: val, Invoked: inv, Returned: ret, Status: chaos.StatusOK}
		if err != nil {
			op.Status = chaos.StatusUnknown
		}
		rs.mu.Lock()
		rs.history = append(rs.history, op)
		rs.seedFailed = rs.seedFailed || err != nil
		rs.mu.Unlock()
	}
}

func (rs *runState) worker(w int, kv KV, maxOps int, opTimeout time.Duration) {
	g := rs.g
	rng := splitmix{s: rs.seed + uint64(w) + 1} //nolint:gosec // w is 0..4
	for n := 0; rs.ctx.Err() == nil; n++ {
		if rs.claimed.Add(1) > int64(maxOps) {
			return
		}
		key := fmt.Sprintf("%sk%d", ReservedPrefix, rng.next()%verifyKeys)
		read := rng.next()%100 < 40
		op := chaos.Op{Client: w, Key: key}
		var err error
		op.Invoked = rs.ts.Add(1)
		if read {
			op.Kind = chaos.OpRead
			var v []byte
			var found bool
			err = rs.timed(opTimeout, func(c context.Context) (e error) { v, found, e = kv.Get(c, key); return e })
			op.Found, op.Value = found, string(v)
		} else {
			op.Kind = chaos.OpWrite
			op.Value = fmt.Sprintf("c%d-%d", w, n)
			err = rs.timed(opTimeout, func(c context.Context) error { return kv.Put(c, key, []byte(op.Value)) })
		}
		op.Returned = rs.ts.Add(1)

		// A read that errors changed nothing, so it did not happen. A write
		// that errors, whatever the error (NotLeader and Aborted included),
		// may still have been applied: it is unknown, never failed.
		switch {
		case err == nil:
			op.Status = chaos.StatusOK
		case read:
			op.Status = chaos.StatusFailed
			op.Found, op.Value = false, ""
		default:
			op.Status = chaos.StatusUnknown
		}
		rs.mu.Lock()
		rs.history = append(rs.history, op)
		if read {
			rs.reads++
		} else {
			rs.writes++
		}
		switch op.Status {
		case chaos.StatusOK:
			rs.okN++
		case chaos.StatusFailed:
			rs.failedN++
		default:
			rs.unknownN++
		}
		rs.mu.Unlock()

		select {
		case <-g.clk.After(verifyPace):
		case <-rs.ctx.Done():
			return
		}
	}
}

// nemesis breaks the cluster on a fixed schedule (offset by the seed): isolate
// the leader, heal, kill the leader.
func (rs *runState) nemesis() {
	g := rs.g
	jitter := time.Duration(rs.seed%3) * time.Second
	steps := []struct {
		at   time.Duration
		name string
	}{{10*time.Second + jitter, "partition"}, {25*time.Second + jitter, "heal"}, {35*time.Second + jitter, "kill"}}
	for _, s := range steps {
		d := rs.start.Add(s.at).Sub(g.clk.Now())
		select {
		case <-g.clk.After(max(d, 0)):
		case <-rs.ctx.Done():
			return
		}
		f := verifyFault{Action: s.name, AtMs: g.clk.Now().Sub(rs.start).Milliseconds(), OK: true}
		if s.name == "heal" {
			_, failed := g.chaosHeal(context.WithoutCancel(g.base), nil)
			f.OK = len(failed) == 0
		} else {
			res, cerr := g.chaosDo(rs.ctx, chaosReq{action: s.name}, callerVerify)
			if cerr != nil {
				f.OK, f.Error = false, cerr.msg
			} else {
				f.Target = res.Target
			}
		}
		rs.mu.Lock()
		rs.faults = append(rs.faults, f)
		rs.mu.Unlock()
		g.hub.publish("verify", map[string]any{"phase": "fault", "id": rs.id, "action": f.Action, "ok": f.OK})
	}
}

// waitRecovered waits, bounded, for every node to answer and not be paused.
func (rs *runState) waitRecovered(ctx context.Context) bool {
	g := rs.g
	deadline := g.clk.After(verifyRecoverWait)
	for {
		g.PollOnce(ctx)
		if g.allHealthy() {
			return true
		}
		if g.base.Err() != nil {
			return false
		}
		select {
		case <-g.clk.After(time.Second):
		case <-deadline:
			return false
		case <-g.base.Done():
			return false
		}
	}
}

func (rs *runState) report(budget int, recovered bool) Report {
	g := rs.g
	rs.mu.Lock()
	defer rs.mu.Unlock()
	end := rs.wallAt
	if end.IsZero() {
		end = g.clk.Now()
	}
	rep := Report{
		ID: rs.id, Seed: strconv.FormatUint(rs.seed, 10), Status: "done",
		StartedAt: rs.start.UTC(), EndedAt: end.UTC(), ElapsedMs: end.Sub(rs.start).Milliseconds(),
		Ops: rs.reads + rs.writes, Reads: rs.reads, Writes: rs.writes,
		OK: rs.okN, Failed: rs.failedN, Unknown: rs.unknownN,
		Recovered: recovered, Faults: append([]verifyFault{}, rs.faults...),
	}
	reason := rs.abort
	if reason == "" && g.base.Err() != nil {
		reason = "the gateway shut down during the run"
	}
	if reason != "" {
		rep.Status, rep.Verdict, rep.Counterexample = "aborted", "inconclusive", reason
		return rep
	}
	res := chaos.CheckWithBudget(rs.history, budget)
	rep.Explored = res.Explored
	switch {
	case res.Verdict == chaos.Linearizable && rep.OK == 0:
		rep.Verdict = "inconclusive"
		rep.Counterexample = "no operation completed, so nothing was checked"
	case res.Verdict == chaos.Linearizable:
		rep.Verdict = "linearizable"
	case res.Verdict == chaos.NotLinearizable && rs.seedFailed:
		rep.Verdict = "inconclusive"
		rep.Counterexample = "a seed write failed, so a value from an earlier run may be visible; " + res.String()
	case res.Verdict == chaos.NotLinearizable:
		rep.Verdict = "not_linearizable"
		rep.Counterexample = res.String()
	default:
		rep.Verdict = "inconclusive"
		rep.Counterexample = res.String()
	}
	return rep
}
