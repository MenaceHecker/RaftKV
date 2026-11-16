package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Chaos limits (RUNBOOK 10.6, card D5b). All of them are enforced by one
// mutex-guarded state machine, chaosCtl.
const (
	// chaosBucketEvery is the public token bucket: one fault per 20 s for
	// everybody together.
	chaosBucketEvery = 20 * time.Second
	// chaosAutoHeal is how long a fault may last. The nodes expire their own
	// faults at 120 s and clamp Pause at 60 s, but those are backstops for a
	// dead gateway; the gateway never relies on them.
	chaosAutoHeal   = 45 * time.Second
	defaultPause    = 20 * time.Second
	maxPause        = 45 * time.Second
	maxNetemDelay   = 500 * time.Millisecond
	maxNetemDrop    = 50
	maxChaosBody    = 1024
	chaosRPCTimeout = 3 * time.Second
)

// chaosSlot is the one active fault. It is released only when the cluster is
// seen healthy again after a heal, never merely because a timer fired.
type chaosSlot struct {
	action  string
	target  uint64
	expires time.Time
	healed  bool          // a heal was issued; waiting to see every node back
	stop    chan struct{} // closed to cancel the auto-heal timer
	stopped bool
}

type chaosCtl struct {
	mu          sync.Mutex
	slot        *chaosSlot
	nextAllowed time.Time // public token bucket
}

type chaosCaller int

const (
	callerPublic chaosCaller = iota
	callerAdmin
	callerVerify
)

type chaosReq struct {
	action string // kill, pause, partition, delay
	node   uint64 // 0 means the leader
	pause  time.Duration
	delay  time.Duration
	drop   uint32
}

type chaosResult struct {
	Action  string    `json:"action"`
	Target  uint64    `json:"target"`
	Expires time.Time `json:"expiresAt"`
}

type chaosError struct {
	code  int
	msg   string
	retry time.Duration
}

func (e *chaosError) Error() string { return e.msg }

// connectionLost reports a Crash reply that never came back. The node replies
// and then exits, so losing the connection right after the call is success.
func connectionLost(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		status.Code(err) == codes.Unavailable
}

// nodeGoneAfterCrash decides whether a Crash error that is not obviously a lost
// connection (an adapter may wrap it in its own sentinel) still means the node
// was told to die: the error must not be an explicit refusal, and the node must
// now fail to answer Status. It depends on no adapter type.
func (g *Gateway) nodeGoneAfterCrash(ctx context.Context, id uint64, err error) bool {
	switch status.Code(err) {
	case codes.Unknown, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
	default:
		return false // PermissionDenied, Unauthenticated, ...: refused, not lost
	}
	_, serr := g.rpcStatus(ctx, id)
	return serr != nil && status.Code(serr) != codes.Unauthenticated && status.Code(serr) != codes.PermissionDenied
}

func (g *Gateway) rpcStatus(ctx context.Context, id uint64) (st NodeStatus, err error) {
	err = g.rpc(ctx, func(c context.Context) (e error) { st, e = g.cl.Status(c, id); return e })
	return st, err
}

// rpc runs one node call under the RPC deadline, on the injected clock.
func (g *Gateway) rpc(ctx context.Context, fn func(context.Context) error) error {
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	timeout := g.clk.After(chaosRPCTimeout)
	res := make(chan error, 1)
	go func() { res <- fn(c) }()
	select {
	case err := <-res:
		return err
	case <-timeout:
		cancel()
		return errTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// chaosDo applies one fault if every rule allows it. A refusal changes
// nothing and sends no fault RPC.
func (g *Gateway) chaosDo(parent context.Context, req chaosReq, who chaosCaller) (chaosResult, *chaosError) {
	// The fault must not be half applied because the visitor hung up.
	ctx := context.WithoutCancel(parent)

	if who != callerVerify && g.VerifyRunning() {
		return chaosResult{}, &chaosError{code: http.StatusConflict, msg: "a verification run is in progress"}
	}

	// A public kill of a node that is not the cached leader needs the token.
	if who == callerPublic && req.action == "kill" && req.node != 0 && req.node != g.cachedLeader() {
		return chaosResult{}, &chaosError{code: http.StatusUnauthorized, msg: "only the leader may be killed without the admin token"}
	}

	c := &g.chaos
	c.mu.Lock()
	now := g.clk.Now()
	if who == callerPublic && now.Before(c.nextAllowed) {
		wait := c.nextAllowed.Sub(now)
		c.mu.Unlock()
		return chaosResult{}, &chaosError{code: http.StatusTooManyRequests, msg: "one fault per 20 seconds", retry: wait}
	}
	waiting := c.slot != nil && c.slot.healed
	c.mu.Unlock()

	// A slot that is only waiting for recovery is released the moment every
	// node is seen healthy, so look before refusing.
	if waiting {
		g.PollOnce(ctx)
	}

	c.mu.Lock()
	if c.slot != nil {
		c.mu.Unlock()
		return chaosResult{}, &chaosError{code: http.StatusConflict, msg: "a fault is already active or still recovering"}
	}
	// Hold the slot while the cluster is looked at, so two requests cannot
	// both pass.
	mine := &chaosSlot{action: req.action, stop: make(chan struct{})}
	c.slot = mine
	c.mu.Unlock()

	release := func() {
		c.mu.Lock()
		if c.slot == mine {
			c.slot = nil
		}
		c.mu.Unlock()
	}

	leader, herr := g.healthyLeader(ctx)
	if herr != nil {
		release()
		return chaosResult{}, herr
	}
	target := req.node
	if target == 0 {
		target = leader
	}
	if req.action == "kill" && who == callerPublic && target != leader {
		release()
		return chaosResult{}, &chaosError{code: http.StatusUnauthorized, msg: "only the leader may be killed without the admin token"}
	}

	c.mu.Lock()
	now = g.clk.Now()
	if who == callerPublic {
		c.nextAllowed = now.Add(chaosBucketEvery)
	}
	mine.target = target
	mine.expires = now.Add(chaosAutoHeal)
	timer := g.clk.After(chaosAutoHeal)
	c.mu.Unlock()

	err := g.rpc(ctx, func(cc context.Context) error { return g.applyFault(cc, req, target) })
	if err != nil && req.action == "kill" && !connectionLost(err) && g.nodeGoneAfterCrash(ctx, target, err) {
		err = nil
	}
	if err != nil && (req.action != "kill" || !connectionLost(err)) {
		// The fault may be partly applied: undo what can be undone, free the
		// slot, and say it failed.
		_ = g.rpc(ctx, func(cc context.Context) error { return g.cl.ClearFaults(cc, target) })
		release()
		g.log.Warn("chaos fault failed", "action", req.action, "target", target)
		return chaosResult{}, &chaosError{code: http.StatusBadGateway, msg: "the node did not accept the fault"}
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		select {
		case <-timer:
			g.chaosHeal(context.WithoutCancel(g.base), mine)
		case <-mine.stop:
		case <-g.base.Done():
		}
	}()
	res := chaosResult{Action: req.action, Target: target, Expires: mine.expires}
	g.log.Info("chaos fault", "action", req.action, "target", target)
	g.publishChaos("fault", res.Action, res.Target)
	return res, nil
}

func (g *Gateway) applyFault(ctx context.Context, req chaosReq, target uint64) error {
	switch req.action {
	case "kill":
		return g.cl.Crash(ctx, target)
	case "pause":
		return g.cl.Pause(ctx, target, req.pause)
	case "partition":
		var blocked []uint64
		for _, n := range g.topology {
			if n.ID != target {
				blocked = append(blocked, n.ID)
			}
		}
		return g.cl.Partition(ctx, target, blocked)
	case "delay":
		return g.cl.Netem(ctx, target, 0, req.delay, req.drop)
	}
	return errors.New("gateway: unknown chaos action")
}

// healthyLeader asks every node for a fresh status and returns the leader.
// Any node that does not answer, or is paused, refuses the fault: with three
// nodes that is what keeps a quorum of two.
func (g *Gateway) healthyLeader(ctx context.Context) (uint64, *chaosError) {
	g.PollOnce(ctx)
	g.pollMu.RLock()
	defer g.pollMu.RUnlock()
	var leader, term uint64
	for _, n := range g.topology {
		c := g.cache[n.ID]
		if !c.up || c.status.Paused {
			return 0, &chaosError{code: http.StatusServiceUnavailable, msg: "a node is not healthy, so no new fault is allowed", retry: 5 * time.Second}
		}
		if c.status.Role == "leader" && (leader == 0 || c.status.Term > term) {
			leader, term = n.ID, c.status.Term
		}
	}
	if leader == 0 {
		return 0, &chaosError{code: http.StatusServiceUnavailable, msg: "no leader is known", retry: 5 * time.Second}
	}
	return leader, nil
}

// cachedLeader is the leader from the 1 Hz poll, with no RPC.
func (g *Gateway) cachedLeader() uint64 {
	g.pollMu.RLock()
	defer g.pollMu.RUnlock()
	for _, n := range g.topology {
		if c := g.cache[n.ID]; c.up && c.status.Role == "leader" {
			return n.ID
		}
	}
	return 0
}

// chaosHeal clears every fault it can reach: partitions, netem and pauses.
// It cannot undo a Crash. The slot stays held until every node is seen up and
// not paused (chaosObserve), so a node still restarting keeps new faults out.
// If only is not nil, it is a no-op when that slot is no longer the current
// one (a stale timer).
func (g *Gateway) chaosHeal(ctx context.Context, only *chaosSlot) (healed, failed []uint64) {
	c := &g.chaos
	c.mu.Lock()
	if only != nil && c.slot != only {
		c.mu.Unlock()
		return nil, nil
	}
	if c.slot != nil && !c.slot.stopped {
		c.slot.stopped = true
		close(c.slot.stop)
	}
	c.mu.Unlock()

	type res struct {
		id  uint64
		err error
	}
	out := make(chan res, len(g.topology))
	for _, n := range g.topology {
		go func() {
			out <- res{n.ID, g.rpc(ctx, func(cc context.Context) error { return g.cl.ClearFaults(cc, n.ID) })}
		}()
	}
	for range g.topology {
		r := <-out
		if r.err != nil {
			failed = append(failed, r.id)
		} else {
			healed = append(healed, r.id)
		}
	}

	c.mu.Lock()
	if c.slot != nil {
		c.slot.healed = true
	}
	c.mu.Unlock()
	g.PollOnce(ctx) // observes recovery and releases the slot
	g.publishChaos("heal", "heal", 0)
	return healed, failed
}

// chaosObserve releases a healed slot once the last poll saw every node up
// and not paused.
func (g *Gateway) chaosObserve() {
	if !g.allHealthy() {
		return
	}
	g.chaos.mu.Lock()
	if g.chaos.slot != nil && g.chaos.slot.healed {
		g.chaos.slot = nil
	}
	g.chaos.mu.Unlock()
}

// allHealthy reports whether the last poll saw every node up and not paused.
func (g *Gateway) allHealthy() bool {
	g.pollMu.RLock()
	defer g.pollMu.RUnlock()
	for _, n := range g.topology {
		if c := g.cache[n.ID]; !c.up || c.status.Paused {
			return false
		}
	}
	return true
}

type chaosView struct {
	State     string     `json:"state"` // idle, active or recovering
	Action    string     `json:"action,omitempty"`
	Target    uint64     `json:"target,omitempty"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

func (g *Gateway) chaosView() chaosView {
	g.chaosObserve()
	g.chaos.mu.Lock()
	defer g.chaos.mu.Unlock()
	s := g.chaos.slot
	switch {
	case s == nil:
		return chaosView{State: "idle"}
	case s.healed:
		return chaosView{State: "recovering", Action: s.action, Target: s.target}
	}
	exp := s.expires
	return chaosView{State: "active", Action: s.action, Target: s.target, ExpiresAt: &exp}
}

// safeStart runs once at process start. A fault set by a dead gateway may
// still be on a node (at most 120 s), so heal every node, count the public
// bucket as just used, and hold the slot until every node answers.
func (g *Gateway) safeStart(ctx context.Context) {
	c := &g.chaos
	c.mu.Lock()
	c.nextAllowed = g.clk.Now().Add(chaosBucketEvery)
	if c.slot == nil {
		c.slot = &chaosSlot{action: "startup", healed: true, stop: make(chan struct{}), stopped: true}
	}
	c.mu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.chaosHeal(ctx, nil)
	}()
}

// --- HTTP ---

func (g *Gateway) chaosHandler(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, "POST")
			return
		}
		who := callerPublic
		if r.Header.Get("Authorization") != "" {
			if !g.authenticated(r) {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			who = callerAdmin
		}
		if action == "heal" {
			g.handleHeal(w, r)
			return
		}
		req, code, msg := g.parseChaos(action, r, w)
		if code != 0 {
			writeError(w, code, msg)
			return
		}
		res, cerr := g.chaosDo(r.Context(), req, who)
		if cerr != nil {
			if cerr.retry > 0 {
				secs := int((cerr.retry + time.Second - 1) / time.Second)
				w.Header().Set("Retry-After", strconv.Itoa(secs))
			}
			writeError(w, cerr.code, cerr.msg)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"action": res.Action, "target": res.Target, "expiresAt": res.Expires,
			"autoHealSeconds": int(chaosAutoHeal / time.Second),
		})
	}
}

func (g *Gateway) handleHeal(w http.ResponseWriter, r *http.Request) {
	healed, failed := g.chaosHeal(context.WithoutCancel(r.Context()), nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"healed": nonNil(healed), "failed": nonNil(failed), "state": g.chaosView().State,
	})
}

func nonNil(v []uint64) []uint64 {
	if v == nil {
		return []uint64{}
	}
	return v
}

// parseChaos reads the tiny JSON body (1 KB, no unknown fields) and the node
// id in the path, and applies the caps.
func (g *Gateway) parseChaos(action string, r *http.Request, w http.ResponseWriter) (chaosReq, int, string) {
	var body struct {
		Seconds     *int64 `json:"seconds"`
		DelayMs     *int64 `json:"delayMs"`
		DropPercent *int64 `json:"dropPercent"`
		Node        *int64 `json:"node"`
	}
	if err := decodeSmall(w, r, &body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return chaosReq{}, http.StatusRequestEntityTooLarge, "request body is too large"
		}
		return chaosReq{}, http.StatusBadRequest, "body must be a small JSON object with known fields"
	}

	req := chaosReq{action: action}
	var id uint64
	switch action {
	case "kill", "pause":
		if p := r.PathValue("id"); p != "" {
			n, err := strconv.ParseUint(p, 10, 64)
			if err != nil || !g.knownNode(n) {
				return req, http.StatusBadRequest, "unknown node"
			}
			id = n
		}
	}
	if body.Node != nil {
		if *body.Node < 1 || !g.knownNode(uint64(*body.Node)) {
			return req, http.StatusBadRequest, "unknown node"
		}
		id = uint64(*body.Node)
	}
	req.node = id

	switch action {
	case "pause":
		req.pause = defaultPause
		if body.Seconds != nil {
			if *body.Seconds <= 0 {
				return req, http.StatusBadRequest, "seconds must be positive"
			}
			if *body.Seconds > int64(maxPause/time.Second) {
				req.pause = maxPause
			} else {
				req.pause = time.Duration(*body.Seconds) * time.Second
			}
		}
	case "delay":
		var delay, drop int64
		if body.DelayMs != nil {
			delay = *body.DelayMs
		}
		if body.DropPercent != nil {
			drop = *body.DropPercent
		}
		switch {
		case delay < 0 || drop < 0:
			return req, http.StatusBadRequest, "delayMs and dropPercent must not be negative"
		case delay > int64(maxNetemDelay/time.Millisecond):
			return req, http.StatusBadRequest, "delayMs is at most 500"
		case drop > maxNetemDrop:
			return req, http.StatusBadRequest, "dropPercent is at most 50"
		case delay == 0 && drop == 0:
			return req, http.StatusBadRequest, "set delayMs or dropPercent"
		}
		req.delay = time.Duration(delay) * time.Millisecond
		req.drop = uint32(drop)
	}
	return req, 0, ""
}

func (g *Gateway) knownNode(id uint64) bool {
	for _, n := range g.topology {
		if n.ID == id {
			return true
		}
	}
	return false
}

// decodeSmall reads one JSON object of at most 1 KB, refusing unknown fields
// and trailing data. An empty body is allowed: every field is optional.
func decodeSmall(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxChaosBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}
