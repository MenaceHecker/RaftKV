package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"
)

var (
	errTimeout = errors.New("gateway: request deadline exceeded")
	errBusy    = errors.New("gateway: no client session available")
)

// runKV runs op on a pooled session under the request deadline.
//
// A session is a single client id and the state machine drops any write whose
// seq is not above that id's last one, so two writes must never share a
// session at once. The session therefore goes back to the pool only when the
// call has actually returned, even if the request gave up waiting for it.
func (g *Gateway) runKV(parent context.Context, op func(context.Context, KV) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	timeout := g.clk.After(g.cfg.RequestTimeout) // before the goroutine: see PollOnce

	res := make(chan error, 1)
	go func() {
		select {
		case kv := <-g.pool:
			defer func() { g.pool <- kv }()
			res <- op(ctx, kv)
		case <-ctx.Done():
			res <- errBusy
		}
	}()

	select {
	case err := <-res:
		return err
	case <-timeout:
		return errTimeout
	case <-parent.Done():
		return parent.Err()
	}
}

// writeBlock says why public writes are refused right now, or "".
func (g *Gateway) writeBlock() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case g.verifying > 0:
		return "verification in progress"
	case g.resetting:
		return "keyspace reset in progress"
	}
	return ""
}

// WithVerifyLock locks public writes while fn runs. The lock is released when
// fn returns, errors or panics.
func (g *Gateway) WithVerifyLock(fn func() error) error {
	g.mu.Lock()
	g.verifying++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.verifying--
		g.mu.Unlock()
	}()
	return fn()
}

// keyOf extracts and validates the key, writing the error response if it is
// not acceptable.
func keyOf(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if code, msg := keyProblem(key); code != 0 {
		writeError(w, code, msg)
		return "", false
	}
	return key, true
}

func (g *Gateway) handleEmptyKey(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusBadRequest, "key is empty")
}

func (g *Gateway) handleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.handleGet(w, r)
	case http.MethodPut:
		g.handlePut(w, r)
	case http.MethodDelete:
		g.handleDelete(w, r)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r)
	if !ok {
		return
	}
	var value []byte
	var found bool
	err := g.runKV(r.Context(), func(ctx context.Context, kv KV) (err error) {
		value, found, err = kv.Get(ctx, key)
		return err
	})
	if err != nil {
		g.kvError(w, err, false)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": string(value)})
}

func (g *Gateway) handlePut(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r)
	if !ok {
		return
	}
	if reason := g.writeBlock(); reason != "" {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, reason)
		return
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}

	// The body is capped before it is parsed; the value limit is checked after.
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	var body struct {
		Value *string `json:"value"`
	}
	err := dec.Decode(&body)
	if err == nil {
		// Anything after the one document is an error.
		if _, terr := dec.Token(); terr != io.EOF {
			err = errors.New("trailing data")
		}
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, `body must be {"value": "<string>"}`)
		return
	}
	if body.Value == nil {
		writeError(w, http.StatusBadRequest, `body must be {"value": "<string>"}`)
		return
	}
	if len(*body.Value) > MaxValueBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "value is larger than 1024 bytes")
		return
	}

	// Register the key first: if the process dies after the write but before
	// the registration, the key could never be counted or reset.
	isNew, err := g.reg.add(key)
	switch {
	case errors.Is(err, errKeyCap):
		writeError(w, http.StatusInsufficientStorage, "the keyspace is full")
		return
	case err != nil:
		g.log.Error("registry append failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	err = g.runKV(r.Context(), func(ctx context.Context, kv KV) error {
		return kv.Put(ctx, key, []byte(*body.Value))
	})
	if err != nil {
		// Only "no leader" proves the write never happened; for anything else
		// the key stays registered, because it may exist.
		if errors.Is(err, ErrNoLeader) && isNew {
			if rerr := g.reg.remove(key); rerr != nil {
				g.log.Error("registry append failed", "err", rerr)
			}
		}
		g.kvError(w, err, true)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key})
}

func (g *Gateway) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, ok := keyOf(w, r)
	if !ok {
		return
	}
	if reason := g.writeBlock(); reason != "" {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, reason)
		return
	}
	err := g.runKV(r.Context(), func(ctx context.Context, kv KV) error { return kv.Delete(ctx, key) })
	if err != nil {
		g.kvError(w, err, true)
		return
	}
	if err := g.reg.remove(key); err != nil {
		g.log.Error("registry append failed", "err", err)
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key})
}

// kvError maps a cluster error to an HTTP answer. It never repeats the
// error's text: that can hold node addresses.
func (g *Gateway) kvError(w http.ResponseWriter, err error, write bool) {
	switch {
	case errors.Is(err, ErrNoLeader), errors.Is(err, errBusy):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "no leader available, try again")
	case errors.Is(err, ErrIndeterminate), errors.Is(err, errTimeout), errors.Is(err, context.DeadlineExceeded):
		if write {
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{
				"error": "the write may or may not have been applied", "outcome": "unknown"})
			return
		}
		writeError(w, http.StatusGatewayTimeout, "the cluster did not answer in time")
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "request cancelled")
	default:
		g.log.Error("cluster call failed", "err", err)
		if write {
			writeJSON(w, http.StatusBadGateway, map[string]string{
				"error": "cluster error", "outcome": "unknown"})
			return
		}
		writeError(w, http.StatusBadGateway, "cluster error")
	}
}

// handleCAS is documented as unimplemented: the wire protocol and the state
// machine have no compare-and-swap (D5 card, Q1).
func (g *Gateway) handleCAS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"error":  "not implemented",
		"detail": "compare-and-swap has no backing: the cluster supports only get, put and delete",
	})
}

// ResetIfDue deletes every registered public key once the reset interval has
// passed and no verification run holds the lock. It reports whether it ran.
// If it cannot finish, the interval is not restarted, so the next call
// retries.
func (g *Gateway) ResetIfDue(ctx context.Context) (bool, error) {
	g.mu.Lock()
	due := g.clk.Now().Sub(g.lastReset) >= g.cfg.KeyspaceReset
	g.mu.Unlock()
	if !due {
		return false, nil
	}
	return g.reset(ctx)
}

func (g *Gateway) reset(ctx context.Context) (bool, error) {
	g.mu.Lock()
	if g.verifying > 0 || g.resetting {
		g.mu.Unlock()
		return false, nil
	}
	g.resetting = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.resetting = false
		g.mu.Unlock()
	}()

	for _, key := range g.reg.list() {
		err := g.runKV(ctx, func(c context.Context, kv KV) error { return kv.Delete(c, key) })
		if err != nil {
			return true, err
		}
		if err := g.reg.remove(key); err != nil {
			return true, err
		}
	}
	if err := g.reg.compact(); err != nil {
		return true, err
	}
	g.mu.Lock()
	g.lastReset = g.clk.Now()
	g.mu.Unlock()
	return true, nil
}

// RunReset checks the reset interval on every tick until ctx ends.
func (g *Gateway) RunReset(ctx context.Context, tick <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			if _, err := g.ResetIfDue(ctx); err != nil {
				g.log.Error("keyspace reset failed, will retry", "err", err)
			}
		}
	}
}

func (g *Gateway) handleAdminReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, "POST")
		return
	}
	ran, err := g.reset(r.Context())
	switch {
	case err != nil:
		g.log.Error("admin keyspace reset failed", "err", err)
		writeError(w, http.StatusBadGateway, "reset did not finish; it will be retried")
	case !ran:
		writeError(w, http.StatusConflict, "verification or another reset is in progress")
	default:
		writeJSON(w, http.StatusOK, map[string]bool{"reset": true})
	}
}
