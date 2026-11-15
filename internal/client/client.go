package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultPerAttempt     = 2 * time.Second
	DefaultTotal          = 30 * time.Second
	DefaultBackoffInitial = 20 * time.Millisecond
	DefaultBackoffCeiling = time.Second
)

// maxExtraConnsFactor bounds connections cached for hint addresses that are not in
// Config.Addrs, as a multiple of len(Addrs).
const maxExtraConnsFactor = 2

var errAttemptTimeout = errors.New("client: attempt timed out")

// KV is the part of the generated KVService client this package uses.
type KV = raftkvv1.KVServiceClient

// Backoff bounds the delay between attempts: Initial, then Initial*Multiplier,
// and so on, never above Max, each drawn with full jitter.
type Backoff struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

// Config configures New. Zero values get the documented defaults.
type Config struct {
	// Addrs is the static node list (required). Rotation uses only these.
	Addrs []string
	// ClientID is the session id. Zero draws a random non-zero id from Rand.
	// A caller that sets it also owns StartSeq: see the package comment.
	ClientID uint64
	// StartSeq is the first sequence number. Zero means 1.
	StartSeq uint64
	// DialOpts are passed to grpc.NewClient. Empty means insecure credentials.
	DialOpts []grpc.DialOption
	// Connect is a test seam replacing the default gRPC dial.
	Connect func(addr string) (KV, io.Closer, error)
	// PerAttempt bounds one RPC. Default DefaultPerAttempt.
	PerAttempt time.Duration
	// DefaultTotal bounds an operation whose context has no deadline.
	DefaultTotal time.Duration
	// Backoff bounds the delay between attempts.
	Backoff Backoff
	// Rand supplies jitter and the random client id. Default RealRand.
	Rand Rand
	// Clock supplies timers. Default RealClock.
	Clock Clock
}

// NodeStatus is one node's answer to Status.
type NodeStatus struct {
	Addr         string
	ID           uint64
	LeaderID     uint64
	LeaderAddr   string
	Term         uint64
	State        string // leader, follower, candidate or unknown
	CommitIndex  uint64
	AppliedIndex uint64
}

type conn struct {
	kv     KV
	closer io.Closer
}

// Client is a leader-following client for KVService. It is safe for
// concurrent use; writes are serialised, reads are not.
type Client struct {
	cfg Config
	id  uint64

	base       context.Context
	baseCancel context.CancelCauseFunc

	// writeSem has capacity 1: one write in flight per client id.
	writeSem chan struct{}
	nextSeq  uint64 // guarded by writeSem

	mu     sync.Mutex
	conns  map[string]*conn
	extra  int
	target string
	closed bool
}

// New creates a Client. No connection is made until the first operation.
func New(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("client: Addrs must not be empty")
	}
	cfg.Addrs = append([]string(nil), cfg.Addrs...)
	if cfg.PerAttempt <= 0 {
		cfg.PerAttempt = DefaultPerAttempt
	}
	if cfg.DefaultTotal <= 0 {
		cfg.DefaultTotal = DefaultTotal
	}
	if cfg.Backoff.Initial <= 0 {
		cfg.Backoff.Initial = DefaultBackoffInitial
	}
	if cfg.Backoff.Max <= 0 {
		cfg.Backoff.Max = DefaultBackoffCeiling
	}
	if cfg.Backoff.Max < cfg.Backoff.Initial {
		cfg.Backoff.Max = cfg.Backoff.Initial
	}
	if cfg.Backoff.Multiplier < 1 {
		cfg.Backoff.Multiplier = 2
	}
	if cfg.Rand == nil {
		cfg.Rand = RealRand{}
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Connect == nil {
		opts := cfg.DialOpts
		if len(opts) == 0 {
			opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		}
		cfg.Connect = func(addr string) (KV, io.Closer, error) {
			cc, err := grpc.NewClient(addr, opts...)
			if err != nil {
				return nil, nil, err
			}
			return raftkvv1.NewKVServiceClient(cc), cc, nil
		}
	}

	id := cfg.ClientID
	if id == 0 {
		// Id 0 means "no session, no dedupe" on the wire, so draw from [1, 2^63-1].
		n := cfg.Rand.Int63n(1<<63 - 1)
		id = uint64(n) + 1 //nolint:gosec // n is in [0, 2^63-1), so the sum cannot overflow
	}
	seq := cfg.StartSeq
	if seq == 0 {
		seq = 1
	}

	base, cancel := context.WithCancelCause(context.Background())
	return &Client{
		cfg:        cfg,
		id:         id,
		base:       base,
		baseCancel: cancel,
		writeSem:   make(chan struct{}, 1),
		nextSeq:    seq,
		conns:      make(map[string]*conn),
		target:     cfg.Addrs[0],
	}, nil
}

// ClientID returns the session id this client puts on every write.
func (c *Client) ClientID() uint64 { return c.id }

// Get reads key linearizably. It retries until an answer, the deadline or
// Close; a failure to get one wraps ErrUnavailable and never means "unknown".
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := c.preflight(ctx); err != nil {
		return nil, false, err.Err
	}
	var value []byte
	var found bool
	err := c.run(ctx, false, 0, func(actx context.Context, kv KV) error {
		resp, err := kv.Get(actx, &raftkvv1.GetRequest{Key: key})
		if err == nil {
			value, found = resp.GetValue(), resp.GetFound()
		}
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return value, found, nil
}

// Put sets key. See the package comment for the three possible outcomes.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.write(ctx, func(actx context.Context, kv KV, seq uint64) error {
		_, err := kv.Put(actx, &raftkvv1.PutRequest{
			Client: &raftkvv1.ClientRequest{ClientId: c.id, Sequence: seq},
			Key:    key,
			Value:  value,
		})
		return err
	})
}

// Delete removes key; deleting a missing key succeeds.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.write(ctx, func(actx context.Context, kv KV, seq uint64) error {
		_, err := kv.Delete(actx, &raftkvv1.DeleteRequest{
			Client: &raftkvv1.ClientRequest{ClientId: c.id, Sequence: seq},
			Key:    key,
		})
		return err
	})
}

// Status asks the nodes in rotation, once each starting at the current
// candidate, and returns the first answer. It records the leader it reports.
func (c *Client) Status(ctx context.Context) (NodeStatus, error) {
	if err := c.preflight(ctx); err != nil {
		return NodeStatus{}, err.Err
	}
	o := c.begin(ctx)
	defer o.end()

	start := c.startIndex()
	var last error
	for i := range c.cfg.Addrs {
		if o.done() {
			break
		}
		addr := c.cfg.Addrs[(start+i)%len(c.cfg.Addrs)]
		st, err := c.statusOnce(o, addr)
		if err == nil {
			return st, nil
		}
		last = err
	}
	return NodeStatus{}, c.unavailable(o.err(), last)
}

// StatusFrom asks one specific node, once, with no retry.
func (c *Client) StatusFrom(ctx context.Context, addr string) (NodeStatus, error) {
	if err := c.preflight(ctx); err != nil {
		return NodeStatus{}, err.Err
	}
	o := c.begin(ctx)
	defer o.end()
	st, err := c.statusOnce(o, addr)
	if err != nil {
		return NodeStatus{}, c.unavailable(o.err(), err)
	}
	return st, nil
}

func (c *Client) statusOnce(o *op, addr string) (NodeStatus, error) {
	cn, err := c.conn(addr)
	if err != nil {
		return NodeStatus{}, err
	}
	var resp *raftkvv1.StatusResponse
	_, err = c.attempt(o, func(actx context.Context) error {
		var e error
		resp, e = cn.kv.Status(actx, &raftkvv1.StatusRequest{})
		return e
	})
	if err != nil {
		return NodeStatus{}, err
	}
	st := NodeStatus{
		Addr:         addr,
		ID:           resp.GetNodeId(),
		LeaderID:     resp.GetLeaderId(),
		LeaderAddr:   resp.GetLeaderAddress(),
		Term:         resp.GetTerm(),
		State:        stateName(resp.GetState()),
		CommitIndex:  resp.GetCommitIndex(),
		AppliedIndex: resp.GetAppliedIndex(),
	}
	switch {
	case st.LeaderAddr != "" && c.configured(st.LeaderAddr):
		c.remember(st.LeaderAddr)
	case st.State == "leader" && c.configured(addr):
		c.remember(addr)
	}
	return st, nil
}

func stateName(s raftkvv1.NodeState) string {
	switch s {
	case raftkvv1.NodeState_NODE_STATE_LEADER:
		return "leader"
	case raftkvv1.NodeState_NODE_STATE_FOLLOWER:
		return "follower"
	case raftkvv1.NodeState_NODE_STATE_CANDIDATE:
		return "candidate"
	default:
		return "unknown"
	}
}

// Close closes every connection and unblocks every waiting operation. It is
// idempotent. Operations started afterwards fail with ErrClosed.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := c.conns
	c.conns = map[string]*conn{}
	c.mu.Unlock()

	c.baseCancel(ErrClosed)
	var errs []error
	for _, cn := range conns {
		if cn.closer != nil {
			if err := cn.closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// preflight rejects an operation that cannot send anything.
func (c *Client) preflight(ctx context.Context) *DefinitiveError {
	if c.base.Err() != nil {
		return &DefinitiveError{Code: codes.Canceled, Err: ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return &DefinitiveError{Code: codeOfContext(err), Err: context.Cause(ctx)}
	}
	return nil
}

func codeOfContext(err error) codes.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	return codes.Canceled
}

// write serialises one write per client and allocates its sequence number.
func (c *Client) write(ctx context.Context, call func(ctx context.Context, kv KV, seq uint64) error) error {
	if de := c.preflight(ctx); de != nil {
		return de
	}
	// One write in flight per client id: the server drops seq <= lastSeq and
	// still reports success, so two concurrent writes applied in the opposite
	// order would silently lose the lower one.
	select {
	case c.writeSem <- struct{}{}:
	case <-ctx.Done():
		return &DefinitiveError{Code: codeOfContext(ctx.Err()), Err: context.Cause(ctx)}
	case <-c.base.Done():
		return &DefinitiveError{Code: codes.Canceled, Err: ErrClosed}
	}
	defer func() { <-c.writeSem }()

	// Raft paper section 8: the sequence number is allocated once, before the
	// first attempt, and reused by every retry so the state machine can
	// recognise a duplicate.
	seq := c.nextSeq
	c.nextSeq++
	return c.run(ctx, true, seq, func(actx context.Context, kv KV) error { return call(actx, kv, seq) })
}

// op is one operation's lifetime: it ends when the caller's context ends, when
// the client is closed, or, if the caller set no deadline, after DefaultTotal
// on the injected Clock. The total timer is polled as well as watched, so an
// expiry is seen at once rather than after a goroutine happens to run.
type op struct {
	c       *Client
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stop    func() bool
	total   Timer // nil when the caller's context carries a deadline
	expired atomic.Bool
}

func (c *Client) begin(ctx context.Context) *op {
	opCtx, cancel := context.WithCancelCause(ctx)
	o := &op{c: c, ctx: opCtx, cancel: cancel}
	o.stop = context.AfterFunc(c.base, func() { cancel(ErrClosed) })
	if _, has := ctx.Deadline(); !has {
		o.total = c.cfg.Clock.NewTimer(c.cfg.DefaultTotal)
	}
	return o
}

func (o *op) end() {
	o.stop()
	if o.total != nil {
		o.total.Stop()
	}
	o.cancel(nil)
}

func (o *op) totalC() <-chan time.Time {
	if o.total == nil {
		return nil
	}
	return o.total.C()
}

// done reports whether the operation has ended, without blocking.
func (o *op) done() bool {
	if o.expired.Load() || o.c.base.Err() != nil || o.ctx.Err() != nil {
		return true
	}
	select {
	case <-o.totalC():
		o.expired.Store(true)
		return true
	default:
		return false
	}
}

// err is why the operation ended; only meaningful once done reports true.
func (o *op) err() error {
	switch {
	case o.expired.Load():
		return fmt.Errorf("client: no answer within %v: %w", o.c.cfg.DefaultTotal, context.DeadlineExceeded)
	case o.c.base.Err() != nil:
		return ErrClosed
	default:
		return context.Cause(o.ctx)
	}
}

// attempt runs one RPC under the per-attempt timeout. The first result is true
// when that timeout, not the operation, ended it.
func (c *Client) attempt(o *op, call func(ctx context.Context) error) (bool, error) {
	actx, cancel := context.WithCancelCause(o.ctx)
	timer := c.cfg.Clock.NewTimer(c.cfg.PerAttempt)
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		select {
		case <-timer.C():
			cancel(errAttemptTimeout)
		case <-o.totalC():
			o.expired.Store(true)
			cancel(context.DeadlineExceeded)
		case <-actx.Done():
		case <-done:
		}
	}()
	err := call(actx)
	close(done)
	<-exited
	timer.Stop()
	timedOut := context.Cause(actx) == errAttemptTimeout && !o.done()
	cancel(nil)
	return timedOut, err
}

type disposition int

const (
	retryRotate   disposition = iota // wait, then try the next node
	retryHint                        // go straight to the hinted leader
	endDefinitive                    // the server refused the request outright
	endOther                         // not retriable and not provably unapplied
)

// classify decides what an attempt's error means. hint is the leader address
// a NotLeader detail carried, if any.
func classify(err error, timedOut bool) (d disposition, hint string, code codes.Code) {
	if timedOut {
		return retryRotate, "", codes.DeadlineExceeded
	}
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status: a transport failure such as a reset connection.
		return retryRotate, "", codes.Unavailable
	}
	code = st.Code()
	switch code {
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated,
		codes.Unimplemented, codes.ResourceExhausted:
		return endDefinitive, "", code
	case codes.FailedPrecondition:
		for _, detail := range st.Details() {
			if nl, ok := detail.(*raftkvv1.NotLeader); ok {
				if nl.GetLeaderAddress() != "" {
					return retryHint, nl.GetLeaderAddress(), code
				}
				return retryRotate, "", code
			}
		}
		return endOther, "", code
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.Canceled:
		return retryRotate, "", code
	default:
		return endOther, "", code
	}
}

// run is the retry loop shared by reads and writes.
func (c *Client) run(ctx context.Context, write bool, seq uint64, call func(ctx context.Context, kv KV) error) error {
	o := c.begin(ctx)
	defer o.end()

	addr, idx := c.start()
	seen := map[string]bool{}
	sent, backoffs := 0, 0
	var last error

	for {
		if o.done() {
			return c.finish(write, seq, sent, o.err(), last)
		}

		cn, err := c.conn(addr)
		if err != nil {
			if errors.Is(err, ErrClosed) {
				return c.finish(write, seq, sent, ErrClosed, last)
			}
			last = err
			addr, idx = c.rotate(idx)
			backoffs++
			c.sleep(o, backoffs)
			seen = map[string]bool{}
			continue
		}

		sent++
		timedOut, err := c.attempt(o, func(actx context.Context) error { return call(actx, cn.kv) })
		if err == nil {
			c.remember(addr)
			return nil
		}
		last = err
		if o.done() {
			continue
		}

		disp, hint, code := classify(err, timedOut)
		switch disp {
		case endDefinitive:
			return &DefinitiveError{Code: code, Err: err}
		case endOther:
			if write {
				return &IndeterminateError{Attempts: sent, Seq: seq, Cause: err}
			}
			return &DefinitiveError{Code: code, Err: err}
		case retryHint:
			if hint != addr && !seen[hint] && c.allowHint(hint) {
				seen[hint] = true
				addr = hint
				continue
			}
		case retryRotate:
		}
		addr, idx = c.rotate(idx)
		backoffs++
		c.sleep(o, backoffs)
		seen = map[string]bool{}
	}
}

// sleep waits for the nth jittered delay on the injected Clock, or until the
// operation context ends.
func (c *Client) sleep(o *op, n int) {
	timer := c.cfg.Clock.NewTimer(c.cfg.Backoff.delay(n, c.cfg.Rand))
	defer timer.Stop()
	select {
	case <-timer.C():
	case <-o.totalC():
		o.expired.Store(true)
	case <-o.ctx.Done():
	}
}

// finish builds the terminal error of an operation that ran out of road.
func (c *Client) finish(write bool, seq uint64, sent int, cause, last error) error {
	if last != nil && !errors.Is(cause, last) {
		cause = fmt.Errorf("%w; last attempt: %w", cause, last)
	}
	if !write {
		return c.unavailable(cause, nil)
	}
	if sent == 0 {
		return &DefinitiveError{Code: codeOfContext(cause), Err: cause}
	}
	return &IndeterminateError{Attempts: sent, Seq: seq, Cause: cause}
}

func (c *Client) unavailable(cause, last error) error {
	if last != nil && !errors.Is(cause, last) {
		cause = fmt.Errorf("%w; last attempt: %w", cause, last)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, cause)
}

// start returns the first node to try and its index in Addrs.
func (c *Client) start() (string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, a := range c.cfg.Addrs {
		if a == c.target {
			return a, i
		}
	}
	return c.target, 0
}

func (c *Client) startIndex() int {
	_, i := c.start()
	return i
}

func (c *Client) rotate(idx int) (string, int) {
	idx = (idx + 1) % len(c.cfg.Addrs)
	return c.cfg.Addrs[idx], idx
}

func (c *Client) remember(addr string) {
	c.mu.Lock()
	c.target = addr
	c.mu.Unlock()
}

func (c *Client) configured(addr string) bool {
	for _, a := range c.cfg.Addrs {
		if a == addr {
			return true
		}
	}
	return false
}

// allowHint reports whether a hinted address may be dialed: it is either
// configured or fits in the bounded budget of extra connections.
func (c *Client) allowHint(addr string) bool {
	if c.configured(addr) {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, ok := c.conns[addr]; ok {
		return true
	}
	return c.extra < maxExtraConnsFactor*len(c.cfg.Addrs)
}

// conn returns the cached connection to addr, dialing lazily.
func (c *Client) conn(addr string) (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if cn, ok := c.conns[addr]; ok {
		return cn, nil
	}
	isExtra := !c.configured(addr)
	if isExtra && c.extra >= maxExtraConnsFactor*len(c.cfg.Addrs) {
		return nil, fmt.Errorf("client: too many connections to unlisted nodes (%s)", addr)
	}
	kv, closer, err := c.cfg.Connect(addr)
	if err != nil {
		return nil, fmt.Errorf("client: dialing %s: %w", addr, err)
	}
	cn := &conn{kv: kv, closer: closer}
	c.conns[addr] = cn
	if isExtra {
		c.extra++
	}
	return cn, nil
}
