package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/client"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Errors the live adapter adds to ErrNoLeader and ErrIndeterminate. None of
// them carries an address or a token.
var (
	// ErrRejected means a node refused the request outright (invalid, too
	// large, not permitted). The write did not happen.
	ErrRejected = errors.New("gateway: the cluster refused the request")
	// ErrNodeUnauthenticated means a node rejected the machine token.
	ErrNodeUnauthenticated = errors.New("gateway: node refused the machine token")
	// ErrChaosDisabled means the node was started without --admin-chaos.
	ErrChaosDisabled = errors.New("gateway: fault injection is not enabled on the node")
	// ErrNodeUnavailable means a node's admin API did not answer, or the
	// connection broke during the call. After Crash that is expected.
	ErrNodeUnavailable = errors.New("gateway: node unreachable")
	// ErrUnknownNode means the id is not in the configured topology.
	ErrUnknownNode = errors.New("gateway: no such node")
)

// Backoff bounds for reconnecting an upstream Events stream. The n-th wait is
// drawn from [d/2, d] with d = min(EventsBackoffMax, EventsBackoffInitial<<n).
const (
	EventsBackoffInitial = 200 * time.Millisecond
	EventsBackoffMax     = 10 * time.Second

	// A stream that lived this long counts as healthy: the next wait starts
	// from the initial backoff again.
	eventsHealthyAfter = 30 * time.Second
	// subscriberBuffer is each local subscriber's queue. A subscriber that
	// falls behind loses events (their seq numbers expose the gap); it never
	// holds up the relay or the node.
	subscriberBuffer = 64
)

// LiveNode is one configured node.
type LiveNode struct {
	ID uint64
	AZ string
	// Addr is the node's gRPC address for KVService (and Raft).
	Addr string
	// AdminAddr is the node's AdminService address.
	AdminAddr string
}

// LiveConfig configures NewLive.
type LiveConfig struct {
	Nodes []LiveNode
	// NodeToken is the machine token sent to every node admin RPC as
	// "authorization: Bearer <token>". It is never logged.
	NodeToken string
	Clock     Clock
	Rand      Rand
	Logger    *slog.Logger // optional; logs node ids and gRPC codes only

	// AdminDial is a test seam. Nil dials plaintext gRPC (the VPC is the
	// trust boundary) with the token on every call.
	AdminDial func(n LiveNode) (raftkvv1.AdminServiceClient, io.Closer, error)
	// DialOpts are passed to the KV clients (nil means insecure credentials).
	DialOpts []grpc.DialOption
}

// Live is the production Cluster: KV through internal/client, node admin
// through AdminServiceClient. It constructs KVService and AdminService clients
// only; there is no membership client anywhere in it (see
// TestGatewayCannotCallMembership).
type Live struct {
	cfg    LiveConfig
	order  []NodeInfo
	nodes  map[uint64]*liveNode
	kvAddr []string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	clients []*client.Client
	closers []io.Closer
	closed  bool
}

type liveNode struct {
	info  LiveNode
	admin raftkvv1.AdminServiceClient
	relay *relay
}

// NewLive validates the configuration, dials the admin API of every node (lazily:
// no connection is made yet) and starts one Events relay per node.
func NewLive(cfg LiveConfig) (*Live, error) {
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("gateway: no nodes configured")
	}
	if cfg.NodeToken == "" {
		return nil, errors.New("gateway: no machine token for the node admin API")
	}
	if cfg.Clock == nil || cfg.Rand == nil {
		return nil, errors.New("gateway: the live cluster needs a Clock and a Rand")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &Live{cfg: cfg, nodes: make(map[uint64]*liveNode, len(cfg.Nodes)), ctx: ctx, cancel: cancel}

	for _, n := range cfg.Nodes {
		if n.ID == 0 || n.Addr == "" || n.AdminAddr == "" {
			_ = l.Close()
			return nil, fmt.Errorf("gateway: node %d is incomplete", n.ID)
		}
		if _, dup := l.nodes[n.ID]; dup {
			_ = l.Close()
			return nil, fmt.Errorf("gateway: node %d configured twice", n.ID)
		}
		dial := cfg.AdminDial
		if dial == nil {
			dial = l.dialAdmin
		}
		admin, closer, err := dial(n)
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("gateway: admin API of node %d: %w", n.ID, err)
		}
		l.closers = append(l.closers, closer)
		ln := &liveNode{info: n, admin: admin}
		ln.relay = &relay{l: l, id: n.ID, admin: admin, subs: map[*subscriber]struct{}{}}
		l.nodes[n.ID] = ln
		l.order = append(l.order, NodeInfo{ID: n.ID, AZ: n.AZ})
		l.kvAddr = append(l.kvAddr, n.Addr)
	}
	for _, n := range l.order {
		r := l.nodes[n.ID].relay
		l.wg.Add(1)
		go func() { defer l.wg.Done(); r.run(ctx) }()
	}
	return l, nil
}

// bearer sends the machine token on every RPC, streams included. It opts out
// of the transport-security requirement because the admin API is plaintext
// inside the VPC; it never prints the token.
type bearer struct{ token string }

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (bearer) RequireTransportSecurity() bool { return false }
func (bearer) String() string                 { return "bearer(redacted)" }
func (bearer) GoString() string               { return "bearer(redacted)" }

func (l *Live) dialAdmin(n LiveNode) (raftkvv1.AdminServiceClient, io.Closer, error) {
	cc, err := grpc.NewClient(n.AdminAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearer{token: l.cfg.NodeToken}))
	if err != nil {
		return nil, nil, err
	}
	return raftkvv1.NewAdminServiceClient(cc), cc, nil
}

// Close stops the relays (closing every subscriber channel), closes the KV
// clients and the admin connections. It is idempotent.
func (l *Live) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	clients, closers := l.clients, l.closers
	l.mu.Unlock()

	l.cancel()
	l.wg.Wait()
	var first error
	for _, c := range clients {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, c := range closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ---- KV ----

// liveKV is one session: one internal/client Client with its own random
// client id. The client serialises its writes, which is what makes retries
// with the same (client id, seq) safe; retries are the client's job, this type
// only translates the outcome.
type liveKV struct{ c *client.Client }

// ClientID is the session id this KV puts on its writes.
func (k *liveKV) ClientID() uint64 { return k.c.ClientID() }

func (k *liveKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, found, err := k.c.Get(ctx, key)
	return v, found, MapKVError(err)
}

func (k *liveKV) Put(ctx context.Context, key string, value []byte) error {
	return MapKVError(k.c.Put(ctx, key, value))
}

func (k *liveKV) Delete(ctx context.Context, key string) error {
	return MapKVError(k.c.Delete(ctx, key))
}

// failedKV is returned when a client cannot even be built; every call fails
// without having been sent, so it reports ErrNoLeader.
type failedKV struct{}

func (failedKV) Get(context.Context, string) ([]byte, bool, error) { return nil, false, ErrNoLeader }
func (failedKV) Put(context.Context, string, []byte) error         { return ErrNoLeader }
func (failedKV) Delete(context.Context, string) error              { return ErrNoLeader }

// NewKV returns a session with a fresh random client id: never a fixed one,
// because two processes sharing an id would have their writes deduplicated
// against each other.
func (l *Live) NewKV() KV {
	c, err := client.New(client.Config{Addrs: l.kvAddr, DialOpts: l.cfg.DialOpts})
	if err != nil {
		return failedKV{}
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = c.Close()
		return failedKV{}
	}
	l.clients = append(l.clients, c)
	l.mu.Unlock()
	return &liveKV{c: c}
}

// MapKVError translates an internal/client error into the gateway's
// vocabulary, keeping the distinction RUNBOOK 7.4 cares about: ErrNoLeader and
// ErrRejected prove the write did not happen, ErrIndeterminate says it may
// have. Anything it does not recognise passes through unchanged.
func MapKVError(err error) error {
	if err == nil {
		return nil
	}
	if client.IsIndeterminate(err) {
		return ErrIndeterminate
	}
	if errors.Is(err, client.ErrUnavailable) {
		return ErrNoLeader
	}
	var de *client.DefinitiveError
	if errors.As(err, &de) {
		switch de.Code { //nolint:exhaustive // the rest pass through
		case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated,
			codes.Unimplemented, codes.ResourceExhausted:
			return ErrRejected
		case codes.Unavailable, codes.DeadlineExceeded:
			return ErrNoLeader // no attempt was sent
		case codes.Canceled:
			if !errors.Is(de.Err, client.ErrClosed) {
				return context.Canceled
			}
		}
	}
	return err
}

// ---- topology, status, faults ----

// Topology lists the configured nodes in configuration order.
func (l *Live) Topology() []NodeInfo { return append([]NodeInfo(nil), l.order...) }

func (l *Live) node(id uint64) (*liveNode, error) {
	n, ok := l.nodes[id]
	if !ok {
		return nil, ErrUnknownNode
	}
	return n, nil
}

func roleName(s raftkvv1.NodeState) string {
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

// Status asks one node for AdminService.Status.
func (l *Live) Status(ctx context.Context, id uint64) (NodeStatus, error) {
	n, err := l.node(id)
	if err != nil {
		return NodeStatus{}, err
	}
	resp, err := n.admin.Status(ctx, &raftkvv1.AdminStatusRequest{})
	if err != nil {
		return NodeStatus{}, mapAdminError(err)
	}
	return NodeStatus{
		ID: resp.GetNodeId(), LeaderID: resp.GetLeaderId(), Term: resp.GetTerm(), Role: roleName(resp.GetState()),
		CommitIndex: resp.GetCommitIndex(), AppliedIndex: resp.GetAppliedIndex(),
		FirstIndex: resp.GetFirstIndex(), LastIndex: resp.GetLastIndex(), SnapshotIndex: resp.GetSnapshotIndex(),
		Paused: resp.GetPaused(),
	}, nil
}

// Crash asks the node to exit. The node replies and then exits about 50 ms
// later, so a lost connection right after the call is the expected outcome:
// it surfaces as ErrNodeUnavailable and the chaos controller treats that as
// success.
func (l *Live) Crash(ctx context.Context, id uint64) error {
	n, err := l.node(id)
	if err != nil {
		return err
	}
	_, err = n.admin.Crash(ctx, &raftkvv1.CrashRequest{})
	return mapAdminError(err)
}

// Pause freezes the node for d (the node clamps it to 60 s). Zero resumes.
func (l *Live) Pause(ctx context.Context, id uint64, d time.Duration) error {
	n, err := l.node(id)
	if err != nil {
		return err
	}
	_, err = n.admin.Pause(ctx, &raftkvv1.PauseRequest{DurationMs: uint64(max(d, 0) / time.Millisecond)})
	return mapAdminError(err)
}

// Partition cuts the node off from the blocked peers, in both directions.
func (l *Live) Partition(ctx context.Context, id uint64, blocked []uint64) error {
	n, err := l.node(id)
	if err != nil {
		return err
	}
	_, err = n.admin.SetPartition(ctx, &raftkvv1.SetPartitionRequest{Blocked: append([]uint64{}, blocked...)})
	return mapAdminError(err)
}

// Netem sets the delay and loss on the node's messages to peer (0 = every peer).
func (l *Live) Netem(ctx context.Context, id, peer uint64, delay time.Duration, dropPercent uint32) error {
	n, err := l.node(id)
	if err != nil {
		return err
	}
	ms := min(max(delay, 0)/time.Millisecond, math.MaxUint32)
	_, err = n.admin.SetNetem(ctx, &raftkvv1.SetNetemRequest{Peer: peer, DelayMs: uint32(ms), DropPercent: dropPercent}) //nolint:gosec // clamped above
	return mapAdminError(err)
}

// ClearFaults lifts a partition and any netem rule. The nodes have no clear
// RPC, so it is SetPartition([]) and SetNetem(0, 0, 0); both are always tried
// and the first error is returned.
func (l *Live) ClearFaults(ctx context.Context, id uint64) error {
	n, err := l.node(id)
	if err != nil {
		return err
	}
	_, e1 := n.admin.SetPartition(ctx, &raftkvv1.SetPartitionRequest{})
	_, e2 := n.admin.SetNetem(ctx, &raftkvv1.SetNetemRequest{})
	if e1 != nil {
		return mapAdminError(e1)
	}
	return mapAdminError(e2)
}

// mapAdminError turns an AdminService failure into one of the package's
// errors. The text of the original is dropped: it can hold addresses.
func mapAdminError(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() { //nolint:exhaustive // the rest are reported by code
		case codes.Unauthenticated:
			return ErrNodeUnauthenticated
		case codes.PermissionDenied:
			return ErrChaosDisabled
		case codes.Unavailable, codes.DeadlineExceeded:
			return ErrNodeUnavailable
		case codes.InvalidArgument, codes.ResourceExhausted:
			return ErrRejected
		case codes.Canceled:
			return context.Canceled
		default:
			return fmt.Errorf("gateway: node admin call failed (%s)", st.Code())
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrNodeUnavailable // io.EOF and friends: the connection broke
}

// ---- events relay ----

// Events subscribes to node id's transitions. The gateway holds exactly one
// upstream stream per node no matter how many local subscribers there are, so
// browsers never consume the node's 16 subscriber slots. The channel closes
// when ctx ends or the adapter is closed. A subscriber that does not keep up
// loses events; each carries the node's seq, so the gap is visible.
func (l *Live) Events(ctx context.Context, id uint64) (<-chan NodeEvent, error) {
	n, err := l.node(id)
	if err != nil {
		return nil, err
	}
	return n.relay.subscribe(ctx), nil
}

type subscriber struct{ ch chan NodeEvent }

type relay struct {
	l     *Live
	id    uint64
	admin raftkvv1.AdminServiceClient

	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func (r *relay) subscribe(ctx context.Context) <-chan NodeEvent {
	s := &subscriber{ch: make(chan NodeEvent, subscriberBuffer)}
	r.mu.Lock()
	r.subs[s] = struct{}{}
	r.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
		case <-r.l.ctx.Done():
		}
		r.mu.Lock()
		delete(r.subs, s)
		close(s.ch)
		r.mu.Unlock()
	}()
	return s.ch
}

func (r *relay) broadcast(ev NodeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := range r.subs {
		select {
		case s.ch <- ev:
		default: // a slow subscriber loses this one
		}
	}
}

// backoff is the n-th wait: full jitter over the upper half of an
// exponential envelope capped at EventsBackoffMax.
func backoff(n int, rnd Rand) time.Duration {
	d := EventsBackoffMax
	if n < 16 { // beyond that the shift overflows and the cap applies anyway
		d = min(d, EventsBackoffInitial<<n)
	}
	half := d / 2
	return half + time.Duration(rnd.Uint64()%uint64(half+1)) //nolint:gosec // half is positive
}

// run holds one upstream stream for the node until ctx ends, reconnecting with
// backoff after any failure, ResourceExhausted and Unauthenticated included.
func (r *relay) run(ctx context.Context) {
	clock, attempt, failing := r.l.cfg.Clock, 0, false
	for ctx.Err() == nil {
		started := clock.Now()
		got, err := r.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		if got || clock.Now().Sub(started) >= eventsHealthyAfter {
			attempt, failing = 0, false
		}
		if !failing {
			failing = true // log the first failure of a run, not every retry
			if log := r.l.cfg.Logger; log != nil {
				log.Warn("events stream ended", "node", r.id, "code", status.Code(err).String())
			}
		}
		select {
		case <-clock.After(backoff(attempt, r.l.cfg.Rand)):
		case <-ctx.Done():
			return
		}
		attempt++
	}
}

// stream opens the upstream Events stream and relays until it breaks. It
// reports whether any event arrived.
func (r *relay) stream(ctx context.Context) (got bool, err error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := r.admin.Events(sctx, &raftkvv1.EventsRequest{})
	if err != nil {
		return false, err
	}
	for {
		ev, err := st.Recv()
		if err != nil {
			return got, err
		}
		got = true
		r.broadcast(NodeEvent{
			Node: ev.GetNode(), Term: ev.GetTerm(), Role: roleName(ev.GetRole()), LeaderID: ev.GetLeader(),
			Seq: ev.GetSeq(), UnixNano: ev.GetUnixNano(),
		})
	}
}

var _ Cluster = (*Live)(nil)
