// Package transport carries Raft peer traffic and the client and admin APIs over gRPC.
//
// # What the admin controls do NOT cover
//
// The admin token, --admin-chaos and the default-off admin listener guard the
// AdminService RPCs only. They do not protect the shared listener, which still
// accepts, with no authentication:
//
//   - AddNode and RemoveNode, so anyone who can reach the shared port can change
//     cluster membership; and
//   - snapshot streams of up to 1 GiB each (MaxSnapshotBytes), with no bound on
//     how many run at once or on the memory they hold in total.
//
// Both are pre-existing and out of scope for this change. They are tracked as
// backlog card T3.18 (T4.7 decision 6). Do not read the admin token as securing
// the cluster.
package transport

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	// CrashDelay is how long after replying Crash terminates the process.
	CrashDelay = 50 * time.Millisecond

	// CrashExitCode is the exit status Crash uses (128 + SIGKILL).
	CrashExitCode = 137

	// maxTokenFileBytes bounds how much of the token file is read.
	maxTokenFileBytes = 4096

	maxPartitionPeers = 256
	maxNetemDelay     = time.Minute
)

// AdminNode is what AdminServer needs from the node.
type AdminNode interface {
	Status() node.Status
	Subscribe() (*node.Subscription, error)
	Pause(d time.Duration) time.Duration
	Paused() bool
}

// AdminFaults is what AdminServer needs from the fault wrapper.
type AdminFaults interface {
	SetPartition(blocked []raft.NodeID)
	SetDelay(peer raft.NodeID, d time.Duration)
	SetDropPercent(peer raft.NodeID, pct int) error
	ClearFaults()
}

// AdminConfig configures an AdminServer.
type AdminConfig struct {
	Node   AdminNode
	Faults AdminFaults

	// Chaos enables Crash, Pause, SetPartition and SetNetem (--admin-chaos).
	// The interceptors enforce it first; the handlers check it again so a server
	// registered without them still fails closed.
	Chaos bool

	// Clock schedules the delayed exit (nil means the real clock); Exit
	// terminates the process (nil means os.Exit). Both are injected so tests can
	// observe a Crash without dying.
	Clock Clock
	Exit  func(code int)
}

// AdminServer implements raftkvv1.AdminServiceServer.
//
// It must be registered only on the admin listener, behind the interceptors
// from NewAdminInterceptors; authentication lives in those, not here.
type AdminServer struct {
	raftkvv1.UnimplementedAdminServiceServer

	cfg            AdminConfig
	crashScheduled atomic.Bool
}

// NewAdminServer validates cfg and returns the server.
func NewAdminServer(cfg AdminConfig) (*AdminServer, error) {
	if cfg.Node == nil {
		return nil, errors.New("transport: AdminServer requires a node")
	}
	if cfg.Chaos && cfg.Faults == nil {
		return nil, errors.New("transport: AdminServer with chaos enabled requires a fault wrapper")
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.Exit == nil {
		cfg.Exit = os.Exit
	}
	return &AdminServer{cfg: cfg}, nil
}

// Register adds AdminService to srv.
func (a *AdminServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterAdminServiceServer(srv, a)
}

var errChaosDisabled = status.Error(codes.PermissionDenied,
	"chaos RPCs are disabled; start the server with --admin-chaos")

func (a *AdminServer) requireChaos() error {
	if !a.cfg.Chaos {
		return errChaosDisabled
	}
	return nil
}

// Status reports the node's state. It keeps working while paused.
func (a *AdminServer) Status(context.Context, *raftkvv1.AdminStatusRequest) (*raftkvv1.AdminStatusResponse, error) {
	return adminStatusResponse(a.cfg.Node.Status(), a.cfg.Node.Paused()), nil
}

func adminStatusResponse(st node.Status, paused bool) *raftkvv1.AdminStatusResponse {
	// Mirror ListMembers: during a joint configuration list the incoming set.
	ids := st.Members.Voters
	if st.Members.Joint {
		ids = st.Members.Incoming
	}
	members := make([]*raftkvv1.Member, 0, len(ids))
	for _, id := range ids {
		members = append(members, &raftkvv1.Member{Id: uint64(id), Address: st.Members.Addrs[id]})
	}

	return &raftkvv1.AdminStatusResponse{
		NodeId:        uint64(st.ID),
		LeaderId:      uint64(st.Leader),
		LeaderAddress: st.Members.Addrs[st.Leader],
		Term:          uint64(st.Term),
		State:         stateToWire(st.State),
		CommitIndex:   uint64(st.Commit),
		AppliedIndex:  uint64(st.Applied),
		Members:       members,
		Joint:         st.Members.Joint,
		FirstIndex:    uint64(st.FirstIndex),
		LastIndex:     uint64(st.LastIndex),
		SnapshotIndex: uint64(st.SnapshotIndex),
		Paused:        paused,
	}
}

// Crash replies, then terminates the process with exit status 137 about 50ms
// later, without running deferred cleanup (os.Exit, not a clean Stop).
//
// The reply is best effort. The handler returning does not prove the client
// received it: the exit follows within CrashDelay, and the connection may close
// first. A caller must treat a lost connection right after Crash as success.
//
// Once scheduled the exit cannot be cancelled by a later RPC: the timer handle
// is not kept. Authentication and the chaos check have already passed before
// this runs, so nothing is scheduled for a rejected call.
func (a *AdminServer) Crash(context.Context, *raftkvv1.CrashRequest) (*raftkvv1.CrashResponse, error) {
	if err := a.requireChaos(); err != nil {
		return nil, err
	}
	if a.crashScheduled.CompareAndSwap(false, true) {
		exit := a.cfg.Exit
		a.cfg.Clock.AfterFunc(CrashDelay, func() { exit(CrashExitCode) })
	}
	return &raftkvv1.CrashResponse{}, nil
}

// Pause stops the node's ticks and drops its peer traffic for the given time,
// clamped to node.MaxPause. Zero resumes at once.
func (a *AdminServer) Pause(_ context.Context, req *raftkvv1.PauseRequest) (*raftkvv1.PauseResponse, error) {
	if err := a.requireChaos(); err != nil {
		return nil, err
	}
	ms := min(req.GetDurationMs(), uint64(node.MaxPause/time.Millisecond))
	a.cfg.Node.Pause(time.Duration(ms) * time.Millisecond)
	return &raftkvv1.PauseResponse{}, nil
}

// SetPartition replaces the set of peers cut off from this node. An empty set heals.
func (a *AdminServer) SetPartition(_ context.Context, req *raftkvv1.SetPartitionRequest) (*raftkvv1.SetPartitionResponse, error) {
	if err := a.requireChaos(); err != nil {
		return nil, err
	}
	if len(req.GetBlocked()) > maxPartitionPeers {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d peers may be blocked", maxPartitionPeers)
	}
	blocked := make([]raft.NodeID, 0, len(req.GetBlocked()))
	for _, id := range req.GetBlocked() {
		if id == 0 {
			return nil, status.Error(codes.InvalidArgument, "node ID must not be zero")
		}
		blocked = append(blocked, raft.NodeID(id))
	}
	a.cfg.Faults.SetPartition(blocked)
	return &raftkvv1.SetPartitionResponse{}, nil
}

// SetNetem replaces the delay and loss applied to messages sent to a peer, or to
// every peer when the peer is zero. All arguments are validated before any is applied.
func (a *AdminServer) SetNetem(_ context.Context, req *raftkvv1.SetNetemRequest) (*raftkvv1.SetNetemResponse, error) {
	if err := a.requireChaos(); err != nil {
		return nil, err
	}
	if req.GetDropPercent() > 100 {
		return nil, status.Error(codes.InvalidArgument, "drop_percent must be between 0 and 100")
	}
	delay := time.Duration(req.GetDelayMs()) * time.Millisecond
	if delay > maxNetemDelay {
		return nil, status.Errorf(codes.InvalidArgument, "delay_ms must be at most %d", maxNetemDelay.Milliseconds())
	}

	peer := raft.NodeID(req.GetPeer())
	a.cfg.Faults.SetDelay(peer, delay)
	if err := a.cfg.Faults.SetDropPercent(peer, int(req.GetDropPercent())); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &raftkvv1.SetNetemResponse{}, nil
}

// Events streams observed (term, role, leader) transitions until the client goes
// away. At most node.MaxSubscribers streams are served at once (ResourceExhausted
// past that), each with a bounded buffer: a client that falls behind is ended with
// ResourceExhausted and its slot is released. The slot is also released when the
// stream ends for any other reason.
func (a *AdminServer) Events(_ *raftkvv1.EventsRequest, stream raftkvv1.AdminService_EventsServer) error {
	sub, err := a.cfg.Node.Subscribe()
	if err != nil {
		if errors.Is(err, node.ErrTooManySubscribers) {
			return status.Error(codes.ResourceExhausted, "too many event subscribers")
		}
		return status.Error(codes.Internal, "cannot subscribe to events")
	}
	defer sub.Close()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-sub.C:
			if !ok {
				if errors.Is(sub.Err(), node.ErrSubscriberOverflow) {
					return status.Error(codes.ResourceExhausted, "event subscriber too slow; stream ended")
				}
				return nil
			}
			if err := stream.Send(eventToWire(ev)); err != nil {
				return err
			}
		}
	}
}

func eventToWire(ev node.Event) *raftkvv1.Event {
	return &raftkvv1.Event{
		Kind:          raftkvv1.EventKind_EVENT_KIND_STATE,
		Node:          uint64(ev.Node),
		Term:          uint64(ev.Term),
		Role:          stateToWire(ev.Role),
		Leader:        uint64(ev.Leader),
		Commit:        uint64(ev.Commit),
		Applied:       uint64(ev.Applied),
		SnapshotIndex: uint64(ev.SnapshotIndex),
		Seq:           ev.Seq,
		UnixNano:      ev.UnixNano,
	}
}

// chaosMethods change the node's behaviour and need --admin-chaos.
var chaosMethods = map[string]bool{
	raftkvv1.AdminService_Crash_FullMethodName:        true,
	raftkvv1.AdminService_Pause_FullMethodName:        true,
	raftkvv1.AdminService_SetPartition_FullMethodName: true,
	raftkvv1.AdminService_SetNetem_FullMethodName:     true,
}

var errUnauthenticated = status.Error(codes.Unauthenticated, "invalid or missing credentials")

// authGate holds the SHA-256 of the token. Both sides of a comparison are
// hashed to a fixed length first, so the constant-time compare reveals nothing
// about the token's length either.
type authGate struct {
	digest [sha256.Size]byte
	set    bool
	chaos  bool
}

func newAuthGate(token string, chaos bool) *authGate {
	return &authGate{digest: sha256.Sum256([]byte(token)), set: token != "", chaos: chaos}
}

// check authenticates the call and then applies the chaos switch, in that order,
// before any handler (and so before any fault, pause or exit) can run. A missing
// header and a wrong token give the same error.
func (g *authGate) check(ctx context.Context, method string) error {
	presented, present := bearerToken(ctx)
	got := sha256.Sum256([]byte(presented))
	equal := subtle.ConstantTimeCompare(got[:], g.digest[:]) == 1
	if !equal || !present || !g.set {
		return errUnauthenticated
	}
	if chaosMethods[method] && !g.chaos {
		return errChaosDisabled
	}
	return nil
}

func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get("authorization")
	if len(vals) != 1 {
		return "", false
	}
	return strings.CutPrefix(vals[0], "Bearer ")
}

// NewAdminInterceptors returns the interceptors for the admin listener. Every
// RPC, streams included, needs "authorization: Bearer <token>"; a missing and a
// wrong token get the same response. The chaos RPCs also need chaos to be true,
// and that is checked only after authentication. The token is never logged.
func NewAdminInterceptors(token string, chaos bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	gate := newAuthGate(token, chaos)
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := gate.check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := gate.check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return h(srv, ss)
	}
	return unary, stream
}

var errPaused = status.Error(codes.Unavailable, "node is paused")

func pausedMethod(full string) bool {
	return strings.HasPrefix(full, "/"+raftkvv1.KVService_ServiceDesc.ServiceName+"/") ||
		strings.HasPrefix(full, "/"+raftkvv1.ClusterService_ServiceDesc.ServiceName+"/")
}

// NewPauseGate returns the interceptors for the shared listener: while paused
// reports true, every KVService and ClusterService method fails with
// Unavailable. The Raft service is not gated here; the ingress filter handles it.
func NewPauseGate(paused func() bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if pausedMethod(info.FullMethod) && paused() {
			return nil, errPaused
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if pausedMethod(info.FullMethod) && paused() {
			return errPaused
		}
		return h(srv, ss)
	}
	return unary, stream
}

// LoadAdminToken reads the token for --admin-token-file. It fails if path is
// empty, the file is missing, it is readable by group or others, or it holds no
// token. Surrounding whitespace is trimmed. Errors name the path and the problem,
// never the file's contents.
func LoadAdminToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("transport: --admin-token-file is required when the admin listener is enabled")
	}
	return loadTokenFile(path, "admin")
}

// loadTokenFile holds the rules shared by the admin and cluster token files;
// kind only words the errors.
func loadTokenFile(path, kind string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // the operator names this file on purpose
	if err != nil {
		return "", fmt.Errorf("transport: %s token file: %w", kind, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("transport: checking %s token file %s: %w", kind, path, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("transport: %s token file %s is not a regular file", kind, path)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return "", fmt.Errorf("transport: %s token file %s has mode %04o, readable by group or others; use chmod 600", kind, path, perm)
	}

	body, err := io.ReadAll(io.LimitReader(f, maxTokenFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("transport: reading %s token file %s: %w", kind, path, err)
	}
	if len(body) > maxTokenFileBytes {
		return "", fmt.Errorf("transport: %s token file %s is larger than %d bytes", kind, path, maxTokenFileBytes)
	}
	token := strings.TrimSpace(string(body))
	if token == "" {
		return "", fmt.Errorf("transport: %s token file %s holds no token", kind, path)
	}
	return token, nil
}
