// Package transport carries Raft peer traffic and the client and admin APIs over gRPC.
package transport

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// The admin surface (T4.7). Its token, the chaos switch and the default-off
// listener guard the admin RPCs only; the shared listener's AddNode, RemoveNode
// and snapshot streams stay unauthenticated, which is pre-existing and out of
// scope here (card decision 6, proposed T3.18).

const (
	// CrashDelay is how long after replying Crash terminates the process.
	CrashDelay = 50 * time.Millisecond

	// CrashExitCode is the exit status Crash uses.
	CrashExitCode = 137
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
	Chaos bool

	// Clock schedules the delayed exit; Exit terminates the process. Both are
	// injected so tests can observe a Crash without dying.
	Clock Clock
	Exit  func(code int)
}

// AdminServer implements raftkvv1.AdminServiceServer.
//
// STUB: every method is Unimplemented.
type AdminServer struct {
	raftkvv1.UnimplementedAdminServiceServer
}

// NewAdminServer validates cfg and returns the server.
func NewAdminServer(cfg AdminConfig) (*AdminServer, error) {
	if cfg.Node == nil {
		return nil, errors.New("transport: AdminServer requires a node")
	}
	return &AdminServer{}, nil
}

// Register adds AdminService to srv.
func (a *AdminServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterAdminServiceServer(srv, a)
}

// NewAdminInterceptors returns the interceptors for the admin listener: every
// RPC, streams included, needs "authorization: Bearer <token>"; the chaos RPCs
// also need chaos to be true. STUB: both pass everything through.
func NewAdminInterceptors(_ string, _ bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	return passUnary, passStream
}

// NewPauseGate returns the interceptors for the shared listener: while paused
// reports true, every KVService and ClusterService method fails with
// Unavailable. STUB: passes everything through.
func NewPauseGate(_ func() bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	return passUnary, passStream
}

// LoadAdminToken reads the token for --admin-token-file. It fails if path is
// empty, the file is missing, or the file holds no token. STUB: accepts anything.
func LoadAdminToken(_ string) (string, error) { return "", nil }

func passUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	return h(ctx, req)
}

func passStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, ss)
}
