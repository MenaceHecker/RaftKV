package transport

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// Fault injection for the admin surface (T4.7).
//
// These faults are application-level. They drop or delay Raft messages inside
// the process, in a wrapper around node.Transport and in the Raft ingress
// handlers. They do not touch the kernel network stack. Dropping or delaying
// a message is something the network may do to any message, so Raft tolerates
// it (paper §5, "Raft safety does not depend on timing"); the wrapper never
// alters a message.

const (
	// FaultExpiry is how long an injected fault lasts at the node before it is
	// removed, measured from the call that set it. Each fault has its own timer.
	FaultExpiry = 120 * time.Second

	// MaxDelayQueue bounds the messages held for one peer while a delay is set.
	MaxDelayQueue = 1024
)

// Timer is the part of time.Timer the wrapper needs.
type Timer interface {
	Stop() bool
}

// Clock supplies time to the wrapper so tests can control it.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) Timer
}

// Rand supplies the draws behind percentage loss so tests can script them.
type Rand interface {
	// Intn returns a value in [0, n).
	Intn(n int) int
}

// RealClock is the production Clock.
type RealClock struct{}

// Now returns the wall clock time.
func (RealClock) Now() time.Time { return time.Now() }

// AfterFunc schedules f after d on the wall clock.
func (RealClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

// RealRand is the production Rand.
type RealRand struct{}

// Intn draws from the process-wide generator. The draw only decides which
// messages a test fault drops, so it needs no cryptographic strength.
func (RealRand) Intn(n int) int { return rand.IntN(n) } //nolint:gosec // loss injection, not a security decision

// FaultConfig configures a FaultTransport.
type FaultConfig struct {
	// Self is this node. Messages addressed to it are never dropped or delayed.
	Self raft.NodeID

	// Inner carries the messages that pass.
	Inner node.Transport

	Clock Clock
	Rand  Rand

	// Paused, if set, reports whether the node is paused. While it is true,
	// messages to and from peers are dropped.
	Paused func() bool
}

// FaultStats counts what the wrapper has done to messages.
type FaultStats struct {
	// Dropped counts messages dropped by a partition, by percentage loss or
	// by a pause.
	Dropped uint64
	// QueueOverflow counts messages dropped because a peer's delay queue was
	// full.
	QueueOverflow uint64
	// Discarded counts queued messages thrown away when a delay expired or
	// was cleared.
	Discarded uint64
}

// FaultTransport wraps a node.Transport and applies injected faults.
//
// STUB: every method passes through or does nothing. The behaviour arrives in
// the commit that follows the red tests.
type FaultTransport struct {
	cfg FaultConfig
}

// NewFaultTransport returns a wrapper with no faults set.
func NewFaultTransport(cfg FaultConfig) *FaultTransport {
	return &FaultTransport{cfg: cfg}
}

// Send passes msgs to the inner transport. STUB: applies no fault.
func (f *FaultTransport) Send(msgs []raft.Message) {
	f.cfg.Inner.Send(msgs)
}

// AddPeer must forward to the inner transport when it is Reconfigurable,
// because node.reconcileTransport silently skips a transport that lacks it.
// STUB: forwards nothing.
func (f *FaultTransport) AddPeer(_ raft.NodeID, _ string) error { return nil }

// RemovePeer is the counterpart of AddPeer. STUB: forwards nothing.
func (f *FaultTransport) RemovePeer(_ raft.NodeID) {}

// SetPartition replaces the set of peers cut off in both directions. An empty
// set heals. STUB: does nothing.
func (f *FaultTransport) SetPartition(_ []raft.NodeID) {}

// SetDelay delays every message sent to peer by d; peer zero means all peers.
// A zero d clears it. STUB: does nothing.
func (f *FaultTransport) SetDelay(_ raft.NodeID, _ time.Duration) {}

// SetDropPercent drops pct percent of messages sent to peer; peer zero means
// all peers. A zero pct clears it. STUB: does nothing.
func (f *FaultTransport) SetDropPercent(_ raft.NodeID, _ int) error { return nil }

// ClearFaults removes every fault and discards queued messages. STUB: does
// nothing.
func (f *FaultTransport) ClearFaults() {}

// AllowIngress reports whether a message claiming to come from from may enter
// the node. Both Deliver and DeliverSnapshot consult it. STUB: allows all.
func (f *FaultTransport) AllowIngress(_ raft.NodeID) bool { return true }

// Stats returns the counters. STUB: all zero.
func (f *FaultTransport) Stats() FaultStats { return FaultStats{} }

// WrapRaftServer returns a RaftService that filters ingress through f before
// handing messages to inner. STUB: delegates without filtering.
func (f *FaultTransport) WrapRaftServer(inner raftkvv1.RaftServiceServer) raftkvv1.RaftServiceServer {
	return &ingressServer{inner: inner, faults: f}
}

type ingressServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	inner  raftkvv1.RaftServiceServer
	faults *FaultTransport
}

func (s *ingressServer) Deliver(ctx context.Context, req *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	return s.inner.Deliver(ctx, req)
}

func (s *ingressServer) DeliverSnapshot(stream raftkvv1.RaftService_DeliverSnapshotServer) error {
	return s.inner.DeliverSnapshot(stream)
}

// Register registers the filtered service.
func (s *ingressServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterRaftServiceServer(srv, s)
}

var _ node.Transport = (*FaultTransport)(nil)
var _ node.Reconfigurable = (*FaultTransport)(nil)
