package gateway

import (
	"context"
	"errors"
	"time"
)

// NodeInfo names one node of the cluster and the availability zone it runs in.
type NodeInfo struct {
	ID uint64
	AZ string
}

// NodeStatus is what one node reports about itself (AdminService.Status).
type NodeStatus struct {
	ID            uint64
	LeaderID      uint64
	Term          uint64
	Role          string // "leader", "follower" or "candidate"
	CommitIndex   uint64
	AppliedIndex  uint64
	FirstIndex    uint64
	LastIndex     uint64
	SnapshotIndex uint64
	Paused        bool
}

// NodeEvent is one observed (term, role, leader) transition of a node.
type NodeEvent struct {
	Node     uint64
	Term     uint64
	Role     string
	LeaderID uint64
	Seq      uint64
	UnixNano int64
}

var (
	// ErrNoLeader means no leader could be found after the client's retries.
	// The write was not sent, so it did not happen.
	ErrNoLeader = errors.New("gateway: no leader available")

	// ErrIndeterminate means a write may or may not have been applied: an
	// attempt was sent and its outcome was never seen (RUNBOOK 7.4).
	ErrIndeterminate = errors.New("gateway: outcome of the write is unknown")
)

// KV is one client session (one client id). It is NOT safe for concurrent
// writes: the gateway keeps a pool and checks one out per request, because a
// write whose seq is not above the session's last seq is silently skipped by
// the state machine (internal/statemachine/session.go).
type KV interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// Cluster is the seam between the gateway and the nodes. Everything the
// gateway does to the cluster goes through it, so tests run on an in-memory
// fake with no AWS and no real node.
//
// There is deliberately no method for membership changes (AddNode,
// RemoveNode, ListMembers): the gateway must not be able to reshape the
// cluster, and TestGatewayCannotCallMembership keeps it that way.
type Cluster interface {
	// Topology lists the nodes the gateway was configured with.
	Topology() []NodeInfo
	Status(ctx context.Context, id uint64) (NodeStatus, error)
	// NewKV returns a new session with its own client id.
	NewKV() KV

	Crash(ctx context.Context, id uint64) error
	Pause(ctx context.Context, id uint64, d time.Duration) error
	Partition(ctx context.Context, id uint64, blocked []uint64) error
	Netem(ctx context.Context, id uint64, peer uint64, delay time.Duration, dropPercent uint32) error
	ClearFaults(ctx context.Context, id uint64) error
	Events(ctx context.Context, id uint64) (<-chan NodeEvent, error)
}
