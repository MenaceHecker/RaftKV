package node

import (
	"context"
	"errors"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Recorder receives observations about what a node is doing.
//
// It exists so this package can be instrumented without depending on any
// particular metrics library. That is the same boundary the consensus core
// keeps for the clock and the network: the driver describes what happened,
// and something above it decides how to expose that. A nil Recorder is
// replaced by one that discards everything, so instrumentation is optional
// and costs an interface call when it is absent.
//
// Implementations are called from the node's loop goroutine and from client
// goroutines, so they must be safe for concurrent use and must not block.
// A Recorder that blocks would stall consensus, which is a far worse outcome
// than a missing measurement.
type Recorder interface {
	// ObserveProposal reports a completed write, labelled with its outcome.
	ObserveProposal(result string, d time.Duration)

	// ObserveRead reports a completed linearizable read.
	ObserveRead(result string, d time.Duration)

	// ObserveApply reports entries handed to the state machine.
	ObserveApply(entries int, d time.Duration)

	// ObservePersist reports entries written to the log, including the
	// fsync. This is usually the slowest thing a node does, and it sits
	// directly in the commit path, so it is worth measuring on its own
	// rather than only as part of proposal latency.
	ObservePersist(entries int, d time.Duration)

	// SnapshotCreated reports a snapshot this node took of its own state.
	SnapshotCreated(index uint64, d time.Duration)

	// SnapshotReceived reports a state machine image installed from a
	// leader, which means this node had fallen behind that leader's
	// compaction point and could not have caught up from the log alone.
	SnapshotReceived()

	// LeaderChanged reports that this node's view of the leadership changed:
	// a new term, a new leader, or both. Counting these is the cheapest way
	// to notice a cluster that is electing rather than serving.
	LeaderChanged(term, leader uint64, isLeader bool)
}

// Outcome labels for ObserveProposal and ObserveRead. They are a closed set
// so the resulting time series stay bounded no matter what clients do.
const (
	// ResultOK means the request committed and applied.
	ResultOK = "ok"

	// ResultNotLeader means the node refused because it does not lead.
	ResultNotLeader = "not_leader"

	// ResultLostLeadership means the request was accepted but leadership
	// moved before it committed, so its outcome is genuinely unknown.
	ResultLostLeadership = "lost_leadership"

	// ResultTimeout means the caller's context expired first.
	ResultTimeout = "timeout"

	// ResultStopped means the node was shutting down.
	ResultStopped = "stopped"

	// ResultError is any other failure.
	ResultError = "error"
)

// classify maps an error from the request path onto a bounded label set.
func classify(err error) string {
	switch {
	case err == nil:
		return ResultOK
	case errors.Is(err, ErrNotLeader):
		return ResultNotLeader
	case errors.Is(err, ErrLostLeadership):
		return ResultLostLeadership
	case errors.Is(err, ErrStopped):
		return ResultStopped
	case errors.Is(err, ErrTimeout),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled):
		return ResultTimeout
	default:
		return ResultError
	}
}

// nopRecorder is used when no Recorder is configured.
type nopRecorder struct{}

func (nopRecorder) ObserveProposal(string, time.Duration) {}
func (nopRecorder) ObserveRead(string, time.Duration)     {}
func (nopRecorder) ObserveApply(int, time.Duration)       {}
func (nopRecorder) ObservePersist(int, time.Duration)     {}
func (nopRecorder) SnapshotCreated(uint64, time.Duration) {}
func (nopRecorder) SnapshotReceived()                     {}
func (nopRecorder) LeaderChanged(uint64, uint64, bool)    {}

// meteredStorage times the durable writes on the commit path.
//
// The consensus core writes through the Storage interface synchronously, so
// this is the only place the fsync can be observed. Wrapping the interface
// rather than instrumenting the disk package keeps the measurement where the
// latency actually matters — inside the call the Raft loop is blocked on —
// and leaves the storage layer free of any knowledge of metrics.
type meteredStorage struct {
	raft.Storage
	rec Recorder
}

func (m meteredStorage) Append(entries []raft.Entry) error {
	start := time.Now()
	err := m.Storage.Append(entries)
	m.rec.ObservePersist(len(entries), time.Since(start))
	return err
}

func (m meteredStorage) SetHardState(hs raft.HardState) error {
	start := time.Now()
	err := m.Storage.SetHardState(hs)
	// A hard state write is an fsync with no entries behind it. Counting it
	// as a zero-entry persist keeps the latency visible without inflating
	// the entry count.
	m.rec.ObservePersist(0, time.Since(start))
	return err
}
