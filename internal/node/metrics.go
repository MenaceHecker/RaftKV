package node

import (
	"context"
	"errors"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

type Recorder interface {
	ObserveProposal(result string, d time.Duration)

	ObserveRead(result string, d time.Duration)

	ObserveApply(entries int, d time.Duration)

	ObservePersist(entries int, d time.Duration)

	SnapshotCreated(index uint64, d time.Duration)

	SnapshotReceived()

	LeaderChanged(term, leader uint64, isLeader bool)
}

const (
	ResultOK = "ok"

	ResultNotLeader = "not_leader"

	ResultLostLeadership = "lost_leadership"

	ResultTimeout = "timeout"

	ResultStopped = "stopped"

	ResultError = "error"
)

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

type nopRecorder struct{}

func (nopRecorder) ObserveProposal(string, time.Duration) {}
func (nopRecorder) ObserveRead(string, time.Duration)     {}
func (nopRecorder) ObserveApply(int, time.Duration)       {}
func (nopRecorder) ObservePersist(int, time.Duration)     {}
func (nopRecorder) SnapshotCreated(uint64, time.Duration) {}
func (nopRecorder) SnapshotReceived()                     {}
func (nopRecorder) LeaderChanged(uint64, uint64, bool)    {}

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
	m.rec.ObservePersist(0, time.Since(start))
	return err
}
