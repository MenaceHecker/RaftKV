package node

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// ackFilter drops append responses that acknowledge index dropFrom or higher,
// once dropFrom is non-zero. It wraps the cluster transport.
type ackFilter struct {
	inner    Transport
	dropFrom *atomic.Uint64
}

func (f ackFilter) Send(msgs []raft.Message) {
	keep := make([]raft.Message, 0, len(msgs))
	for _, m := range msgs {
		if d := f.dropFrom.Load(); d != 0 && m.Type == raft.MsgAppendResponse && uint64(m.MatchIndex) >= d {
			continue
		}
		keep = append(keep, m)
	}
	f.inner.Send(keep)
}

// T3.16 test 8: AddNode returns when the joint entry commits, and the automatic
// leave is appended in that same step. A following change is refused as
// pending until the leave commits (Raft paper §6, dissertation §4.1).
func TestClientAPIRejectsASecondChangeWhileTheLeaveIsUncommitted(t *testing.T) {
	var dropFrom atomic.Uint64
	c := newTunedTestCluster(t, 3, func(cfg *Config) {
		cfg.Transport = ackFilter{inner: cfg.Transport, dropFrom: &dropFrom}
	})
	leader := c.awaitLeader()
	mustPut(t, leader, 1, 1, "k", "v")
	c.awaitAllApplied()

	ctx, cancel := testContext(t)
	defer cancel()

	// The joint entry lands at commit+1 and the automatic leave at commit+2.
	leaveIdx := uint64(leader.Status().Commit) + 2
	dropFrom.Store(leaveIdx)

	if err := leader.AddNode(ctx, 4, "host:4"); err != nil {
		t.Fatalf("AddNode(4): %v", err)
	}

	err := leader.AddNode(ctx, 5, "host:5")
	if !errors.Is(err, raft.ErrConfChangePending) {
		t.Fatalf("AddNode(5) while the leave is uncommitted gave %v, want ErrConfChangePending", err)
	}

	dropFrom.Store(0)
	c.eventually("the leave to commit and a second change to be accepted", func() bool {
		err := leader.AddNode(ctx, 5, "host:5")
		if err != nil && !errors.Is(err, raft.ErrConfChangePending) {
			t.Fatalf("AddNode(5): %v", err)
		}
		return err == nil
	})
}
