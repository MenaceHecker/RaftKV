package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

// Tests for the metrics layer.
//
// The collectors themselves are close to trivial, so testing them alone would
// mostly confirm that Prometheus works. What is genuinely worth testing is the
// wiring: that a real node driving real consensus produces the numbers an
// operator would be reading during an incident. Most of these tests therefore
// run an actual single-node cluster and assert on what comes out of the
// registry, which is the only thing that catches a hook that was never called.

// discardTransport satisfies the driver's Transport for a single-node cluster,
// where there is nobody to send to.
type discardTransport struct{}

func (discardTransport) Send([]raft.Message) {}

// startNode brings up a one-node cluster wired to a fresh registry.
//
// One node is enough for everything here: it commits through the same code
// path a five-node cluster does, including the fsync, and it reaches a leader
// in a single election with no network to coordinate.
func startNode(t *testing.T) (*node.Node, *Metrics, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewRegistry()
	m := New(reg)

	n, err := node.Start(node.Config{
		ID:           1,
		Peers:        []raft.NodeID{1},
		DataDir:      t.TempDir(),
		Transport:    discardTransport{},
		TickInterval: 5 * time.Millisecond,
		Metrics:      m,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	// Wait for the node to elect itself; nothing can be proposed before that.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().State == raft.Leader {
			return n, m, reg
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("node did not become leader")
	return nil, nil, nil
}

func TestProposalMetricsRecordSuccess(t *testing.T) {
	n, _, reg := startNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const writes = 3
	for i := range writes {
		cmd := statemachine.Command{
			ClientID: 1,
			Seq:      uint64(i + 1),
			Op:       statemachine.OpPut,
			Key:      "k",
			Value:    []byte("v"),
		}
		if err := n.Propose(ctx, cmd); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	if got := counter(t, reg, "raftkv_proposals_total", "ok"); got != writes {
		t.Errorf("proposals_total{result=ok} = %v, want %d", got, writes)
	}
	// Every other outcome must still be zero, not merely absent: a hook that
	// mislabelled its result would otherwise pass the check above.
	for _, result := range []string{"not_leader", "lost_leadership", "timeout", "error"} {
		if got := counter(t, reg, "raftkv_proposals_total", result); got != 0 {
			t.Errorf("proposals_total{result=%s} = %v, want 0", result, got)
		}
	}

	// The histogram must have observed the same number of writes. A counter
	// that moved without a matching observation would mean latency is being
	// silently lost.
	if got := histogramCount(t, reg, "raftkv_proposal_duration_seconds"); got != writes {
		t.Errorf("proposal_duration_seconds count = %d, want %d", got, writes)
	}
}

func TestFailedProposalIsLabelled(t *testing.T) {
	n, _, reg := startNode(t)

	// A context that has already expired never reaches the Raft loop, so the
	// proposal fails on the way in. That is the path a client hits when the
	// node is overloaded, and it must not be counted as a success.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	})
	if err == nil {
		t.Fatal("expected the proposal to fail")
	}

	if got := counter(t, reg, "raftkv_proposals_total", "ok"); got != 0 {
		t.Errorf("proposals_total{result=ok} = %v, want 0", got)
	}
	if got := counter(t, reg, "raftkv_proposals_total", "timeout"); got != 1 {
		t.Errorf("proposals_total{result=timeout} = %v, want 1", got)
	}
}

func TestReadMetricsRecordSuccess(t *testing.T) {
	n, _, reg := startNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if _, _, err := n.Get(ctx, "k"); err != nil {
		t.Fatalf("get: %v", err)
	}

	if got := counter(t, reg, "raftkv_reads_total", "ok"); got != 1 {
		t.Errorf("reads_total{result=ok} = %v, want 1", got)
	}
	if got := histogramCount(t, reg, "raftkv_read_duration_seconds"); got != 1 {
		t.Errorf("read_duration_seconds count = %d, want 1", got)
	}
}

func TestPersistAndApplyAreObserved(t *testing.T) {
	n, _, reg := startNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Persisting happens inside the consensus core, through the wrapped
	// Storage. If that decorator were dropped the node would still work
	// perfectly and only this would notice.
	if got := histogramCount(t, reg, "raftkv_persist_duration_seconds"); got == 0 {
		t.Error("persist_duration_seconds recorded nothing; the storage decorator is not in the path")
	}
	if got := gatherValue(t, reg, "raftkv_persisted_entries_total"); got == 0 {
		t.Error("persisted_entries_total is zero")
	}

	// At minimum the leader's no-op and this write were applied.
	if got := gatherValue(t, reg, "raftkv_applied_entries_total"); got < 2 {
		t.Errorf("applied_entries_total = %v, want at least 2", got)
	}
	if got := histogramCount(t, reg, "raftkv_apply_duration_seconds"); got == 0 {
		t.Error("apply_duration_seconds recorded nothing")
	}
}

func TestLeadershipIsCountedAsAnEvent(t *testing.T) {
	n, _, reg := startNode(t)

	// Becoming leader is itself a change, so it must already be counted by
	// the time the node reports Leader.
	if got := gatherValue(t, reg, "raftkv_leader_changes_total"); got < 1 {
		t.Errorf("leader_changes_total = %v, want at least 1", got)
	}
	if got := gatherValue(t, reg, "raftkv_is_leader"); got != 1 {
		t.Errorf("is_leader = %v, want 1", got)
	}
	if got := gatherValue(t, reg, "raftkv_leader_id"); got != 1 {
		t.Errorf("leader_id = %v, want 1", got)
	}
	if got := gatherValue(t, reg, "raftkv_term"); got < 1 {
		t.Errorf("term = %v, want at least 1", got)
	}

	_ = n
}

func TestSnapshotMetricsFollowCompaction(t *testing.T) {
	n, _, reg := startNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 3 {
		if err := n.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut, Key: "k", Value: []byte("v"),
		}); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	if got := gatherValue(t, reg, "raftkv_snapshots_created_total"); got != 0 {
		t.Fatalf("snapshots_created_total = %v before any compaction, want 0", got)
	}

	if err := n.Compact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if got := gatherValue(t, reg, "raftkv_snapshots_created_total"); got != 1 {
		t.Errorf("snapshots_created_total = %v, want 1", got)
	}
	// The index must be the applied index, not a placeholder.
	applied := float64(n.Status().Applied)
	if got := gatherValue(t, reg, "raftkv_snapshot_index"); got != applied {
		t.Errorf("snapshot_index = %v, want the applied index %v", got, applied)
	}
}

// fakeStatus lets the collector be tested against values a real node would
// take a long time to reach.
type fakeStatus struct{ st node.Status }

func (f fakeStatus) Status() node.Status { return f.st }

type fakePeers struct{ stats []transport.PeerStats }

func (f fakePeers) Stats() []transport.PeerStats { return f.stats }

func TestCollectorReportsNodeState(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(fakeStatus{node.Status{
		Commit:  100,
		Applied: 90,
		Members: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}},
	}}, fakePeers{[]transport.PeerStats{
		{ID: 2, Address: "localhost:9002", Sent: 10, Dropped: 1, Failed: 2},
	}}))

	want := `
# HELP raftkv_apply_lag_entries Committed entries not yet applied. Sustained above zero means the state machine is the bottleneck, not consensus.
# TYPE raftkv_apply_lag_entries gauge
raftkv_apply_lag_entries 10
# HELP raftkv_cluster_voters Number of voting members in the configuration this node believes in. Nodes disagreeing here are mid-membership-change or partitioned.
# TYPE raftkv_cluster_voters gauge
raftkv_cluster_voters 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"raftkv_apply_lag_entries", "raftkv_cluster_voters"); err != nil {
		t.Error(err)
	}
}

func TestCollectorClampsImpossibleLag(t *testing.T) {
	// Applied ahead of commit should never happen. If it ever did, the
	// unsigned subtraction would wrap to something astronomically large and
	// an operator would chase a phantom backlog, so it reports zero instead.
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(fakeStatus{node.Status{Commit: 5, Applied: 9}}, nil))

	if got := gatherValue(t, reg, "raftkv_apply_lag_entries"); got != 0 {
		t.Errorf("apply_lag_entries = %v, want 0", got)
	}
}

func TestCollectorReportsPeerCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(fakeStatus{node.Status{}}, fakePeers{[]transport.PeerStats{
		{ID: 2, Address: "localhost:9002", Sent: 10, Dropped: 1, Failed: 2},
		{ID: 3, Address: "localhost:9003", Sent: 20, Dropped: 0, Failed: 0},
	}}))

	want := `
# HELP raftkv_peer_messages_dropped_total Messages discarded because a peer's queue was full, meaning it is slower than the traffic being produced for it.
# TYPE raftkv_peer_messages_dropped_total counter
raftkv_peer_messages_dropped_total{address="localhost:9002",peer="2"} 1
raftkv_peer_messages_dropped_total{address="localhost:9003",peer="3"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"raftkv_peer_messages_dropped_total"); err != nil {
		t.Error(err)
	}
}

func TestCollectorToleratesNoTransport(t *testing.T) {
	// A single-node cluster has no peers at all. Collecting must still work
	// rather than panicking on a nil source.
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(fakeStatus{node.Status{Commit: 3, Applied: 3}}, nil))

	if got := gatherValue(t, reg, "raftkv_commit_index"); got != 3 {
		t.Errorf("commit_index = %v, want 3", got)
	}
}

func TestHandlerServesExpositionFormat(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.ObserveProposal("ok", 5*time.Millisecond)

	body := scrape(t, reg)
	for _, want := range []string{
		"# TYPE raftkv_proposals_total counter",
		`raftkv_proposals_total{result="ok"} 1`,
		"# TYPE raftkv_proposal_duration_seconds histogram",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output missing %q\ngot:\n%s", want, body)
		}
	}
}

func TestOutcomeCountersStartAtZero(t *testing.T) {
	// A dashboard panel reading "no data" is ambiguous in a way that "0" is
	// not, so every outcome exists before the first one occurs.
	reg := prometheus.NewRegistry()
	New(reg)

	body := scrape(t, reg)
	for _, want := range []string{
		`raftkv_proposals_total{result="not_leader"} 0`,
		`raftkv_reads_total{result="lost_leadership"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output missing %q", want)
		}
	}
}
