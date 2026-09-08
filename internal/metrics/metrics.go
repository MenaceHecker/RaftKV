// Package metrics exposes what a RaftKV node is doing in Prometheus format.
//
// It is the only package that knows about Prometheus. Everything below it
// reports through small interfaces defined where the events happen, which
// keeps the consensus core and the driver free of a metrics dependency and
// keeps them testable without a registry.
//
// The metrics here are chosen to answer the questions an operator actually
// has during an incident, in roughly this order: is there a leader, is the
// cluster committing, is any node falling behind, and is the disk keeping up.
// A metric that does not help answer one of those is not worth the
// cardinality.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric this package exports.
const Namespace = "raftkv"

// latencyBuckets spans the range consensus operations actually occupy.
//
// The interesting values are spread very wide: an fsync on a healthy SSD is
// well under a millisecond, while a write that has to wait out an election
// takes seconds. Default buckets bunch up around 10ms and would put both of
// those in the same place, so these run from 100us to roughly 3s.
var latencyBuckets = prometheus.ExponentialBuckets(0.0001, 2, 16)

// Metrics implements node.Recorder and holds every collector it feeds.
//
// It satisfies that interface structurally rather than by importing the node
// package, so nothing here depends on the driver.
type Metrics struct {
	proposals        *prometheus.CounterVec
	proposalDuration prometheus.Histogram

	reads        *prometheus.CounterVec
	readDuration prometheus.Histogram

	appliedEntries prometheus.Counter
	applyDuration  prometheus.Histogram

	persistedEntries prometheus.Counter
	persistDuration  prometheus.Histogram

	snapshotsCreated  prometheus.Counter
	snapshotDuration  prometheus.Histogram
	snapshotIndex     prometheus.Gauge
	snapshotsReceived prometheus.Counter

	leaderChanges prometheus.Counter
	term          prometheus.Gauge
	isLeader      prometheus.Gauge
	leaderID      prometheus.Gauge
}

// New creates the metrics and registers them.
//
// It takes a Registerer rather than using the default one so that tests, and
// a process running more than one node, can each have their own registry.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		proposals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "proposals_total",
			Help:      "Client writes by outcome. A rising lost_leadership count means elections are interrupting work in progress.",
		}, []string{"result"}),

		proposalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "proposal_duration_seconds",
			Help:      "Time from accepting a write to applying it, covering replication to a majority and the fsync behind it.",
			Buckets:   latencyBuckets,
		}),

		reads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reads_total",
			Help:      "Linearizable reads by outcome.",
		}, []string{"result"}),

		readDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "read_duration_seconds",
			Help:      "Time to serve a linearizable read, including the round trip that confirms this node still leads.",
			Buckets:   latencyBuckets,
		}),

		appliedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "applied_entries_total",
			Help:      "Log entries handed to the state machine.",
		}),

		applyDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "apply_duration_seconds",
			Help:      "Time spent applying one batch of committed entries. This runs on the Raft loop, so time here is time not spent on consensus.",
			Buckets:   latencyBuckets,
		}),

		persistedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "persisted_entries_total",
			Help:      "Log entries written to the write-ahead log.",
		}),

		persistDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "persist_duration_seconds",
			Help:      "Time to make a log write durable, including the fsync. This is usually the floor on write latency.",
			Buckets:   latencyBuckets,
		}),

		snapshotsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshots_created_total",
			Help:      "Snapshots this node took of its own state machine.",
		}),

		snapshotDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "snapshot_duration_seconds",
			Help:      "Time to serialize the state machine and write the snapshot durably.",
			Buckets:   latencyBuckets,
		}),

		snapshotIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "snapshot_index",
			Help:      "Log index of the most recent snapshot. The gap between this and the applied index is how much log a restart would have to replay.",
		}),

		snapshotsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "snapshots_received_total",
			Help:      "State machine images installed from a leader, meaning this node had fallen behind that leader's compaction point.",
		}),

		leaderChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "leader_changes_total",
			Help:      "Observed changes of term or leader. Counted as they happen, so elections between two scrapes are not missed.",
		}),

		term: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "term",
			Help:      "Current Raft term as this node sees it. Terms that climb without work completing mean an election loop.",
		}),

		isLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "is_leader",
			Help:      "1 if this node is the leader, 0 otherwise. Summed across a cluster this should be exactly 1.",
		}),

		leaderID: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "leader_id",
			Help:      "Node ID this node believes is the leader, or 0 if there is none. Disagreement across nodes indicates a partition.",
		}),
	}

	reg.MustRegister(
		m.proposals, m.proposalDuration,
		m.reads, m.readDuration,
		m.appliedEntries, m.applyDuration,
		m.persistedEntries, m.persistDuration,
		m.snapshotsCreated, m.snapshotDuration, m.snapshotIndex, m.snapshotsReceived,
		m.leaderChanges, m.term, m.isLeader, m.leaderID,
	)

	// Initialize the outcome counters so a dashboard shows a flat zero
	// rather than a gap before the first failure of each kind. A panel that
	// reads "no data" is ambiguous in a way that "0" is not.
	for _, result := range []string{"ok", "not_leader", "lost_leadership", "timeout", "stopped", "error"} {
		m.proposals.WithLabelValues(result)
		m.reads.WithLabelValues(result)
	}

	return m
}

// ObserveProposal records a completed write.
func (m *Metrics) ObserveProposal(result string, d time.Duration) {
	m.proposals.WithLabelValues(result).Inc()
	m.proposalDuration.Observe(d.Seconds())
}

// ObserveRead records a completed linearizable read.
func (m *Metrics) ObserveRead(result string, d time.Duration) {
	m.reads.WithLabelValues(result).Inc()
	m.readDuration.Observe(d.Seconds())
}

// ObserveApply records a batch of entries applied to the state machine.
func (m *Metrics) ObserveApply(entries int, d time.Duration) {
	m.appliedEntries.Add(float64(entries))
	m.applyDuration.Observe(d.Seconds())
}

// ObservePersist records a durable log write.
func (m *Metrics) ObservePersist(entries int, d time.Duration) {
	m.persistedEntries.Add(float64(entries))
	m.persistDuration.Observe(d.Seconds())
}

// SnapshotCreated records a snapshot taken locally.
func (m *Metrics) SnapshotCreated(index uint64, d time.Duration) {
	m.snapshotsCreated.Inc()
	m.snapshotDuration.Observe(d.Seconds())
	m.snapshotIndex.Set(float64(index))
}

// SnapshotReceived records an image installed from a leader.
func (m *Metrics) SnapshotReceived() { m.snapshotsReceived.Inc() }

// LeaderChanged records a change of term or leader.
func (m *Metrics) LeaderChanged(term, leader uint64, isLeader bool) {
	m.leaderChanges.Inc()
	m.term.Set(float64(term))
	m.leaderID.Set(float64(leader))
	if isLeader {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
}

// Handler serves the registry in Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		// A failing collector should be visible in the scrape rather than
		// taking the whole endpoint down: partial metrics during an incident
		// are considerably more useful than none.
		ErrorHandling: promhttp.ContinueOnError,
	})
}
