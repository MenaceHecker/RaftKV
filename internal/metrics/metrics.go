package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const Namespace = "raftkv"

var latencyBuckets = prometheus.ExponentialBuckets(0.0001, 2, 16)

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

	for _, result := range []string{"ok", "not_leader", "lost_leadership", "timeout", "stopped", "error"} {
		m.proposals.WithLabelValues(result)
		m.reads.WithLabelValues(result)
	}

	return m
}

func (m *Metrics) ObserveProposal(result string, d time.Duration) {
	m.proposals.WithLabelValues(result).Inc()
	m.proposalDuration.Observe(d.Seconds())
}

func (m *Metrics) ObserveRead(result string, d time.Duration) {
	m.reads.WithLabelValues(result).Inc()
	m.readDuration.Observe(d.Seconds())
}

func (m *Metrics) ObserveApply(entries int, d time.Duration) {
	m.appliedEntries.Add(float64(entries))
	m.applyDuration.Observe(d.Seconds())
}

func (m *Metrics) ObservePersist(entries int, d time.Duration) {
	m.persistedEntries.Add(float64(entries))
	m.persistDuration.Observe(d.Seconds())
}

func (m *Metrics) SnapshotCreated(index uint64, d time.Duration) {
	m.snapshotsCreated.Inc()
	m.snapshotDuration.Observe(d.Seconds())
	m.snapshotIndex.Set(float64(index))
}

func (m *Metrics) SnapshotReceived() { m.snapshotsReceived.Inc() }

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

func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
}
