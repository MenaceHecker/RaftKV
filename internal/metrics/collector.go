package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

// Metrics must satisfy the driver's Recorder. Asserting it here turns a
// mismatch into a compile error rather than a silently unrecorded metric.
var _ node.Recorder = (*Metrics)(nil)

// StatusSource is the part of a node this collector reads.
type StatusSource interface {
	Status() node.Status
}

// PeerSource is the part of the transport this collector reads.
type PeerSource interface {
	Stats() []transport.PeerStats
}

// Collector reports the values that are cheaper to read at scrape time than
// to track as they change.
//
// Commit and applied indexes move on every entry; mirroring each step into a
// gauge would put work on the Raft loop to produce a number only ever read
// once every scrape interval. Sampling them is both cheaper and sufficient,
// because unlike an election they cannot move and move back unnoticed.
//
// Events that a sample would genuinely miss are counted as they happen
// instead; see Metrics.LeaderChanged.
type Collector struct {
	status StatusSource
	peers  PeerSource

	commitIndex  *prometheus.Desc
	appliedIndex *prometheus.Desc
	applyLag     *prometheus.Desc
	clusterSize  *prometheus.Desc
	peerSent     *prometheus.Desc
	peerDropped  *prometheus.Desc
	peerFailed   *prometheus.Desc
}

// NewCollector builds a collector over a node and its transport. The peer
// source may be nil, in which case no per-peer metrics are reported.
func NewCollector(status StatusSource, peers PeerSource) *Collector {
	return &Collector{
		status: status,
		peers:  peers,

		commitIndex: prometheus.NewDesc(
			Namespace+"_commit_index",
			"Highest log index known to be committed.",
			nil, nil,
		),
		appliedIndex: prometheus.NewDesc(
			Namespace+"_applied_index",
			"Highest log index applied to the state machine.",
			nil, nil,
		),
		applyLag: prometheus.NewDesc(
			Namespace+"_apply_lag_entries",
			"Committed entries not yet applied. Sustained above zero means the state machine is the bottleneck, not consensus.",
			nil, nil,
		),
		clusterSize: prometheus.NewDesc(
			Namespace+"_cluster_voters",
			"Number of voting members in the configuration this node believes in. Nodes disagreeing here are mid-membership-change or partitioned.",
			nil, nil,
		),
		peerSent: prometheus.NewDesc(
			Namespace+"_peer_messages_sent_total",
			"Raft messages handed to a peer's send queue.",
			[]string{"peer", "address"}, nil,
		),
		peerDropped: prometheus.NewDesc(
			Namespace+"_peer_messages_dropped_total",
			"Messages discarded because a peer's queue was full, meaning it is slower than the traffic being produced for it.",
			[]string{"peer", "address"}, nil,
		),
		peerFailed: prometheus.NewDesc(
			Namespace+"_peer_messages_failed_total",
			"Message deliveries that returned an error, which usually means the peer is unreachable.",
			[]string{"peer", "address"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.commitIndex
	ch <- c.appliedIndex
	ch <- c.applyLag
	ch <- c.clusterSize
	ch <- c.peerSent
	ch <- c.peerDropped
	ch <- c.peerFailed
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.status.Status()

	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(st.Commit))
	ch <- prometheus.MustNewConstMetric(c.appliedIndex, prometheus.GaugeValue, float64(st.Applied))

	// Applied should never exceed commit, but the subtraction is on unsigned
	// indexes, so a violated assumption would wrap into an enormous gauge
	// rather than a visible zero. Clamping keeps the failure legible.
	lag := float64(0)
	if st.Commit > st.Applied {
		lag = float64(st.Commit - st.Applied)
	}
	ch <- prometheus.MustNewConstMetric(c.applyLag, prometheus.GaugeValue, lag)
	ch <- prometheus.MustNewConstMetric(c.clusterSize, prometheus.GaugeValue, float64(len(st.Members.Voters)))

	if c.peers == nil {
		return
	}
	for _, p := range c.peers.Stats() {
		id := strconv.FormatUint(uint64(p.ID), 10)
		ch <- prometheus.MustNewConstMetric(c.peerSent, prometheus.CounterValue, float64(p.Sent), id, p.Address)
		ch <- prometheus.MustNewConstMetric(c.peerDropped, prometheus.CounterValue, float64(p.Dropped), id, p.Address)
		ch <- prometheus.MustNewConstMetric(c.peerFailed, prometheus.CounterValue, float64(p.Failed), id, p.Address)
	}
}
