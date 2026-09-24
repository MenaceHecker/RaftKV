package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

var _ node.Recorder = (*Metrics)(nil)

type StatusSource interface {
	Status() node.Status
}

type PeerSource interface {
	Stats() []transport.PeerStats
}

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

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.commitIndex
	ch <- c.appliedIndex
	ch <- c.applyLag
	ch <- c.clusterSize
	ch <- c.peerSent
	ch <- c.peerDropped
	ch <- c.peerFailed
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.status.Status()

	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(st.Commit))
	ch <- prometheus.MustNewConstMetric(c.appliedIndex, prometheus.GaugeValue, float64(st.Applied))

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
