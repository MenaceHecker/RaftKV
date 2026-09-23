package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MenaceHecker/raftkv/internal/metrics"
	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

// The two probes Kubernetes drives the cluster with, and the difference
// between them is the whole point.
//
// Readiness asks whether this node can serve, which means a leader exists.
// Liveness asks only whether the Raft loop is answering, and it must not
// depend on a leader: during an election no node has one, so a liveness probe
// that checked for a leader would fail on every node at once and Kubernetes
// would restart the entire cluster. A recoverable few hundred milliseconds
// would become a full restart of every member, and the more unstable the
// cluster the harder it would be hit.
//
// That distinction is written down in the manifest and in the handler, and
// until now it was enforced by nobody. It is one line apart in the source:
// the same Status call, with and without a check on the leader.

// silentTransport drops everything, which is what makes a node configured
// with peers that do not exist stay leaderless.
type silentTransport struct{}

func (silentTransport) Send([]raft.Message) {}

// startProbeNode starts a node and serves its probes, returning the base URL.
//
// With one peer it elects itself almost immediately. With three, two of which
// do not exist and none of which can be reached, it campaigns forever and
// never has a leader, which is the state the two probes must disagree about.
func startProbeNode(t *testing.T, peers []raft.NodeID) (*node.Node, string) {
	t.Helper()

	reg := prometheus.NewRegistry()

	n, err := node.Start(node.Config{
		ID:            1,
		Peers:         peers,
		DataDir:       t.TempDir(),
		Transport:     silentTransport{},
		TickInterval:  5 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
		Metrics:       metrics.New(reg),
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { n.Stop() })

	srv, err := serveMetrics("127.0.0.1:0", reg, n)
	if err != nil {
		t.Fatalf("serving metrics: %v", err)
	}
	t.Cleanup(func() { srv.Close() })

	return n, "http://" + srv.Addr
}

// awaitLeader waits for the node to elect itself, or fails.
func awaitLeader(t *testing.T, n *node.Node) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n.Status().Leader != 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node did not elect a leader; status %+v", n.Status())
}

// get fetches a path and returns its status and body.
func get(t *testing.T, url string) (int, string) {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

func TestLivenessDoesNotDependOnALeader(t *testing.T) {
	// The one that would restart the cluster during every election.
	n, base := startProbeNode(t, []raft.NodeID{1, 2, 3})

	if leader := n.Status().Leader; leader != 0 {
		t.Fatalf("this node was meant to be leaderless, but it reports leader %d", leader)
	}

	code, body := get(t, base+"/health")
	if code != http.StatusOK {
		t.Fatalf("/health with no leader returned %d, want 200: %s", code, body)
	}
}

func TestReadinessRefusesWithoutALeader(t *testing.T) {
	n, base := startProbeNode(t, []raft.NodeID{1, 2, 3})

	if leader := n.Status().Leader; leader != 0 {
		t.Fatalf("this node was meant to be leaderless, but it reports leader %d", leader)
	}

	code, _ := get(t, base+"/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/ready with no leader returned %d, want 503", code)
	}
}

func TestReadinessAcceptsOnceThereIsALeader(t *testing.T) {
	n, base := startProbeNode(t, []raft.NodeID{1})
	awaitLeader(t, n)

	code, body := get(t, base+"/ready")
	if code != http.StatusOK {
		t.Fatalf("/ready with a leader returned %d, want 200: %s", code, body)
	}
}

func TestHealthReportsTheNodeAsJSON(t *testing.T) {
	// The body is assembled by hand with Fprintf rather than marshalled, so
	// nothing but a test says it is JSON at all.
	n, base := startProbeNode(t, []raft.NodeID{1})
	awaitLeader(t, n)

	code, body := get(t, base+"/health")
	if code != http.StatusOK {
		t.Fatalf("/health returned %d, want 200", code)
	}

	var got struct {
		ID      uint64 `json:"id"`
		State   string `json:"state"`
		Term    uint64 `json:"term"`
		Leader  uint64 `json:"leader"`
		Commit  uint64 `json:"commit"`
		Applied uint64 `json:"applied"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("/health did not return JSON: %v\nbody: %s", err, body)
	}

	st := n.Status()
	if got.ID != uint64(st.ID) {
		t.Errorf("id = %d, want %d", got.ID, st.ID)
	}
	if got.State != st.State.String() {
		t.Errorf("state = %q, want %q", got.State, st.State)
	}
	if got.Leader != uint64(st.Leader) {
		t.Errorf("leader = %d, want %d", got.Leader, st.Leader)
	}
}

func TestMetricsAreServedOnTheSameListener(t *testing.T) {
	_, base := startProbeNode(t, []raft.NodeID{1})

	code, body := get(t, base+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics returned %d, want 200", code)
	}
	if !strings.Contains(body, "raftkv_") {
		t.Fatalf("/metrics served nothing from this project:\n%s", body)
	}
}

func TestNoMetricsAddressMeansNoServer(t *testing.T) {
	// An operator who leaves the flag empty gets a node with no HTTP
	// listener at all, rather than one bound to a port they did not choose.
	srv, err := serveMetrics("", prometheus.NewRegistry(), nil)
	if err != nil {
		t.Fatalf("an empty address was an error: %v", err)
	}
	if srv != nil {
		srv.Close()
		t.Fatal("an empty address still started a server")
	}
}

func TestLivenessFailsOnceTheNodeHasStopped(t *testing.T) {
	// Liveness is documented as asking whether the Raft loop is answering,
	// and Status is how it asks. But Status short-circuits when the loop is
	// gone and returns a zero value, so the reply proves nothing: a node
	// whose loop has exited answers exactly like a healthy one that has not
	// yet elected anybody.
	//
	// It matters because the loop can now exit on its own. A node that can
	// no longer write stops rather than carrying on, and if it still passes
	// liveness nothing ever replaces it: out of the client service for
	// failing readiness, never restarted, and reported up.
	n, base := startProbeNode(t, []raft.NodeID{1})
	awaitLeader(t, n)

	if code, _ := get(t, base+"/health"); code != http.StatusOK {
		t.Fatalf("/health on a healthy node returned %d, want 200", code)
	}

	if err := n.Stop(); err != nil {
		t.Fatalf("stopping the node: %v", err)
	}

	code, body := get(t, base+"/health")
	if code == http.StatusOK {
		t.Fatalf("/health still reports the node as live after its loop exited: %s", body)
	}
}
