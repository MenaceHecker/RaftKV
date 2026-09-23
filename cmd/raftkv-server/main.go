// Command raftkv-server runs one node of a RaftKV cluster.
//
// It is deliberately thin. Everything it does is assemble pieces that already
// exist and are already tested — the consensus core, the durable storage, the
// state machine, the transport — and then get out of the way. Logic that lives
// here would be logic no test covers, because a main package is the one part of
// a Go program that cannot be exercised from another package.
//
// A three-node cluster on one machine:
//
//	raftkv-server --id 1 --peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 --data-dir /tmp/n1
//	raftkv-server --id 2 --peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 --data-dir /tmp/n2
//	raftkv-server --id 3 --peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 --data-dir /tmp/n3
//
// Every node is given the same peer list, including itself. Nodes may be
// started in any order: a node that comes up first will campaign, fail to reach
// a majority, and keep trying until the others appear.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/metrics"
	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

func main() {
	if err := run(); err != nil {
		slog.Error("raftkv-server exited", "error", err)
		os.Exit(1)
	}
}

// options is everything the process is configured with.
type options struct {
	id       uint64
	peers    string
	listen   string
	dataDir  string
	tick     time.Duration
	election int
	beat     int
	fsync    bool
	snapshot uint64
	logLevel string
	metrics  string
}

func run() error {
	var opt options

	flag.Uint64Var(&opt.id, "id", 0,
		"this node's ID; must be non-zero and appear in -peers")
	flag.StringVar(&opt.peers, "peers", "",
		"comma-separated id=host:port for every member, including this one")
	flag.StringVar(&opt.listen, "listen", "",
		"address to bind (default: this node's address from -peers)")
	flag.StringVar(&opt.dataDir, "data-dir", "",
		"directory for the write-ahead log and snapshots")
	flag.DurationVar(&opt.tick, "tick", node.DefaultTickInterval,
		"how much wall time one logical tick represents")
	flag.IntVar(&opt.election, "election-ticks", node.DefaultElectionTick,
		"ticks without hearing from a leader before starting an election")
	flag.IntVar(&opt.beat, "heartbeat-ticks", node.DefaultHeartbeatTick,
		"ticks between a leader's heartbeats; must be well below -election-ticks")
	flag.BoolVar(&opt.fsync, "fsync", true,
		"fsync every write-ahead log append; disabling it survives a process crash but not power loss")
	flag.Uint64Var(&opt.snapshot, "snapshot-threshold", node.DefaultSnapshotThreshold,
		"entries applied past the last snapshot before another is taken")
	flag.StringVar(&opt.logLevel, "log-level", "info", "debug, info, warn, or error")
	flag.StringVar(&opt.metrics, "metrics-listen", "",
		"address to serve Prometheus metrics and health on, e.g. 127.0.0.1:9101 (empty disables it)")
	flag.Parse()

	logger, err := newLogger(opt.logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	peers, err := parsePeers(opt.peers)
	if err != nil {
		return err
	}
	self := raft.NodeID(opt.id)
	if _, ok := peers[self]; !ok {
		return fmt.Errorf("-id %d does not appear in -peers", opt.id)
	}
	if opt.dataDir == "" {
		return errors.New("-data-dir is required")
	}

	bind := opt.listen
	if bind == "" {
		// Default to the address the rest of the cluster was told to use. A
		// node listening somewhere other than where it is advertised is a
		// misconfiguration that shows up only as unexplained unavailability,
		// so the two agree unless someone deliberately separates them — which
		// is what -listen is for, when the bind address differs from the
		// routable one.
		bind = peers[self]
	}

	ids := sortedIDs(peers)
	slog.Info("starting", "id", self, "listen", bind, "peers", ids, "data-dir", opt.dataDir)

	// Bind before starting the node, so a port conflict fails immediately
	// rather than after the node has written to its data directory.
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", bind, err)
	}

	peerTransport, err := transport.NewPeerTransport(transport.PeerConfig{
		Self:      self,
		Addresses: peers,
	})
	if err != nil {
		listener.Close()
		return err
	}
	defer peerTransport.Close()

	// The registry is built before the node so the node can be handed the
	// recorder at construction. Metrics that only start once an HTTP server
	// is up would miss the first election, which is the one most worth
	// seeing.
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	recorder := metrics.New(registry)

	sync := storage.SyncAlways
	if !opt.fsync {
		sync = storage.SyncNever
		slog.Warn("fsync disabled; committed writes will not survive power loss")
	}

	n, err := node.Start(node.Config{
		ID:                self,
		Peers:             ids,
		DataDir:           opt.dataDir,
		Transport:         peerTransport,
		TickInterval:      opt.tick,
		ElectionTick:      opt.election,
		HeartbeatTick:     opt.beat,
		SnapshotThreshold: opt.snapshot,
		Sync:              sync,
		Metrics:           recorder,
	})
	if err != nil {
		listener.Close()
		return err
	}

	// The collector samples the node and the transport at scrape time, so it
	// can only be registered once both exist.
	registry.MustRegister(metrics.NewCollector(n, peerTransport))

	metricsServer, err := serveMetrics(opt.metrics, registry, n)
	if err != nil {
		n.Stop()
		listener.Close()
		return err
	}

	// Close the loop for messages this node addresses to itself. Until this is
	// set they are dropped, which is harmless but pointless.
	peerTransport.SetLocal(n)

	raftServer, err := transport.NewRaftServer(n)
	if err != nil {
		n.Stop()
		listener.Close()
		return err
	}
	kvServer, err := transport.NewKVServer(n, peers)
	if err != nil {
		n.Stop()
		listener.Close()
		return err
	}

	grpcServer := grpc.NewServer()
	raftServer.Register(grpcServer)
	kvServer.Register(grpcServer)

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("serving", "address", listener.Addr().String())
		serveErr <- grpcServer.Serve(listener)
	}()

	// A leader election is the first thing anyone wants to see, so report it
	// once rather than leaving the operator to poll.
	go watchLeadership(n)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-signals:
		slog.Info("shutting down", "signal", sig.String())
	case err := <-serveErr:
		if err != nil {
			n.Stop()
			return fmt.Errorf("serving: %w", err)
		}
	case <-n.Done():
		// The node stopped without being asked to, which it does when it can
		// no longer write. Carrying on would leave a process accepting
		// connections for a node that is gone: out of the client service for
		// failing readiness, still passing liveness, and never replaced.
		// Exiting non-zero is what gets it restarted and noticed.
		stopServer(grpcServer)
		if metricsServer != nil {
			metricsServer.Close()
		}
		return errors.New("the consensus loop stopped")
	}

	// Stop accepting work before stopping the node, so nothing arrives for a
	// node that is on its way down and would only fail it.
	stopServer(grpcServer)

	if metricsServer != nil {
		// Close rather than Shutdown: a scrape in flight has nothing worth
		// waiting for, and a held-open connection should not delay the node
		// releasing its files.
		metricsServer.Close()
	}

	if err := n.Stop(); err != nil {
		return fmt.Errorf("stopping node: %w", err)
	}
	slog.Info("stopped", "id", self)
	return nil
}

// shutdownGrace is how long in-flight requests are given to finish before the
// server stops waiting for them.
//
// It has to exist because GracefulStop alone is not bounded by anything this
// process controls. It refuses new calls on every service at once, Raft's
// included, so a leader stops receiving the follower responses it needs to
// commit and the client writes already in its hands can no longer finish.
// They then sit there until the client gives up, and GracefulStop waits for
// them: measured, shutdown took 3, 10 and 20 seconds for clients whose
// request timeouts were 3, 10 and 20 seconds. A client that waits longer than
// the orchestrator's grace period turns an orderly stop into a kill.
//
// Five seconds is comfortably longer than a healthy write and comfortably
// shorter than the thirty second grace period the Kubernetes manifest asks
// for, leaving room for the node to close its files afterwards.
const shutdownGrace = 5 * time.Second

// stopServer stops accepting work, waiting a bounded time for calls already
// in progress.
func stopServer(srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownGrace):
		// Whatever is still outstanding is waiting on something this node
		// has already stopped being able to provide. Cutting it off returns
		// an error to those clients, which is the honest answer and one they
		// will retry against the new leader.
		slog.Warn("in-flight requests did not finish in time; closing connections",
			"grace", shutdownGrace)
		srv.Stop()
		<-done
	}
}

// serveMetrics starts the observability endpoint, or returns nil if no
// address was configured.
//
// It runs on its own listener rather than alongside the Raft and client
// services. Metrics are most valuable exactly when the cluster is unhealthy,
// and sharing a server with the traffic that is failing is how a monitoring
// endpoint ends up unavailable during the incident it exists to explain. A
// separate port also keeps it easy to expose internally without exposing the
// data plane.
func serveMetrics(addr string, registry *prometheus.Registry, n *node.Node) (*http.Server, error) {
	if addr == "" {
		return nil, nil
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listening for metrics on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(registry))

	// Liveness: the process is up and its Raft loop is still running.
	//
	// The stopped check is not belt and braces. Status answers from the loop
	// while there is one and returns a zero value once there is not, so on
	// its own it cannot tell a node whose loop has exited from a healthy one
	// that has yet to elect anybody. Both would report live, and a node that
	// stopped because it could no longer write would keep passing liveness
	// for as long as the process ran.
	//
	// It must not depend on there being a leader. During an election nobody
	// has one, and a liveness probe that checked would fail on every node at
	// once and have the whole cluster restarted.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if n.Stopped() {
			http.Error(w, "the consensus loop has stopped", http.StatusServiceUnavailable)
			return
		}
		st := n.Status()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%d,"state":%q,"term":%d,"leader":%d,"commit":%d,"applied":%d}`+"\n",
			st.ID, st.State, st.Term, st.Leader, st.Commit, st.Applied)
	})

	// Readiness: this node can serve. A node with no leader is running
	// correctly but cannot answer a linearizable read, so a load balancer
	// should route around it rather than send traffic it will refuse.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if n.Status().Leader == 0 {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})

	// Addr records what was actually bound, which matters when the
	// configured address asked for port zero. Serve ignores it in favour of
	// the listener, so this is a label rather than an instruction.
	srv := &http.Server{Handler: mux, Addr: listener.Addr().String()}
	go func() {
		slog.Info("serving metrics", "address", srv.Addr)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// The node keeps running: losing observability is bad, but
			// stopping a healthy cluster member over it is worse.
			slog.Error("metrics server stopped", "error", err)
		}
	}()
	return srv, nil
}

// watchLeadership logs leadership changes.
//
// The metrics record the same transitions, but a log line is what somebody
// reads first when a cluster will not serve: one that cannot elect a leader
// looks identical to one that is merely idle.
func watchLeadership(n *node.Node) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastLeader raft.NodeID
	var lastTerm raft.Term

	for range ticker.C {
		st := n.Status()
		if st.Leader == lastLeader && st.Term == lastTerm {
			continue
		}
		lastLeader, lastTerm = st.Leader, st.Term

		if st.Leader == raft.None {
			slog.Info("no leader", "term", st.Term, "state", st.State.String())
			continue
		}
		slog.Info("leader",
			"leader", st.Leader,
			"term", st.Term,
			"self", st.State.String(),
		)
	}
}

// parsePeers reads the id=address list.
func parsePeers(spec string) (map[raft.NodeID]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, errors.New("-peers is required, as id=host:port,id=host:port")
	}

	peers := make(map[raft.NodeID]string)
	// Addresses are tracked alongside IDs so a collision in either is caught.
	addresses := make(map[string]raft.NodeID)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		idText, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q is not of the form id=host:port", part)
		}

		id, err := strconv.ParseUint(strings.TrimSpace(idText), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q has an unreadable ID: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("peer %q uses the reserved ID 0", part)
		}

		addr = strings.TrimSpace(addr)
		if addr == "" {
			return nil, fmt.Errorf("peer %d has no address", id)
		}
		if _, exists := peers[raft.NodeID(id)]; exists {
			return nil, fmt.Errorf("peer %d appears more than once", id)
		}
		if other, taken := addresses[addr]; taken {
			// Two members advertising one address is a typo with an
			// expensive failure mode. The second node to start cannot bind
			// the port and dies, and everyone else dialing it reaches the
			// first node's process instead, so the cluster believes it has a
			// member it does not. Quorum is still met by the survivors, which
			// is the problem: nothing looks broken, and the fault tolerance
			// that was paid for is quietly gone.
			return nil, fmt.Errorf("peers %d and %d are both at %s", other, id, addr)
		}
		addresses[addr] = raft.NodeID(id)
		peers[raft.NodeID(id)] = addr
	}

	if len(peers) == 0 {
		return nil, errors.New("-peers listed no members")
	}
	return peers, nil
}

// sortedIDs returns the member IDs in ascending order.
//
// The order is not arbitrary: every node derives its initial configuration
// from this list, and giving them the same membership in a different order
// would be a needless source of difference between nodes.
func sortedIDs(peers map[raft.NodeID]string) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// newLogger builds the process logger.
func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown -log-level %q", level)
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}
