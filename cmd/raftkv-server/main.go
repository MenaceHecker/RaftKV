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
		bind = peers[self]
	}

	ids := sortedIDs(peers)
	slog.Info("starting", "id", self, "listen", bind, "peers", ids, "data-dir", opt.dataDir)

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

	registry.MustRegister(metrics.NewCollector(n, peerTransport))

	metricsServer, err := serveMetrics(opt.metrics, registry, n)
	if err != nil {
		n.Stop()
		listener.Close()
		return err
	}

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
		stopServer(grpcServer)
		if metricsServer != nil {
			metricsServer.Close()
		}
		return errors.New("the consensus loop stopped")
	}

	stopServer(grpcServer)

	if metricsServer != nil {
		metricsServer.Close()
	}

	if err := n.Stop(); err != nil {
		return fmt.Errorf("stopping node: %w", err)
	}
	slog.Info("stopped", "id", self)
	return nil
}

const shutdownGrace = 5 * time.Second

func stopServer(srv *grpc.Server) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownGrace):
		slog.Warn("in-flight requests did not finish in time; closing connections",
			"grace", shutdownGrace)
		srv.Stop()
		<-done
	}
}

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

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if n.Status().Leader == 0 {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})

	srv := &http.Server{Handler: mux, Addr: listener.Addr().String()}
	go func() {
		slog.Info("serving metrics", "address", srv.Addr)
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server stopped", "error", err)
		}
	}()
	return srv, nil
}

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

func parsePeers(spec string) (map[raft.NodeID]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, errors.New("-peers is required, as id=host:port,id=host:port")
	}

	peers := make(map[raft.NodeID]string)
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

func sortedIDs(peers map[raft.NodeID]string) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

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
