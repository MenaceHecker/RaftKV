// Command raftkv-server runs one RaftKV node.
//
// The optional admin API (-admin-listen, -admin-token-file, -admin-chaos) is OFF
// by default, and what it protects is narrow. Its bearer token, the chaos switch
// and the separate listener guard the AdminService RPCs only. They do NOT cover
// the shared -listen port, which still accepts without authentication:
//
//   - AddNode and RemoveNode, so anyone who can reach that port can change
//     cluster membership; and
//   - snapshot streams of up to 1 GiB each, with no bound on how many run at
//     once or on the memory they hold in total.
//
// Both are pre-existing and tracked as backlog card T3.18. Keep the shared port
// on a trusted network; the admin token does not make it safe to expose.
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
	"sync/atomic"
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
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
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

	adminListen    string
	adminTokenFile string
	adminChaos     bool

	clusterTokenFile string
}

// defineFlags registers the server's flags on fs. The admin flags default to off.
func defineFlags(fs *flag.FlagSet, opt *options) {
	fs.Uint64Var(&opt.id, "id", 0,
		"this node's ID; must be non-zero and appear in -peers")
	fs.StringVar(&opt.peers, "peers", "",
		"comma-separated id=host:port for every member, including this one")
	fs.StringVar(&opt.listen, "listen", "",
		"address to bind (default: this node's address from -peers)")
	fs.StringVar(&opt.dataDir, "data-dir", "",
		"directory for the write-ahead log and snapshots")
	fs.DurationVar(&opt.tick, "tick", node.DefaultTickInterval,
		"how much wall time one logical tick represents")
	fs.IntVar(&opt.election, "election-ticks", node.DefaultElectionTick,
		"ticks without hearing from a leader before starting an election")
	fs.IntVar(&opt.beat, "heartbeat-ticks", node.DefaultHeartbeatTick,
		"ticks between a leader's heartbeats; must be well below -election-ticks")
	fs.BoolVar(&opt.fsync, "fsync", true,
		"fsync every write-ahead log append; disabling it survives a process crash but not power loss")
	fs.Uint64Var(&opt.snapshot, "snapshot-threshold", node.DefaultSnapshotThreshold,
		"entries applied past the last snapshot before another is taken")
	fs.StringVar(&opt.logLevel, "log-level", "info", "debug, info, warn, or error")
	fs.StringVar(&opt.metrics, "metrics-listen", "",
		"address to serve Prometheus metrics and health on, e.g. 127.0.0.1:9101 (empty disables it)")
	fs.StringVar(&opt.adminListen, "admin-listen", "",
		"address for the admin API (status, events, fault injection); empty disables it. Needs -admin-token-file")
	fs.StringVar(&opt.adminTokenFile, "admin-token-file", "",
		"file holding the admin bearer token, readable only by its owner (mode 0600); required with -admin-listen")
	fs.BoolVar(&opt.adminChaos, "admin-chaos", false,
		"also allow the admin RPCs that inject faults: Crash, Pause, SetPartition, SetNetem")
	fs.StringVar(&opt.clusterTokenFile, "cluster-token-file", "",
		"file holding a bearer token required for AddNode and RemoveNode on the shared listener (mode 0600); empty leaves them open")
}

// validateClusterToken fails closed: an unset -cluster-token-file means off, but a
// set one must name a usable token file or the server does not start. It never
// includes the token in an error.
func validateClusterToken(opt options) (string, error) {
	if opt.clusterTokenFile == "" {
		return "", nil
	}
	return transport.LoadClusterToken(opt.clusterTokenFile)
}

// validateAdminOptions fails closed. The admin listener starts only with a usable
// token; the chaos switch and the token file mean nothing without the listener, so
// they are refused too rather than silently ignored. It returns the token, or ""
// when the admin API is off. It never includes the token in an error.
func validateAdminOptions(opt options) (token string, err error) {
	if opt.adminListen == "" {
		if opt.adminChaos {
			return "", errors.New("-admin-chaos has no effect without -admin-listen")
		}
		if opt.adminTokenFile != "" {
			return "", errors.New("-admin-token-file has no effect without -admin-listen")
		}
		return "", nil
	}
	return transport.LoadAdminToken(opt.adminTokenFile)
}

func run() error {
	var opt options

	defineFlags(flag.CommandLine, &opt)
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

	// Fail closed before anything starts: the admin listener needs a usable token.
	adminToken, err := validateAdminOptions(opt)
	if err != nil {
		return err
	}
	adminOn := opt.adminListen != ""

	clusterToken, err := validateClusterToken(opt)
	if err != nil {
		return err
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

	// With the admin API off nothing is wrapped and behaviour is unchanged. With
	// it on, faults sit between the node and the peer transport, and the ingress
	// filter and pause gate are installed below.
	var (
		nodeTransport node.Transport = peerTransport
		faults        *transport.FaultTransport
		nodeRef       atomic.Pointer[node.Node]
	)
	if adminOn {
		faults = transport.NewFaultTransport(transport.FaultConfig{
			Self:   self,
			Inner:  peerTransport,
			Clock:  transport.RealClock{},
			Rand:   transport.RealRand{},
			Paused: func() bool { n := nodeRef.Load(); return n != nil && n.Paused() },
		})
		nodeTransport = faults
	}

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
		Transport:         nodeTransport,
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

	nodeRef.Store(n)

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

	var faultsForServer *transport.FaultTransport
	if adminOn {
		faultsForServer = faults
	}
	grpcServer := newSharedServerWithClusterToken(n, raftServer, kvServer, faultsForServer, clusterToken)

	// AdminService is served on its own listener and nowhere else.
	var (
		adminServer *grpc.Server
		adminErr    chan error
	)
	if adminOn {
		adminListener, err := net.Listen("tcp", opt.adminListen)
		if err != nil {
			_ = n.Stop() // best effort: already failing
			_ = listener.Close()
			return fmt.Errorf("listening for the admin API on %s: %w", opt.adminListen, err)
		}
		adminServer, err = newAdminServer(n, faults, adminToken, opt.adminChaos)
		if err != nil {
			_ = n.Stop() // best effort: already failing
			_ = listener.Close()
			_ = adminListener.Close()
			return err
		}

		adminErr = make(chan error, 1)
		go func() { adminErr <- adminServer.Serve(adminListener) }()
		slog.Info("admin API enabled", "address", adminListener.Addr().String(), "chaos", opt.adminChaos)
		if opt.adminChaos {
			slog.Warn("fault injection is enabled (-admin-chaos): Crash, Pause, SetPartition and SetNetem will act")
		}
	}

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
	case err := <-adminErr: // nil channel, so never ready, when the admin API is off
		if err != nil {
			_ = n.Stop() // best effort: already failing
			return fmt.Errorf("serving the admin API: %w", err)
		}
	case <-n.Done():
		stopAdmin(adminServer)
		stopServer(grpcServer)
		if metricsServer != nil {
			metricsServer.Close()
		}
		return errors.New("the consensus loop stopped")
	}

	stopAdmin(adminServer)
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

// newSharedServer builds the gRPC server for the shared -listen port: the Raft
// and client services and, deliberately, never AdminService. With faults nil (the
// admin API is off) it is exactly the plain server. With faults set, the Raft
// endpoint goes through the ingress filter and the pause gate fails the client
// services while the node is paused.
func newSharedServer(n *node.Node, raftServer *transport.RaftServer, kvServer *transport.KVServer, faults *transport.FaultTransport) *grpc.Server {
	return newSharedServerWithClusterToken(n, raftServer, kvServer, faults, "")
}

// newSharedServerWithClusterToken is newSharedServer plus, when clusterToken is
// not empty, a bearer-token check on AddNode and RemoveNode only.
func newSharedServerWithClusterToken(n *node.Node, raftServer *transport.RaftServer, kvServer *transport.KVServer, faults *transport.FaultTransport, clusterToken string) *grpc.Server {
	var (
		unary  []grpc.UnaryServerInterceptor
		stream []grpc.StreamServerInterceptor
	)
	if clusterToken != "" {
		u, s := transport.NewClusterTokenInterceptors(clusterToken)
		unary, stream = append(unary, u), append(stream, s)
	}
	if faults != nil {
		u, s := transport.NewPauseGate(n.Paused)
		unary, stream = append(unary, u), append(stream, s)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
	if faults != nil {
		raftkvv1.RegisterRaftServiceServer(srv, faults.WrapRaftServer(raftServer))
	} else {
		raftServer.Register(srv)
	}
	kvServer.Register(srv)
	return srv
}

// newAdminServer builds the gRPC server for -admin-listen: AdminService behind
// the token interceptors, and nothing else.
func newAdminServer(n *node.Node, faults *transport.FaultTransport, token string, chaos bool) (*grpc.Server, error) {
	svc, err := transport.NewAdminServer(transport.AdminConfig{Node: n, Faults: faults, Chaos: chaos})
	if err != nil {
		return nil, err
	}
	authUnary, authStream := transport.NewAdminInterceptors(token, chaos)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(authUnary), grpc.ChainStreamInterceptor(authStream))
	svc.Register(srv)
	return srv, nil
}

const shutdownGrace = 5 * time.Second

// stopAdmin stops the admin server, if there is one, without waiting for streams:
// an Events client would otherwise hold a graceful stop open until the grace expires.
func stopAdmin(srv *grpc.Server) {
	if srv != nil {
		srv.Stop()
	}
}

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
