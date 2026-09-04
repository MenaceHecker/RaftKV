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
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

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
	})
	if err != nil {
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
	}

	// Stop accepting work before stopping the node, so nothing arrives for a
	// node that is on its way down and would only fail it.
	grpcServer.GracefulStop()

	if err := n.Stop(); err != nil {
		return fmt.Errorf("stopping node: %w", err)
	}
	slog.Info("stopped", "id", self)
	return nil
}

// watchLeadership logs leadership changes.
//
// This is the one piece of observability worth having before Phase 5's metrics
// exist: a cluster that cannot elect a leader looks identical to one that is
// merely idle, and this is what distinguishes them.
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
