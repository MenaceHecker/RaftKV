package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// The peer transport: gRPC between cluster members.
//
// The whole design follows from one line in the driver's contract: Send is
// called inline from the Raft loop and must not block. A leader replicates to
// every follower on the same tick, so if one unreachable peer could stall that
// call, a single dead node would stop the cluster from making progress with
// the others. That is the failure Raft is supposed to survive, so the
// transport must not reintroduce it.
//
// So each peer gets its own goroutine and its own bounded queue. Send converts
// messages, drops them into the right queues, and returns immediately.
// Delivery happens elsewhere, at whatever pace each peer can manage, and a
// peer that cannot keep up affects only its own queue.

// Defaults for a peer transport.
const (
	// DefaultQueueSize is how many messages may be waiting for one peer.
	//
	// It is deliberately modest. A deep queue does not help a peer that is
	// down, it just stores stale messages that will be superseded by the next
	// heartbeat before they are ever sent. Shallow queues fail fast and let
	// the retry machinery Raft already has do its job.
	DefaultQueueSize = 256

	// DefaultSendTimeout bounds one delivery attempt, so a peer that accepts
	// a connection and then stops responding cannot block its sender
	// goroutine indefinitely.
	DefaultSendTimeout = 5 * time.Second
)

// Stepper receives Raft messages arriving from other nodes.
//
// It is declared here rather than imported so that this package does not
// depend on the driver. The dependency runs one way: the driver defines what a
// transport must do, and this satisfies it structurally.
type Stepper interface {
	Step(m raft.Message)
}

// PeerConfig describes how to reach the rest of the cluster.
type PeerConfig struct {
	// Self is this node's ID. Messages addressed to it are delivered locally
	// rather than sent over the network.
	Self raft.NodeID

	// Addresses maps every other node's ID to its address.
	Addresses map[raft.NodeID]string

	// Local receives messages this node sends to itself. It may be nil, in
	// which case such messages are dropped.
	//
	// It is usually left unset here and supplied by SetLocal instead, because
	// a node needs its transport before it can be constructed, so the
	// transport cannot be given the node at the same time.
	Local Stepper

	// QueueSize is the per-peer send queue depth. Zero means the default.
	QueueSize int

	// SendTimeout bounds one delivery attempt. Zero means the default.
	SendTimeout time.Duration

	// DialOptions are passed to gRPC. When empty, connections are insecure,
	// which suits a single-datacenter deployment behind a trusted network and
	// is what the local cluster uses. Anything exposed further needs
	// credentials supplied here.
	DialOptions []grpc.DialOption
}

// peer is one outbound connection, with the goroutine that drains its queue.
type peer struct {
	id   raft.NodeID
	addr string

	conn   *grpc.ClientConn
	client raftkvv1.RaftServiceClient

	queue chan *raftkvv1.Message
	stop  chan struct{}
	done  chan struct{}

	timeout time.Duration

	// dropped counts messages discarded because the queue was full, and
	// failed counts delivery attempts that errored. Both are observability
	// rather than control flow: Raft recovers from either on its own, but a
	// climbing count is the clearest signal that a peer is unhealthy.
	dropped atomic.Uint64
	failed  atomic.Uint64
	sent    atomic.Uint64
}

// PeerTransport sends Raft messages to other cluster members over gRPC.
//
// It satisfies the driver's Transport interface.
type PeerTransport struct {
	self raft.NodeID

	// local is guarded because SetLocal runs during startup while the Raft
	// loop may already be calling Send.
	localMu sync.RWMutex
	local   Stepper

	peers map[raft.NodeID]*peer

	closeOnce sync.Once
}

// SetLocal supplies the node that should receive messages this node addresses
// to itself.
//
// This exists because construction is circular: a node cannot be started
// without a transport, so the transport cannot be handed the node up front.
// Rather than pretend otherwise with a two-phase constructor, the dependency
// is filled in afterwards.
//
// Call it immediately after starting the node. Messages sent before it is set
// are dropped, which is harmless in practice because the Raft core never
// addresses a message to itself, but the window should be closed anyway.
func (t *PeerTransport) SetLocal(s Stepper) {
	t.localMu.Lock()
	defer t.localMu.Unlock()
	t.local = s
}

// localStepper returns the local receiver, if one has been set.
func (t *PeerTransport) localStepper() Stepper {
	t.localMu.RLock()
	defer t.localMu.RUnlock()
	return t.local
}

// NewPeerTransport creates connections to every configured peer.
//
// Dialling is lazy: gRPC establishes and re-establishes connections in the
// background, so a node can start before its peers exist and will connect when
// they appear. That matters for cluster startup, where some ordering is always
// wrong.
func NewPeerTransport(cfg PeerConfig) (*PeerTransport, error) {
	if cfg.Self == raft.None {
		return nil, errors.New("transport: Self must be a non-zero node ID")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.SendTimeout <= 0 {
		cfg.SendTimeout = DefaultSendTimeout
	}

	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}

	t := &PeerTransport{
		self:  cfg.Self,
		local: cfg.Local,
		peers: make(map[raft.NodeID]*peer, len(cfg.Addresses)),
	}

	for id, addr := range cfg.Addresses {
		if id == cfg.Self {
			// A node does not dial itself; those messages are handed
			// straight to the local Stepper.
			continue
		}
		if addr == "" {
			t.Close()
			return nil, fmt.Errorf("transport: node %d has no address", id)
		}

		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("transport: creating client for node %d at %s: %w", id, addr, err)
		}

		p := &peer{
			id:      id,
			addr:    addr,
			conn:    conn,
			client:  raftkvv1.NewRaftServiceClient(conn),
			queue:   make(chan *raftkvv1.Message, cfg.QueueSize),
			stop:    make(chan struct{}),
			done:    make(chan struct{}),
			timeout: cfg.SendTimeout,
		}
		t.peers[id] = p
		go p.run()
	}

	return t, nil
}

// Send implements the driver's Transport interface.
//
// It never blocks. Messages are converted, queued, and left for the per-peer
// goroutines to deliver. A message for a peer whose queue is full is dropped,
// which is safe because Raft retransmits: an append that never arrives is sent
// again on the next heartbeat, and correctness never depends on a particular
// message arriving.
func (t *PeerTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		// A message a node addresses to itself skips the network entirely.
		// This happens in a single-node cluster and during configuration
		// changes; routing it through gRPC would be a pointless round trip
		// through the loopback interface.
		if m.To == t.self {
			if local := t.localStepper(); local != nil {
				local.Step(m)
			}
			continue
		}

		p, ok := t.peers[m.To]
		if !ok {
			// Not a known peer. Phase 4's membership changes make this
			// reachable; for now it means a stray message.
			continue
		}

		wire, err := MessageToWire(m)
		if err != nil {
			// The core produced something with no wire form, which is a bug
			// here rather than a network condition. Dropping it keeps the
			// cluster running; the counter makes it visible.
			p.failed.Add(1)
			continue
		}

		select {
		case p.queue <- wire:
		default:
			p.dropped.Add(1)
		}
	}
}

// run drains one peer's queue. It is the only goroutine that touches that
// peer's gRPC client.
func (p *peer) run() {
	defer close(p.done)

	for {
		select {
		case <-p.stop:
			return
		case msg := <-p.queue:
			p.deliver(msg)
		}
	}
}

// deliver makes one attempt to hand a message to its peer.
//
// A failure is logged in a counter and otherwise ignored. There is no retry
// loop here on purpose: Raft's own retransmission is smarter than anything
// this layer could do, because by the time a retry would fire the leader
// usually has a newer message that supersedes the failed one. Retrying here
// would send stale state and compete with the correct mechanism.
func (p *peer) deliver(msg *raftkvv1.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	if _, err := p.client.Deliver(ctx, &raftkvv1.DeliverRequest{Message: msg}); err != nil {
		p.failed.Add(1)
		return
	}
	p.sent.Add(1)
}

// close shuts down one peer's goroutine and connection.
func (p *peer) close() {
	close(p.stop)
	<-p.done
	p.conn.Close()
}

// Close shuts down every peer connection. It is safe to call more than once.
func (t *PeerTransport) Close() error {
	t.closeOnce.Do(func() {
		for _, p := range t.peers {
			p.close()
		}
	})
	return nil
}

// PeerStats reports what has happened on one peer's link, for metrics and for
// tests.
type PeerStats struct {
	ID      raft.NodeID
	Address string
	// Sent counts successful deliveries.
	Sent uint64
	// Dropped counts messages discarded because the queue was full, which
	// means this peer is slower than the leader is producing traffic for it.
	Dropped uint64
	// Failed counts delivery attempts that returned an error, which usually
	// means the peer is unreachable.
	Failed uint64
}

// Stats returns per-peer counters.
//
// These are the raw material for the replication-lag and connectivity metrics
// Phase 5 exposes to Prometheus. They are surfaced now because a transport
// that silently drops messages is otherwise impossible to distinguish from one
// that is working.
func (t *PeerTransport) Stats() []PeerStats {
	out := make([]PeerStats, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, PeerStats{
			ID:      p.id,
			Address: p.addr,
			Sent:    p.sent.Load(),
			Dropped: p.dropped.Load(),
			Failed:  p.failed.Load(),
		})
	}
	return out
}

// RaftServer is the receiving half: it accepts messages from other nodes and
// hands them to the local Raft node.
type RaftServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	node Stepper
}

// NewRaftServer wraps a node so it can receive messages from peers.
func NewRaftServer(node Stepper) (*RaftServer, error) {
	if node == nil {
		return nil, errors.New("transport: RaftServer requires a node")
	}
	return &RaftServer{node: node}, nil
}

// Deliver implements the Raft service.
//
// It returns as soon as the message is handed to the node, without waiting for
// any consequence of it. The reply carries no information: a sender that
// learned "delivered" would do nothing differently, and one that learned
// "failed" would find out anyway when the next heartbeat produced no progress.
// Responses travel as their own Deliver calls in the other direction.
func (s *RaftServer) Deliver(ctx context.Context, req *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	m, err := MessageFromWire(req.GetMessage())
	if err != nil {
		// A message that cannot be decoded exactly is refused rather than
		// approximated. Acting on a half-understood message is worse than
		// dropping it, and the sender will retransmit.
		return nil, fmt.Errorf("transport: rejecting message: %w", err)
	}

	s.node.Step(m)
	return &raftkvv1.DeliverResponse{}, nil
}

// Register attaches the Raft service to a gRPC server.
func (s *RaftServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterRaftServiceServer(srv, s)
}
