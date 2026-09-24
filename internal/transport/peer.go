package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

const (
	DefaultQueueSize = 256

	DefaultSendTimeout = 5 * time.Second
)

type Stepper interface {
	Step(m raft.Message)
}

type PeerConfig struct {
	Self raft.NodeID

	Addresses map[raft.NodeID]string

	Local Stepper

	QueueSize int

	SendTimeout time.Duration

	DialOptions []grpc.DialOption
}

type peer struct {
	id   raft.NodeID
	addr string

	conn   *grpc.ClientConn
	client raftkvv1.RaftServiceClient

	queue chan *raftkvv1.Message
	done  chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	timeout time.Duration

	dropped atomic.Uint64
	failed  atomic.Uint64
	sent    atomic.Uint64
}

type PeerTransport struct {
	self raft.NodeID

	localMu sync.RWMutex
	local   Stepper

	peersMu sync.RWMutex
	peers   map[raft.NodeID]*peer

	closed bool

	dialOpts    []grpc.DialOption
	queueSize   int
	sendTimeout time.Duration

	closeOnce sync.Once
}

func (t *PeerTransport) SetLocal(s Stepper) {
	t.localMu.Lock()
	defer t.localMu.Unlock()
	t.local = s
}

func (t *PeerTransport) localStepper() Stepper {
	t.localMu.RLock()
	defer t.localMu.RUnlock()
	return t.local
}

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
		self:        cfg.Self,
		local:       cfg.Local,
		peers:       make(map[raft.NodeID]*peer, len(cfg.Addresses)),
		dialOpts:    dialOpts,
		queueSize:   cfg.QueueSize,
		sendTimeout: cfg.SendTimeout,
	}

	for id, addr := range cfg.Addresses {
		if err := t.AddPeer(id, addr); err != nil {
			t.Close()
			return nil, err
		}
	}

	return t, nil
}

func (t *PeerTransport) AddPeer(id raft.NodeID, addr string) error {
	if id == t.self {
		return nil
	}
	if addr == "" {
		return fmt.Errorf("transport: node %d has no address", id)
	}

	t.peersMu.Lock()
	if t.closed {
		t.peersMu.Unlock()
		return errors.New("transport: cannot add a peer to a closed transport")
	}
	existing, moved := t.peers[id]
	if moved && existing.addr == addr {
		t.peersMu.Unlock()
		return nil
	}
	p, err := t.dial(id, addr)
	if err != nil {
		t.peersMu.Unlock()
		return err
	}
	t.peers[id] = p
	t.peersMu.Unlock()

	if moved {
		existing.close()
	}
	return nil
}

func (t *PeerTransport) RemovePeer(id raft.NodeID) {
	t.peersMu.Lock()
	p, ok := t.peers[id]
	delete(t.peers, id)
	t.peersMu.Unlock()

	if ok {
		p.close()
	}
}

func (t *PeerTransport) dial(id raft.NodeID, addr string) (*peer, error) {
	conn, err := grpc.NewClient(addr, t.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("transport: creating client for node %d at %s: %w", id, addr, err)
	}

	pctx, pcancel := context.WithCancel(context.Background())
	p := &peer{
		id:      id,
		addr:    addr,
		conn:    conn,
		client:  raftkvv1.NewRaftServiceClient(conn),
		queue:   make(chan *raftkvv1.Message, t.queueSize),
		done:    make(chan struct{}),
		ctx:     pctx,
		cancel:  pcancel,
		timeout: t.sendTimeout,
	}
	go p.run()
	return p, nil
}

func (t *PeerTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if m.To == t.self {
			if local := t.localStepper(); local != nil {
				local.Step(m)
			}
			continue
		}

		t.peersMu.RLock()
		p, ok := t.peers[m.To]
		t.peersMu.RUnlock()
		if !ok {
			continue
		}

		wire, err := MessageToWire(m)
		if err != nil {
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

func (p *peer) run() {
	defer close(p.done)

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.queue:
			p.deliver(msg)
		}
	}
}

func (p *peer) deliver(msg *raftkvv1.Message) {
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()

	if msg.GetSnapshot() != nil && len(msg.GetSnapshot().GetData()) > 0 {
		if err := p.deliverSnapshot(ctx, msg); err != nil {
			p.failed.Add(1)
			return
		}
		p.sent.Add(1)
		return
	}

	if _, err := p.client.Deliver(ctx, &raftkvv1.DeliverRequest{Message: msg}); err != nil {
		p.failed.Add(1)
		return
	}
	p.sent.Add(1)
}

const SnapshotChunkSize = 1 << 20

func (p *peer) deliverSnapshot(ctx context.Context, msg *raftkvv1.Message) error {
	stream, err := p.client.DeliverSnapshot(ctx)
	if err != nil {
		return err
	}

	data := msg.GetSnapshot().GetData()
	header := proto.Clone(msg).(*raftkvv1.Message)
	header.Snapshot.Data = nil

	if err := stream.Send(&raftkvv1.SnapshotChunk{
		Frame: &raftkvv1.SnapshotChunk_Header{Header: header},
	}); err != nil {
		return err
	}

	for off := 0; off < len(data); off += SnapshotChunkSize {
		end := min(off+SnapshotChunkSize, len(data))
		if err := stream.Send(&raftkvv1.SnapshotChunk{
			Frame: &raftkvv1.SnapshotChunk_Data{Data: data[off:end]},
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func (p *peer) close() {
	p.cancel()
	<-p.done
	p.conn.Close()
}

func (t *PeerTransport) Close() error {
	t.closeOnce.Do(func() {
		t.peersMu.Lock()
		closing := make([]*peer, 0, len(t.peers))
		for _, p := range t.peers {
			closing = append(closing, p)
		}
		t.peers = nil
		t.closed = true
		t.peersMu.Unlock()

		for _, p := range closing {
			p.close()
		}
	})
	return nil
}

type PeerStats struct {
	ID      raft.NodeID
	Address string
	Sent    uint64
	Dropped uint64
	Failed  uint64
}

func (t *PeerTransport) Stats() []PeerStats {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()

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

type RaftServer struct {
	raftkvv1.UnimplementedRaftServiceServer

	node Stepper
}

func NewRaftServer(node Stepper) (*RaftServer, error) {
	if node == nil {
		return nil, errors.New("transport: RaftServer requires a node")
	}
	return &RaftServer{node: node}, nil
}

func (s *RaftServer) Deliver(ctx context.Context, req *raftkvv1.DeliverRequest) (*raftkvv1.DeliverResponse, error) {
	m, err := MessageFromWire(req.GetMessage())
	if err != nil {
		return nil, fmt.Errorf("transport: rejecting message: %w", err)
	}

	s.node.Step(m)
	return &raftkvv1.DeliverResponse{}, nil
}

const MaxSnapshotBytes = 1 << 30

func (s *RaftServer) DeliverSnapshot(stream raftkvv1.RaftService_DeliverSnapshotServer) error {
	var (
		header *raftkvv1.Message
		data   []byte
	)

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch frame := chunk.GetFrame().(type) {
		case *raftkvv1.SnapshotChunk_Header:
			if header != nil {
				return errors.New("transport: snapshot stream sent a second header")
			}
			header = frame.Header

		case *raftkvv1.SnapshotChunk_Data:
			if header == nil {
				return errors.New("transport: snapshot stream sent data before its header")
			}
			if len(data)+len(frame.Data) > MaxSnapshotBytes {
				return fmt.Errorf("transport: snapshot exceeds the %d byte limit", MaxSnapshotBytes)
			}
			data = append(data, frame.Data...)

		default:
			return errors.New("transport: snapshot stream sent an empty frame")
		}
	}

	if header == nil {
		return errors.New("transport: snapshot stream carried no header")
	}
	if header.GetSnapshot() == nil {
		return errors.New("transport: snapshot stream header carries no snapshot")
	}
	header.Snapshot.Data = data

	m, err := MessageFromWire(header)
	if err != nil {
		return fmt.Errorf("transport: rejecting snapshot: %w", err)
	}

	s.node.Step(m)
	return stream.SendAndClose(&raftkvv1.DeliverResponse{})
}

func (s *RaftServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterRaftServiceServer(srv, s)
}
