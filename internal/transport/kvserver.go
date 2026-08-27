package transport

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// The client-facing services.
//
// Two things distinguish this from the peer transport. The first is
// redirection: a client may talk to any node, and one that is not the leader
// has to say so and name who is, rather than serving from state it cannot
// vouch for. The second is that these calls block until the cluster has
// actually agreed — a write returns when its entry commits, not when it is
// accepted — because a client that was told "yes" and then lost its write has
// no way to find out.

// Store is the part of a node these services need.
//
// It is declared here rather than taking a *node.Node so that the services can
// be tested against a stub, and so the dependency stays one-way: the driver
// knows nothing about gRPC.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Propose(ctx context.Context, cmd statemachine.Command) error
	AddNode(ctx context.Context, id raft.NodeID, addr string) error
	RemoveNode(ctx context.Context, id raft.NodeID) error
	Status() node.Status
}

// KVServer serves the key-value API and cluster administration.
type KVServer struct {
	raftkvv1.UnimplementedKVServiceServer
	raftkvv1.UnimplementedClusterServiceServer

	store Store

	// staticAddrs is a fallback for resolving a leader's address when the
	// cluster's own membership does not carry one — which is the case for
	// nodes configured at startup, before any membership change has recorded
	// an address for them.
	staticAddrs map[raft.NodeID]string
}

// NewKVServer wraps a node so clients can reach it.
//
// The address map is used only to describe other nodes to clients. Membership
// changes carry addresses of their own, and those take precedence.
func NewKVServer(store Store, addrs map[raft.NodeID]string) (*KVServer, error) {
	if store == nil {
		return nil, errors.New("transport: KVServer requires a store")
	}

	static := make(map[raft.NodeID]string, len(addrs))
	for id, addr := range addrs {
		static[id] = addr
	}
	return &KVServer{store: store, staticAddrs: static}, nil
}

// Register attaches both client-facing services to a gRPC server.
func (s *KVServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterKVServiceServer(srv, s)
	raftkvv1.RegisterClusterServiceServer(srv, s)
}

// addressOf resolves a node's address, preferring what the cluster itself
// believes over what this node was configured with.
//
// The order matters once membership has changed: a node added after startup
// appears only in the cluster's configuration, and a node whose address was
// updated there is no longer where the static map says.
func (s *KVServer) addressOf(id raft.NodeID) string {
	if id == raft.None {
		return ""
	}
	if addrs := s.store.Status().Members.Addrs; addrs != nil {
		if addr, ok := addrs[id]; ok && addr != "" {
			return addr
		}
	}
	return s.staticAddrs[id]
}

// notLeaderError builds the error a non-leader returns.
//
// The redirect travels as an error detail rather than a successful response
// carrying a flag. That is what it is: the request did not happen. A response
// with an "actually, no" field would make every client remember to check, and
// the ones that forgot would treat a redirect as a result.
func (s *KVServer) notLeaderError() error {
	st := s.store.Status()

	detail := &raftkvv1.NotLeader{
		LeaderId:      uint64(st.Leader),
		LeaderAddress: s.addressOf(st.Leader),
	}

	msg := "not the leader"
	if st.Leader != raft.None {
		msg = "not the leader; try node " + strconv.FormatUint(uint64(st.Leader), 10)
		if detail.GetLeaderAddress() != "" {
			msg += " at " + detail.GetLeaderAddress()
		}
	}

	// FailedPrecondition rather than Unavailable: the node is perfectly
	// healthy and the request is well-formed, it is simply addressed to the
	// wrong member. Unavailable would invite gRPC's automatic retry against
	// the same node, which cannot ever succeed.
	st2 := status.New(codes.FailedPrecondition, msg)
	withDetail, err := st2.WithDetails(detail)
	if err != nil {
		// Attaching the detail failed, which should not happen for a message
		// this simple. The plain error still tells the client to look
		// elsewhere, so it is better than failing the call for a different
		// reason.
		return st2.Err()
	}
	return withDetail.Err()
}

// translate converts a node error into a gRPC status.
//
// Each mapping says something different to a client: redirect, retry, or give
// up. Collapsing them into one code would leave a client unable to tell a
// wrong-node error from a genuine failure.
func (s *KVServer) translate(err error) error {
	switch {
	case err == nil:
		return nil

	case errors.Is(err, node.ErrNotLeader):
		return s.notLeaderError()

	case errors.Is(err, node.ErrLostLeadership):
		// The write may or may not have taken effect. Retrying is safe
		// because commands carry a client ID and sequence number, so a
		// duplicate is ignored rather than applied twice.
		return status.Error(codes.Aborted,
			"leadership changed before the request committed; retry")

	case errors.Is(err, node.ErrStopped):
		return status.Error(codes.Unavailable, "node is shutting down")

	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())

	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())

	case errors.Is(err, raft.ErrConfChangeInFlight):
		return status.Error(codes.FailedPrecondition,
			"a membership change is already in progress")

	case errors.Is(err, raft.ErrNoChange),
		errors.Is(err, raft.ErrEmptyConfiguration),
		errors.Is(err, raft.ErrNotInJoint):
		return status.Error(codes.InvalidArgument, err.Error())

	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// Get implements the key-value service.
//
// The read is linearizable: the node confirms with a majority that it is still
// leader before answering. A follower refuses rather than serving from local
// state, because its state may be arbitrarily behind and nothing in the reply
// would say so.
func (s *KVServer) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	value, found, err := s.store.Get(ctx, req.GetKey())
	if err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.GetResponse{Value: value, Found: found}, nil
}

// Put implements the key-value service.
func (s *KVServer) Put(ctx context.Context, req *raftkvv1.PutRequest) (*raftkvv1.PutResponse, error) {
	err := s.store.Propose(ctx, statemachine.Command{
		ClientID: req.GetClient().GetClientId(),
		Seq:      req.GetClient().GetSequence(),
		Op:       statemachine.OpPut,
		Key:      req.GetKey(),
		Value:    req.GetValue(),
	})
	if err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.PutResponse{}, nil
}

// Delete implements the key-value service.
//
// Removing a key that does not exist succeeds, because a command has to be
// applicable on every replica whatever that replica holds.
func (s *KVServer) Delete(ctx context.Context, req *raftkvv1.DeleteRequest) (*raftkvv1.DeleteResponse, error) {
	err := s.store.Propose(ctx, statemachine.Command{
		ClientID: req.GetClient().GetClientId(),
		Seq:      req.GetClient().GetSequence(),
		Op:       statemachine.OpDelete,
		Key:      req.GetKey(),
	})
	if err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.DeleteResponse{}, nil
}

// Status implements the key-value service.
//
// It answers on any node, leader or not. That is the point: a client that has
// been redirected needs somewhere to ask, and an operator needs to see a node
// that is unhealthy precisely because it is not participating.
func (s *KVServer) Status(ctx context.Context, _ *raftkvv1.StatusRequest) (*raftkvv1.StatusResponse, error) {
	st := s.store.Status()

	return &raftkvv1.StatusResponse{
		NodeId:        uint64(st.ID),
		LeaderId:      uint64(st.Leader),
		LeaderAddress: s.addressOf(st.Leader),
		Term:          uint64(st.Term),
		State:         stateToWire(st.State),
		CommitIndex:   uint64(st.Commit),
		AppliedIndex:  uint64(st.Applied),
	}, nil
}

// AddNode implements the cluster service.
func (s *KVServer) AddNode(ctx context.Context, req *raftkvv1.AddNodeRequest) (*raftkvv1.AddNodeResponse, error) {
	if req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "node ID must not be zero")
	}
	if req.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "a new member needs an address")
	}

	if err := s.store.AddNode(ctx, raft.NodeID(req.GetNodeId()), req.GetAddress()); err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.AddNodeResponse{}, nil
}

// RemoveNode implements the cluster service.
func (s *KVServer) RemoveNode(ctx context.Context, req *raftkvv1.RemoveNodeRequest) (*raftkvv1.RemoveNodeResponse, error) {
	if req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "node ID must not be zero")
	}

	if err := s.store.RemoveNode(ctx, raft.NodeID(req.GetNodeId())); err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.RemoveNodeResponse{}, nil
}

// ListMembers implements the cluster service.
//
// Like Status it answers on any node. Membership is derived from the log, so
// every node that has caught up reports the same thing, and one that has not
// is worth being able to see.
func (s *KVServer) ListMembers(ctx context.Context, _ *raftkvv1.ListMembersRequest) (*raftkvv1.ListMembersResponse, error) {
	st := s.store.Status()
	conf := st.Members

	// During a transition the incoming configuration is the one being moved
	// to, and it is the more useful answer: it is what the cluster will be.
	ids := conf.Voters
	if conf.Joint {
		ids = conf.Incoming
	}

	members := make([]*raftkvv1.Member, 0, len(ids))
	for _, id := range ids {
		members = append(members, &raftkvv1.Member{
			Id:      uint64(id),
			Address: s.addressOf(id),
		})
	}

	return &raftkvv1.ListMembersResponse{Members: members, Joint: conf.Joint}, nil
}

// The server must satisfy both generated interfaces. Asserting it here means a
// protocol change is a compile error rather than a runtime surprise.
var (
	_ raftkvv1.KVServiceServer      = (*KVServer)(nil)
	_ raftkvv1.ClusterServiceServer = (*KVServer)(nil)
	_ Store                         = (*node.Node)(nil)
)
