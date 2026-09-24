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

type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Propose(ctx context.Context, cmd statemachine.Command) error
	AddNode(ctx context.Context, id raft.NodeID, addr string) error
	RemoveNode(ctx context.Context, id raft.NodeID) error
	Status() node.Status
}

type KVServer struct {
	raftkvv1.UnimplementedKVServiceServer
	raftkvv1.UnimplementedClusterServiceServer

	store Store

	staticAddrs map[raft.NodeID]string
}

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

func (s *KVServer) Register(srv grpc.ServiceRegistrar) {
	raftkvv1.RegisterKVServiceServer(srv, s)
	raftkvv1.RegisterClusterServiceServer(srv, s)
}

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

	st2 := status.New(codes.FailedPrecondition, msg)
	withDetail, err := st2.WithDetails(detail)
	if err != nil {
		return st2.Err()
	}
	return withDetail.Err()
}

func (s *KVServer) translate(err error) error {
	switch {
	case err == nil:
		return nil

	case errors.Is(err, node.ErrNotLeader):
		return s.notLeaderError()

	case errors.Is(err, node.ErrLostLeadership):
		return status.Error(codes.Aborted,
			"leadership changed before the request committed; retry")

	case errors.Is(err, node.ErrStopped):
		return status.Error(codes.Unavailable, "node is not serving")

	case errors.Is(err, raft.ErrStorage):
		return status.Error(codes.Unavailable, "node cannot persist writes")

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

func (s *KVServer) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	value, found, err := s.store.Get(ctx, req.GetKey())
	if err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.GetResponse{Value: value, Found: found}, nil
}

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

func (s *KVServer) RemoveNode(ctx context.Context, req *raftkvv1.RemoveNodeRequest) (*raftkvv1.RemoveNodeResponse, error) {
	if req.GetNodeId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "node ID must not be zero")
	}

	if err := s.store.RemoveNode(ctx, raft.NodeID(req.GetNodeId())); err != nil {
		return nil, s.translate(err)
	}
	return &raftkvv1.RemoveNodeResponse{}, nil
}

func (s *KVServer) ListMembers(ctx context.Context, _ *raftkvv1.ListMembersRequest) (*raftkvv1.ListMembersResponse, error) {
	st := s.store.Status()
	conf := st.Members

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

var (
	_ raftkvv1.KVServiceServer      = (*KVServer)(nil)
	_ raftkvv1.ClusterServiceServer = (*KVServer)(nil)
	_ Store                         = (*node.Node)(nil)
)
