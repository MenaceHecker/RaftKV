package transport

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

// clusterMutations are the ClusterService methods that change membership. They
// are the only methods --cluster-token-file protects; ListMembers and KVService
// stay open.
var clusterMutations = map[string]bool{
	"/" + raftkvv1.ClusterService_ServiceDesc.ServiceName + "/AddNode":    true,
	"/" + raftkvv1.ClusterService_ServiceDesc.ServiceName + "/RemoveNode": true,
}

// NewClusterTokenInterceptors returns interceptors for the shared listener that
// require "authorization: Bearer <token>" on AddNode and RemoveNode and nothing
// else. They reuse the admin listener's constant-time check, so a missing and a
// wrong token get the same response. The token is never logged.
func NewClusterTokenInterceptors(token string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	gate := newAuthGate(token, false)
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if clusterMutations[info.FullMethod] {
			if err := gate.check(ctx, info.FullMethod); err != nil {
				return nil, err
			}
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if clusterMutations[info.FullMethod] {
			if err := gate.check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
		}
		return h(srv, ss)
	}
	return unary, stream
}

// LoadClusterToken reads the token for --cluster-token-file with the same rules
// as LoadAdminToken: a regular file, not readable by group or others, holding a
// token. Errors name the path and the problem, never the contents.
func LoadClusterToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("transport: --cluster-token-file is empty")
	}
	return loadTokenFile(path, "cluster")
}
