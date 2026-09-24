package transport

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
)

// The status code is a client-facing contract, not a label. gRPC clients act
// on it without reading the message: Unavailable is retried, usually against
// another address; Aborted is retried against the same one; InvalidArgument
// and Internal are not retried at all, and a library will hand them straight
// back to the caller as a failure.
//
// Getting one wrong turns a condition the cluster recovers from on its own
// into an error the application sees. The case that prompted this: a node
// that can no longer write now stops, and the write in its hands fails with
// a storage error on the way. That fell through to Internal, so a client
// gave up and reported a fault at the exact moment a new leader was about to
// be elected and a retry would have succeeded.

// stubStore is the least a KVServer needs. Only Status is ever called by the
// code under test here, to describe a leader when redirecting.
type stubStore struct{ status node.Status }

func (s stubStore) Get(context.Context, string) ([]byte, bool, error) { return nil, false, nil }
func (s stubStore) Propose(context.Context, statemachine.Command) error {
	return nil
}
func (s stubStore) AddNode(context.Context, raft.NodeID, string) error { return nil }
func (s stubStore) RemoveNode(context.Context, raft.NodeID) error      { return nil }
func (s stubStore) Status() node.Status                                { return s.status }

func newStubServer(t *testing.T) *KVServer {
	t.Helper()

	srv, err := NewKVServer(stubStore{status: node.Status{ID: 1, Leader: 2}},
		map[raft.NodeID]string{2: "127.0.0.1:9002"})
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	return srv
}

func TestEveryFailureGetsTheCodeItsHandlingNeeds(t *testing.T) {
	srv := newStubServer(t)

	cases := []struct {
		name string
		err  error
		want codes.Code
		why  string
	}{
		{
			"not the leader", node.ErrNotLeader, codes.FailedPrecondition,
			"the client is redirected to the leader named in the details",
		},
		{
			"leadership changed mid-write", node.ErrLostLeadership, codes.Aborted,
			"retrying is safe because the command carries a sequence number",
		},
		{
			"the node has stopped", node.ErrStopped, codes.Unavailable,
			"another node can serve",
		},
		{
			"the node cannot write", fmt.Errorf("proposing: %w", raft.ErrStorage),
			codes.Unavailable,
			"this node is on its way down and the next leader will take the retry",
		},
		{
			"the caller gave up", context.Canceled, codes.Canceled,
			"the client already stopped waiting",
		},
		{
			"the caller ran out of time", context.DeadlineExceeded, codes.DeadlineExceeded,
			"distinguishable from a refusal, because the write may still commit",
		},
		{
			"a change is already in flight", raft.ErrConfChangeInFlight,
			codes.FailedPrecondition,
			"waiting and retrying is the right response, not a different request",
		},
		{
			"the change asks for nothing", raft.ErrNoChange, codes.InvalidArgument,
			"retrying the same request would fail the same way",
		},
		{
			"something unrecognised", errors.New("a new failure nobody mapped"),
			codes.Internal,
			"an unmapped failure is a fault here, and saying so is better than guessing",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, ok := status.FromError(srv.translate(c.err))
			if !ok {
				t.Fatalf("translate(%v) did not produce a status", c.err)
			}
			if st.Code() != c.want {
				t.Errorf("translate(%v) = %v, want %v, because %s",
					c.err, st.Code(), c.want, c.why)
			}
		})
	}
}

func TestNoFailureTranslatesToSuccess(t *testing.T) {
	// A nil error must stay nil, or a successful call would be reported as a
	// failure with an empty message.
	if err := newStubServer(t).translate(nil); err != nil {
		t.Errorf("translate(nil) = %v, want nil", err)
	}
}

func TestAStorageFailureIsRetryableRatherThanAFault(t *testing.T) {
	// Stated on its own because it is the one the durability work changed,
	// and because Internal and Unavailable differ by whether the client ever
	// tries again.
	srv := newStubServer(t)

	err := srv.translate(fmt.Errorf("raft: appending entries: %w: %w",
		raft.ErrStorage, errors.New("the disk is gone")))

	st, _ := status.FromError(err)
	if st.Code() == codes.Internal {
		t.Fatal("a node that cannot write reports a fault, so the client gives up " +
			"instead of retrying against the node that takes over")
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("code = %v, want Unavailable", st.Code())
	}
}
