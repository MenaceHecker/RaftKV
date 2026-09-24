package transport

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MenaceHecker/raftkv/internal/raft"
	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func clientCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), grpcSettleTimeout)
}

func notLeaderDetail(t *testing.T, err error) *raftkvv1.NotLeader {
	t.Helper()

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition: %v", st.Code(), err)
	}

	for _, d := range st.Details() {
		if nl, ok := d.(*raftkvv1.NotLeader); ok {
			return nl
		}
	}
	t.Fatalf("no NotLeader detail attached to %v; a client could not redirect itself", err)
	return nil
}

func TestClientWriteAndReadThroughTheAPI(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := clientCtx(t)
	defer cancel()

	kv := c.kv(leaderID)

	if _, err := kv.Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "x", Value: []byte("hello"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || string(got.GetValue()) != "hello" {
		t.Fatalf("x = %q (found %v), want hello", got.GetValue(), got.GetFound())
	}
}

func TestMissingKeyIsFoundFalseNotAnError(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := clientCtx(t)
	defer cancel()

	got, err := c.kv(leader.Status().ID).Get(ctx, &raftkvv1.GetRequest{Key: "absent"})
	if err != nil {
		t.Fatalf("Get on a missing key returned an error: %v", err)
	}
	if got.GetFound() {
		t.Fatal("a key that was never written reports as found")
	}
}

func TestEmptyValueIsDistinctFromMissing(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := clientCtx(t)
	defer cancel()

	kv := c.kv(leader.Status().ID)
	if _, err := kv.Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "empty", Value: []byte{},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: "empty"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("a key with an empty value reports as missing")
	}
	if len(got.GetValue()) != 0 {
		t.Fatalf("value = %q, want empty", got.GetValue())
	}
}

func TestFollowerRedirectsWritesAndReads(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("the follower to learn who leads", func() bool {
		return c.nodes[c.followerOf(leaderID)].Status().Leader == leaderID
	})

	ctx, cancel := clientCtx(t)
	defer cancel()

	follower := c.followerOf(leaderID)
	kv := c.kv(follower)

	_, err := kv.Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "x", Value: []byte("v"),
	})
	detail := notLeaderDetail(t, err)
	if raft.NodeID(detail.GetLeaderId()) != leaderID {
		t.Fatalf("redirect names node %d, want %d", detail.GetLeaderId(), leaderID)
	}
	if detail.GetLeaderAddress() != c.addrs[leaderID] {
		t.Fatalf("redirect address = %q, want %q", detail.GetLeaderAddress(), c.addrs[leaderID])
	}

	_, err = kv.Get(ctx, &raftkvv1.GetRequest{Key: "x"})
	detail = notLeaderDetail(t, err)
	if raft.NodeID(detail.GetLeaderId()) != leaderID {
		t.Fatalf("read redirect names node %d, want %d", detail.GetLeaderId(), leaderID)
	}
}

func TestARedirectIsEnoughToFindTheLeader(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("the follower to learn who leads", func() bool {
		return c.nodes[c.followerOf(leaderID)].Status().Leader == leaderID
	})

	ctx, cancel := clientCtx(t)
	defer cancel()

	_, err := c.kv(c.followerOf(leaderID)).Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "x", Value: []byte("v"),
	})
	detail := notLeaderDetail(t, err)

	var target raft.NodeID
	for id, addr := range c.addrs {
		if addr == detail.GetLeaderAddress() {
			target = id
		}
	}
	if target == 0 {
		t.Fatalf("the redirect address %q matches no node", detail.GetLeaderAddress())
	}

	if _, err := c.kv(target).Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "x", Value: []byte("v"),
	}); err != nil {
		t.Fatalf("following the redirect still failed: %v", err)
	}
}

func TestRedirectUsesFailedPreconditionNotUnavailable(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("the follower to learn who leads", func() bool {
		return c.nodes[c.followerOf(leaderID)].Status().Leader == leaderID
	})

	ctx, cancel := clientCtx(t)
	defer cancel()

	_, err := c.kv(c.followerOf(leaderID)).Get(ctx, &raftkvv1.GetRequest{Key: "x"})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition; Unavailable would make gRPC "+
			"retry the same node forever", got)
	}
}

func TestStatusAnswersOnAnyNode(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("every node to agree who leads", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Status().Leader != leaderID {
				return false
			}
		}
		return true
	})

	ctx, cancel := clientCtx(t)
	defer cancel()

	for _, id := range c.ids {
		st, err := c.kv(id).Status(ctx, &raftkvv1.StatusRequest{})
		if err != nil {
			t.Fatalf("Status on node %d: %v", id, err)
		}
		if raft.NodeID(st.GetNodeId()) != id {
			t.Fatalf("node %d reports itself as %d", id, st.GetNodeId())
		}
		if raft.NodeID(st.GetLeaderId()) != leaderID {
			t.Fatalf("node %d names leader %d, want %d", id, st.GetLeaderId(), leaderID)
		}
		if st.GetLeaderAddress() != c.addrs[leaderID] {
			t.Fatalf("node %d gives leader address %q, want %q",
				id, st.GetLeaderAddress(), c.addrs[leaderID])
		}

		wantState := raftkvv1.NodeState_NODE_STATE_FOLLOWER
		if id == leaderID {
			wantState = raftkvv1.NodeState_NODE_STATE_LEADER
		}
		if st.GetState() != wantState {
			t.Fatalf("node %d state = %s, want %s", id, st.GetState(), wantState)
		}
	}
}

func TestRetriedWriteIsDeduplicatedThroughTheAPI(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := clientCtx(t)
	defer cancel()

	kv := c.kv(leader.Status().ID)
	put := func(client, seq uint64, key, value string) {
		t.Helper()
		if _, err := kv.Put(ctx, &raftkvv1.PutRequest{
			Client: &raftkvv1.ClientRequest{ClientId: client, Sequence: seq},
			Key:    key, Value: []byte(value),
		}); err != nil {
			t.Fatalf("Put client %d seq %d: %v", client, seq, err)
		}
	}
	read := func(key string) string {
		t.Helper()
		got, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		return string(got.GetValue())
	}

	put(7, 1, "x", "first")
	put(7, 2, "x", "second")
	put(7, 1, "x", "first")

	if got := read("x"); got != "second" {
		t.Fatalf("x = %q after a stale retry, want second", got)
	}

	put(7, 3, "y", "mine")
	put(8, 1, "y", "somebody else")
	put(7, 3, "y", "mine")

	if got := read("y"); got != "somebody else" {
		t.Fatalf("y = %q after a client resent its latest write, want somebody else", got)
	}
}

func TestDeleteThroughTheAPI(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := clientCtx(t)
	defer cancel()

	kv := c.kv(leader.Status().ID)
	if _, err := kv.Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "k", Value: []byte("v"),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := kv.Delete(ctx, &raftkvv1.DeleteRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 2},
		Key:    "k",
	}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetFound() {
		t.Fatal("the key is still present after a delete")
	}

	if _, err := kv.Delete(ctx, &raftkvv1.DeleteRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 3},
		Key:    "k",
	}); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
}

func TestListMembersReportsTheCluster(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := clientCtx(t)
	defer cancel()

	for _, id := range c.ids {
		got, err := c.cluster(id).ListMembers(ctx, &raftkvv1.ListMembersRequest{})
		if err != nil {
			t.Fatalf("ListMembers on node %d: %v", id, err)
		}
		if len(got.GetMembers()) != 3 {
			t.Fatalf("node %d lists %d members, want 3", id, len(got.GetMembers()))
		}
		if got.GetJoint() {
			t.Fatalf("node %d reports a membership change in progress", id)
		}
		for _, m := range got.GetMembers() {
			if m.GetAddress() != c.addrs[raft.NodeID(m.GetId())] {
				t.Fatalf("member %d has address %q, want %q",
					m.GetId(), m.GetAddress(), c.addrs[raft.NodeID(m.GetId())])
			}
		}
	}
	_ = leader
}

func TestAddAndRemoveNodeThroughTheAPI(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := clientCtx(t)
	defer cancel()

	admin := c.cluster(leaderID)

	if _, err := admin.AddNode(ctx, &raftkvv1.AddNodeRequest{
		NodeId: 4, Address: "127.0.0.1:9004",
	}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the new member to appear in the membership", func() bool {
		got, err := admin.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
		return err == nil && len(got.GetMembers()) == 4 && !got.GetJoint()
	})

	members, err := admin.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	var found bool
	for _, m := range members.GetMembers() {
		if m.GetId() == 4 {
			found = true
			if m.GetAddress() != "127.0.0.1:9004" {
				t.Fatalf("the new member's address = %q, want 127.0.0.1:9004", m.GetAddress())
			}
		}
	}
	if !found {
		t.Fatalf("node 4 is missing from the membership: %v", members.GetMembers())
	}

	if _, err := admin.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 4}); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	c.eventually("the member to be removed", func() bool {
		got, err := admin.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
		return err == nil && len(got.GetMembers()) == 3 && !got.GetJoint()
	})
}

func TestMembershipChangesRedirectToTheLeader(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.eventually("the follower to learn who leads", func() bool {
		return c.nodes[c.followerOf(leaderID)].Status().Leader == leaderID
	})

	ctx, cancel := clientCtx(t)
	defer cancel()

	admin := c.cluster(c.followerOf(leaderID))

	_, err := admin.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 4, Address: "127.0.0.1:9004"})
	if d := notLeaderDetail(t, err); raft.NodeID(d.GetLeaderId()) != leaderID {
		t.Fatalf("AddNode redirect names %d, want %d", d.GetLeaderId(), leaderID)
	}

	_, err = admin.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 2})
	if d := notLeaderDetail(t, err); raft.NodeID(d.GetLeaderId()) != leaderID {
		t.Fatalf("RemoveNode redirect names %d, want %d", d.GetLeaderId(), leaderID)
	}
}

func TestInvalidMembershipRequestsAreRejected(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	ctx, cancel := clientCtx(t)
	defer cancel()

	admin := c.cluster(leaderID)

	cases := map[string]func() error{
		"zero node ID": func() error {
			_, err := admin.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 0, Address: "a:1"})
			return err
		},
		"no address": func() error {
			_, err := admin.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 9})
			return err
		},
		"already a member": func() error {
			_, err := admin.AddNode(ctx, &raftkvv1.AddNodeRequest{NodeId: 2, Address: "a:2"})
			return err
		},
		"not a member": func() error {
			_, err := admin.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 99})
			return err
		},
		"zero removal": func() error {
			_, err := admin.RemoveNode(ctx, &raftkvv1.RemoveNodeRequest{NodeId: 0})
			return err
		},
	}

	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatalf("%s was accepted", name)
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("%s gave code %s, want InvalidArgument", name, got)
			}
		})
	}
}

func TestNewKVServerRequiresAStore(t *testing.T) {
	if _, err := NewKVServer(nil, nil); err == nil {
		t.Fatal("a server was created with no store")
	}
}

func TestClientCallsRespectTheirDeadline(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()
	leaderID := leader.Status().ID

	c.servers[c.followerOf(leaderID)].Stop()
	for _, id := range c.ids {
		if id != leaderID && id != c.followerOf(leaderID) {
			c.servers[id].Stop()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.kv(leaderID).Put(ctx, &raftkvv1.PutRequest{
		Client: &raftkvv1.ClientRequest{ClientId: 1, Sequence: 1},
		Key:    "x", Value: []byte("v"),
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a write committed with no majority reachable")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the call took %v to respect a 300ms deadline", elapsed)
	}
}
