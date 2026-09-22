package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

// Membership over the real transport.
//
// The deterministic tests in internal/raft prove that joint consensus reaches
// the right configuration, and the chaos suite drives membership changes hard.
// Neither one exercises this layer: the chaos network routes by node ID and
// every node it was built with is reachable forever, so a member added at
// runtime is reachable there by construction. Over gRPC it is not. Nothing
// reaches a node whose address the sender never learned, and the address
// arrives in the configuration change rather than in the static peer list.

// joinNode starts a node that is not yet a member and returns it.
//
// The listener is bound before the caller proposes the change, because a
// member that cannot answer counts toward every majority while contributing
// nothing.
func (c *grpcCluster) joinNode(id raft.NodeID) *node.Node {
	c.t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("listening for node %d: %v", id, err)
	}
	c.addrs[id] = l.Addr().String()

	// The joining node is told the membership it is joining. Its own
	// transport therefore knows where everyone is; the question this test
	// asks is whether the nodes already running learn where it is.
	members := append(append([]raft.NodeID{}, c.ids...), id)

	tr, err := NewPeerTransport(PeerConfig{Self: id, Addresses: c.addrs})
	if err != nil {
		c.t.Fatalf("creating transport for node %d: %v", id, err)
	}
	c.transports[id] = tr
	c.t.Cleanup(func() { tr.Close() })

	c.dataDirs[id] = filepath.Join(c.t.TempDir(), "joiner")
	n, err := node.Start(node.Config{
		ID:            id,
		Peers:         members,
		DataDir:       c.dataDirs[id],
		Transport:     tr,
		TickInterval:  10 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
	})
	if err != nil {
		c.t.Fatalf("starting node %d: %v", id, err)
	}
	c.nodes[id] = n
	c.t.Cleanup(func() { n.Stop() })
	tr.SetLocal(n)

	srv, err := NewRaftServer(n)
	if err != nil {
		c.t.Fatalf("creating server for node %d: %v", id, err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	c.servers[id] = gs
	go gs.Serve(l)
	c.t.Cleanup(gs.Stop)

	c.ids = append(c.ids, id)
	return n
}

func TestANodeAddedAtRuntimeReceivesTheLog(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	// Something written before the change, so catching up requires the new
	// member to receive history rather than just the entries that follow.
	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "before", Value: []byte("joined"),
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the added node to catch up with the leader", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	// Applied parity is the check available from here: a follower cannot
	// serve a read, and the node exposes no local one. It is still a real
	// check, because the joiner starts at zero and the only way its applied
	// index reaches the leader's is by receiving the log, "before" included.

	// It must also have learned the configuration it joined, not just the
	// entries. A node that replicates the log but believes in the old
	// membership would vote on the wrong quorum.
	c.eventually("the added node to agree about the membership", func() bool {
		return joiner.Status().Members.String() == leader.Status().Members.String()
	})

	// And the log must have got there over the network rather than by some
	// route. Without a link the leader is not replicating to it at all.
	var toJoiner uint64
	for _, st := range c.transports[leader.Status().ID].Stats() {
		if st.ID == 4 {
			toJoiner = st.Sent
		}
	}
	if toJoiner == 0 {
		t.Fatalf("the leader sent nothing to the added node\n%s", c.dump())
	}
}

func TestASingleNodeClusterCanGrow(t *testing.T) {
	// The case that decides when the transport has to be reconfigured. Going
	// from one node to two produces a joint configuration needing a majority
	// of {1} and a majority of {1,2}, so the entry admitting node 2 cannot
	// commit until node 2 replies. A transport that only learned about new
	// members once their entry committed would wait forever for a reply from
	// a node it had no way to contact.
	c := newGRPCCluster(t, 1)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	joiner := c.joinNode(2)
	if err := leader.AddNode(ctx, 2, c.addrs[2]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the second node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	// The cluster must still be able to commit, now needing both nodes.
	if err := leader.Propose(ctx, statemachine.Command{
		ClientID: 1, Seq: 1, Op: statemachine.OpPut, Key: "grown", Value: []byte("two"),
	}); err != nil {
		t.Fatalf("Propose after growing: %v", err)
	}
}

func TestARemovedNodeLosesItsLink(t *testing.T) {
	// A member admitted at runtime and then removed should not leave the
	// leader holding a connection to it.
	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tr := c.transports[leader.Status().ID]
	c.eventually("the leader to hold a link to the added node", func() bool {
		for _, st := range tr.Stats() {
			if st.ID == 4 {
				return true
			}
		}
		return false
	})

	if err := leader.RemoveNode(ctx, 4); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	c.eventually("the leader to drop the link to the removed node", func() bool {
		for _, st := range tr.Stats() {
			if st.ID == 4 {
				return false
			}
		}
		return true
	})
}

func TestAMemberAddedAfterCompactionCatchesUpBySnapshot(t *testing.T) {
	// The operator flow the deployment guide describes: compact before
	// admitting a member, so it does not have to replay everything ever
	// written. That makes the log useless for catching it up. A node joining
	// a cluster starts at index zero, which is below any compaction point, so
	// the only thing that can bring it up to date is an image over gRPC.
	//
	// Snapshot transfer is already covered for a member that restarts. This
	// is the other shape: a member that has never held anything at all, and
	// whose address the leader only learns from the change that admits it.
	const (
		writes    = 32
		valueSize = 4 << 10
	)

	c := newGRPCCluster(t, 3)
	leader := c.awaitLeader()

	ctx, cancel := context.WithTimeout(context.Background(), grpcSettleTimeout)
	defer cancel()

	value := bytes.Repeat([]byte("x"), valueSize)
	for i := range writes {
		err := leader.Propose(ctx, statemachine.Command{
			ClientID: 1, Seq: uint64(i + 1), Op: statemachine.OpPut,
			Key: fmt.Sprintf("key-%d", i), Value: value,
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Every node compacts, not just the one leading now. Leadership can move,
	// and a leader that still holds the entries would catch the new member up
	// from the log, which is the path this test exists to avoid.
	for _, id := range c.ids {
		if err := c.nodes[id].Compact(ctx); err != nil {
			t.Fatalf("compacting node %d: %v", id, err)
		}
	}

	joiner := c.joinNode(4)
	if err := leader.AddNode(ctx, 4, c.addrs[4]); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	c.eventually("the added node to catch up", func() bool {
		return joiner.Status().Applied >= leader.Status().Applied
	})

	// Without this the test would pass on ordinary replication and say
	// nothing about the path it is named for.
	if got := joiner.Status().SnapshotsReceived; got == 0 {
		t.Fatalf("the added node caught up without receiving a snapshot\n%s", c.dump())
	}
}
