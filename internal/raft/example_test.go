package raft_test

import (
	"fmt"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// ExampleNode shows the contract the whole package is built around.
//
// A Node computes nothing on its own. The caller advances its clock with
// Tick, hands it messages with Step, and then collects everything that
// produced with Ready: messages to send, entries to apply, reads to answer.
// Advance says that work is done. Nothing here touches a clock, a socket or
// another goroutine, which is what lets a whole cluster run inside one test.
func ExampleNode() {
	n, err := raft.NewNode(raft.Config{
		ID:            1,
		Peers:         []raft.NodeID{1},
		Storage:       raft.NewMemoryStorage(),
		ElectionTick:  10,
		HeartbeatTick: 1,
	})
	if err != nil {
		panic(err)
	}

	// Time only passes when the caller says so. A follower that goes a whole
	// election timeout without hearing from a leader starts an election, and
	// the timeout is randomized, so this ticks past the longest it can be.
	for range 2 * 10 {
		if err := n.Tick(); err != nil {
			panic(err)
		}
	}
	fmt.Println("after its election timeout:", n.State())

	// A proposal is accepted by the leader and becomes a log entry. This
	// node is the whole cluster, so it is its own majority and the entry
	// commits without anyone else being asked.
	if err := n.Propose([]byte("x=1")); err != nil {
		panic(err)
	}

	// Ready hands back everything that happened. Committed entries are the
	// caller's to apply: the core has no idea what the bytes mean.
	rd := n.Ready()
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raft.EntryNoOp:
			fmt.Println("apply: the no-op a new leader appends on election")
		default:
			fmt.Printf("apply: %s\n", e.Data)
		}
	}

	// Advance tells the node the caller is done with that batch, so the
	// entries are not handed back again.
	n.Advance(rd)

	fmt.Println("nothing left to do:", n.Ready().IsEmpty())

	// Output:
	// after its election timeout: Leader
	// apply: the no-op a new leader appends on election
	// apply: x=1
	// nothing left to do: true
}

// ExampleNode_messages shows the other half of the contract: the caller is
// the network.
//
// A node never sends anything. It puts messages in Ready and expects the
// caller to deliver them, and it learns what happened only when the caller
// Steps a reply back in. Losing, delaying, duplicating or reordering those
// messages is therefore entirely up to the caller, which is how the chaos
// suite injects faults without any of that machinery living in the core.
func ExampleNode_messages() {
	newNode := func(id raft.NodeID) *raft.Node {
		n, err := raft.NewNode(raft.Config{
			ID:            id,
			Peers:         []raft.NodeID{1, 2, 3},
			Storage:       raft.NewMemoryStorage(),
			ElectionTick:  10,
			HeartbeatTick: 1,
		})
		if err != nil {
			panic(err)
		}
		return n
	}
	nodes := map[raft.NodeID]*raft.Node{1: newNode(1), 2: newNode(2), 3: newNode(3)}

	// Nudge node 1 into campaigning rather than waiting out a timeout.
	if err := nodes[1].Step(raft.Message{Type: raft.MsgCampaign}); err != nil {
		panic(err)
	}

	// Carry messages until the network goes quiet. A real caller would put
	// them on a socket; this one is the socket.
	for round := 0; round < 4; round++ {
		var inFlight []raft.Message
		for _, id := range []raft.NodeID{1, 2, 3} {
			rd := nodes[id].Ready()
			inFlight = append(inFlight, rd.Messages...)
			nodes[id].Advance(rd)
		}
		if len(inFlight) == 0 {
			break
		}
		for _, m := range inFlight {
			if err := nodes[m.To].Step(m); err != nil {
				panic(err)
			}
		}
	}

	fmt.Println("node 1:", nodes[1].State())
	fmt.Println("node 2:", nodes[2].State())
	fmt.Println("node 3:", nodes[3].State())

	// Output:
	// node 1: Leader
	// node 2: Follower
	// node 3: Follower
}
