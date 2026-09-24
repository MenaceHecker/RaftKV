package raft_test

import (
	"fmt"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

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

	for range 2 * 10 {
		if err := n.Tick(); err != nil {
			panic(err)
		}
	}
	fmt.Println("after its election timeout:", n.State())

	if err := n.Propose([]byte("x=1")); err != nil {
		panic(err)
	}

	rd := n.Ready()
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raft.EntryNoOp:
			fmt.Println("apply: the no-op a new leader appends on election")
		default:
			fmt.Printf("apply: %s\n", e.Data)
		}
	}

	n.Advance(rd)

	fmt.Println("nothing left to do:", n.Ready().IsEmpty())

	// Output:
	// after its election timeout: Leader
	// apply: the no-op a new leader appends on election
	// apply: x=1
	// nothing left to do: true
}

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

	if err := nodes[1].Step(raft.Message{Type: raft.MsgCampaign}); err != nil {
		panic(err)
	}

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
