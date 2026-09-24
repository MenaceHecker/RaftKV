package raft

import (
	"fmt"
	"sort"
)

type ConfState struct {
	Voters []NodeID

	Incoming []NodeID

	Joint bool

	Addrs map[NodeID]string
}

func (cs ConfState) IsEmpty() bool { return len(cs.Voters) == 0 }

func (cs ConfState) String() string {
	if !cs.Joint {
		return fmt.Sprintf("voters=%v", cs.Voters)
	}
	return fmt.Sprintf("joint old=%v new=%v", cs.Voters, cs.Incoming)
}

func (cs ConfState) Members() []NodeID {
	seen := make(map[NodeID]struct{}, len(cs.Voters)+len(cs.Incoming))
	for _, id := range cs.Voters {
		seen[id] = struct{}{}
	}
	for _, id := range cs.Incoming {
		seen[id] = struct{}{}
	}
	return sortedNodeIDs(seen)
}

func (n *Node) ConfState() ConfState { return n.conf.toState() }

func (n *Node) ConfSeq() uint64 { return n.confSeq }

func (c config) toState() ConfState {
	cs := ConfState{
		Voters: sortedNodeIDs(c.voters),
		Joint:  c.joint,
	}
	if c.joint {
		cs.Incoming = sortedNodeIDs(c.incoming)
	}
	if len(c.addrs) > 0 {
		cs.Addrs = copyAddrs(c.addrs)
	}
	return cs
}

func configFromState(cs ConfState) config {
	c := config{
		voters: make(map[NodeID]struct{}, len(cs.Voters)),
		addrs:  make(map[NodeID]string, len(cs.Addrs)),
		joint:  cs.Joint,
	}
	for _, id := range cs.Voters {
		c.voters[id] = struct{}{}
	}
	if cs.Joint {
		c.incoming = make(map[NodeID]struct{}, len(cs.Incoming))
		for _, id := range cs.Incoming {
			c.incoming[id] = struct{}{}
		}
	}
	for id, addr := range cs.Addrs {
		c.addrs[id] = addr
	}
	return c
}

func sortedNodeIDs(s map[NodeID]struct{}) []NodeID {
	out := make([]NodeID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
