package raft

import (
	"fmt"
	"sort"
)

// ConfState is the cluster configuration in a form that can leave the package.
//
// The internal config is a set of maps, which suits membership arithmetic and
// suits nothing else: maps have no stable order, so they cannot be encoded
// reproducibly, and unexported fields cannot cross a package boundary at all.
// ConfState is the shape that travels — into a snapshot, back out on restart,
// and eventually across the wire to a node being caught up.
//
// It is deliberately a plain value with exported fields. Anything that has to
// survive a restart should be inspectable without the package that produced it.
type ConfState struct {
	// Voters is the authoritative voter set, sorted. During a joint
	// transition it is the outgoing configuration.
	Voters []NodeID

	// Incoming is the target configuration during a joint transition, sorted.
	// It is nil when no transition is in progress.
	Incoming []NodeID

	// Joint reports whether a transition is in progress.
	//
	// It is recorded rather than inferred from Incoming, for the same reason
	// the internal config carries a flag: a shrinking transition can produce
	// a smaller incoming set, and at the limit an empty one, which would
	// otherwise read as "no transition" and fall back to a single majority.
	Joint bool

	// Addrs maps node IDs to network addresses, so a node restored from a
	// snapshot can reach members it was never statically configured with.
	Addrs map[NodeID]string
}

// IsEmpty reports whether the state describes no cluster at all, which is what
// a node that has never taken a snapshot reads back.
func (cs ConfState) IsEmpty() bool { return len(cs.Voters) == 0 }

// String renders the configuration for logs and failure messages.
func (cs ConfState) String() string {
	if !cs.Joint {
		return fmt.Sprintf("voters=%v", cs.Voters)
	}
	return fmt.Sprintf("joint old=%v new=%v", cs.Voters, cs.Incoming)
}

// Members returns every node in the configuration, sorted.
//
// During a joint transition that is the union of both sides. A leader has to
// be able to reach all of them: the outgoing majority and the incoming
// majority must each agree before the transition can complete, so a member of
// either one that cannot be contacted stalls the change.
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

// ConfState returns this node's current configuration.
func (n *Node) ConfState() ConfState { return n.conf.toState() }

// ConfSeq returns a counter that increases every time this node's
// configuration changes.
//
// A driver watches it to learn that the membership moved. It is a counter
// rather than a comparison of two ConfStates because the driver checks on
// every pass through its loop, and building a ConfState to diff would
// allocate two slices and a map each time. The value means nothing to any
// other node and nothing across a restart: it is only ever compared against
// one this same node reported earlier.
func (n *Node) ConfSeq() uint64 { return n.confSeq }

// toState converts the internal configuration into its portable form.
//
// Both sets are sorted. That is not cosmetic: the state is written into
// snapshots, and two nodes holding identical membership must produce identical
// bytes or there is no cheap way to tell convergence from divergence.
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

// configFromState rebuilds the internal configuration from its portable form.
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

// sortedNodeIDs returns a set's members in ascending order.
func sortedNodeIDs(s map[NodeID]struct{}) []NodeID {
	out := make([]NodeID, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
