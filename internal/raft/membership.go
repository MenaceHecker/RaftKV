package raft

import (
	"errors"
	"fmt"
	"sort"
)

type config struct {
	voters   map[NodeID]struct{}
	incoming map[NodeID]struct{}
	joint    bool
	addrs    map[NodeID]string
}

func newConfig(peers []NodeID) config {
	v := make(map[NodeID]struct{}, len(peers))
	for _, id := range peers {
		v[id] = struct{}{}
	}
	return config{voters: v, addrs: make(map[NodeID]string)}
}

func (c *config) inJoint() bool { return c.joint }

func (c *config) hasVoter(id NodeID) bool {
	if _, ok := c.voters[id]; ok {
		return true
	}
	_, ok := c.incoming[id]
	return ok
}

func (c *config) members() []NodeID {
	seen := make(map[NodeID]struct{}, len(c.voters)+len(c.incoming))
	for id := range c.voters {
		seen[id] = struct{}{}
	}
	for id := range c.incoming {
		seen[id] = struct{}{}
	}
	ids := make([]NodeID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

var (
	ErrConfChangeInFlight = errors.New("raft: a configuration change is already in progress")

	ErrNotInJoint = errors.New("raft: no configuration change is in progress")

	ErrEmptyConfiguration = errors.New("raft: configuration change would leave no voters")

	ErrNoChange = errors.New("raft: configuration change has no effect")
)

func (c config) enterJoint(cc ConfChange) (config, error) {
	if c.inJoint() {
		return config{}, ErrConfChangeInFlight
	}

	next := config{
		voters:   copySet(c.voters),
		incoming: copySet(c.voters),
		addrs:    copyAddrs(c.addrs),
		joint:    true,
	}

	switch cc.Type {
	case ConfChangeAddNode:
		if cc.NodeID == None {
			return config{}, fmt.Errorf("raft: cannot add the zero node ID")
		}
		if _, exists := next.incoming[cc.NodeID]; exists {
			return config{}, fmt.Errorf("%w: node %d is already a voter", ErrNoChange, cc.NodeID)
		}
		next.incoming[cc.NodeID] = struct{}{}
		if cc.Addr != "" {
			next.addrs[cc.NodeID] = cc.Addr
		}

	case ConfChangeRemoveNode:
		if _, exists := next.incoming[cc.NodeID]; !exists {
			return config{}, fmt.Errorf("%w: node %d is not a voter", ErrNoChange, cc.NodeID)
		}
		delete(next.incoming, cc.NodeID)
		if len(next.incoming) == 0 {
			return config{}, ErrEmptyConfiguration
		}

	case ConfChangeLeaveJoint:
		return config{}, fmt.Errorf("raft: leave-joint is not a membership change")

	default:
		return config{}, fmt.Errorf("raft: unknown configuration change type %d", cc.Type)
	}

	return next, nil
}

func (c config) leaveJoint() (config, error) {
	if !c.inJoint() {
		return config{}, ErrNotInJoint
	}
	return config{
		voters: copySet(c.incoming),
		addrs:  copyAddrs(c.addrs),
	}, nil
}

func (c *config) commitReady(idx Index, matchFor func(NodeID) Index) bool {
	if !c.inJoint() {
		return majorityHas(c.voters, idx, matchFor)
	}
	return majorityHas(c.voters, idx, matchFor) &&
		majorityHas(c.incoming, idx, matchFor)
}

func (c *config) voteGranted(votes map[NodeID]bool) bool {
	if !c.inJoint() {
		return majorityGranted(c.voters, votes)
	}
	return majorityGranted(c.voters, votes) &&
		majorityGranted(c.incoming, votes)
}

func (c *config) voteLost(votes map[NodeID]bool) bool {
	if !c.inJoint() {
		return majorityDenied(c.voters, votes)
	}
	return majorityDenied(c.voters, votes) ||
		majorityDenied(c.incoming, votes)
}

func majorityHas(voters map[NodeID]struct{}, idx Index, matchFor func(NodeID) Index) bool {
	if len(voters) == 0 {
		return false
	}
	need := len(voters)/2 + 1
	have := 0
	for id := range voters {
		if matchFor(id) >= idx {
			have++
		}
	}
	return have >= need
}

func majorityGranted(voters map[NodeID]struct{}, votes map[NodeID]bool) bool {
	if len(voters) == 0 {
		return false
	}
	need := len(voters)/2 + 1
	have := 0
	for id := range voters {
		if votes[id] {
			have++
		}
	}
	return have >= need
}

func majorityDenied(voters map[NodeID]struct{}, votes map[NodeID]bool) bool {
	need := len(voters)/2 + 1
	possible := 0
	for id := range voters {
		v, seen := votes[id]
		if !seen || v {
			possible++
		}
	}
	return possible < need
}

func copySet(s map[NodeID]struct{}) map[NodeID]struct{} {
	out := make(map[NodeID]struct{}, len(s))
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}

func copyAddrs(a map[NodeID]string) map[NodeID]string {
	out := make(map[NodeID]string, len(a))
	for k, v := range a {
		out[k] = v
	}
	return out
}
