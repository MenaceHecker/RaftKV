package raft

import "sort"

// config holds the current cluster membership as the Raft core sees it.
//
// Outside a joint-consensus transition the cluster has a single voter set
// (voters). During a transition it holds the old set in voters and the
// target set in incoming. A commit requires a majority of *both* sets, and an
// election requires a majority of *both* sets to grant. Once the transition
// completes (the leave-joint entry commits) incoming is cleared and voters
// becomes the new authoritative set.
//
// config is a value type: all mutating operations return a new config, leaving
// the receiver unchanged. That lets the Raft core speculatively compute the
// next config without touching the one currently in use.
type config struct {
	// voters is C_old outside a joint transition, C_new once it completes.
	voters map[NodeID]struct{}
	// incoming is C_new during a joint transition, nil otherwise.
	incoming map[NodeID]struct{}
	// addrs maps every known node ID to its network address. The transport
	// layer reads this when opening a connection to a newly added peer.
	addrs map[NodeID]string
}

// newConfig constructs the initial configuration from a static peer list.
// This is used at boot time before any membership changes have been proposed;
// there is no joint phase, so only voters is populated.
func newConfig(peers []NodeID) config {
	v := make(map[NodeID]struct{}, len(peers))
	for _, id := range peers {
		v[id] = struct{}{}
	}
	return config{voters: v, addrs: make(map[NodeID]string)}
}

// inJoint reports whether the cluster is currently in the joint phase of a
// membership transition.
func (c *config) inJoint() bool { return len(c.incoming) > 0 }

// hasVoter reports whether id is a voter in any currently active config. A
// node in only the incoming set counts: it participates in the double-majority
// check while in the joint phase.
func (c *config) hasVoter(id NodeID) bool {
	if _, ok := c.voters[id]; ok {
		return true
	}
	_, ok := c.incoming[id]
	return ok
}

// members returns every node ID that appears in any active voter set, sorted
// in ascending order. This is the full broadcast set: messages and progress
// entries must reach every member regardless of which config they belong to.
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

// enterJoint produces the joint configuration for a membership change.
// voters stays unchanged as C_old; incoming is derived by applying cc to a
// copy of voters to form C_new.
//
// Precondition: c must not already be in joint.
func (c config) enterJoint(cc ConfChange) config {
	next := config{
		voters:   copySet(c.voters),
		incoming: copySet(c.voters), // C_new starts as a copy of C_old
		addrs:    copyAddrs(c.addrs),
	}
	switch cc.Type {
	case ConfChangeAddNode:
		next.incoming[cc.NodeID] = struct{}{}
		if cc.Addr != "" {
			next.addrs[cc.NodeID] = cc.Addr
		}
	case ConfChangeRemoveNode:
		delete(next.incoming, cc.NodeID)
	}
	return next
}

// leaveJoint produces the final configuration after the leave-joint entry
// commits. incoming becomes the new authoritative voters; the joint phase ends.
func (c config) leaveJoint() config {
	return config{
		voters: copySet(c.incoming),
		addrs:  copyAddrs(c.addrs),
	}
}

// commitReady reports whether idx is safe to commit given the supplied match
// function. Outside joint consensus a simple majority of voters suffices.
// During joint consensus *both* majorities must be satisfied — any two
// quorums from the combined set must intersect, and the double-majority is what
// guarantees that (Raft §6).
func (c *config) commitReady(idx Index, matchFor func(NodeID) Index) bool {
	if !c.inJoint() {
		return majorityHas(c.voters, idx, matchFor)
	}
	return majorityHas(c.voters, idx, matchFor) &&
		majorityHas(c.incoming, idx, matchFor)
}

// voteGranted reports whether a vote tally is a win: a majority of every
// active voter set has granted. During a joint transition both majorities must
// grant before the election is decided.
func (c *config) voteGranted(votes map[NodeID]bool) bool {
	if !c.inJoint() {
		return majorityGranted(c.voters, votes)
	}
	return majorityGranted(c.voters, votes) &&
		majorityGranted(c.incoming, votes)
}

// voteLost reports whether the election is irreversibly lost: a majority in
// any active voter set is no longer achievable (enough no-votes have come in).
// The asymmetry with voteGranted is deliberate: winning requires both
// majorities; losing requires only one to be unachievable.
func (c *config) voteLost(votes map[NodeID]bool) bool {
	if !c.inJoint() {
		return majorityDenied(c.voters, votes)
	}
	// Either majority being unachievable ends the election.
	return majorityDenied(c.voters, votes) ||
		majorityDenied(c.incoming, votes)
}

// majorityHas reports whether a majority of the given voter set has a match
// index at or above idx.
func majorityHas(voters map[NodeID]struct{}, idx Index, matchFor func(NodeID) Index) bool {
	need := len(voters)/2 + 1
	have := 0
	for id := range voters {
		if matchFor(id) >= idx {
			have++
		}
	}
	return have >= need
}

// majorityGranted reports whether a majority of the given voter set has voted
// yes in votes.
func majorityGranted(voters map[NodeID]struct{}, votes map[NodeID]bool) bool {
	need := len(voters)/2 + 1
	have := 0
	for id := range voters {
		if votes[id] {
			have++
		}
	}
	return have >= need
}

// majorityDenied reports whether achieving a majority is no longer possible:
// the number of nodes that could still vote yes (those that have not yet
// refused) is less than the majority threshold.
func majorityDenied(voters map[NodeID]struct{}, votes map[NodeID]bool) bool {
	need := len(voters)/2 + 1
	possible := 0
	for id := range voters {
		v, seen := votes[id]
		if !seen || v {
			// Not yet voted, or already granted — could still contribute.
			possible++
		}
	}
	return possible < need
}

// copySet returns a deep copy of a node-ID set.
func copySet(s map[NodeID]struct{}) map[NodeID]struct{} {
	out := make(map[NodeID]struct{}, len(s))
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}

// copyAddrs returns a deep copy of an address map.
func copyAddrs(a map[NodeID]string) map[NodeID]string {
	out := make(map[NodeID]string, len(a))
	for k, v := range a {
		out[k] = v
	}
	return out
}
