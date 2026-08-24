package raft

import "fmt"

// Applying configuration changes from the log (§6).
//
// The rule that shapes this whole file is stated once in the paper and is easy
// to read past: a server uses the latest configuration *in its log*, whether or
// not that entry has committed. Waiting for the commit would be circular —
// deciding whether the entry is committed is itself a question about which
// configuration is in force — so the change takes effect the moment it is
// appended.
//
// The price is that a configuration can be un-made. An entry that was appended
// but never committed can be overwritten by a new leader, and a node that
// already adopted it has to go back. So the configuration is never edited in
// place: it is recomputed from a known base plus every conf-change entry the
// log currently holds. Rebuilding is the only operation that cannot get out of
// step with a log that just changed underneath it.

// ProposeConfChange asks the leader to change cluster membership.
//
// The change is appended as an ordinary log entry, which means it replicates,
// survives a crash, and is ordered against every other entry. It takes effect
// on this node immediately; the rest of the cluster adopts it as the entry
// reaches them.
//
// Only one change may be in flight. Overlapping changes could produce two
// configurations neither of which contains the other, and Raft's whole safety
// argument rests on any two quorums intersecting.
func (n *Node) ProposeConfChange(cc ConfChange) error {
	if n.state != Leader {
		return ErrNotLeader
	}

	// Leaving a transition is the one change that requires one to be open; a
	// membership change is the one that requires none. Treating them alike
	// would make it impossible to finish what was started.
	if cc.Type == ConfChangeLeaveJoint {
		if !n.conf.inJoint() {
			return ErrNotInJoint
		}
	} else if n.conf.inJoint() {
		return ErrConfChangeInFlight
	}

	// Validate against the current configuration before writing anything. A
	// rejected change should never reach the log, where every node would then
	// have to decide independently to ignore it.
	if _, err := applyConfChange(n.conf, cc); err != nil {
		return err
	}

	entry := Entry{
		Term:  n.term,
		Index: n.log.lastIndex() + 1,
		Type:  EntryConfChange,
		Data:  cc.Encode(),
	}
	if _, err := n.log.append([]Entry{entry}); err != nil {
		return err
	}

	// Adopt it now, before it commits. A leader that kept using the old
	// configuration while the new one sat in its log would decide commitment
	// by the wrong majority.
	if err := n.rebuildConfig(); err != nil {
		return err
	}

	n.progress[n.id].match = n.log.lastIndex()
	n.progress[n.id].next = n.log.lastIndex() + 1

	if n.isSoleVoter() {
		n.maybeCommit()
		return nil
	}
	n.broadcastAppend()
	return nil
}

// containsConfChange reports whether any entry changes the configuration,
// which is what tells the append path that a rebuild is needed.
func containsConfChange(entries []Entry) bool {
	for _, e := range entries {
		if e.Type == EntryConfChange {
			return true
		}
	}
	return false
}

// rebuildConfig recomputes the configuration from the base plus every
// conf-change entry currently in the log.
//
// Recomputing rather than editing in place is what makes reverting work. When
// a new leader overwrites uncommitted entries, some of which changed
// membership, there is no undo record to consult — but the log after the
// truncation is the whole truth, so deriving the configuration from it again
// lands on exactly the right answer with no bookkeeping.
func (n *Node) rebuildConfig() error {
	conf := n.baseConf.clone()
	jointAt := Index(0)

	first := n.log.firstIndex()
	last := n.log.lastIndex()
	if last < first {
		n.adoptConfig(conf)
		return nil
	}

	entries, err := n.log.entries(first, last+1)
	if err != nil {
		return fmt.Errorf("raft: rebuilding configuration: %w", err)
	}

	for _, e := range entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			return fmt.Errorf("raft: configuration change at index %d is unreadable: %w", e.Index, err)
		}

		next, err := applyConfChange(conf, cc)
		if err != nil {
			// A change that made sense when it was appended can be
			// meaningless by the time the log is replayed on another node —
			// but the log is the agreed history, so refusing it here would
			// leave this node's membership out of step with everyone else's.
			// Skipping keeps every replica deriving the same configuration
			// from the same entries.
			continue
		}
		conf = next

		// Remember which entry opened the transition currently in force. The
		// leader may not finish a transition until the entry that started it
		// has committed, and this is the only place that index is known
		// without scanning the log again.
		if conf.inJoint() {
			jointAt = e.Index
		} else {
			jointAt = 0
		}
	}

	n.jointEntryIndex = jointAt
	n.adoptConfig(conf)
	return nil
}

// maybeFinishConfChange completes a membership transition once the entry that
// began it has committed.
//
// Raft describes both halves of a change, but only the first is triggered by
// anyone: an operator asks to add or remove a node, and nobody asks to leave
// the joint configuration. If the leader did not propose that second entry
// itself, a cluster would sit in joint consensus indefinitely — still correct,
// since a double majority is stricter than a single one, but permanently
// unable to make any further membership change and needlessly harder to reach
// a quorum in.
//
// Waiting for the enter-joint entry to commit is what makes this safe. Until
// then the transition could still be undone by a new leader overwriting it,
// and finishing on top of an entry that might vanish would leave this node in
// a configuration no one else ever adopted.
func (n *Node) maybeFinishConfChange() error {
	if n.state != Leader || !n.conf.inJoint() {
		return nil
	}
	if n.jointEntryIndex == 0 || n.log.committed < n.jointEntryIndex {
		return nil
	}

	// A leader may not commit anything in a term until it has committed an
	// entry of its own (§5.4.2), and the entry finishing this transition would
	// be exactly such an entry — proposing it before the leader's no-op has
	// committed would append something that cannot yet be acted on.
	if !n.hasCommittedInCurrentTerm() {
		return nil
	}

	// Appending the leave-joint entry takes effect immediately, so the
	// configuration stops being joint and this will not fire again.
	return n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint})
}

// applyConfChange folds one change into a configuration.
func applyConfChange(c config, cc ConfChange) (config, error) {
	if cc.Type == ConfChangeLeaveJoint {
		return c.leaveJoint()
	}
	return c.enterJoint(cc)
}

// adoptConfig installs a configuration and brings the leader's replication
// state into line with it.
//
// A newly added member needs a progress entry before the leader can send it
// anything, and a removed one must stop counting toward any majority. Both
// have to happen at the moment the configuration changes, not lazily, because
// the very next commit decision is made against the new membership.
func (n *Node) adoptConfig(c config) {
	n.conf = c

	if n.state != Leader {
		// Only a leader tracks replication progress; a follower that later
		// wins an election rebuilds it from scratch in becomeLeader.
		return
	}

	members := n.conf.members()
	for _, id := range members {
		if n.progress[id] == nil {
			// A new member's log is unknown, so the leader starts optimistic
			// and backs off on rejection, exactly as it does on election.
			n.progress[id] = &progress{next: n.log.lastIndex() + 1}
		}
	}

	// Drop anyone no longer in any active configuration. Leaving them would
	// let a node that is no longer a member contribute to a majority.
	inConfig := make(map[NodeID]bool, len(members))
	for _, id := range members {
		inConfig[id] = true
	}
	for id := range n.progress {
		if !inConfig[id] {
			delete(n.progress, id)
		}
	}
}

// clone returns a deep copy of a configuration.
func (c config) clone() config {
	out := config{
		voters: copySet(c.voters),
		addrs:  copyAddrs(c.addrs),
		joint:  c.joint,
	}
	if c.incoming != nil {
		out.incoming = copySet(c.incoming)
	}
	return out
}
