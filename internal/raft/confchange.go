package raft

import (
	"errors"
	"fmt"
)

func (n *Node) ProposeConfChange(cc ConfChange) error {
	if n.state != Leader {
		return ErrNotLeader
	}

	if cc.Type == ConfChangeLeaveJoint {
		if !n.conf.inJoint() {
			return ErrNotInJoint
		}
	} else if n.conf.inJoint() {
		return ErrConfChangeInFlight
	}

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

	if err := n.rebuildConfig(); err != nil {
		return err
	}

	if pr := n.progress[n.id]; pr != nil {
		pr.match = n.log.lastIndex()
		pr.next = n.log.lastIndex() + 1
	}

	if n.isSoleVoter() {
		n.maybeCommit()
		return nil
	}
	n.broadcastAppend()

	if !n.conf.hasVoter(n.id) {
		return n.becomeFollower(n.term, None)
	}
	return nil
}

func containsConfChange(entries []Entry) bool {
	for _, e := range entries {
		if e.Type == EntryConfChange {
			return true
		}
	}
	return false
}

// refreshBase makes baseConf the configuration at the snapshot the stored log
// sits on. The base only moves when storage's first index does (a local
// compaction or a restored snapshot), so the common case costs nothing: a disk
// Snapshot() loads the snapshot data and must not run on every rebuild.
func (n *Node) refreshBase() error {
	wanted := n.storage.FirstIndex() - 1
	if n.baseLoaded && wanted == n.baseIndex {
		return nil
	}

	idx, conf := Index(0), n.initialConf
	snap, err := n.storage.Snapshot()
	switch {
	case errors.Is(err, ErrSnapshotUnavailable):
	case err != nil:
		return fmt.Errorf("raft: reading the snapshot base: %w: %w", ErrStorage, err)
	default:
		idx = snap.Index
		if !snap.Conf.IsEmpty() {
			conf = configFromState(snap.Conf)
		}
	}

	if n.baseLoaded && idx < n.baseIndex {
		return fmt.Errorf("raft: storage snapshot at index %d is older than the configuration base at %d: %w",
			idx, n.baseIndex, ErrStorage)
	}
	n.baseConf, n.baseIndex, n.baseLoaded = conf, idx, true
	return nil
}

// replayConfig derives the configuration as of index upTo from the base plus
// the configuration entries in (baseIndex, upTo]. jointAt is the index of the
// entry that opened the joint phase it ends in, or 0 when it is not joint.
func (n *Node) replayConfig(upTo Index) (conf config, jointAt Index, err error) {
	conf = n.baseConf.clone()
	if upTo <= n.baseIndex {
		return conf, 0, nil
	}

	// Replay starts at the log's first index. With a stored snapshot as the base
	// that must be baseIndex+1: if it is higher, configuration entries between
	// the snapshot and the first stored entry are missing, and carrying on would
	// adopt a possibly wrong configuration, so fail instead. The lenient start
	// is only for baseIndex == 0, where storage may report a first index without
	// holding a snapshot (the initial configuration is then all there is).
	first := n.log.firstIndex()
	if n.baseIndex > 0 && first > n.baseIndex+1 {
		return config{}, 0, fmt.Errorf("raft: entries %d..%d between the snapshot and the log are missing: %w",
			n.baseIndex+1, first-1, ErrStorage)
	}
	entries, err := n.log.entries(max(n.baseIndex+1, first), upTo+1)
	if err != nil {
		return config{}, 0, fmt.Errorf("raft: rebuilding configuration: %w", err)
	}

	for _, e := range entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			return config{}, 0, fmt.Errorf("raft: configuration change at index %d is unreadable: %w", e.Index, err)
		}

		next, err := applyConfChange(conf, cc)
		if err != nil {
			continue
		}
		conf = next

		if conf.inJoint() {
			jointAt = e.Index
		} else {
			jointAt = 0
		}
	}
	return conf, jointAt, nil
}

func (n *Node) rebuildConfig() error {
	if err := n.refreshBase(); err != nil {
		return err
	}
	conf, jointAt, err := n.replayConfig(n.log.lastIndex())
	if err != nil {
		return err
	}
	n.jointEntryIndex = jointAt
	n.adoptConfig(conf)
	return nil
}

// ConfStateAt returns the configuration as of log index i: the snapshot's Conf
// at the snapshot index, plus the configuration entries above it up to i. It is
// what a snapshot at i must store, as opposed to ConfState, which also reflects
// entries appended above i that may never commit (Raft §6: a server uses the
// latest configuration in its log, but a snapshot covers only what it includes).
//
// Below the snapshot the entries are gone and the answer cannot be derived, so
// it returns ErrCompacted rather than guess; above the last index it returns
// ErrUnavailable. With no stored snapshot the base is the initial configuration
// at index 0.
func (n *Node) ConfStateAt(i Index) (ConfState, error) {
	if err := n.refreshBase(); err != nil {
		return ConfState{}, err
	}
	if i < n.baseIndex {
		return ConfState{}, fmt.Errorf("raft: configuration at index %d: %w", i, ErrCompacted)
	}
	if last := n.log.lastIndex(); i > last {
		return ConfState{}, fmt.Errorf("raft: configuration at index %d, the log ends at %d: %w", i, last, ErrUnavailable)
	}
	conf, _, err := n.replayConfig(i)
	if err != nil {
		return ConfState{}, err
	}
	return conf.toState(), nil
}

func (n *Node) maybeFinishConfChange() error {
	if n.state != Leader || !n.conf.inJoint() {
		return nil
	}
	if n.jointEntryIndex == 0 || n.log.committed < n.jointEntryIndex {
		return nil
	}

	if !n.hasCommittedInCurrentTerm() {
		return nil
	}

	return n.ProposeConfChange(ConfChange{Type: ConfChangeLeaveJoint})
}

func applyConfChange(c config, cc ConfChange) (config, error) {
	if cc.Type == ConfChangeLeaveJoint {
		return c.leaveJoint()
	}
	return c.enterJoint(cc)
}

func (n *Node) adoptConfig(c config) {
	n.conf = c

	n.confSeq++

	if n.state != Leader {
		return
	}

	members := n.conf.members()
	for _, id := range members {
		if n.progress[id] == nil {
			n.progress[id] = &progress{next: n.log.lastIndex() + 1}
		}
	}

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
