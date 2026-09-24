package raft

import "fmt"

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
			continue
		}
		conf = next

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
