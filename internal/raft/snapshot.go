package raft

import (
	"errors"
	"fmt"
)

func (n *Node) sendSnapshot(to NodeID) {
	pr := n.progress[to]
	if pr == nil {
		return
	}

	snap, err := n.storage.Snapshot()
	if err != nil {
		return
	}
	if snap.Index == 0 {
		return
	}

	pr.next = snap.Index + 1

	n.send(Message{
		Type:     MsgInstallSnapshot,
		To:       to,
		Term:     n.term,
		Snapshot: &snap,
	})
}

func (n *Node) handleInstallSnapshot(m Message) error {
	switch n.state {
	case Leader:
		return errors.New("raft: received a snapshot from a peer in this node's own leader term")
	case Candidate:
		if err := n.becomeFollower(m.Term, m.From); err != nil {
			return err
		}
	}

	n.leader = m.From
	n.electionElapsed = 0

	snap := m.Snapshot
	if snap.IsEmpty() {
		return fmt.Errorf("raft: node %d sent an empty snapshot", m.From)
	}

	if snap.Index <= n.log.committed {
		n.send(Message{
			Type:       MsgInstallSnapshotResponse,
			To:         m.From,
			Term:       n.term,
			Success:    true,
			MatchIndex: n.log.committed,
		})
		return nil
	}

	if err := n.restore(*snap); err != nil {
		return err
	}

	n.send(Message{
		Type:       MsgInstallSnapshotResponse,
		To:         m.From,
		Term:       n.term,
		Success:    true,
		MatchIndex: n.log.lastIndex(),
	})
	return nil
}

func (n *Node) restore(snap Snapshot) error {
	if err := n.storage.ApplySnapshot(snap); err != nil {
		return fmt.Errorf("raft: applying snapshot at index %d: %w: %w", snap.Index, ErrStorage, err)
	}

	n.log.committed = snap.Index
	n.log.applied = snap.Index

	if !snap.Conf.IsEmpty() {
		n.baseConf = configFromState(snap.Conf)
	}
	if err := n.rebuildConfig(); err != nil {
		return err
	}

	n.pendingSnapshot = &snap
	return nil
}

func (n *Node) handleInstallSnapshotResponse(m Message) error {
	if n.state != Leader {
		return nil
	}
	pr := n.progress[m.From]
	if pr == nil {
		return nil
	}

	pr.active = true

	if !m.Success {
		pr.next = pr.match + 1
		return nil
	}

	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	if pr.match+1 > pr.next {
		pr.next = pr.match + 1
	}

	if n.maybeCommit() {
		if err := n.maybeFinishConfChange(); err != nil {
			return err
		}
	}
	n.sendAppend(m.From)
	return nil
}
