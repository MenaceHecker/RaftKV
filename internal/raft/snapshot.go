package raft

import (
	"errors"
	"fmt"
)

// Snapshot transfer (§7).
//
// Log replication assumes the leader can always find a position where its log
// and a follower's agree, and back up until it does. Compaction breaks that
// assumption: once the entries a follower still needs have been deleted, no
// amount of backing off will find them, and the follower is stuck forever.
//
// The answer is to stop reconciling and start over. The leader sends its state
// machine image, the follower discards whatever it had, and replication
// resumes from the snapshot's index. This is the only place in Raft where a
// node's log moves backwards, and it is safe for one reason: a snapshot covers
// a committed prefix, so nothing being discarded was ever agreed to differ.
//
// It matters most for the case Phase 4 introduces. A node added to a
// long-running cluster starts with an empty log and needs entries the leader
// compacted away long ago, so without this a membership change could never
// finish.

// sendSnapshot sends a follower the leader's state machine image.
//
// It is the fallback when the log alone cannot catch that follower up. If no
// snapshot exists the leader can do nothing useful right now, which is not an
// error: a cluster that has never compacted has every entry it needs, so this
// only happens in the moment between a follower falling behind and a snapshot
// being taken.
func (n *Node) sendSnapshot(to NodeID) {
	pr := n.progress[to]
	if pr == nil {
		return
	}

	snap, err := n.storage.Snapshot()
	if err != nil {
		// Nothing to send. The next heartbeat tries again, by which time a
		// snapshot may exist.
		return
	}
	if snap.Index == 0 {
		return
	}

	// Assume it will be accepted. A follower that installs this snapshot has
	// everything through its index, so the next append can start immediately
	// after — and if the message is lost, the rejection path backs off again
	// as usual.
	pr.next = snap.Index + 1

	n.send(Message{
		Type:     MsgInstallSnapshot,
		To:       to,
		Term:     n.term,
		Snapshot: &snap,
	})
}

// handleInstallSnapshot is a follower receiving a state machine image. Step has
// already applied the term rules, so m.Term equals n.term here.
func (n *Node) handleInstallSnapshot(m Message) error {
	switch n.state {
	case Leader:
		// Two leaders in one term would break Election Safety.
		return errors.New("raft: received a snapshot from a peer in this node's own leader term")
	case Candidate:
		// Someone else won this term. Concede.
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

	// A snapshot that covers no more than this node already has is not worth
	// installing, and installing it would throw away entries accepted since it
	// was sent. Delayed messages make this reachable, so the answer is to
	// acknowledge the position already held rather than to regress.
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

// restore installs a snapshot over this node's state.
//
// Everything moves together: the log is replaced, the commit and applied
// cursors jump to the snapshot's index, and the membership becomes the one
// recorded in the image. Doing any of those separately would leave the node
// briefly describing a state that never existed — a log that starts after its
// own commit index, or a configuration derived from entries that are gone.
func (n *Node) restore(snap Snapshot) error {
	if err := n.storage.ApplySnapshot(snap); err != nil {
		return fmt.Errorf("raft: applying snapshot at index %d: %w", snap.Index, err)
	}

	// Everything a snapshot covers is committed and applied by definition, so
	// the cursors move with it. Leaving them behind would send the log looking
	// for entries the snapshot just replaced.
	n.log.committed = snap.Index
	n.log.applied = snap.Index

	// The configuration in the image supersedes anything derived from the log,
	// because the conf-change entries it came from are exactly what compaction
	// removed.
	if !snap.Conf.IsEmpty() {
		n.baseConf = configFromState(snap.Conf)
	}
	if err := n.rebuildConfig(); err != nil {
		return err
	}

	// Hand it to the caller so the state machine can be rebuilt from it. The
	// core has no idea what the bytes mean; only the layer above does.
	n.pendingSnapshot = &snap
	return nil
}

// handleInstallSnapshotResponse updates the leader's view of a follower that
// has installed a snapshot.
func (n *Node) handleInstallSnapshotResponse(m Message) error {
	if n.state != Leader {
		return nil
	}
	pr := n.progress[m.From]
	if pr == nil {
		return nil
	}

	if !m.Success {
		// The follower could not install it. Back the optimistic guess out so
		// the next attempt starts from what is actually known.
		pr.next = pr.match + 1
		return nil
	}

	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	if pr.match+1 > pr.next {
		pr.next = pr.match + 1
	}

	// The follower may now hold enough for something to commit, and there may
	// be entries after the snapshot it still needs.
	if n.maybeCommit() {
		if err := n.maybeFinishConfChange(); err != nil {
			return err
		}
	}
	n.sendAppend(m.From)
	return nil
}
