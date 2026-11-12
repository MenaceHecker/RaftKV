package chaos

import (
	"errors"
	"fmt"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The committed-log monitor checks, after every tick, the parts of RUNBOOK 7.1
// that Election Safety alone does not cover. What it checks, exactly:
//
//   - State Machine Safety, approximated: every node that reports an entry
//     committed must hold the same term at that index as every node that
//     reported it before. Equal terms at one index mean equal commands only if
//     Log Matching holds, which is assumed rather than checked. A state machine
//     must also never have applied past its node's commit index.
//   - A node's log never ends before its commit index, and its newest committed
//     entries keep the terms they were recorded with.
//   - Leader Completeness (Raft paper section 5.4): every leader of a later term
//     must hold every entry recorded committed in an earlier term, with the
//     recorded term.
//
// It does not check Log Matching directly or Leader Append-Only. Entries a node
// has compacted are skipped, since their terms are gone.

// recentWindow is how many of a node's newest committed entries are checked
// again every tick. Checking every committed entry every tick would cost the
// length of the log per node per tick.
const recentWindow = 16

// logMonitor is the state of the committed-log monitor.
type logMonitor struct {
	// terms is the term of every entry that some node has reported committed.
	terms map[raft.Index]raft.Term
	// top is the highest index in terms.
	top raft.Index
	// cursor is, per node, the highest index already recorded or checked.
	cursor map[raft.NodeID]raft.Index
	// verified is, per leader and term, the highest recorded index already
	// checked against that leader's log.
	verified map[leaderTerm]raft.Index
}

type leaderTerm struct {
	id   raft.NodeID
	term raft.Term
}

func newLogMonitor() logMonitor {
	return logMonitor{
		terms:    make(map[raft.Index]raft.Term),
		cursor:   make(map[raft.NodeID]raft.Index),
		verified: make(map[leaderTerm]raft.Index),
	}
}

func (c *Cluster) committedLogError(format string, args ...any) error {
	return fmt.Errorf("%w at tick %d: %s", ErrCommittedLog, c.net.Now(), fmt.Sprintf(format, args...))
}

// checkCommittedLog is called after every tick.
func (c *Cluster) checkCommittedLog() error {
	m := &c.mon

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.nodes[id]
		commit := n.CommitIndex()

		if applied := c.machines[id].Applied(); applied > commit {
			return c.committedLogError("node %d applied index %d beyond its commit index %d", id, applied, commit)
		}

		// A node never gives up a committed entry: its log reaches its commit index.
		if last := n.LastIndex(); last < commit {
			return c.committedLogError("node %d's log ends at %d, before its commit index %d", id, last, commit)
		}

		// Check what is new, plus the newest recentWindow entries again, since a
		// committed entry that is rewritten changes near the commit index.
		from := m.cursor[id] + 1
		recheck := raft.Index(1)
		if commit > recentWindow {
			recheck = commit - recentWindow + 1
		}
		from = min(from, recheck)
		for i := from; i <= commit; i++ {
			term, err := n.TermAt(i)
			if err != nil {
				if errors.Is(err, raft.ErrCompacted) || errors.Is(err, raft.ErrUnavailable) {
					continue
				}
				return fmt.Errorf("chaos: node %d term of committed index %d: %w", id, i, err)
			}
			if prev, seen := m.terms[i]; seen {
				if prev != term {
					return c.committedLogError("node %d holds term %d at committed index %d, which was committed with term %d",
						id, term, i, prev)
				}
				continue
			}
			m.terms[i] = term
			m.top = max(m.top, i)
		}
		m.cursor[id] = max(m.cursor[id], commit)
	}

	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.nodes[id]
		if n.State() != raft.Leader {
			continue
		}

		key := leaderTerm{id, n.Term()}
		for i := m.verified[key] + 1; i <= m.top; i++ {
			committedTerm, ok := m.terms[i]
			if !ok || committedTerm >= n.Term() {
				continue
			}
			term, err := n.TermAt(i)
			if errors.Is(err, raft.ErrCompacted) {
				continue
			}
			if err != nil {
				return c.committedLogError("leader %d of term %d lacks index %d, which was committed in term %d (%v)",
					id, n.Term(), i, committedTerm, err)
			}
			if term != committedTerm {
				return c.committedLogError("leader %d of term %d holds term %d at index %d, which was committed in term %d",
					id, n.Term(), term, i, committedTerm)
			}
		}
		m.verified[key] = m.top
	}
	return nil
}
