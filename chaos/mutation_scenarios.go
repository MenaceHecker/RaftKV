package chaos

import (
	"fmt"
	"math/rand"
	"slices"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
)

// MutationScenarios returns the scenarios the mutation suite uses beyond the
// catalog: each one reaches a state the catalog's scenarios do not, so that one
// of the nine injected bugs (RUNBOOK 8.6) has a way to show. They are kept out of
// AllScenarios, which is the chaosrun catalog and has a pinned size.
//
// Each is scripted, so a seed varies message fate and election timeouts only.
// Where a scenario depends on a timing window that some seeds miss, it retries
// and fails with an error, never with a pass, when the window never opened: an
// error is inconclusive to the suite and is not a detection.
func MutationScenarios() []Scenario {
	return []Scenario{
		scenarioPowerLossAfterAck(),
		scenarioFigure8(),
		scenarioStaleReadWindow(),
		scenarioReadAfterElection(),
		scenarioResendAfterSnapshotInstall(),
	}
}

// writeAcked writes until the leader answers and fails unless the write was
// acknowledged, so a scenario that depends on an acked write cannot go on
// without one.
func writeAcked(c *Cluster, client int, key, value string) (*Op, error) {
	op, err := WriteWithRetry(c, client, key, value, 500)
	if err != nil {
		return nil, err
	}
	if op.Status != StatusOK {
		return nil, fmt.Errorf("chaos: the write of %q to %q was %s, not acknowledged\n%s", value, key, op.Status, c.Dump())
	}
	return op, nil
}

// readServed retries a read, a tick apart, until some leader answers it, and
// returns the answered op. A read the leader refuses fails at once, so the retry
// has to tick for the cluster to make progress.
func readServed(c *Cluster, client int, key string, maxTicks int) (*Op, error) {
	for range maxTicks {
		op, err := ReadAndSettle(c, client, key, 20)
		if err != nil {
			return nil, err
		}
		if op.Status == StatusOK {
			return op, nil
		}
		if err := c.Tick(); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("chaos: no read of %q was served within %d ticks\n%s", key, maxTicks, c.Dump())
}

func scenarioPowerLossAfterAck() Scenario {
	return Scenario{
		Name: "power loss after an acknowledged write",
		Hypothesis: "a write acknowledged to a client has been fsynced on a majority, so " +
			"a power cut that takes every node at the same instant and drops all unsynced " +
			"bytes must not lose it: the first read after the cluster comes back, before " +
			"any new write can repair the history, still sees it (RUNBOOK 7.2, paper " +
			"Figure 2 persistent state)",
		Nodes: 3,

		// Every node fsyncs on every write, on an in-memory disk that models what a
		// power cut keeps. The default (SyncNever, in memory) would hide a missing
		// fsync completely.
		Tune: func(cfg *Config) {
			cfg.DataDir = "/raft"
			cfg.Sync = storage.SyncAlways
			cfg.FS = faultfs.New(rand.New(rand.NewSource(cfg.Seed)), faultfs.Options{}) //nolint:gosec // deterministic simulation, not security
		},

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 400); err != nil {
				return err
			}
			for i := range 4 {
				if _, err := writeAcked(c, 1+i%2, "k", fmt.Sprintf("acked-%d", i)); err != nil {
					return err
				}
			}
			// Quiet, so a node that fsynced late is not credited: the writes above
			// are acknowledged, and an acknowledged write must already be durable.

			if err := c.PowerLoss(); err != nil {
				return err
			}
			if err := c.TickN(10); err != nil {
				return err
			}
			for _, id := range c.IDs() {
				if err := c.Restart(id); err != nil {
					return err
				}
			}
			if _, err := SettleLeader(c, 800); err != nil {
				return err
			}

			// Read before any new write: a later write would hide a lost one.
			if _, err := readServed(c, 3, "k", 600); err != nil {
				return err
			}

			if err := Workload(c, 2, "k", "after", 2, 600); err != nil {
				return err
			}
			return c.TickN(200)
		},
	}
}

// scenarioFigure8 scripts Figure 8 of the Raft paper (section 5.4.2): an entry
// from an earlier term is replicated on a majority by a later leader and must not
// be counted committed, because a node with a later-term entry at that index can
// still win and overwrite it.
//
// Roles, with node ids chosen by whichever leader the seed elects:
//
//	L   the first leader; it replicates one entry, then crashes
//	H   the node that received that entry too
//	X   a node that wins a later term but appends only its no-op and crashes
//	    before any message of its reaches the wire
//	Y,Z the two nodes that voted for X and never saw the entry
//	W   whichever of L and H leads next; it holds the old entry
//
// The cluster pages one entry per AppendEntries, so when W replicates the old
// entry to Y that acknowledgement is, for a moment, the only thing counted. A
// correct leader waits for an entry of its own term; the injected bug commits the
// old one on that count. W crashes at that moment, before it can replicate its
// own no-op, X is elected and overwrites the entry on Y.
//
// On a correct core W cannot commit early, Y ends up holding W's no-op, X cannot
// win, and the script ends by healing the cluster.
func scenarioFigure8() Scenario {
	return Scenario{
		Name: "figure 8: a previous-term entry committed by counting",
		Hypothesis: "a leader must not count replicas to commit an entry from an earlier " +
			"term, even when it is on a majority, because a node with a later-term entry " +
			"at that index can still be elected and overwrite it (Raft paper section 5.4.2, " +
			"Figure 8)",
		Nodes:         5,
		RequireFaults: requirePartitioned,
		Tune:          func(cfg *Config) { cfg.MaxAppendBytes = 1 },

		Run: func(c *Cluster) error {
			l, err := SettleLeader(c, 400)
			if err != nil {
				return err
			}
			if err := Workload(c, 3, "f", "before", 2, 500); err != nil {
				return err
			}

			h := OtherThan(c, l)
			rest := slices.DeleteFunc(slices.Clone(c.IDs()), func(id raft.NodeID) bool { return id == l || id == h })
			slices.Sort(rest)

			// (a) L accepts one entry and replicates it to H only, then dies.
			c.Network().Partition([]raft.NodeID{l, h}, rest)
			old := c.WriteTo(2, l, "f", "old-term")
			if old.Status != StatusPending {
				return fmt.Errorf("chaos: figure 8: the leader refused the entry (%s)\n%s", old.Status, c.Dump())
			}
			if err := c.TickN(3); err != nil {
				return err
			}
			idx := old.index
			if c.nodes[h].LastIndex() < idx {
				return fmt.Errorf("chaos: figure 8: the entry at %d did not reach the second node\n%s", idx, c.Dump())
			}
			c.Crash(l)

			// (b) Three nodes elect X. Its no-op goes to nobody, and it crashes.
			c.Network().Partition([]raft.NodeID{h}, rest)
			x, err := c.AwaitLeader(600)
			if err != nil {
				return err
			}
			if !slices.Contains(rest, x) {
				return fmt.Errorf("chaos: figure 8: node %d led instead of one of %v\n%s", x, rest, c.Dump())
			}
			c.Network().DropInFlightFrom(x)
			c.Crash(x)
			others := slices.DeleteFunc(slices.Clone(rest), func(id raft.NodeID) bool { return id == x })
			y, z := others[0], others[1]

			// (c) L returns. L or H, whichever leads, replicates the old entry to Y.
			// Stop the instant it counts as committed, before the leader's own no-op
			// can reach Y.
			if err := c.Restart(l); err != nil {
				return err
			}
			c.Network().Partition([]raft.NodeID{l, h, y}, []raft.NodeID{z})
			w, err := awaitCommitAtLeast(c, idx, 600)
			if err != nil {
				return err
			}
			c.Network().DropInFlightFrom(w)
			c.Crash(w)
			held := l
			if w == l {
				held = h
			}

			// (d) X returns with the later-term entry at idx. It can win only if the
			// entry at idx on Y is still the old one.
			c.Network().Partition([]raft.NodeID{x, y, z}, []raft.NodeID{held})
			if err := c.Restart(x); err != nil {
				return err
			}
			if err := c.TickN(400); err != nil {
				return err
			}

			// Heal everything and check the cluster still serves clients.
			if err := c.Restart(w); err != nil {
				return err
			}
			c.Network().Heal()
			if _, err := SettleLeader(c, 800); err != nil {
				return err
			}
			if err := Workload(c, 3, "f", "after", 2, 800); err != nil {
				return err
			}
			return c.TickN(300)
		},
	}
}

// awaitCommitAtLeast ticks until some leader has a commit index of at least idx
// and returns it.
func awaitCommitAtLeast(c *Cluster, idx raft.Index, maxTicks int) (raft.NodeID, error) {
	for range maxTicks {
		if err := c.Tick(); err != nil {
			return 0, err
		}
		id, ok, err := c.Leader()
		if err != nil {
			return 0, err
		}
		if ok && c.nodes[id].CommitIndex() >= idx {
			return id, nil
		}
	}
	return 0, fmt.Errorf("chaos: no leader committed index %d within %d ticks\n%s", idx, maxTicks, c.Dump())
}

// scenarioStaleReadWindow reads from a leader in the interval after a new leader
// has committed a write and before CheckQuorum makes the old one step down. The
// catalog's stale-leader scenario reads after more than 120 ticks, by which time
// the old leader has long since stepped down and refuses the read at once, so it
// never reaches the code that decides whether to ask for a quorum.
func scenarioStaleReadWindow() Scenario {
	const rounds = 12

	return Scenario{
		Name: "a read at a deposed leader before it steps down",
		Hypothesis: "a leader that has been replaced but has not yet noticed must not answer " +
			"a read from local state: the read has to wait for a heartbeat quorum that it " +
			"cannot get (Raft paper section 8)",
		Nodes:         5,
		RequireFaults: requirePartitioned,

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 400); err != nil {
				return err
			}
			if err := Workload(c, 3, "r", "before", 2, 500); err != nil {
				return err
			}

			reached := false

			for round := 0; round < rounds && !reached; round++ {
				old, err := SettleLeader(c, 800)
				if err != nil {
					return err
				}
				if _, err := writeAcked(c, 1, "r", fmt.Sprintf("round-%d-old", round)); err != nil {
					return err
				}

				c.Network().Partition([]raft.NodeID{old}, MajorityWithout(c, old))

				// Tick one at a time until the majority has a new leader.
				replaced := false
				for range 120 {
					if err := c.Tick(); err != nil {
						return err
					}
					if lead, ok, err := c.Leader(); err != nil {
						return err
					} else if ok && lead != old {
						replaced = true
						break
					}
				}

				if replaced {
					op, err := WriteAndSettle(c, 2, "r", fmt.Sprintf("round-%d-new", round), 60)
					if err != nil {
						return err
					}
					// The write is acknowledged, so a read that starts now must see it.
					// Whether the old leader still believes it leads is up to its
					// CheckQuorum timer; read only if it does.
					if op.Status == StatusOK && c.nodes[old].State() == raft.Leader {
						if _, err := ReadFromAndSettle(c, 9, old, "r", 6); err != nil {
							return err
						}
						reached = true
					}
				}

				c.Network().Heal()
				if err := c.TickN(200); err != nil {
					return err
				}
			}

			if !reached {
				return fmt.Errorf("chaos: the old leader stepped down before a new leader committed a write in all %d rounds, so no read reached it\n%s",
					rounds, c.Dump())
			}
			return c.TickN(200)
		},
	}
}

// scenarioReadAfterElection has a new leader take over a cluster whose last
// entries are from an earlier term, with no write arriving afterwards. A read
// needs the leader to have committed an entry of its own term, which only its
// no-op can supply (Raft paper section 8), so reads must become available by
// themselves. No existing oracle notices a leader that skipped its no-op: writes
// still commit, and a liveness check that tries a write still passes.
func scenarioReadAfterElection() Scenario {
	return Scenario{
		Name: "reads after an election with no new write",
		Hypothesis: "a new leader commits a no-op of its own term, so reads are served " +
			"without any client having to write first (Raft paper section 8)",
		Nodes: 5,

		Run: func(c *Cluster) error {
			leader, err := SettleLeader(c, 400)
			if err != nil {
				return err
			}
			if err := Workload(c, 3, "e", "before", 2, 500); err != nil {
				return err
			}

			c.Crash(leader)
			if _, err := SettleLeader(c, 800); err != nil {
				return err
			}

			// Reads only: nothing writes until the cluster has answered one.
			if err := c.AwaitReadable(300); err != nil {
				return err
			}
			if _, err := readServed(c, 4, "e", 300); err != nil {
				return err
			}

			if err := c.Restart(leader); err != nil {
				return err
			}
			if err := c.TickN(100); err != nil {
				return err
			}
			if _, err := readServed(c, 5, "e", 300); err != nil {
				return err
			}
			return c.TickN(200)
		},
	}
}

// scenarioResendAfterSnapshotInstall resends a write after a node was rebuilt from
// a snapshot. The session table is part of the snapshot (RUNBOOK 7.5), so the
// rebuilt node must reject the resend like every other node. The catalog's
// snapshot scenarios write again after the install, and later writes make the
// session bytes agree again before the replicas are compared.
func scenarioResendAfterSnapshotInstall() Scenario {
	return Scenario{
		Name: "a resend after a snapshot install",
		Hypothesis: "a node rebuilt from a snapshot must know which client requests were " +
			"already applied, or a resent request is applied there and nowhere else " +
			"(RUNBOOK 7.4, 7.5)",
		Nodes: 5,

		Run: func(c *Cluster) error {
			leader, err := SettleLeader(c, 400)
			if err != nil {
				return err
			}
			victim := OtherThan(c, leader)
			c.Crash(victim)

			first, err := writeAcked(c, 1, "s", "first")
			if err != nil {
				return err
			}
			if _, err := writeAcked(c, 2, "s", "second"); err != nil {
				return err
			}
			if err := Workload(c, 3, "pad", "away", 3, 600); err != nil {
				return err
			}
			if err := c.CompactAll(); err != nil {
				return err
			}
			if err := Workload(c, 3, "pad", "more", 2, 500); err != nil {
				return err
			}
			if err := c.CompactAll(); err != nil {
				return err
			}

			if err := c.Restart(victim); err != nil {
				return err
			}
			if err := c.TickN(500); err != nil {
				return err
			}
			if err := requireSnapshotInstalled(c, victim); err != nil {
				return err
			}

			// The client never heard back and sends the same request again.
			if err := c.Resend(first); err != nil {
				return err
			}
			if err := c.TickN(200); err != nil {
				return err
			}
			if _, err := readServed(c, 4, "s", 400); err != nil {
				return err
			}
			return c.TickN(300)
		},
	}
}
