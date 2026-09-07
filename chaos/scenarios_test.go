package chaos

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The adversarial scenarios.
//
// Each one describes a specific way the cluster could go wrong, drives it into
// that situation, and hands the resulting history to the linearizability
// checker. The scenario itself asserts nothing about correctness: it only
// creates the conditions. What has to hold afterwards is the same for all of
// them, and is checked by the runner.
//
// Every scenario runs across several seeds, because one seed is one
// interleaving. That is not a proof — no amount of testing is — but it is the
// difference between testing a behaviour and testing an anecdote.

// seeds are the interleavings every scenario is run against.
var seeds = []int64{1, 2, 3, 5, 8}

// runScenario executes a scenario and fails the test if any run did not hold.
func runScenario(t *testing.T, s Scenario) Report {
	t.Helper()

	report := RunScenario(s, seeds)
	t.Logf("\n%s", report)

	if !report.Passed() {
		t.Errorf("scenario %q did not hold", s.Name)
	}
	return report
}

// requireDropped insists messages were actually lost.
func requireDropped(st Stats) error {
	if st.Dropped == 0 {
		return fmt.Errorf("no messages were dropped")
	}
	return nil
}

// requirePartitioned insists a partition actually blocked traffic.
func requirePartitioned(st Stats) error {
	if st.Partitions == 0 {
		return fmt.Errorf("no messages were blocked by a partition")
	}
	return nil
}

func scenarioLeaderPartitioned() Scenario {
	return Scenario{
		Name: "leader partitioned from the majority mid-write",
		Hypothesis: "a leader cut off while a write is in flight must not be able to " +
			"commit it, and the majority must elect a new leader whose history " +
			"does not contradict anything already returned to a client",
		Nodes:         5,
		RequireFaults: requirePartitioned,

		Run: func(c *Cluster) error {
			leader, err := SettleLeader(c, 300)
			if err != nil {
				return err
			}

			// Build a real history before anything goes wrong, so the
			// checker has something substantial to reconcile against.
			if err := Workload(c, 3, "x", "before", 3, 400); err != nil {
				return err
			}

			// Start a write, then cut the leader off before it can commit.
			c.Write(2, "x", "during-partition")
			c.Network().Partition([]raft.NodeID{leader}, MajorityWithout(c, leader))
			if err := c.TickN(10); err != nil {
				return err
			}

			// The majority elects someone else and carries on serving.
			if err := c.TickN(150); err != nil {
				return err
			}
			if err := Workload(c, 3, "x", "after", 3, 500); err != nil {
				return err
			}

			// The old leader rejoins and must reconcile.
			c.Network().Heal()
			return c.TickN(300)
		},
	}
}

func TestScenarioLeaderPartitionedMidWrite(t *testing.T) {
	runScenario(t, scenarioLeaderPartitioned())
}

func scenarioLeaderCrash() Scenario {
	return Scenario{
		Name: "leader crashes with a write in flight",
		Hypothesis: "a write whose leader died is either committed everywhere or " +
			"nowhere; the client cannot tell which, and either answer must be " +
			"consistent with everything read afterwards",
		Nodes: 5,

		Run: func(c *Cluster) error {
			leader, err := SettleLeader(c, 300)
			if err != nil {
				return err
			}
			if err := Workload(c, 3, "x", "before", 3, 400); err != nil {
				return err
			}

			// Kill the leader with a write outstanding. Its outcome becomes
			// genuinely unknown, which is the case the checker must handle.
			c.Write(2, "x", "in-flight")
			c.Crash(leader)

			if err := c.TickN(200); err != nil {
				return err
			}
			if err := Workload(c, 3, "x", "after", 3, 500); err != nil {
				return err
			}

			if err := c.Restart(leader); err != nil {
				return err
			}
			return c.TickN(300)
		},
	}
}

func TestScenarioLeaderCrashesMidWrite(t *testing.T) {
	runScenario(t, scenarioLeaderCrash())
}

func scenarioRollingRestarts() Scenario {
	return Scenario{
		Name: "every node restarted in turn while clients keep writing",
		Hypothesis: "a cluster survives losing any single node at any moment, and a " +
			"restarted node rebuilds its state machine from the log without " +
			"contradicting what clients already observed",
		Nodes:  5,
		Faults: Faults{MinDelay: 0, MaxDelay: 2},

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 300); err != nil {
				return err
			}

			for round, victim := range c.IDs() {
				if _, err := WriteWithRetry(c, 1, "x", fmt.Sprintf("round-%d", round), 400); err != nil {
					return err
				}

				c.Crash(victim)
				if err := c.TickN(60); err != nil {
					return err
				}

				// Keep writing while a node is down.
				if _, err := WriteWithRetry(c, 2, "x", fmt.Sprintf("down-%d", round), 400); err != nil {
					return err
				}

				if err := c.Restart(victim); err != nil {
					return err
				}
				if err := c.TickN(120); err != nil {
					return err
				}
			}
			return c.TickN(300)
		},
	}
}

func TestScenarioRollingRestarts(t *testing.T) {
	runScenario(t, scenarioRollingRestarts())
}

func scenarioLossy() Scenario {
	return Scenario{
		Name: "sustained loss, delay, duplication and reordering",
		Hypothesis: "Raft's retransmission covers every dropped message, and no " +
			"handler assumed a message arrived once, in order, or at all",
		Nodes: 5,
		Faults: Faults{
			LossRate:      0.25,
			MinDelay:      0,
			MaxDelay:      6,
			DuplicateRate: 0.1,
		},
		RequireFaults: func(st Stats) error {
			if err := requireDropped(st); err != nil {
				return err
			}
			if st.Duplicated == 0 {
				return fmt.Errorf("no messages were duplicated")
			}
			if st.Delayed == 0 {
				return fmt.Errorf("no messages were delayed")
			}
			return nil
		},

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 500); err != nil {
				return err
			}

			for i := range 12 {
				if _, err := WriteWithRetry(c, 1+i%3, "x", fmt.Sprintf("v%d", i), 500); err != nil {
					return err
				}
				if i%4 == 0 {
					if _, err := ReadAndSettle(c, 3, "x", 300); err != nil {
						return err
					}
				}
			}
			return c.TickN(400)
		},
	}
}

func TestScenarioMessageLossAndReordering(t *testing.T) {
	runScenario(t, scenarioLossy())
}

func scenarioSplitVote() Scenario {
	return Scenario{
		Name: "candidates competing under heavy delay",
		Hypothesis: "delay long enough to produce competing candidates still yields " +
			"at most one leader per term, and the cluster eventually settles " +
			"rather than campaigning forever",
		// An even-sized cluster makes a tied vote possible, which is the case
		// that deadlocks an implementation with a fixed election timeout.
		Nodes:         4,
		Faults:        Faults{MinDelay: 2, MaxDelay: 12, LossRate: 0.1},
		RequireFaults: requireDropped,

		Run: func(c *Cluster) error {
			// Election safety is asserted on every call to Leader, so simply
			// ticking through the contested period exercises it continuously.
			if err := c.TickN(200); err != nil {
				return err
			}
			if _, err := SettleLeader(c, 600); err != nil {
				return err
			}

			if err := Workload(c, 3, "x", "settled", 4, 600); err != nil {
				return err
			}
			return c.TickN(300)
		},
	}
}

func TestScenarioSplitVoteUnderDelay(t *testing.T) {
	runScenario(t, scenarioSplitVote())
}

func scenarioMinorityPartition() Scenario {
	return Scenario{
		Name: "cluster split into a majority and a minority",
		Hypothesis: "only the side with a majority makes progress; the minority side " +
			"accepts nothing that later contradicts it, and both converge once " +
			"the partition heals",
		Nodes:         5,
		RequireFaults: requirePartitioned,

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 300); err != nil {
				return err
			}
			if err := Workload(c, 3, "x", "before-split", 2, 400); err != nil {
				return err
			}

			// Three nodes on one side, two on the other.
			ids := c.IDs()
			majority := ids[:3]
			minority := ids[3:]
			c.Network().Partition(majority, minority)

			if err := c.TickN(200); err != nil {
				return err
			}

			// The majority side keeps working.
			if err := Workload(c, 2, "x", "majority", 3, 500); err != nil {
				return err
			}
			// The minority side is asked for a read; it must refuse rather
			// than answer from its own stale state.
			if _, err := ReadAndSettle(c, 3, "x", 100); err != nil {
				return err
			}

			c.Network().Heal()
			return c.TickN(400)
		},
	}
}

func TestScenarioMinorityPartitionCannotCommit(t *testing.T) {
	runScenario(t, scenarioMinorityPartition())
}

func scenarioLeaderChurn() Scenario {
	return Scenario{
		Name: "leadership repeatedly forced to move",
		Hypothesis: "back-to-back leader changes never lose a committed write and " +
			"never let two leaders act in the same term",
		Nodes:  5,
		Faults: Faults{MinDelay: 0, MaxDelay: 3},

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 300); err != nil {
				return err
			}

			for round := range 4 {
				if err := Workload(c, 2, "x", fmt.Sprintf("round%d", round), 1, 500); err != nil {
					return err
				}

				leader, ok, err := c.Leader()
				if err != nil {
					return err
				}
				if !ok {
					if err := c.TickN(100); err != nil {
						return err
					}
					continue
				}

				// Isolate the leader so the rest must replace it, then bring
				// it back to reconcile.
				c.Network().Partition([]raft.NodeID{leader}, MajorityWithout(c, leader))
				if err := c.TickN(150); err != nil {
					return err
				}
				c.Network().Heal()
				if err := c.TickN(150); err != nil {
					return err
				}
			}
			return c.TickN(300)
		},
	}
}

func TestScenarioRepeatedLeaderChurn(t *testing.T) {
	runScenario(t, scenarioLeaderChurn())
}

func scenarioConcurrentClients() Scenario {
	return Scenario{
		Name: "several clients writing the same key at once",
		Hypothesis: "concurrent writes to one register are ordered consistently, and " +
			"every read observes a value some ordering could produce",
		Nodes:  3,
		Faults: Faults{MinDelay: 0, MaxDelay: 4, LossRate: 0.05},

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 300); err != nil {
				return err
			}

			// Writes are issued in small overlapping batches rather than all
			// at once. Fully concurrent writes to one key are what make a
			// history expensive to decide exactly, and a scenario that ends
			// Undecided proves nothing either way.
			for round := range 6 {
				for client := 1; client <= 3; client++ {
					c.Write(client, "x", fmt.Sprintf("r%d-c%d", round, client))
				}
				if err := c.TickN(25); err != nil {
					return err
				}
			}

			if _, err := ReadAndSettle(c, 4, "x", 300); err != nil {
				return err
			}
			return c.TickN(300)
		},
	}
}

func TestScenarioConcurrentClientsOnOneKey(t *testing.T) {
	runScenario(t, scenarioConcurrentClients())
}

func scenarioIndependentKeys() Scenario {
	return Scenario{
		Name: "many keys under loss and leader changes",
		Hypothesis: "operations on independent keys do not interfere, and each " +
			"register is individually linearizable through leadership changes",
		Nodes:         5,
		Faults:        Faults{LossRate: 0.15, MinDelay: 0, MaxDelay: 4},
		RequireFaults: requireDropped,

		Run: func(c *Cluster) error {
			if _, err := SettleLeader(c, 400); err != nil {
				return err
			}

			for round := range 5 {
				for k := range 4 {
					key := fmt.Sprintf("k%d", k)
					value := fmt.Sprintf("r%d", round)
					if _, err := WriteWithRetry(c, 1+k%3, key, value, 400); err != nil {
						return err
					}
				}

				// Force a leader change between rounds.
				if leader, ok, err := c.Leader(); err == nil && ok {
					c.Crash(leader)
					if err := c.TickN(120); err != nil {
						return err
					}
					if err := c.Restart(leader); err != nil {
						return err
					}
					if err := c.TickN(80); err != nil {
						return err
					}
				}
			}
			return c.TickN(400)
		},
	}
}

func TestScenarioIndependentKeys(t *testing.T) {
	runScenario(t, scenarioIndependentKeys())
}

func scenarioStaleLeaderRead() Scenario {
	return Scenario{
		Name: "client keeps reading from a leader that was partitioned away",
		Hypothesis: "a node that still believes it leads, but has lost contact with " +
			"the majority, must never answer a read; its state may be arbitrarily " +
			"behind and nothing in the reply would say so",
		Nodes:         5,
		RequireFaults: requirePartitioned,

		Run: func(c *Cluster) error {
			leader, err := SettleLeader(c, 300)
			if err != nil {
				return err
			}
			if err := Workload(c, 3, "x", "before", 2, 400); err != nil {
				return err
			}

			// Cut the leader off. It does not know yet, and a client that
			// remembers where the leader was keeps asking there.
			c.Network().Partition([]raft.NodeID{leader}, MajorityWithout(c, leader))
			if err := c.TickN(120); err != nil {
				return err
			}

			// The majority moves on without it, several times over.
			if err := Workload(c, 3, "x", "moved-on", 3, 500); err != nil {
				return err
			}

			// Now the stranded client asks the old leader. Every one of these
			// must be refused or left unanswered — an answer here is a stale
			// read, and the checker will say so.
			for range 5 {
				if _, err := ReadFromAndSettle(c, 9, leader, "x", 40); err != nil {
					return err
				}
			}

			c.Network().Heal()
			return c.TickN(400)
		},
	}
}

// allScenarios is every adversarial scenario, in report order.
//
// The individual tests and the report are built from this one list, so the
// document can never describe a scenario the suite no longer runs.
func allScenarios() []Scenario {
	return []Scenario{
		scenarioLeaderPartitioned(),
		scenarioLeaderCrash(),
		scenarioRollingRestarts(),
		scenarioLossy(),
		scenarioSplitVote(),
		scenarioMinorityPartition(),
		scenarioLeaderChurn(),
		scenarioConcurrentClients(),
		scenarioIndependentKeys(),
		scenarioStaleLeaderRead(),
	}
}

// TestChaosReport runs every scenario and writes the report the roadmap calls
// for.
//
// It is a test rather than a script so the document can never describe a state
// of the world that no longer exists: regenerating it re-runs everything, and a
// scenario that stopped holding fails here rather than quietly producing a
// stale file.
func TestScenarioStaleLeaderRead(t *testing.T) {
	runScenario(t, scenarioStaleLeaderRead())
}

func TestChaosReport(t *testing.T) {
	if testing.Short() {
		t.Skip("the full chaos report is slow; skipped under -short")
	}

	reports := make([]Report, 0, len(allScenarios()))
	for _, s := range allScenarios() {
		reports = append(reports, RunScenario(s, seeds))
	}

	passed := 0
	for _, r := range reports {
		if r.Passed() {
			passed++
		}
	}

	var b strings.Builder
	b.WriteString("# RaftKV chaos report\n\n")
	b.WriteString("Generated by `go test ./chaos -run TestChaosReport`.\n\n")
	b.WriteString("Every scenario drives a cluster of real Raft nodes over a simulated,\n")
	b.WriteString("fault-injecting network, then hands the observed client history to a\n")
	b.WriteString("linearizability checker. A scenario passes only when every node converged\n")
	b.WriteString("to identical state *and* an ordering exists that explains every result a\n")
	b.WriteString("client was given.\n\n")
	b.WriteString("Each scenario runs across several seeds. A seed fixes the entire schedule,\n")
	b.WriteString("including message delays, drops, and election timeouts, so any failure here\n")
	b.WriteString("can be reproduced exactly by re-running with that seed.\n\n")
	b.WriteString("The operation counts read ok/failed/unknown. An unknown operation is one a\n")
	b.WriteString("client never got an answer for, which is not a defect: it is the ambiguity\n")
	b.WriteString("a real client faces, and the checker is required to find an ordering that\n")
	b.WriteString("works whether or not each one took effect.\n\n")

	fmt.Fprintf(&b, "## Summary\n\n%d of %d scenarios held across all seeds.\n\n",
		passed, len(reports))

	b.WriteString("| Scenario | Seeds | Result |\n|---|---|---|\n")
	for _, r := range reports {
		result := "pass"
		if !r.Passed() {
			result = "**FAIL**"
		}
		fmt.Fprintf(&b, "| %s | %d | %s |\n", r.Scenario, len(r.Runs), result)
	}

	b.WriteString("\n## Detail\n\n")
	for _, r := range reports {
		fmt.Fprintf(&b, "```\n%s```\n\n", r)
	}

	if err := os.MkdirAll("../docs", 0o755); err != nil {
		t.Fatalf("creating the docs directory: %v", err)
	}
	if err := os.WriteFile("../docs/chaos-report.md", []byte(b.String()), 0o644); err != nil {
		t.Fatalf("writing the report: %v", err)
	}
	t.Logf("wrote docs/chaos-report.md: %d of %d scenarios held", passed, len(reports))

	if passed != len(reports) {
		t.Errorf("%d scenarios did not hold", len(reports)-passed)
	}
}
