package chaos

import (
	"fmt"
	"sort"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Running adversarial scenarios and reporting what happened.
//
// A scenario describes a way to abuse the cluster; the runner supplies
// everything that has to be true afterwards regardless of the abuse. Keeping
// those apart matters: a scenario that also decided what counted as success
// could quietly weaken its own assertion, which is the failure mode this whole
// suite is built to avoid.
//
// Every scenario is run across several seeds. One seed exercises one
// interleaving, and passing it says only that this particular schedule was
// survivable. Several seeds is not a proof either, but it is the difference
// between testing a behaviour and testing an anecdote.

// Scenario is one adversarial situation.
type Scenario struct {
	// Name identifies the scenario in the report.
	Name string

	// Hypothesis states what the scenario is trying to break, in the terms
	// someone reading the report would want. It is required: a scenario whose
	// purpose is not stated tends to drift into testing something easier.
	Hypothesis string

	// Nodes is the cluster size, defaulting to three.
	Nodes int

	// Faults is the network's initial behaviour.
	Faults Faults

	// Run drives the cluster. It should invoke client operations and inject
	// whatever faults the scenario is about, and it must not assert
	// correctness — that is the runner's job.
	Run func(c *Cluster) error

	// MinOps is the fewest operations the run must produce, counting those
	// that definitely or possibly took effect.
	//
	// It guards against a scenario that still runs but no longer exercises
	// anything: a history of three operations is close to trivially
	// linearizable, and a scenario producing one is reporting a pass it did
	// not earn. Zero means the default.
	MinOps int

	// RequireFaults, when set, insists the run actually experienced the fault
	// it configured.
	//
	// This exists because a scenario that quietly stopped injecting anything
	// would keep passing and prove nothing, which has happened often enough in
	// this project to be worth a mechanism rather than a habit.
	RequireFaults func(Stats) error
}

// DefaultMinOps is the fewest operations a scenario must produce for its
// history to be worth checking.
//
// A handful of operations can be linearized almost regardless of what the
// system did, so a scenario that shrinks below this is no longer evidence.
const DefaultMinOps = 8

// RunResult is what one seed produced.
type RunResult struct {
	Seed int64

	// Linearizable is the checker's verdict on the observed history.
	Linearizable Result

	// Converged reports whether every live node ended with identical state.
	Converged bool

	// Stats are the network conditions the run actually experienced, which is
	// not the same as the ones it asked for.
	Stats Stats

	// Ticks is how long the run took in simulated time.
	Ticks int64

	// Ops counts operations by outcome.
	OK, Failed, Unknown int

	// Err is set when the scenario itself could not run.
	Err error
}

// Passed reports whether this run met every requirement.
func (r RunResult) Passed() bool {
	return r.Err == nil && r.Converged && r.Linearizable.OK()
}

// Report is a scenario's results across every seed.
type Report struct {
	Scenario   string
	Hypothesis string
	Runs       []RunResult
}

// Passed reports whether every run met every requirement.
func (r Report) Passed() bool {
	for _, run := range r.Runs {
		if !run.Passed() {
			return false
		}
	}
	return len(r.Runs) > 0
}

// Totals sums the operation outcomes across every run.
func (r Report) Totals() (ok, failed, unknown int) {
	for _, run := range r.Runs {
		ok += run.OK
		failed += run.Failed
		unknown += run.Unknown
	}
	return ok, failed, unknown
}

// String renders the report for a person.
//
// It leads with the conditions rather than the verdict, because a pass under
// conditions that never materialised is the thing worth noticing, and a reader
// who only sees "passed" has no way to tell.
func (r Report) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s\n", r.Scenario)
	fmt.Fprintf(&b, "  hypothesis: %s\n", r.Hypothesis)

	for _, run := range r.Runs {
		verdict := "PASS"
		switch {
		case run.Err != nil:
			verdict = "ERROR"
		case !run.Converged:
			verdict = "DIVERGED"
		case !run.Linearizable.OK():
			verdict = strings.ToUpper(run.Linearizable.Verdict.String())
		}

		fmt.Fprintf(&b,
			"  seed %-4d %-16s ticks=%-5d ops=%d/%d/%d (ok/failed/unknown) "+
				"msgs=%d dropped=%d partitioned=%d dup=%d delayed=%d\n",
			run.Seed, verdict, run.Ticks,
			run.OK, run.Failed, run.Unknown,
			run.Stats.Sent, run.Stats.Dropped, run.Stats.Partitions,
			run.Stats.Duplicated, run.Stats.Delayed)

		if run.Err != nil {
			fmt.Fprintf(&b, "    error: %v\n", run.Err)
		}
		if !run.Linearizable.OK() {
			for _, line := range strings.Split(run.Linearizable.String(), "\n") {
				if line != "" {
					fmt.Fprintf(&b, "    %s\n", line)
				}
			}
		}
	}
	return b.String()
}

// RunScenario executes a scenario once per seed and checks the results.
func RunScenario(s Scenario, seeds []int64) Report {
	report := Report{Scenario: s.Name, Hypothesis: s.Hypothesis}

	for _, seed := range seeds {
		report.Runs = append(report.Runs, runOnce(s, seed))
	}
	return report
}

// runOnce executes a scenario against one seed.
func runOnce(s Scenario, seed int64) RunResult {
	res := RunResult{Seed: seed}

	c, err := NewCluster(Config{
		Nodes:  s.Nodes,
		Seed:   seed,
		Faults: s.Faults,
	})
	if err != nil {
		res.Err = err
		return res
	}

	if err := s.Run(c); err != nil {
		res.Err = err
		res.Stats = c.Network().Stats()
		res.Ticks = c.Now()
		return res
	}

	// Anything still in flight has an unknown outcome. Calling it a failure
	// would let the checker rule out orderings that really happened.
	c.FailPending()

	res.Stats = c.Network().Stats()
	res.Ticks = c.Now()

	history := c.History()
	for _, op := range history {
		switch op.Status {
		case StatusOK:
			res.OK++
		case StatusFailed:
			res.Failed++
		default:
			res.Unknown++
		}
	}

	converged, err := c.Converged()
	if err != nil {
		res.Err = err
		return res
	}
	res.Converged = converged

	res.Linearizable = Check(history)

	// A history the checker could not decide is not evidence of anything, so
	// it is surfaced as an error rather than counted as a pass.
	if res.Linearizable.Verdict == Undecided {
		res.Err = fmt.Errorf("the history could not be decided: %s", res.Linearizable)
		return res
	}

	// Two ways a scenario can keep passing while proving nothing: it stopped
	// injecting its fault, or it stopped doing enough work for the history to
	// mean anything. Both are errors rather than quiet successes.
	minOps := s.MinOps
	if minOps == 0 {
		minOps = DefaultMinOps
	}
	if got := res.OK + res.Unknown; got < minOps {
		res.Err = fmt.Errorf(
			"the run produced only %d operations that could have taken effect, "+
				"which is too few for the history to be meaningful (want at least %d)",
			got, minOps)
		return res
	}

	if s.RequireFaults != nil {
		if err := s.RequireFaults(res.Stats); err != nil {
			res.Err = fmt.Errorf("the run was not adverse: %w", err)
		}
	}
	return res
}

// --- Helpers scenarios use to drive a cluster ---

// SettleLeader ticks until a leader exists, and reports an error if none
// appears in time.
func SettleLeader(c *Cluster, maxTicks int) (raft.NodeID, error) {
	return c.AwaitLeader(maxTicks)
}

// WriteAndSettle issues a write and ticks until it settles or the budget runs
// out.
//
// An operation left pending is not a failure: the scenario may have been
// interrupted by exactly the fault it was testing, and the checker treats an
// unresolved operation as unknown rather than as anything stronger.
func WriteAndSettle(c *Cluster, client int, key, value string, maxTicks int) (*Op, error) {
	op := c.Write(client, key, value)
	for range maxTicks {
		if op.Status != StatusPending {
			return op, nil
		}
		if err := c.Tick(); err != nil {
			return op, err
		}
	}
	return op, nil
}

// ReadAndSettle issues a read and ticks until it settles or the budget runs
// out.
func ReadAndSettle(c *Cluster, client int, key string, maxTicks int) (*Op, error) {
	op := c.Read(client, key)
	for range maxTicks {
		if op.Status != StatusPending {
			return op, nil
		}
		if err := c.Tick(); err != nil {
			return op, err
		}
	}
	return op, nil
}

// WriteWithRetry keeps trying a write until one is accepted or the budget runs
// out.
//
// Real clients retry, and a scenario whose writes all failed because there
// happened to be no leader would produce a history with nothing in it to
// check. Retrying is safe here for the same reason it is safe in production:
// commands carry a client ID and sequence number, so a duplicate is ignored.
func WriteWithRetry(c *Cluster, client int, key, value string, maxTicks int) (*Op, error) {
	for range maxTicks {
		op := c.Write(client, key, value)
		if op.Status != StatusFailed {
			// Accepted, or already settled. Let it finish.
			for range maxTicks {
				if op.Status != StatusPending {
					return op, nil
				}
				if err := c.Tick(); err != nil {
					return op, err
				}
			}
			return op, nil
		}
		if err := c.Tick(); err != nil {
			return op, err
		}
	}
	return nil, fmt.Errorf("chaos: no write was accepted within %d ticks\n%s", maxTicks, c.Dump())
}

// Workload drives several clients through rounds of writes and reads.
//
// Scenarios use it so their histories are substantial enough to be worth
// checking. A handful of operations can be linearized almost regardless of
// what the system did; it takes a sustained, overlapping workload for the
// checker's verdict to mean anything.
//
// Reads are interleaved deliberately. A history of writes alone is nearly
// always linearizable, because nothing ever observes the state — it is the
// reads that can contradict an ordering.
func Workload(c *Cluster, clients int, key, prefix string, rounds, maxTicks int) error {
	for round := range rounds {
		for client := 1; client <= clients; client++ {
			value := fmt.Sprintf("%s-r%d-c%d", prefix, round, client)
			if _, err := WriteWithRetry(c, client, key, value, maxTicks); err != nil {
				return err
			}
		}
		if _, err := ReadAndSettle(c, clients+1, key, maxTicks); err != nil {
			return err
		}
	}
	return nil
}

// ReadFromAndSettle issues a read against a specific node and ticks until it
// settles or the budget runs out.
func ReadFromAndSettle(c *Cluster, client int, node raft.NodeID, key string, maxTicks int) (*Op, error) {
	op := c.ReadFrom(client, node, key)
	for range maxTicks {
		if op.Status != StatusPending {
			return op, nil
		}
		if err := c.Tick(); err != nil {
			return op, err
		}
	}
	return op, nil
}

// OtherThan returns some node that is not the given one.
func OtherThan(c *Cluster, id raft.NodeID) raft.NodeID {
	for _, other := range c.IDs() {
		if other != id {
			return other
		}
	}
	return 0
}

// MajorityWithout splits the cluster so that one node is isolated and the rest
// stay connected, and returns the isolated node's former peers.
func MajorityWithout(c *Cluster, isolated raft.NodeID) []raft.NodeID {
	var rest []raft.NodeID
	for _, id := range c.IDs() {
		if id != isolated {
			rest = append(rest, id)
		}
	}
	sort.Slice(rest, func(i, j int) bool { return rest[i] < rest[j] })
	return rest
}
