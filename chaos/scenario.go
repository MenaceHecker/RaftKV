package chaos

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"path/filepath"
	"sort"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

type Scenario struct {
	Name string

	Hypothesis string

	Nodes int

	Faults Faults

	Run func(c *Cluster) error

	MinOps int

	RequireFaults func(Stats) error

	// Tune adjusts the cluster configuration of every run of this scenario, for
	// a scenario that needs something the defaults do not give: a different
	// filesystem, sync policy or append size. It is called once per run, so it
	// may build per-run state such as a seeded filesystem.
	Tune func(*Config)
}

const DefaultMinOps = 8

type RunResult struct {
	Seed int64

	Linearizable Result

	Converged bool

	Stats Stats

	Ticks int64

	OK, Failed, Unknown int

	Err error

	History []Op

	Digest string
}

// RunSeeds runs every scenario over every seed, in scenario then seed order.
func RunSeeds(scenarios []Scenario, seeds []int64) []Report {
	reports := make([]Report, 0, len(scenarios))
	for _, s := range scenarios {
		reports = append(reports, RunScenario(s, seeds))
	}
	return reports
}

// historyDigest hashes a canonical text encoding of everything a run
// observed, so two runs of one seed can be compared byte for byte.
func historyDigest(history []Op, stats Stats, ticks int64) string {
	var b strings.Builder
	for _, op := range history {
		fmt.Fprintf(&b, "%d|%d|%s|%q|%t|%d|%d|%d\n",
			op.Kind, op.Client, op.Key, op.Value, op.Found, op.Invoked, op.Returned, op.Status)
	}
	fmt.Fprintf(&b, "stats|%d|%d|%d|%d|%d|%d\n",
		stats.Sent, stats.Delivered, stats.Dropped, stats.Partitions, stats.Duplicated, stats.Delayed)
	fmt.Fprintf(&b, "ticks|%d\n", ticks)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func (r RunResult) Passed() bool {
	return r.Err == nil && r.Converged && r.Linearizable.OK()
}

// Verdict names the outcome of one run: PASS, ERROR, DIVERGED, or the
// checker's verdict in upper case.
func (r RunResult) Verdict() string {
	switch {
	case r.Err != nil:
		return "ERROR"
	case !r.Converged:
		return "DIVERGED"
	case !r.Linearizable.OK():
		return strings.ToUpper(r.Linearizable.Verdict.String())
	}
	return "PASS"
}

type Report struct {
	Scenario   string
	Hypothesis string
	Runs       []RunResult
}

func (r Report) Passed() bool {
	for _, run := range r.Runs {
		if !run.Passed() {
			return false
		}
	}
	return len(r.Runs) > 0
}

func (r Report) Totals() (ok, failed, unknown int) {
	for _, run := range r.Runs {
		ok += run.OK
		failed += run.Failed
		unknown += run.Unknown
	}
	return ok, failed, unknown
}

func (r Report) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s\n", r.Scenario)
	fmt.Fprintf(&b, "  hypothesis: %s\n", r.Hypothesis)

	for _, run := range r.Runs {
		verdict := run.Verdict()

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

func RunScenario(s Scenario, seeds []int64) Report {
	return RunScenarioOnDisk(s, seeds, "")
}

func RunScenarioOnDisk(s Scenario, seeds []int64, dir string) Report {
	report := Report{Scenario: s.Name, Hypothesis: s.Hypothesis}

	for _, seed := range seeds {
		seedDir := dir
		if seedDir != "" {
			seedDir = filepath.Join(dir, fmt.Sprintf("seed-%d", seed))
		}
		report.Runs = append(report.Runs, runOnce(s, seed, seedDir))
	}
	return report
}

func runOnce(s Scenario, seed int64, dataDir string) RunResult {
	res := RunResult{Seed: seed}

	cfg := Config{
		Nodes:   s.Nodes,
		Seed:    seed,
		Faults:  s.Faults,
		DataDir: dataDir,
		Sync:    storage.SyncNever,
	}
	if s.Tune != nil {
		s.Tune(&cfg)
	}

	c, err := NewCluster(cfg)
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

	c.FailPending()

	res.Stats = c.Network().Stats()
	res.Ticks = c.Now()

	history := c.History()
	res.History = history
	res.Digest = historyDigest(history, res.Stats, res.Ticks)
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

	if res.Linearizable.Verdict == Undecided {
		res.Err = fmt.Errorf("the history could not be decided: %s", res.Linearizable)
		return res
	}

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

func SettleLeader(c *Cluster, maxTicks int) (raft.NodeID, error) {
	return c.AwaitLeader(maxTicks)
}

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

func WriteWithRetry(c *Cluster, client int, key, value string, maxTicks int) (*Op, error) {
	for range maxTicks {
		op := c.Write(client, key, value)
		if op.Status != StatusFailed {
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

func OtherThan(c *Cluster, id raft.NodeID) raft.NodeID {
	for _, other := range c.IDs() {
		if other != id {
			return other
		}
	}
	return 0
}

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
