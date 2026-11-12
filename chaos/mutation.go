//go:build faultinject

package chaos

import (
	"fmt"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/faultinject"
)

// Mutation is one injected bug (RUNBOOK 8.6) with the scenarios and seeds that
// are searched for a run an oracle flags. A seed varies message fate and
// election timeouts only (the scenarios are scripted), so the seed range is a
// search, not a proof.
type Mutation struct {
	Bug       int
	Name      string
	Switches  []faultinject.Switch
	Scenarios []Scenario
	Seeds     []int64

	// Expect is the oracle the plan named. A different oracle catching the bug
	// is recorded, not a failure.
	Expect Oracle
}

// Detection is what searching one mutation found.
type Detection struct {
	Mutation

	Detected bool
	Oracle   Oracle

	// Scenario, Seed and Detail describe the first run an oracle flagged.
	Scenario string
	Seed     int64
	Detail   string

	// Tried counts the runs made; Inconclusive counts those that ended without a
	// verdict. An inconclusive run is never a detection.
	Tried             int
	Inconclusive      int
	FirstInconclusive string
}

func seedRange(lo, hi int64) []int64 {
	var out []int64
	for s := lo; s <= hi; s++ {
		out = append(out, s)
	}
	return out
}

func catalog(name string) Scenario {
	for _, s := range AllScenarios() {
		if strings.Contains(s.Name, name) {
			return s
		}
	}
	panic("chaos: no catalog scenario contains " + name)
}

func extra(name string) Scenario {
	for _, s := range MutationScenarios() {
		if strings.Contains(s.Name, name) {
			return s
		}
	}
	panic("chaos: no mutation scenario contains " + name)
}

// Mutations returns the nine bugs of RUNBOOK 8.6 in order.
func Mutations() []Mutation {
	return []Mutation{
		{
			Bug: 1, Name: "skip fsync before acknowledging",
			Switches:  []faultinject.Switch{faultinject.SkipFsync},
			Scenarios: []Scenario{extra("power loss after an acknowledged write")},
			Seeds:     seedRange(1, 20), Expect: OracleLinearizability,
		},
		{
			Bug: 2, Name: "truncate the log on a stale append",
			Switches: []faultinject.Switch{faultinject.TruncateOnStaleAppend},
			Scenarios: []Scenario{
				catalog("sustained loss, delay, duplication"),
				catalog("leadership repeatedly forced to move"),
			},
			Seeds: seedRange(1, 50), Expect: OracleCommittedLog,
		},
		{
			Bug: 3, Name: "commit a previous-term entry by counting replicas",
			Switches:  []faultinject.Switch{faultinject.CommitPreviousTerm},
			Scenarios: []Scenario{extra("figure 8")},
			Seeds:     seedRange(1, 50), Expect: OracleCommittedLog,
		},
		{
			Bug: 4, Name: "grant votes without the up-to-date check",
			Switches: []faultinject.Switch{faultinject.SkipVoteUpToDate},
			Scenarios: []Scenario{
				catalog("cluster split into a majority and a minority"),
				catalog("leadership repeatedly forced to move"),
				catalog("leader partitioned from the majority mid-write"),
				extra("a lagging node campaigns"),
			},
			Seeds: seedRange(1, 50), Expect: OracleCommittedLog,
		},
		{
			Bug: 5, Name: "serve a read from a deposed leader",
			Switches:  []faultinject.Switch{faultinject.LocalRead},
			Scenarios: []Scenario{extra("a read at a deposed leader")},
			Seeds:     seedRange(1, 50), Expect: OracleLinearizability,
		},
		{
			Bug: 6, Name: "skip client dedupe",
			Switches: []faultinject.Switch{faultinject.SkipDedupe},
			Scenarios: []Scenario{
				catalog("a client resends a write it never got an answer to"),
				catalog("resent writes while leadership keeps moving"),
			},
			Seeds: seedRange(1, 5), Expect: OracleLinearizability,
		},
		{
			Bug: 7, Name: "leader skips the no-op after election",
			Switches:  []faultinject.Switch{faultinject.SkipNoop},
			Scenarios: []Scenario{extra("reads after an election")},
			Seeds:     seedRange(1, 20), Expect: OracleLiveness,
		},
		{
			Bug: 8, Name: "snapshot omits the session table",
			Switches:  []faultinject.Switch{faultinject.SnapshotOmitsSessions},
			Scenarios: []Scenario{extra("a resend after a snapshot install")},
			Seeds:     seedRange(1, 20), Expect: OracleConvergence,
		},
		{
			Bug: 9, Name: "apply entries beyond commitIndex",
			Switches: []faultinject.Switch{faultinject.ApplyBeyondCommit},
			Scenarios: []Scenario{
				catalog("leader partitioned from the majority mid-write"),
				catalog("leader crashes with a write in flight"),
			},
			Seeds: seedRange(1, 50), Expect: OracleCommittedLog,
		},
	}
}

func enable(switches []faultinject.Switch) (reset func()) {
	faultinject.Reset()
	for _, s := range switches {
		faultinject.Set(s, true)
	}
	return faultinject.Reset
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	if len(line) > 200 {
		line = line[:200] + "..."
	}
	return line
}

func detail(res RunResult, o Oracle) string {
	switch {
	case res.Err != nil:
		return firstLine(res.Err.Error())
	case o == OracleLinearizability:
		return firstLine(res.Linearizable.String())
	case o == OracleConvergence:
		return "the replicas' state machines differ"
	}
	return ""
}

// Detect enables the mutation's switches and searches its scenarios and seeds,
// stopping at the first run an oracle flags. An undetected mutation runs its
// whole range. The switches are restored before it returns.
func Detect(m Mutation) Detection {
	defer enable(m.Switches)()

	d := Detection{Mutation: m}
	for _, s := range m.Scenarios {
		for _, seed := range m.Seeds {
			res := RunRecovering(s, seed)
			d.Tried++

			o := Classify(res)
			if o == OracleInconclusive {
				d.Inconclusive++
				if d.FirstInconclusive == "" {
					d.FirstInconclusive = fmt.Sprintf("%s, seed %d: %s", s.Name, seed, detail(res, o))
				}
				continue
			}
			if o.Detects() {
				d.Detected, d.Oracle = true, o
				d.Scenario, d.Seed, d.Detail = s.Name, seed, detail(res, o)
				return d
			}
		}
	}
	return d
}

// Control runs a mutation's scenarios and seeds with every switch off and
// returns one line for each run that was not clean. A suite whose control is
// not clean proves nothing, because a detection could be the scenario's own
// failure.
func Control(m Mutation) (runs int, dirty []string) {
	defer enable(nil)()

	for _, s := range m.Scenarios {
		for _, seed := range m.Seeds {
			runs++
			res := RunRecovering(s, seed)
			if o := Classify(res); o != OracleNone {
				dirty = append(dirty, fmt.Sprintf("%s, seed %d: %s: %s", s.Name, seed, o, detail(res, o)))
			}
		}
	}
	return runs, dirty
}

func seedSpan(seeds []int64) string {
	if len(seeds) == 0 {
		return "none"
	}
	return fmt.Sprintf("%d-%d", seeds[0], seeds[len(seeds)-1])
}

// Verify returns an error naming every mutation no oracle flagged, so a suite
// with an undetected bug is red. A mutation that was never injected cannot be
// detected, which is why a run that ended without a verdict does not count.
func Verify(ds []Detection) error {
	var missed []string
	for _, d := range ds {
		if d.Detected {
			continue
		}
		msg := fmt.Sprintf("bug %d (%s) was not detected: %d runs over seeds %s, %d inconclusive",
			d.Bug, d.Name, d.Tried, seedSpan(d.Seeds), d.Inconclusive)
		if d.FirstInconclusive != "" {
			msg += " (first: " + d.FirstInconclusive + ")"
		}
		missed = append(missed, msg)
	}
	if len(missed) > 0 {
		return fmt.Errorf("%d of %d injected bugs went undetected:\n  %s",
			len(missed), len(ds), strings.Join(missed, "\n  "))
	}
	return nil
}

// ResultsMarkdown renders docs/CHAOS_RESULTS.md. sites names where each switch
// is injected; controlRuns and controlScenarios describe the clean control.
// There are no timestamps or timings, so regenerating it changes nothing unless
// a result changed.
func ResultsMarkdown(ds []Detection, sites map[faultinject.Switch]string, controlRuns, controlScenarios int) string {
	var b strings.Builder
	detected := 0
	for _, d := range ds {
		if d.Detected {
			detected++
		}
	}

	fmt.Fprintf(&b, "# Chaos results: the mutation suite\n\n")
	fmt.Fprintf(&b, "Generated by `WRITE_CHAOS_RESULTS=1 go test -tags faultinject ./chaos -run TestMutationSuite`. Do not edit by hand: CI regenerates it and fails on a difference.\n\n")
	fmt.Fprintf(&b, "**%d of %d injected bugs detected** (RUNBOOK 8.6).\n\n", detected, len(ds))
	fmt.Fprintf(&b, "Each bug is a build-tagged switch (`internal/faultinject`). A bug counts as detected when an oracle flags a run: the linearizability checker, the replica convergence check, the election-safety check, the committed-log monitor, a core assertion (panic), or the read-liveness check. A run that ends without a verdict (an undecided history, too few operations, a fault that never happened, a harness error) is inconclusive and never counts. The scenarios are scripted, so a seed varies message fate and election timeouts only: the seed range is a search, not a proof.\n\n")
	fmt.Fprintf(&b, "| # | bug | switch | injected at | scenario | seeds tried | first seed | oracle | planned oracle |\n")
	fmt.Fprintf(&b, "|---|-----|--------|-------------|----------|-------------|------------|--------|----------------|\n")
	for _, d := range ds {
		var names []string
		var at []string
		for _, s := range d.Switches {
			names = append(names, "`"+s.Name()+"`")
			at = append(at, sites[s])
		}
		scenario, first, oracle := "-", "-", "UNDETECTED"
		if d.Detected {
			scenario, first, oracle = d.Scenario, fmt.Sprint(d.Seed), d.Oracle.String()
		}
		fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %d (seeds %s) | %s | %s | %s |\n",
			d.Bug, d.Name, strings.Join(names, ", "), strings.Join(at, "; "), scenario,
			d.Tried, seedSpan(d.Seeds), first, oracle, d.Expect)
	}

	fmt.Fprintf(&b, "\n`seeds tried` is the number of runs made before the first detection, or all of them when the bug was not detected. `planned oracle` is the oracle the task card expected; a different oracle catching the bug is recorded, not a failure.\n\n")
	fmt.Fprintf(&b, "## What each detection saw\n\n")
	for _, d := range ds {
		if d.Detected {
			fmt.Fprintf(&b, "- **%d. %s**: %s, seed %d, %s. %s\n", d.Bug, d.Name, d.Scenario, d.Seed, d.Oracle, d.Detail)
			continue
		}
		fmt.Fprintf(&b, "- **%d. %s**: not detected after %d runs, %d inconclusive. %s\n",
			d.Bug, d.Name, d.Tried, d.Inconclusive, d.FirstInconclusive)
	}

	fmt.Fprintf(&b, "\n## Control\n\n")
	fmt.Fprintf(&b, "With every switch off, the same scenarios over the same seed ranges (%d runs across %d scenario and mutation pairs) are clean: no oracle fires and none is inconclusive. A detection above is therefore the injected bug, not the scenario's own failure.\n", controlRuns, controlScenarios)
	return b.String()
}
