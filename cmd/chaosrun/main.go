// Command chaosrun runs the chaos scenarios against the simulated cluster.
//
//	chaosrun -sim -seeds 1-200
//	chaosrun -sim -seed 7 -scenario "generated nemesis"
//
// Exit status: 0 when every run passed, 1 when any run failed, errored,
// diverged or was not linearizable, 2 on a usage error. A failing run prints
// the seed and a reproduce line (RUNBOOK 8.1). Runs are sequential and in a
// fixed order, so output is stable.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/MenaceHecker/raftkv/chaos"
)

const (
	exitOK      = 0
	exitFailed  = 1
	exitUsage   = 2
	defaultSeed = "1-20"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, chaos.AllScenarios()))
}

// record is the machine-readable result of one run (-json).
type record struct {
	Scenario string `json:"scenario"`
	Seed     int64  `json:"seed"`
	Verdict  string `json:"verdict"`
	Error    string `json:"error,omitempty"`
	Ticks    int64  `json:"ticks"`
	OK       int    `json:"ok"`
	Failed   int    `json:"failed"`
	Unknown  int    `json:"unknown"`
	Digest   string `json:"digest,omitempty"`
}

func run(args []string, stdout, stderr io.Writer, scenarios []chaos.Scenario) int {
	fs := flag.NewFlagSet("chaosrun", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sim := fs.Bool("sim", false, "run against the deterministic simulator (required)")
	seedsSpec := fs.String("seeds", defaultSeed, "seeds to run: 1-200, 7 or 1,3,5-9")
	oneSeed := fs.Int64("seed", 0, "run exactly this seed (exclusive with -seeds)")
	filter := fs.String("scenario", "", "run only scenarios whose name contains this text (case-insensitive)")
	list := fs.Bool("list", false, "list the scenario names and exit")
	asJSON := fs.Bool("json", false, "print one JSON object per run on stdout; text goes to stderr")
	disk := fs.String("disk", "", "use disk-backed storage under this new or empty directory")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return exitOK
		}
		return exitUsage
	}

	usage := func(format string, a ...any) int {
		say(stderr, "chaosrun: "+format+"\n", a...)
		return exitUsage
	}

	if *list {
		for _, s := range scenarios {
			say(stdout, "%s\n", s.Name)
		}
		return exitOK
	}
	if !*sim {
		return usage("-sim is required: chaosrun only runs the simulator for now")
	}

	setFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
	if setFlags["seed"] && setFlags["seeds"] {
		return usage("-seed and -seeds are mutually exclusive")
	}

	spec := *seedsSpec
	if setFlags["seed"] {
		spec = fmt.Sprint(*oneSeed)
	}
	seeds, err := chaos.ParseSeeds(spec)
	if err != nil {
		return usage("%v", err)
	}

	var selected []chaos.Scenario
	for _, s := range scenarios {
		if strings.Contains(strings.ToLower(s.Name), strings.ToLower(*filter)) {
			selected = append(selected, s)
		}
	}
	if len(selected) == 0 {
		return usage("no scenario matches %q (see -list)", *filter)
	}

	if *disk != "" {
		if entries, err := os.ReadDir(*disk); err == nil && len(entries) > 0 {
			return usage("-disk %s is not empty; reusing old state would break determinism", *disk)
		}
	}

	text := stdout
	if *asJSON {
		text = stderr
	}
	enc := json.NewEncoder(stdout)

	runs, failures := 0, 0
	for _, s := range selected {
		for _, seed := range seeds {
			var report chaos.Report
			if *disk != "" {
				report = chaos.RunScenarioOnDisk(s, []int64{seed}, filepath.Join(*disk, dirName(s.Name)))
			} else {
				report = chaos.RunScenario(s, []int64{seed})
			}
			res := report.Runs[0]
			runs++

			if *asJSON {
				rec := record{
					Scenario: s.Name, Seed: seed, Verdict: res.Verdict(),
					Ticks: res.Ticks, OK: res.OK, Failed: res.Failed, Unknown: res.Unknown,
					Digest: res.Digest,
				}
				if res.Err != nil {
					rec.Error = res.Err.Error()
				}
				if err := enc.Encode(rec); err != nil {
					say(stderr, "chaosrun: writing JSON: %v\n", err)
					return exitFailed
				}
			}

			if res.Passed() {
				say(text, "PASS %s seed %d ticks=%d ops=%d/%d/%d\n",
					s.Name, seed, res.Ticks, res.OK, res.Failed, res.Unknown)
				continue
			}

			failures++
			say(text, "FAIL %s seed %d (%s)\n", s.Name, seed, res.Verdict())
			say(text, "reproduce: chaosrun -sim -seed %d -scenario %q\n", seed, s.Name)
			say(text, "%s", report.String())
		}
	}

	say(text, "chaosrun: %d runs, %d failed\n", runs, failures)
	if failures > 0 {
		return exitFailed
	}
	return exitOK
}

// say writes formatted output; a failed write to a closed pipe is not worth a
// different exit status.
func say(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}

// dirName makes a scenario name safe to use as a directory name.
func dirName(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '-'
		}
	}, name)
}
