//go:build faultinject

package chaos

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/faultinject"
)

// These tests run only under the faultinject tag (RUNBOOK 8.6). The switches
// are plain booleans, so nothing here is parallel.

const resultsFile = "../docs/CHAOS_RESULTS.md"

var (
	detectMu   sync.Mutex
	detections = map[int]Detection{}
)

func detection(bug int) Detection {
	detectMu.Lock()
	defer detectMu.Unlock()

	if d, ok := detections[bug]; ok {
		return d
	}
	for _, m := range Mutations() {
		if m.Bug == bug {
			d := Detect(m)
			detections[bug] = d
			return d
		}
	}
	panic(fmt.Sprintf("no mutation %d", bug))
}

func requireDetected(t *testing.T, bug int) {
	t.Helper()

	d := detection(bug)
	if !d.Detected {
		t.Fatalf("bug %d (%s) was NOT detected: %d runs over seeds %s, %d inconclusive. First inconclusive: %s",
			d.Bug, d.Name, d.Tried, seedSpan(d.Seeds), d.Inconclusive, d.FirstInconclusive)
	}
	if !d.Oracle.Detects() {
		t.Fatalf("bug %d was recorded as detected by %s, which is not a detection", bug, d.Oracle)
	}
	t.Logf("bug %d (%s): %s, seed %d, caught by %s after %d runs: %s",
		d.Bug, d.Name, d.Scenario, d.Seed, d.Oracle, d.Tried, d.Detail)
	if d.Oracle != d.Expect {
		// A different oracle than planned is recorded, not a failure.
		t.Logf("bug %d was caught by %s; the plan named %s", d.Bug, d.Oracle, d.Expect)
	}
}

func TestMutation_SkipFsync(t *testing.T)             { requireDetected(t, 1) }
func TestMutation_TruncateOnStaleAppend(t *testing.T) { requireDetected(t, 2) }
func TestMutation_CommitPreviousTerm(t *testing.T)    { requireDetected(t, 3) }
func TestMutation_SkipVoteUpToDate(t *testing.T)      { requireDetected(t, 4) }
func TestMutation_LocalRead(t *testing.T)             { requireDetected(t, 5) }
func TestMutation_SkipDedupe(t *testing.T)            { requireDetected(t, 6) }
func TestMutation_SkipNoop(t *testing.T)              { requireDetected(t, 7) }
func TestMutation_SnapshotOmitsSessions(t *testing.T) { requireDetected(t, 8) }
func TestMutation_ApplyBeyondCommit(t *testing.T)     { requireDetected(t, 9) }

func TestMutationTableCoversTheNineSwitches(t *testing.T) {
	ms := Mutations()
	if len(ms) != 9 {
		t.Fatalf("the table has %d mutations, want 9", len(ms))
	}

	covered := map[faultinject.Switch]int{}
	for i, m := range ms {
		if m.Bug != i+1 {
			t.Errorf("entry %d is bug %d; the table must follow RUNBOOK 8.6 order", i+1, m.Bug)
		}
		if len(m.Switches) == 0 || len(m.Scenarios) == 0 || len(m.Seeds) == 0 {
			t.Errorf("bug %d has no switch, scenario or seed to search", m.Bug)
		}
		for _, s := range m.Switches {
			covered[s]++
		}
	}
	for _, s := range faultinject.All() {
		if covered[s] != 1 {
			t.Errorf("switch %s is used by %d mutations, want exactly one", s.Name(), covered[s])
		}
	}
}

func TestControlRunIsClean(t *testing.T) {
	total := 0
	for _, m := range Mutations() {
		runs, dirty := Control(m)
		total += runs
		for _, line := range dirty {
			t.Errorf("bug %d control (no switch on): %s", m.Bug, line)
		}
	}
	if total == 0 {
		t.Fatal("the control ran nothing, so it proved nothing")
	}
	t.Logf("control: %d runs, all clean", total)
}

// The suite must not be able to pass vacuously: a switch that does nothing, or
// a run that never reaches a verdict, is a bug nobody detected.
func TestSuiteReportsInertMutationUndetected(t *testing.T) {
	inert := Mutation{
		Bug: 99, Name: "an inert switch",
		Scenarios: []Scenario{scenarioRetriedWriteIsNotAppliedTwice()},
		Seeds:     seedRange(1, 3), Expect: OracleLinearizability,
	}
	d := Detect(inert)
	if d.Detected {
		t.Fatalf("a mutation with no call site was reported detected by %s", d.Oracle)
	}
	if d.Tried != 3 {
		t.Fatalf("an undetected mutation ran %d of its 3 seeds", d.Tried)
	}
	err := Verify([]Detection{d})
	if err == nil || !strings.Contains(err.Error(), "bug 99") || !strings.Contains(err.Error(), "an inert switch") {
		t.Fatalf("Verify did not name the undetected mutation: %v", err)
	}

	endsWithoutAVerdict := Mutation{
		Bug: 98, Name: "never reaches a verdict",
		Switches: []faultinject.Switch{faultinject.SkipDedupe},
		Scenarios: []Scenario{{
			Name: "ends in a harness error", Nodes: 3,
			Run: func(*Cluster) error { return errors.New("chaos: no leader within 5 ticks") },
		}},
		Seeds: seedRange(1, 4), Expect: OracleLinearizability,
	}
	d = Detect(endsWithoutAVerdict)
	if d.Detected {
		t.Fatalf("runs that ended in a harness error were counted as a detection by %s", d.Oracle)
	}
	if d.Inconclusive != 4 {
		t.Fatalf("%d of 4 runs were inconclusive; a harness error must be inconclusive", d.Inconclusive)
	}
	err = Verify([]Detection{d})
	if err == nil || !strings.Contains(err.Error(), "4 inconclusive") {
		t.Fatalf("Verify did not report the inconclusive runs: %v", err)
	}

	if err := Verify([]Detection{{Mutation: inert, Detected: true, Oracle: OracleLinearizability}}); err != nil {
		t.Fatalf("Verify rejected a detected mutation: %v", err)
	}
}

func TestDetectRestoresTheSwitches(t *testing.T) {
	Detect(Mutation{
		Bug: 97, Name: "restores", Switches: []faultinject.Switch{faultinject.SkipDedupe},
		Scenarios: []Scenario{scenarioRetriedWriteIsNotAppliedTwice()}, Seeds: seedRange(1, 1),
	})
	for _, s := range faultinject.All() {
		if faultinject.Active(s) {
			t.Errorf("switch %s is still on after Detect", s.Name())
		}
	}
}

func digestOf(res RunResult) string { return res.Verdict() + "\t" + res.Digest }

// A switch that never changes a run is not wired in: the mutated binary would
// behave exactly like the clean one and the suite would measure nothing. Each
// switch, alone, must change the verdict or history digest of some run of its
// scenarios compared with the same seed with every switch off. (That the clean
// runs themselves are unchanged from before any switch existed is the golden
// digest test.)
func TestEveryMutationSwitchChangesADigest(t *testing.T) {
	const maxSeeds = 20

	changed := map[faultinject.Switch]string{}
	for _, m := range Mutations() {
		for _, sw := range m.Switches {
		scan:
			for _, s := range m.Scenarios {
				for _, seed := range m.Seeds[:min(maxSeeds, len(m.Seeds))] {
					clean := digestOf(func() RunResult { defer enable(nil)(); return RunRecovering(s, seed) }())
					mutated := digestOf(func() RunResult { defer enable([]faultinject.Switch{sw})(); return RunRecovering(s, seed) }())
					if clean != mutated {
						changed[sw] = fmt.Sprintf("%s, seed %d", s.Name, seed)
						break scan
					}
				}
			}
		}
	}

	for _, s := range faultinject.All() {
		if changed[s] == "" {
			t.Errorf("switch %s changed no run of its scenarios over its first %d seeds", s.Name(), maxSeeds)
			continue
		}
		t.Logf("%s first changes %s", s.Name(), changed[s])
	}
}

// injectionSites finds, from the source, the function each switch is injected
// in, so the results table cannot drift from the code.
func injectionSites(t *testing.T) map[faultinject.Switch]string {
	t.Helper()

	sites := map[faultinject.Switch][]string{}
	for _, dir := range []string{"../internal/raft", "../internal/statemachine", "../internal/storage"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parsing %s: %v", path, err)
			}
			rel := strings.TrimPrefix(filepath.ToSlash(path), "../")
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if pkgID, ok := sel.X.(*ast.Ident); !ok || pkgID.Name != "faultinject" {
						return true
					}
					for _, s := range faultinject.All() {
						if strings.EqualFold(strings.ReplaceAll(s.Name(), "-", ""), sel.Sel.Name) {
							sites[s] = append(sites[s], rel+" ("+funcName(fn)+")")
						}
					}
					return true
				})
			}
		}
	}

	out := map[faultinject.Switch]string{}
	for s, list := range sites {
		seen := map[string]bool{}
		var uniq []string
		for _, l := range list {
			if !seen[l] {
				seen[l] = true
				uniq = append(uniq, l)
			}
		}
		out[s] = strings.Join(uniq, ", ")
	}
	return out
}

func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if id, ok := recv.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// TestMutationSuite is the suite of record: it searches all nine bugs, writes
// docs/CHAOS_RESULTS.md when WRITE_CHAOS_RESULTS=1 (CI regenerates it and fails
// on a difference), and is red if any bug is undetected.
func TestMutationSuite(t *testing.T) {
	var ds []Detection
	for _, m := range Mutations() {
		ds = append(ds, detection(m.Bug))
	}

	sites := injectionSites(t)
	for _, s := range faultinject.All() {
		if sites[s] == "" {
			t.Errorf("switch %s has no call site in raft, statemachine or storage", s.Name())
		}
	}

	controlRuns, pairs := 0, 0
	for _, m := range Mutations() {
		runs, dirty := Control(m)
		controlRuns += runs
		pairs += len(m.Scenarios)
		for _, line := range dirty {
			t.Errorf("bug %d control: %s", m.Bug, line)
		}
	}

	md := ResultsMarkdown(ds, sites, controlRuns, pairs)
	if os.Getenv("WRITE_CHAOS_RESULTS") == "1" {
		if err := os.WriteFile(resultsFile, []byte(md), 0o600); err != nil {
			t.Fatalf("writing %s: %v", resultsFile, err)
		}
	} else {
		have, err := os.ReadFile(resultsFile)
		if err != nil {
			t.Fatalf("reading %s: %v (regenerate with WRITE_CHAOS_RESULTS=1)", resultsFile, err)
		}
		if !bytes.Equal(have, []byte(md)) {
			t.Errorf("%s is stale; regenerate it with WRITE_CHAOS_RESULTS=1 go test -tags faultinject ./chaos -run TestMutationSuite", resultsFile)
		}
	}

	if err := Verify(ds); err != nil {
		t.Fatal(err)
	}
}

// The reproduction path: a failing seed is replayed with the CLI and the
// environment variable, and a typo in the switch name must not run clean.
func TestChaosrunReproducesAMutation(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chaosrun")
	build := exec.Command("go", "build", "-tags", "faultinject", "-o", bin, "./cmd/chaosrun") //nolint:gosec // fixed toolchain command
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building chaosrun with the tag: %v\n%s", err, out)
	}

	run := func(env string) (string, int) {
		cmd := exec.Command(bin, "-sim", "-seeds", "1-5", "-scenario", "resends a write")
		cmd.Env = append(os.Environ(), faultinject.EnvVar+"="+env)
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		switch {
		case err == nil:
			return string(out), 0
		case errors.As(err, &exit):
			return string(out), exit.ExitCode()
		}
		t.Fatalf("running chaosrun: %v", err)
		return "", -1
	}

	if out, code := run(""); code != 0 {
		t.Fatalf("chaosrun with no switch exited %d, want 0:\n%s", code, out)
	}
	out, code := run("skip-dedupe")
	if code != 1 || !strings.Contains(out, "FAIL") {
		t.Fatalf("chaosrun with skip-dedupe exited %d, want 1 with a FAIL line:\n%s", code, out)
	}
	if out, code := run("skip-dedup"); code == 0 || !strings.Contains(out, `unknown switch "skip-dedup"`) {
		t.Fatalf("a misspelt switch exited %d, want a failure naming it:\n%s", code, out)
	}
}

// The suite finds bug 4 in a catalog scenario first. The purpose-built scenario
// must also catch it on its own, so the detection does not rest on one scenario
// that happens to involve a joining node with an empty log.
func TestLaggingNodeScenarioAloneDetectsSkippedVoteCheck(t *testing.T) {
	d := Detect(Mutation{
		Bug: 96, Name: "grant votes without the up-to-date check",
		Switches:  []faultinject.Switch{faultinject.SkipVoteUpToDate},
		Scenarios: []Scenario{scenarioLaggingNodeCampaigns()},
		Seeds:     seedRange(1, 50), Expect: OracleCommittedLog,
	})
	if !d.Detected {
		t.Fatalf("the lagging-node scenario alone did not detect the skipped vote check in %d runs", d.Tried)
	}
	if d.Oracle != OracleCommittedLog {
		t.Errorf("it was caught by %s, want the committed-log monitor", d.Oracle)
	}
	t.Logf("caught at seed %d: %s", d.Seed, d.Detail)
}
