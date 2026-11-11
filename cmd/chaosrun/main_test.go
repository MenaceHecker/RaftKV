package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/chaos"
)

var errBoom = errors.New("injected failure")

func passing(name string) chaos.Scenario {
	return chaos.Scenario{
		Name:       name,
		Hypothesis: "a healthy cluster serves writes",
		Nodes:      3,
		Run: func(c *chaos.Cluster) error {
			if _, err := chaos.SettleLeader(c, 500); err != nil {
				return err
			}
			return chaos.Workload(c, 2, "k", "v", 5, 500)
		},
	}
}

func failingAtSeed(name string, bad int64) chaos.Scenario {
	s := passing(name)
	inner := s.Run
	s.Run = func(c *chaos.Cluster) error {
		if c.Seed() == bad {
			return errBoom
		}
		return inner(c)
	}
	return s
}

func runArgs(args []string, scenarios []chaos.Scenario) (code int, stdout, stderr string) {
	var out, errb bytes.Buffer
	code = run(args, &out, &errb, scenarios)
	return code, out.String(), errb.String()
}

func TestRunExitCodes(t *testing.T) {
	one := []chaos.Scenario{passing("alpha scenario")}

	cases := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
		wantStdout []string
		passLines  int
	}{
		{name: "missing -sim", args: []string{"-seeds", "1-2"}, wantCode: 2, wantStderr: "-sim"},
		{name: "seed zero", args: []string{"-sim", "-seeds", "0"}, wantCode: 2, wantStderr: "must be at least 1"},
		{name: "unknown scenario", args: []string{"-sim", "-scenario", "nonexistent", "-seeds", "1"}, wantCode: 2, wantStderr: "no scenario matches"},
		{name: "seed and seeds", args: []string{"-sim", "-seed", "3", "-seeds", "1-2"}, wantCode: 2, wantStderr: "mutually exclusive"},
		{name: "passing run", args: []string{"-sim", "-seeds", "1-3"}, wantCode: 0, passLines: 3},
		{name: "list", args: []string{"-list"}, wantCode: 0, wantStdout: []string{"alpha scenario"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runArgs(tc.args, one)
			if code != tc.wantCode {
				t.Fatalf("exit code %d, want %d\nstdout:\n%s\nstderr:\n%s", code, tc.wantCode, stdout, stderr)
			}
			if tc.wantStderr != "" && !strings.Contains(stderr, tc.wantStderr) {
				t.Errorf("stderr does not mention %q:\n%s", tc.wantStderr, stderr)
			}
			for _, want := range tc.wantStdout {
				if !strings.Contains(stdout, want) {
					t.Errorf("stdout does not contain %q:\n%s", want, stdout)
				}
			}
			if tc.passLines > 0 {
				if got := strings.Count(stdout, "PASS"); got != tc.passLines {
					t.Errorf("stdout has %d PASS lines, want %d:\n%s", got, tc.passLines, stdout)
				}
			}
		})
	}
}

func TestRunFailureExitsOneWithReproLine(t *testing.T) {
	scenarios := []chaos.Scenario{failingAtSeed("flaky scenario", 2)}

	code, stdout, stderr := runArgs([]string{"-sim", "-seeds", "1-3"}, scenarios)
	if code != 1 {
		t.Fatalf("exit code %d, want 1\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"FAIL", "seed 2", "reproduce: chaosrun -sim -seed 2 -scenario", "injected failure"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout)
		}
	}
	if got := strings.Count(stdout, "PASS"); got != 2 {
		t.Errorf("stdout has %d PASS lines, want 2 (seeds 1 and 3):\n%s", got, stdout)
	}
}

func TestJSONOutputOneObjectPerRun(t *testing.T) {
	code, stdout, stderr := runArgs([]string{"-sim", "-json", "-seeds", "1-2"}, []chaos.Scenario{passing("alpha scenario")})
	if code != 0 {
		t.Fatalf("exit code %d, want 0\nstderr:\n%s", code, stderr)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 2 {
		t.Fatalf("stdout has %d lines, want 2:\n%s", len(lines), stdout)
	}
	for i, line := range lines {
		var rec struct {
			Scenario string `json:"scenario"`
			Seed     int64  `json:"seed"`
			Verdict  string `json:"verdict"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i, err, line)
		}
		if rec.Scenario != "alpha scenario" || rec.Seed != int64(i+1) || rec.Verdict != "PASS" {
			t.Errorf("line %d decoded to %+v", i, rec)
		}
	}
}

func TestChaosrunAcceptance(t *testing.T) {
	// The RUNBOOK T5.6 acceptance is seeds 1-200, run in the plain `go test`
	// pass. Under -short (the pre-commit hook) a smaller range keeps the same
	// assertions without a skip. The race detector and a single processor make
	// the full range take minutes (250s on a CI runner) and starve the other
	// packages that `go test ./...` runs in parallel, which made a timing-
	// sensitive transport test fail in CI; those passes use the smoke range.
	seeds := "1-200"
	switch {
	case testing.Short():
		seeds = "1-10"
	case raceEnabled || runtime.GOMAXPROCS(0) == 1:
		seeds = "1-20"
	}

	code, stdout, stderr := runArgs([]string{"-sim", "-seeds", seeds}, chaos.AllScenarios())
	if code != 0 {
		t.Fatalf("exit code %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
}
