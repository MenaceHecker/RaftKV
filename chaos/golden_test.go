package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The golden digests are the proof that adding the fault-injection switches left
// the default behaviour of the simulator untouched: every scenario, over seeds
// 1-5, must reproduce the verdict and the history digest recorded before any
// switch existed. The test runs in the default build and under the
// faultinject tag with no switch enabled, and both must match the same file.
//
// Regenerate (only when the simulator's behaviour is meant to change) with
// UPDATE_GOLDEN=1 go test ./chaos -run TestGoldenDigests.

const goldenFile = "golden_digests.txt"

var goldenSeeds = []int64{1, 2, 3, 4, 5}

func goldenLines(scenarios []Scenario) []string {
	var lines []string
	for _, s := range scenarios {
		for _, seed := range goldenSeeds {
			res := RunScenario(s, []int64{seed}).Runs[0]
			lines = append(lines, fmt.Sprintf("%s\t%d\t%s\t%s", s.Name, seed, res.Verdict(), res.Digest))
		}
	}
	return lines
}

func readGolden(t *testing.T, name string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name)) //nolint:gosec // a fixed name under testdata
	if err != nil {
		t.Fatalf("reading the golden file: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestGoldenDigests(t *testing.T) {
	got := goldenLines(AllScenarios())
	if len(got) == 0 {
		t.Fatal("no digests were produced, so this test checked nothing")
	}

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		out := strings.Join(got, "\n") + "\n"
		if err := os.WriteFile(filepath.Join("testdata", goldenFile), []byte(out), 0o600); err != nil {
			t.Fatalf("writing the golden file: %v", err)
		}
		return
	}

	want := readGolden(t, goldenFile)
	if len(want) != len(got) {
		t.Fatalf("the golden file has %d entries; this run produced %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("run %d differs from the recorded behaviour:\n  want %s\n  got  %s", i, want[i], got[i])
		}
	}
}
