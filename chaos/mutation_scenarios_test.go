package chaos

import (
	"strings"
	"testing"
)

// The scenarios the mutation suite adds must hold on the unmutated core, or a
// detection could be the scenario's own failure.

func TestMutationScenariosAreNamedAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range MutationScenarios() {
		if s.Name == "" || s.Hypothesis == "" || s.Run == nil {
			t.Errorf("scenario %q is incomplete", s.Name)
		}
		if strings.Contains(s.Hypothesis, "not implemented") {
			t.Errorf("scenario %q is still a placeholder", s.Name)
		}
		if seen[s.Name] {
			t.Errorf("scenario name %q is used twice", s.Name)
		}
		seen[s.Name] = true
	}
	for _, s := range AllScenarios() {
		if seen[s.Name] {
			t.Errorf("mutation scenario name %q duplicates a catalog scenario", s.Name)
		}
	}
	if len(seen) != 6 {
		t.Errorf("there are %d mutation scenarios, want the 6 the nine bugs need beyond the catalog", len(seen))
	}
}

func TestScenarioPowerLossAfterAckHolds(t *testing.T) {
	runMutationScenario(t, scenarioPowerLossAfterAck())
}

func TestScenarioFigure8Holds(t *testing.T) {
	runMutationScenario(t, scenarioFigure8())
}

func TestScenarioStaleReadWindowHolds(t *testing.T) {
	runMutationScenario(t, scenarioStaleReadWindow())
}

func TestScenarioLaggingNodeCampaignsHolds(t *testing.T) {
	runMutationScenario(t, scenarioLaggingNodeCampaigns())
}

func TestScenarioReadAfterElectionHolds(t *testing.T) {
	runMutationScenario(t, scenarioReadAfterElection())
}

func TestScenarioResendAfterSnapshotInstallHolds(t *testing.T) {
	runMutationScenario(t, scenarioResendAfterSnapshotInstall())
}

func runMutationScenario(t *testing.T, s Scenario) {
	t.Helper()
	report := RunScenario(s, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	t.Logf("\n%s", report)
	for _, run := range report.Runs {
		if o := Classify(run); o != OracleNone {
			t.Errorf("seed %d: %s on the unmutated core: %v", run.Seed, o, run.Err)
		}
	}
	if !report.Passed() {
		t.Errorf("scenario %q did not hold", s.Name)
	}
}
