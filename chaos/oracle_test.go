package chaos

import (
	"errors"
	"fmt"
	"testing"
)

func TestOracleClassify(t *testing.T) {
	notLinearizable := Result{Verdict: NotLinearizable, Key: "x"}

	tests := []struct {
		name string
		res  RunResult
		want Oracle
	}{
		{"a clean run", RunResult{Converged: true}, OracleNone},
		{"an undecided history",
			RunResult{Converged: true, Err: errors.New("the history could not be decided: undecided after 2000000 states")},
			OracleInconclusive},
		{"too few operations",
			RunResult{Converged: true, Err: errors.New("the run produced only 3 operations that could have taken effect")},
			OracleInconclusive},
		{"a fault that never happened",
			RunResult{Converged: true, Err: fmt.Errorf("the run was not adverse: %w", errors.New("no messages were dropped"))},
			OracleInconclusive},
		{"a harness error", RunResult{Err: errors.New("chaos: no leader within 300 ticks")}, OracleInconclusive},
		{"election safety",
			RunResult{Err: fmt.Errorf("%w at tick 9: nodes 1 and 2 both lead term 3", ErrElectionSafety)},
			OracleElectionSafety},
		{"election safety wrapped by a scenario",
			RunResult{Err: fmt.Errorf("scenario: %w", fmt.Errorf("%w at tick 9", ErrElectionSafety))},
			OracleElectionSafety},
		{"the committed-log monitor",
			RunResult{Err: fmt.Errorf("%w at tick 4: node 2 reports term 3 at index 7", ErrCommittedLog)},
			OracleCommittedLog},
		{"a read that never succeeds",
			RunResult{Err: fmt.Errorf("%w within 300 ticks", ErrNotReadable)},
			OracleLiveness},
		{"a core panic", RunResult{Err: &PanicError{Value: "raft: entry 4 conflicts with committed index 5"}}, OraclePanic},
		{"a core panic wrapped", RunResult{Err: fmt.Errorf("run: %w", &PanicError{Value: "x"})}, OraclePanic},
		{"a non-linearizable history", RunResult{Converged: true, Linearizable: notLinearizable}, OracleLinearizability},
		{"a violation in a run that also had too few operations",
			RunResult{Converged: true, Linearizable: notLinearizable, Err: errors.New("the run produced only 3 operations")},
			OracleLinearizability},
		{"replicas that disagree", RunResult{Converged: false}, OracleConvergence},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.res); got != tc.want {
				t.Fatalf("Classify = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestOracleDetectsOnlyRealDetections(t *testing.T) {
	want := map[Oracle]bool{
		OracleNone:            false,
		OracleInconclusive:    false,
		OracleLinearizability: true,
		OracleConvergence:     true,
		OracleElectionSafety:  true,
		OracleCommittedLog:    true,
		OraclePanic:           true,
		OracleLiveness:        true,
	}
	for o, detects := range want {
		if o.Detects() != detects {
			t.Errorf("%s.Detects() = %v, want %v", o, o.Detects(), detects)
		}
	}
}

func TestRunRecoveringTurnsAPanicIntoAResult(t *testing.T) {
	boom := Scenario{
		Name:  "panics",
		Nodes: 3,
		Run:   func(*Cluster) error { panic("raft: entry 4 conflicts with committed index 5") },
	}

	res := RunRecovering(boom, 7)
	if res.Seed != 7 {
		t.Errorf("the result's seed is %d, want 7", res.Seed)
	}
	var pe *PanicError
	if !errors.As(res.Err, &pe) {
		t.Fatalf("the result's Err is %v, want a *PanicError", res.Err)
	}
	if got := fmt.Sprint(pe.Value); got != "raft: entry 4 conflicts with committed index 5" {
		t.Errorf("the panic value was lost: %q", got)
	}
	if Classify(res) != OraclePanic {
		t.Errorf("a recovered panic is classified %s, want %s", Classify(res), OraclePanic)
	}
}

func TestRunRecoveringRunsAnOrdinaryScenarioUnchanged(t *testing.T) {
	s := scenarioRetriedWriteIsNotAppliedTwice()
	want := RunScenario(s, []int64{3}).Runs[0]
	got := RunRecovering(s, 3)

	if got.Digest == "" || got.Digest != want.Digest {
		t.Fatalf("RunRecovering changed the run: digest %q, want %q", got.Digest, want.Digest)
	}
	if Classify(got) != OracleNone {
		t.Fatalf("a healthy scenario classifies as %s", Classify(got))
	}
}

func TestElectionSafetyErrorsCarryTheSentinel(t *testing.T) {
	c := newTestCluster(t, Config{Nodes: 3, Seed: 1})
	err := c.electionSafetyError(12, 1, 2, 5)
	if !errors.Is(err, ErrElectionSafety) {
		t.Fatalf("the error does not wrap ErrElectionSafety: %v", err)
	}
	if got := err.Error(); got != "chaos: election safety violated at tick 12: nodes 1 and 2 both lead term 5" {
		t.Fatalf("the message changed: %q", got)
	}
}
