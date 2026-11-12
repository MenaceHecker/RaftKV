package chaos

import (
	"errors"
	"fmt"
)

// Oracle names the check that flagged a run. The mutation suite needs to know
// which one fired, so it can tell a bug that was caught from a run that merely
// did not finish (RUNBOOK 8.6: "the verifier or the invariant monitor must
// flag each").
type Oracle int

// The oracles, in the order Classify prefers them.
const (
	// OracleNone means the run passed every check.
	OracleNone Oracle = iota
	// OracleLinearizability is the per-key linearizability checker (RUNBOOK 8.3).
	OracleLinearizability
	// OracleConvergence is the comparison of the replicas' state machines.
	OracleConvergence
	// OracleElectionSafety is the per-tick check that no term has two leaders.
	OracleElectionSafety
	// OracleCommittedLog is the per-tick committed-log monitor.
	OracleCommittedLog
	// OraclePanic is a core assertion that panicked.
	OraclePanic
	// OracleLiveness is the check that the cluster still serves a read.
	OracleLiveness
	// OracleInconclusive is a run that ended without a verdict: an undecided
	// history, too few operations, a fault that never happened, or a harness
	// error. It is never a detection.
	OracleInconclusive
)

func (o Oracle) String() string {
	return [...]string{
		"none", "linearizability", "convergence", "election safety",
		"committed log", "core panic", "liveness", "inconclusive",
	}[o]
}

// Detects reports whether the oracle is a real detection of a bug.
func (o Oracle) Detects() bool { return o != OracleNone && o != OracleInconclusive }

var (
	// ErrElectionSafety is wrapped by the error a run returns when two nodes
	// lead one term.
	ErrElectionSafety = errors.New("chaos: election safety violated")
	// ErrCommittedLog is wrapped by the error the committed-log monitor returns.
	ErrCommittedLog = errors.New("chaos: committed log violated")
	// ErrNotReadable is wrapped by the error AwaitReadable returns when a
	// healthy cluster does not serve a read in time.
	ErrNotReadable = errors.New("chaos: no read was served")
)

// PanicError is the Err of a run in which the core panicked.
type PanicError struct {
	Value any
}

func (e *PanicError) Error() string { return fmt.Sprintf("chaos: the core panicked: %v", e.Value) }

// Classify names the oracle that flagged a run. The order matters: a core panic
// and the monitors' sentinels are checked first because they end a run early;
// a non-linearizable history is a detection even when the run also failed a
// later bookkeeping check; any other error is a run that ended without a
// verdict.
func Classify(r RunResult) Oracle {
	var pe *PanicError
	switch {
	case errors.As(r.Err, &pe):
		return OraclePanic
	case errors.Is(r.Err, ErrElectionSafety):
		return OracleElectionSafety
	case errors.Is(r.Err, ErrCommittedLog):
		return OracleCommittedLog
	case errors.Is(r.Err, ErrNotReadable):
		return OracleLiveness
	case r.Linearizable.Verdict == NotLinearizable:
		return OracleLinearizability
	case r.Err != nil:
		return OracleInconclusive
	case !r.Converged:
		return OracleConvergence
	}
	return OracleNone
}

// RunRecovering runs one seed of a scenario and turns a panic in the core into a
// result carrying a *PanicError, so one panicking seed does not end the search
// over the rest.
func RunRecovering(s Scenario, seed int64) (res RunResult) {
	defer func() {
		if r := recover(); r != nil {
			res = RunResult{Seed: seed, Err: &PanicError{Value: r}}
		}
	}()
	return RunScenario(s, []int64{seed}).Runs[0]
}
