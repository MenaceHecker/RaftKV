package chaos

import "github.com/MenaceHecker/raftkv/internal/raft"

// StepKind names one nemesis action in a generated schedule (RUNBOOK 8.4).
type StepKind int

// The step kinds a generated schedule draws from.
const (
	StepPartitionLeader StepKind = iota
	StepSplit
	StepIsolate
	StepCrash
	StepRestart
	StepSetFaults
	StepCompact
	StepHeal
	StepWork
)

// Step is one entry of a generated schedule. Node is a 1-based node id.
type Step struct {
	Kind     StepKind
	Node     raft.NodeID
	Loss     float64
	Dup      float64
	MinDelay int
	MaxDelay int
	Ticks    int
}

// Schedule derives the nemesis schedule for a seed (RUNBOOK 8.4).
func Schedule(_ int64) []Step {
	return nil
}

// Referenced until AllScenarios includes it.
var _ = scenarioGenerated

func scenarioGenerated() Scenario {
	return Scenario{
		Name:       "generated nemesis and workload",
		Hypothesis: "a schedule drawn from the seed keeps every invariant and the cluster serves a write after healing",
		Nodes:      5,
		Run: func(_ *Cluster) error {
			return errNotImplemented
		},
	}
}
