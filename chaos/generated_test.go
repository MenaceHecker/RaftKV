package chaos

import (
	"reflect"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func TestAllScenariosCatalog(t *testing.T) {
	all := AllScenarios()
	if len(all) != 24 {
		t.Fatalf("the catalog has %d scenarios, want 24", len(all))
	}

	seen := map[string]bool{}
	for i, s := range all {
		if s.Name == "" {
			t.Errorf("scenario %d has no name", i)
		}
		if seen[s.Name] {
			t.Errorf("scenario name %q is used twice", s.Name)
		}
		seen[s.Name] = true
		if s.Run == nil {
			t.Errorf("scenario %q has no Run", s.Name)
		}
		if s.Hypothesis == "" {
			t.Errorf("scenario %q has no hypothesis", s.Name)
		}
	}
}

func TestScheduleIsPureFunctionOfSeed(t *testing.T) {
	a := Schedule(7)
	if len(a) == 0 {
		t.Fatal("Schedule(7) is empty")
	}
	if !reflect.DeepEqual(a, Schedule(7)) {
		t.Error("Schedule(7) differs between two calls")
	}
	if reflect.DeepEqual(a, Schedule(8)) {
		t.Error("Schedule(7) equals Schedule(8); the seed does not shape the schedule")
	}

	for _, seed := range []int64{1, 7, 8, 200} {
		s := Schedule(seed)
		const nodes = 5
		if len(s) < nodes+1 {
			t.Fatalf("Schedule(%d) has only %d steps", seed, len(s))
		}
		tail := s[len(s)-(nodes+1):]
		if tail[0].Kind != StepHeal {
			t.Errorf("Schedule(%d): step %+v before the final restarts is not a heal", seed, tail[0])
		}
		want := raft.NodeID(1)
		for i, st := range tail[1:] {
			if st.Kind != StepRestart || st.Node != want {
				t.Errorf("Schedule(%d): final step %d is %+v, want restart of node %d", seed, i, st, want)
			}
			want++
		}
	}
}

func TestGeneratedScenarioHolds(t *testing.T) {
	s := scenarioGenerated()
	if s.RequireFaults == nil {
		t.Fatal("the generated scenario does not require an adverse run")
	}

	seeds := make([]int64, 0, 20)
	for i := int64(1); i <= 20; i++ {
		seeds = append(seeds, i)
	}
	reports := RunSeeds([]Scenario{s}, seeds)
	if len(reports) != 1 {
		t.Fatalf("RunSeeds returned %d reports for one scenario", len(reports))
	}
	if len(reports[0].Runs) != len(seeds) {
		t.Fatalf("the report has %d runs, want %d", len(reports[0].Runs), len(seeds))
	}
	if !reports[0].Passed() {
		t.Fatalf("the generated scenario did not hold\n%s", reports[0])
	}

	for _, run := range reports[0].Runs {
		if len(run.History) == 0 {
			t.Errorf("seed %d recorded no history", run.Seed)
			continue
		}
		last := run.History[len(run.History)-1]
		if last.Kind != OpWrite || last.Status != StatusOK {
			t.Errorf("seed %d: the last operation is %v/%v, want a successful write after healing",
				run.Seed, last.Kind, last.Status)
		}
	}
}
