package chaos

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestSameSeedSameTrace(t *testing.T) {
	const seed = 4

	for _, s := range AllScenarios() {
		t.Run(s.Name, func(t *testing.T) {
			a := runOnce(s, seed, "")
			b := runOnce(s, seed, "")
			requireSameTrace(t, a, b)
		})
	}

	disk := []Scenario{scenarioLeaderCrash(), scenarioLossy(), scenarioWholeClusterRestarts()}
	for _, s := range disk {
		t.Run("disk/"+s.Name, func(t *testing.T) {
			dir := t.TempDir()
			a := runOnce(s, seed, filepath.Join(dir, "a"))
			b := runOnce(s, seed, filepath.Join(dir, "b"))
			requireSameTrace(t, a, b)
		})
	}
}

func requireSameTrace(t *testing.T, a, b RunResult) {
	t.Helper()

	if len(a.History) == 0 {
		t.Fatalf("the run recorded no history (err=%v)", a.Err)
	}
	if a.Digest == "" {
		t.Fatal("the run has no digest")
	}
	if !reflect.DeepEqual(a.History, b.History) {
		t.Error("the same seed produced different histories")
	}
	if a.Stats != b.Stats {
		t.Errorf("the same seed produced different stats: %+v vs %+v", a.Stats, b.Stats)
	}
	if a.Ticks != b.Ticks {
		t.Errorf("the same seed ran for %d and %d ticks", a.Ticks, b.Ticks)
	}
	if a.Digest != b.Digest {
		t.Errorf("the same seed produced digests %s and %s", a.Digest, b.Digest)
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	s := scenarioLossy()
	a := runOnce(s, 1, "")
	b := runOnce(s, 2, "")

	if a.Digest == "" || b.Digest == "" {
		t.Fatalf("a digest is empty (%q, %q)", a.Digest, b.Digest)
	}
	if a.Digest == b.Digest {
		t.Error("seeds 1 and 2 of a lossy scenario produced the same digest")
	}
}
