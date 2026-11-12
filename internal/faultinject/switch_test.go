package faultinject

import "testing"

func TestSwitchNamesAreNineAndUnique(t *testing.T) {
	want := []string{
		"skip-fsync",
		"truncate-on-stale-append",
		"commit-previous-term",
		"skip-vote-uptodate",
		"local-read",
		"skip-dedupe",
		"skip-noop",
		"snapshot-omits-sessions",
		"apply-beyond-commit",
	}

	all := All()
	if len(all) != 9 {
		t.Fatalf("All() has %d switches, want the 9 of RUNBOOK 8.6", len(all))
	}

	seen := map[string]bool{}
	for i, s := range all {
		name := s.Name()
		if name != want[i] {
			t.Errorf("switch %d is named %q, want %q", i+1, name, want[i])
		}
		if seen[name] {
			t.Errorf("switch name %q is used twice", name)
		}
		seen[name] = true

		back, ok := Lookup(name)
		if !ok || back != s {
			t.Errorf("Lookup(%q) = %v, %v; want %v", name, back, ok, s)
		}
	}
}

func TestLookupRejectsAnUnknownName(t *testing.T) {
	if _, ok := Lookup("skip-fsyncc"); ok {
		t.Fatal("Lookup accepted a misspelt name")
	}
	if got := Switch(99).Name(); got != "unknown" {
		t.Fatalf("an out-of-range switch is named %q, want %q", got, "unknown")
	}
}
