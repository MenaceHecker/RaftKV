//go:build faultinject

package faultinject

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestSetAndResetAreIndependent(t *testing.T) {
	t.Cleanup(Reset)
	Reset()

	Set(SkipNoop, true)
	if !Active(SkipNoop) {
		t.Fatal("Set(SkipNoop, true) did not turn the switch on")
	}
	for _, s := range All() {
		if s != SkipNoop && Active(s) {
			t.Errorf("turning on skip-noop also turned on %s", s.Name())
		}
	}

	Reset()
	if Active(SkipNoop) {
		t.Fatal("Reset left a switch on")
	}
}

func TestEnableUnknownNameEnablesNothing(t *testing.T) {
	t.Cleanup(Reset)
	Reset()

	err := Enable("skip-noop,skip-fsyncc")
	if err == nil || !strings.Contains(err.Error(), `unknown switch "skip-fsyncc"`) {
		t.Fatalf("Enable with a typo returned %v, want an unknown switch error", err)
	}
	if Active(SkipNoop) {
		t.Fatal("a list with a typo still enabled its valid names; a half-injected run is misleading")
	}

	if err := Enable("skip-noop, local-read"); err != nil {
		t.Fatalf("Enable of two valid names: %v", err)
	}
	if !Active(SkipNoop) || !Active(LocalRead) {
		t.Fatal("Enable did not turn on both named switches")
	}
}

// A typo in RAFTKV_FAULTINJECT must stop the process, or a mutated run could
// start clean and report a bug as undetected for the wrong reason.
func TestEnableUnknownNamePanics(t *testing.T) {
	if os.Getenv("FAULTINJECT_CHILD") == "1" {
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestEnableUnknownNamePanics$") //nolint:gosec // re-runs this test binary
	cmd.Env = append(os.Environ(), "FAULTINJECT_CHILD=1", EnvVar+"=skip-noop,not-a-switch")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("a process started with an unknown switch name exited cleanly:\n%s", out)
	}
	if !strings.Contains(string(out), `unknown switch "not-a-switch"`) {
		t.Fatalf("the process failed, but not for the unknown name:\n%s", out)
	}
}
