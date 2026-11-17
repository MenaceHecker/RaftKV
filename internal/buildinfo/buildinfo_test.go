package buildinfo

import (
	"bytes"
	"runtime"
	"testing"
)

func setVars(t *testing.T, v, c, d string) {
	t.Helper()
	ov, oc, od := Version, Commit, Date
	Version, Commit, Date = v, c, d
	t.Cleanup(func() { Version, Commit, Date = ov, oc, od })
}

func TestLineContainsVersionCommitDateAndPlatform(t *testing.T) {
	setVars(t, "v1.2.3-4-gabcdef012345", "abcdef0123456789abcdef0123456789abcdef01", "2026-11-17T10:00:00Z")
	want := "raftkv-server v1.2.3-4-gabcdef012345 abcdef0123456789abcdef0123456789abcdef01 2026-11-17T10:00:00Z " +
		runtime.GOOS + "/" + runtime.GOARCH + " " + runtime.Version()
	if got := Line("raftkv-server"); got != want {
		t.Fatalf("Line = %q, want %q", got, want)
	}
}

func TestLineDefaults(t *testing.T) {
	setVars(t, "dev", "unknown", "unknown")
	want := "gateway dev unknown unknown " + runtime.GOOS + "/" + runtime.GOARCH + " " + runtime.Version()
	if got := Line("gateway"); got != want {
		t.Fatalf("Line = %q, want %q", got, want)
	}
}

func TestHandleFlagPrintsOnlyForExactFirstArg(t *testing.T) {
	setVars(t, "dev", "unknown", "unknown")
	want := Line("p") + "\n"
	for _, args := range [][]string{{"-version"}, {"--version"}, {"-version", "--listen=:0"}} {
		var buf bytes.Buffer
		if !HandleFlag("p", args, &buf) {
			t.Fatalf("HandleFlag(%v) = false, want true", args)
		}
		if buf.String() != want {
			t.Fatalf("HandleFlag(%v) wrote %q, want %q", args, buf.String(), want)
		}
	}
	for _, args := range [][]string{nil, {}, {"--listen=:0", "-version"}, {"-versions"}, {"--version=1"}, {"version"}, {"-v"}} {
		var buf bytes.Buffer
		if HandleFlag("p", args, &buf) {
			t.Fatalf("HandleFlag(%v) = true, want false", args)
		}
		if buf.Len() != 0 {
			t.Fatalf("HandleFlag(%v) wrote %q, want nothing", args, buf.String())
		}
	}
}
