package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/buildinfo"
)

// The server's -version is checked in a child process running the real main(),
// so the exit code and the flag package's own behaviour are both exercised.
func TestMain(m *testing.M) {
	if os.Getenv("RAFTKV_TEST_AS_MAIN") == "1" {
		args := []string{os.Args[0]}
		for i, a := range os.Args {
			if a == "--" {
				args = append(args, os.Args[i+1:]...)
			}
		}
		os.Args = args
		main()
		return
	}
	os.Exit(m.Run())
}

func runServer(t *testing.T, dir string, args ...string) (stdout string, code int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"--"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "RAFTKV_TEST_AS_MAIN=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		return out.String(), ee.ExitCode()
	}
	if err != nil {
		t.Fatal(err)
	}
	return out.String(), 0
}

func TestServerVersionFlagPrintsAndExitsZero(t *testing.T) {
	want := buildinfo.Line("raftkv-server") + "\n"
	for _, flagName := range []string{"-version", "--version"} {
		dir := t.TempDir()
		out, code := runServer(t, dir, flagName)
		if code != 0 || out != want {
			t.Fatalf("%s: exit %d, stdout %q, want exit 0 and %q", flagName, code, out, want)
		}
		if ents, _ := filepath.Glob(filepath.Join(dir, "*")); len(ents) != 0 {
			t.Fatalf("%s created files: %v", flagName, ents)
		}
	}
}

func TestServerVersionOnlyAsFirstArg(t *testing.T) {
	out, code := runServer(t, t.TempDir(), "-id=1", "-version")
	if code == 0 || bytes.Contains([]byte(out), []byte(runtime.GOOS+"/"+runtime.GOARCH)) {
		t.Fatalf("-version after another flag: exit %d, stdout %q, want a failure and no banner", code, out)
	}
}
