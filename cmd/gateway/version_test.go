package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/buildinfo"
)

// captureStdout runs f with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	os.Stdout = old
	_ = w.Close()
	return <-done
}

func TestGatewayVersionFlagPrintsAndExitsZero(t *testing.T) {
	want := buildinfo.Line("gateway") + "\n"
	for _, flagName := range []string{"-version", "--version"} {
		t.Chdir(t.TempDir())
		var err error
		out := captureStdout(t, func() {
			err = run(context.Background(), []string{flagName}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		})
		if err != nil {
			t.Fatalf("%s: run returned %v, want nil (no token files or --state-dir needed)", flagName, err)
		}
		if out != want {
			t.Fatalf("%s: stdout %q, want %q", flagName, out, want)
		}
		if ents, _ := filepath.Glob("*"); len(ents) != 0 {
			t.Fatalf("%s created files: %v", flagName, ents)
		}
	}
}

func TestGatewayVersionOnlyAsFirstArg(t *testing.T) {
	var err error
	out := captureStdout(t, func() {
		err = run(context.Background(), []string{"--listen=:0", "-version"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	if err == nil || bytes.Contains([]byte(out), []byte(runtime.GOOS+"/"+runtime.GOARCH)) {
		t.Fatalf("-version after another flag: err %v, stdout %q, want an error and no banner", err, out)
	}
}

func TestGatewayStillRequiresTokenFiles(t *testing.T) {
	_, err := loadSettings([]string{"--listen=127.0.0.1:0", "--state-dir=" + t.TempDir()})
	if err == nil {
		t.Fatal("loadSettings without token files succeeded, want an error")
	}
}
