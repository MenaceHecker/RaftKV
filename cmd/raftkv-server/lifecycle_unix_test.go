//go:build unix

package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	startupBudget = 30 * time.Second

	shutdownBudget = 15 * time.Second
)

func buildServer(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "raftkv-server")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the server: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type server struct {
	cmd     *exec.Cmd
	metrics string
	log     *os.File
}

func startServer(t *testing.T, bin, dataDir string, raftPort, metricsPort int) *server {
	t.Helper()

	log, err := os.CreateTemp(t.TempDir(), "server-*.log")
	if err != nil {
		t.Fatalf("creating a log file: %v", err)
	}

	cmd := exec.Command(bin,
		"--id", "1",
		"--peers", fmt.Sprintf("1=127.0.0.1:%d", raftPort),
		"--data-dir", dataDir,
		"--metrics-listen", fmt.Sprintf("127.0.0.1:%d", metricsPort),
		"--fsync=false",
	)
	cmd.Stdout = log
	cmd.Stderr = log

	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the server: %v", err)
	}

	return &server{cmd: cmd, metrics: fmt.Sprintf("http://127.0.0.1:%d", metricsPort), log: log}
}

func (s *server) output(t *testing.T) string {
	t.Helper()

	b, err := os.ReadFile(s.log.Name())
	if err != nil {
		return fmt.Sprintf("(could not read the server's output: %v)", err)
	}
	return string(b)
}

func (s *server) awaitReady(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(startupBudget)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.metrics + "/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the server never became ready\n%s", s.output(t))
}

func (s *server) terminate(t *testing.T) (time.Duration, error) {
	t.Helper()

	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signalling the server: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()

	start := time.Now()
	select {
	case err := <-done:
		return time.Since(start), err
	case <-time.After(shutdownBudget):
		s.cmd.Process.Kill()
		<-done
		t.Fatalf("the server did not exit within %v of SIGTERM\n%s", shutdownBudget, s.output(t))
		return 0, nil
	}
}

func TestTheServerStartsServesAndStopsOnSIGTERM(t *testing.T) {
	bin := buildServer(t)
	dataDir := filepath.Join(t.TempDir(), "data")

	s := startServer(t, bin, dataDir, freePort(t), freePort(t))
	s.awaitReady(t)

	took, err := s.terminate(t)
	if err != nil {
		t.Fatalf("SIGTERM produced a failed exit after %v: %v\n%s", took, err, s.output(t))
	}
	t.Logf("shut down in %v", took)
}

func TestTheServerCanBeRestartedOnItsOwnDataDirectory(t *testing.T) {
	bin := buildServer(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	raftPort, metricsPort := freePort(t), freePort(t)

	first := startServer(t, bin, dataDir, raftPort, metricsPort)
	first.awaitReady(t)
	if _, err := first.terminate(t); err != nil {
		t.Fatalf("the first process failed to stop: %v\n%s", err, first.output(t))
	}

	second := startServer(t, bin, dataDir, raftPort, metricsPort)
	second.awaitReady(t)
	if _, err := second.terminate(t); err != nil {
		t.Fatalf("the second process failed to stop: %v\n%s", err, second.output(t))
	}
}

func TestTheServerRefusesAnIncoherentPeerList(t *testing.T) {
	bin := buildServer(t)

	cmd := exec.Command(bin,
		"--id", "9",
		"--peers", fmt.Sprintf("1=127.0.0.1:%d", freePort(t)),
		"--data-dir", filepath.Join(t.TempDir(), "data"),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the server started with an ID that is not a member\n%s", out)
	}
	if !strings.Contains(string(out), "does not appear in -peers") {
		t.Fatalf("the failure does not say what was wrong:\n%s", out)
	}
}
