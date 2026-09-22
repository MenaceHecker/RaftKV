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

// The process as an orchestrator sees it: started with flags, stopped with a
// signal, started again on the same directory.
//
// run cannot be called in process more than once, because it registers its
// flags on the default FlagSet, so the only honest way to exercise it is to
// run the binary. That is also the only way to exercise the part that matters
// most here, which is what happens on SIGTERM. Kubernetes sends one and then
// waits: a process that exits non-zero is recorded as having failed, one that
// ignores the signal is killed when the grace period runs out, and a killed
// process is exactly the case the write-ahead log has to recover from rather
// than the orderly stop it was given the chance to perform.
//
// Restarting on the same data directory is part of the same property. The
// directory is held under a lock for as long as the process lives, so a
// rolling restart only works if stopping really did release it.

const (
	// startupBudget is generous: this builds nothing, but a loaded machine
	// still has to start a process, replay a log and win an election.
	startupBudget = 30 * time.Second

	// shutdownBudget is what an orchestrator would allow. The Kubernetes
	// manifest asks for thirty seconds, and the server bounds its own
	// graceful stop at five, so anything close to thirty means the bound is
	// not working.
	shutdownBudget = 15 * time.Second
)

// buildServer compiles the binary once for the tests in this file.
func buildServer(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "raftkv-server")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the server: %v\n%s", err, out)
	}
	return bin
}

// freePort returns a port nothing is listening on.
//
// There is a race between closing this listener and the server binding it,
// which is unavoidable without having the server report the port it chose.
// It is narrow, and a collision fails loudly rather than silently.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// server is one running raftkv-server process.
type server struct {
	cmd     *exec.Cmd
	metrics string
	log     *os.File
}

// startServer launches the binary as a single-node cluster.
func startServer(t *testing.T, bin, dataDir string, raftPort, metricsPort int) *server {
	t.Helper()

	// Output goes to a file so that a failure can show what the process
	// said. A server that will not start says so, and the test should not
	// have to guess.
	log, err := os.CreateTemp(t.TempDir(), "server-*.log")
	if err != nil {
		t.Fatalf("creating a log file: %v", err)
	}

	cmd := exec.Command(bin,
		"--id", "1",
		"--peers", fmt.Sprintf("1=127.0.0.1:%d", raftPort),
		"--data-dir", dataDir,
		"--metrics-listen", fmt.Sprintf("127.0.0.1:%d", metricsPort),
		// Power loss is not what this test is about, and an fsync per append
		// makes it slow for no gain here.
		"--fsync=false",
	)
	cmd.Stdout = log
	cmd.Stderr = log

	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the server: %v", err)
	}

	return &server{cmd: cmd, metrics: fmt.Sprintf("http://127.0.0.1:%d", metricsPort), log: log}
}

// output returns everything the process has written, for a failure message.
func (s *server) output(t *testing.T) string {
	t.Helper()

	b, err := os.ReadFile(s.log.Name())
	if err != nil {
		return fmt.Sprintf("(could not read the server's output: %v)", err)
	}
	return string(b)
}

// awaitReady polls until the server reports that it can serve, which means
// every stage of startup succeeded: flags, data directory, listener, node,
// election and the metrics server this is asking through.
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

// terminate sends SIGTERM and returns how long the process took to exit.
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
	// A rolling restart is this, one pod at a time. The data directory is
	// held under a lock for the life of the process, so coming back up at
	// all is what proves the first process let go of it.
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
	// The check that an operator is most likely to need: an ID that is not in
	// the peer list is a cluster that can never form, and it has to fail at
	// startup rather than look healthy and never elect anybody.
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
