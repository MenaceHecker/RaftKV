package main

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func capture(t *testing.T, f func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating a pipe: %v", err)
	}

	saved := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	f()

	os.Stdout = saved
	w.Close()
	out := <-done
	r.Close()
	return out
}

func line(t *testing.T, out, label string) string {
	t.Helper()

	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, label) {
			return strings.TrimSpace(strings.TrimPrefix(l, label))
		}
	}
	t.Fatalf("no %q line in the report:\n%s", label, out)
	return ""
}

func TestThroughputCountsOnlySuccessfulOperations(t *testing.T) {
	r := result{
		reads:     400,
		writes:    200,
		elapsed:   2 * time.Second,
		latencies: make([]time.Duration, 600),
		errors:    map[string]int{"DeadlineExceeded": 40},
	}
	for i := range r.latencies {
		r.latencies[i] = time.Millisecond
	}

	out := capture(t, func() { r.print(&options{}) })

	if got := line(t, out, "throughput"); got != "300 ops/s" {
		t.Errorf("throughput = %q, want 300 ops/s", got)
	}
	if got := line(t, out, "operations"); got != "600 (400 reads, 200 writes)" {
		t.Errorf("operations = %q, want 600 (400 reads, 200 writes)", got)
	}
	if !strings.Contains(out, "DeadlineExceeded") {
		t.Errorf("the failures are not reported at all:\n%s", out)
	}
}

func TestTheMeanIsTakenOverEverySample(t *testing.T) {
	r := result{
		reads:     2,
		elapsed:   time.Second,
		latencies: []time.Duration{3 * time.Millisecond, time.Millisecond},
		errors:    map[string]int{},
	}

	out := capture(t, func() { r.print(&options{}) })

	if got := line(t, out, "latency mean"); got != "2ms" {
		t.Errorf("mean = %q, want 2ms", got)
	}
	if got := line(t, out, "latency max"); got != "3ms" {
		t.Errorf("max = %q, want 3ms", got)
	}
	if got := line(t, out, "latency p50"); got != "1ms" {
		t.Errorf("p50 = %q, want 1ms", got)
	}
}

func TestARunThatAchievedNothingSaysSo(t *testing.T) {
	r := result{
		elapsed: time.Second,
		errors:  map[string]int{"Unavailable": 17},
	}

	out := capture(t, func() { r.print(&options{}) })

	if !strings.Contains(out, "no successful operations") {
		t.Fatalf("a run with no successes did not say so:\n%s", out)
	}
	if !strings.Contains(out, "Unavailable") {
		t.Errorf("the reason is not reported:\n%s", out)
	}
	if strings.Contains(out, "throughput") {
		t.Errorf("a run with no successes still printed a throughput:\n%s", out)
	}
}

func TestClassifyNamesTheStatusCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", status.Error(codes.DeadlineExceeded, "too slow"), "DeadlineExceeded"},
		{"unavailable", status.Error(codes.Unavailable, "down"), "Unavailable"},
		{"not a status", errors.New("a plain error"), "local:a plain error"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classify(c.err); got != c.want {
				t.Errorf("classify(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestNotLeaderFindsTheRedirect(t *testing.T) {
	st, err := status.New(codes.FailedPrecondition, "not the leader").
		WithDetails(&raftkvv1.NotLeader{LeaderId: 2, LeaderAddress: "10.0.0.2:9001"})
	if err != nil {
		t.Fatalf("building the status: %v", err)
	}

	nl, ok := notLeader(st.Err())
	if !ok {
		t.Fatal("a redirect was not recognised")
	}
	if nl.GetLeaderAddress() != "10.0.0.2:9001" {
		t.Errorf("address = %q, want 10.0.0.2:9001", nl.GetLeaderAddress())
	}
}

func TestOtherFailuresAreNotMistakenForRedirects(t *testing.T) {
	for _, err := range []error{
		status.Error(codes.Unavailable, "no connection"),
		status.Error(codes.FailedPrecondition, "failed, but with no redirect attached"),
		errors.New("a plain error"),
	} {
		if _, ok := notLeader(err); ok {
			t.Errorf("%v was read as a redirect", err)
		}
	}
}
