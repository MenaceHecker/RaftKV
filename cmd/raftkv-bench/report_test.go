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

// The numbers this tool prints are quoted in the README and in
// docs/benchmarks.md as measurements. Every one of them comes out of this
// arithmetic, and until now only the percentile function was tested, which is
// the one place a mistake had already been found and fixed.
//
// The rest matters for the same reason. A throughput figure that counted
// failures, or a mean taken over a different set of samples than the count it
// is printed beside, is not a number that is slightly off: it is a number
// that says the system did something it did not do, in a document that
// presents it as evidence.

// capture runs f with stdout redirected and returns what it wrote.
func capture(t *testing.T, f func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating a pipe: %v", err)
	}

	saved := os.Stdout
	os.Stdout = w

	// Read concurrently so that output larger than the pipe buffer cannot
	// deadlock the writer.
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

// line returns the value on the report line starting with the given label.
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
	// Six hundred operations in two seconds is three hundred a second. The
	// forty failures are reported, and are not operations the system
	// performed.
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
	// One sample of 1ms and one of 3ms is a mean of 2ms. Printed beside a
	// count of two, so the two have to describe the same set.
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
	// And sorting happened, so the percentiles and the max are not reading
	// the samples in arrival order.
	if got := line(t, out, "latency max"); got != "3ms" {
		t.Errorf("max = %q, want 3ms", got)
	}
	if got := line(t, out, "latency p50"); got != "1ms" {
		t.Errorf("p50 = %q, want 1ms", got)
	}
}

func TestARunThatAchievedNothingSaysSo(t *testing.T) {
	// The case that would otherwise divide by zero, and the one most worth
	// getting right: a run where the cluster refused everything must not
	// print a latency table that looks like a result.
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
		// Named for where it came from, not for a code it does not have.
		// "unknown" would differ from gRPC's Unknown only in case.
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
	// Without this the benchmark cannot find the leader, and a run against a
	// healthy cluster reports failures rather than numbers.
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
	// A redirect is retried; anything else is an error to report. Treating
	// an ordinary failure as a redirect would spin until the attempt limit
	// and then report the wrong reason.
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
