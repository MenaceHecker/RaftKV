package main

import (
	"testing"
	"time"
)

// Tests for the benchmark's arithmetic.
//
// Every performance figure in this repository came out of this tool, which
// makes it an instrument rather than a convenience: if it computes a
// percentile slightly wrong, every number quoted from it is slightly wrong in
// the same direction and nothing else in the project would notice. It had no
// tests at all until these, which is a poor position from which to publish
// measurements.

func durations(ms ...int) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, m := range ms {
		out[i] = time.Duration(m) * time.Millisecond
	}
	return out
}

func TestPercentileUsesNearestRank(t *testing.T) {
	// Ten samples, 1ms through 10ms. Nearest rank puts the median at the
	// fifth, not the sixth: ceil(0.5*10) = 5.
	sorted := durations(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	for _, tc := range []struct {
		name string
		f    float64
		want int
	}{
		{"p50 lands on a whole rank", 0.50, 5},
		{"p90 lands on a whole rank", 0.90, 9},
		{"p99 rounds up to the last", 0.99, 10},
		{"p100 is the maximum", 1.00, 10},
		{"p10 is the first", 0.10, 1},
		{"a fractional rank rounds up", 0.25, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := percentile(sorted, tc.f)
			if want := time.Duration(tc.want) * time.Millisecond; got != want {
				t.Errorf("percentile(%.2f) = %v, want %v", tc.f, got, want)
			}
		})
	}
}

func TestPercentileOfASingleSample(t *testing.T) {
	one := durations(7)
	for _, f := range []float64{0, 0.5, 0.99, 1} {
		if got := percentile(one, f); got != 7*time.Millisecond {
			t.Errorf("percentile(%.2f) of one sample = %v, want 7ms", f, got)
		}
	}
}

func TestPercentileOfNothingIsZero(t *testing.T) {
	// A run that recorded nothing must report zero rather than reaching past
	// the end of an empty slice.
	if got := percentile(nil, 0.99); got != 0 {
		t.Errorf("percentile of an empty slice = %v, want 0", got)
	}
}

func TestPercentileNeverReadsOutOfRange(t *testing.T) {
	// The fraction comes from a table in this file today, but the guard
	// matters more than where the value came from: an index past the end
	// would panic in the middle of reporting results, losing the whole run.
	sorted := durations(1, 2, 3)
	for _, f := range []float64{-1, 0, 0.5, 1, 2, 1e9} {
		got := percentile(sorted, f)
		if got < time.Millisecond || got > 3*time.Millisecond {
			t.Errorf("percentile(%v) = %v, outside the sample range", f, got)
		}
	}
}
