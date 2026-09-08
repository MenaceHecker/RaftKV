package metrics

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Helpers for reading values back out of a registry.
//
// They gather through the registry rather than reading the collectors
// directly, so what a test asserts on is what a Prometheus server would
// actually scrape. A metric that was built but never registered would pass a
// direct read and fail here, which is the point.

// find returns the named metric family, failing the test if it is absent.
func find(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric %q was not registered", name)
	return nil
}

// counter returns a counter's value for one value of its "result" label.
func counter(t *testing.T, reg *prometheus.Registry, name, result string) float64 {
	t.Helper()

	for _, m := range find(t, reg, name).GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "result" && l.GetValue() == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %q has no series with result=%q", name, result)
	return 0
}

// gatherValue returns the value of an unlabelled gauge or counter.
func gatherValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	metrics := find(t, reg, name).GetMetric()
	if len(metrics) != 1 {
		t.Fatalf("metric %q has %d series, want exactly 1", name, len(metrics))
	}
	m := metrics[0]
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	default:
		t.Fatalf("metric %q is neither a gauge nor a counter", name)
		return 0
	}
}

// histogramCount returns how many observations a histogram has taken.
func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()

	metrics := find(t, reg, name).GetMetric()
	if len(metrics) != 1 {
		t.Fatalf("histogram %q has %d series, want exactly 1", name, len(metrics))
	}
	return metrics[0].GetHistogram().GetSampleCount()
}

// scrape returns what an HTTP scrape of the registry produces.
func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()

	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("scrape returned %d, want 200", rec.Code)
	}
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("reading scrape body: %v", err)
	}
	return string(body)
}
