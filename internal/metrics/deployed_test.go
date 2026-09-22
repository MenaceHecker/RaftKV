package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

// A dashboard panel or an alert rule naming a metric that does not exist does
// not fail. It renders an empty graph, or it is an alert that can never fire,
// and both look exactly like a healthy cluster. That is the worst failure mode
// observability has: the thing you added in order to find out what is wrong
// tells you nothing is wrong.
//
// Nothing connects the name in a JSON dashboard to the name in Go. Renaming a
// metric compiles, passes every test, and silently blanks whatever was
// watching it. So the deployed files are checked against what a registry
// actually exports.

// metricRef matches a metric name in a config file, including the suffixes
// Prometheus appends to histogram samples.
var metricRef = regexp.MustCompile(`raftkv_[a-z0-9_]+`)

// sampleSuffixes are added by Prometheus to a histogram's samples; the family
// underneath carries the bare name.
var sampleSuffixes = []string{"_bucket", "_sum", "_count"}

func TestDeployedFilesOnlyNameMetricsThatExist(t *testing.T) {
	exported := exportedMetricNames(t)

	for _, path := range deployedFiles(t) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}

		for _, ref := range metricRef.FindAllString(string(b), -1) {
			if exported[family(ref)] {
				continue
			}
			t.Errorf("%s names %q, which no registry exports.\nExported: %v",
				path, ref, sortedKeys(exported))
		}
	}
}

func TestTheDashboardIsValidJSON(t *testing.T) {
	// Provisioning reads this file at startup. A dashboard that does not
	// parse is not provisioned, and the graphs an operator goes looking for
	// during an incident are simply absent.
	//
	// It is also what makes the check above mean what it says: the metric
	// names are found by scanning text, which would happily scan a file that
	// Grafana could never load.
	const path = "../../deploy/grafana/raftkv-dashboard.json"

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	var dashboard map[string]any
	if err := json.Unmarshal(b, &dashboard); err != nil {
		t.Fatalf("%s is not valid JSON: %v", path, err)
	}
	if len(dashboard) == 0 {
		t.Fatalf("%s parsed to nothing", path)
	}
	if _, ok := dashboard["panels"]; !ok {
		t.Errorf("%s has no panels, so it would provision as an empty dashboard", path)
	}
}

// family strips the suffix Prometheus appends to a histogram sample, leaving
// the name the registry reports.
func family(ref string) string {
	for _, suffix := range sampleSuffixes {
		if strings.HasSuffix(ref, suffix) {
			return strings.TrimSuffix(ref, suffix)
		}
	}
	return ref
}

// exportedMetricNames gathers from a registry holding everything a running
// node registers.
//
// Every recorder method is called once first. A vector with labels reports no
// family at all until some combination of labels has been observed, so a
// registry that has never been written to would look emptier than a running
// node and the check would pass by knowing nothing.
func exportedMetricNames(t *testing.T) map[string]bool {
	t.Helper()

	reg := prometheus.NewRegistry()
	m := New(reg)

	m.ObserveProposal("ok", time.Millisecond)
	m.ObserveRead("ok", time.Millisecond)
	m.ObserveApply(1, time.Millisecond)
	m.ObservePersist(1, time.Millisecond)
	m.SnapshotCreated(1, time.Millisecond)
	m.SnapshotReceived()
	m.LeaderChanged(1, 1, true)

	reg.MustRegister(NewCollector(
		fakeStatus{node.Status{
			Commit:  10,
			Applied: 10,
			Members: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}},
		}},
		fakePeers{[]transport.PeerStats{
			{ID: 2, Address: "localhost:9002", Sent: 1, Dropped: 1, Failed: 1},
		}},
	))

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}

	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}
	if len(names) == 0 {
		t.Fatal("the registry exported nothing, so this check would pass vacuously")
	}
	return names
}

// deployedFiles are the files that name metrics outside Go: the alert rules,
// the dashboard, and the document that tells an operator what to look at.
func deployedFiles(t *testing.T) []string {
	t.Helper()

	var out []string
	for _, root := range []string{"../../deploy", "../../docs"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", root, err)
		}
	}
	if len(out) == 0 {
		t.Fatal("found no deployed files, so this check would pass vacuously")
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
