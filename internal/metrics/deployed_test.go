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

var metricRef = regexp.MustCompile(`raftkv_[a-z0-9_]+`)

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

func family(ref string) string {
	for _, suffix := range sampleSuffixes {
		if strings.HasSuffix(ref, suffix) {
			return strings.TrimSuffix(ref, suffix)
		}
	}
	return ref
}

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
