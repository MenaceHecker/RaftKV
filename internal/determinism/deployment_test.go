package determinism

import (
	"errors"
	"io"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// The deployment document calls four things in the Kubernetes manifest load
// bearing, and says so from measurement rather than reasoning: with ordered
// startup the cluster deadlocked on its first pod and stayed there. Nothing
// connects those sentences to the file they describe. Tidying the YAML, or
// copying it as a starting point for a different cluster, drops a line and
// leaves a document still promising the behaviour it bought.
//
// Three of them are constants and are checked as such. The fourth is not:
// a disruption budget has to hold a majority of whatever the replica count
// is, so five replicas need three and seven need four. Leaving it at three
// while scaling up gives a drain permission to take four of seven pods, which
// stops the cluster, and the only sign beforehand is a number that still
// looks like the one in the document.

const manifestPath = repoRoot + "/deploy/kubernetes/raftkv.yaml"

// documents decodes every YAML document in a file. It is also what says the
// manifest parses at all; kubectl would reject it otherwise, which is a
// discovery best made before a deploy rather than during one.
func documents(t *testing.T, path string) []map[string]any {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()

	var out []map[string]any
	dec := yaml.NewDecoder(f)
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("%s is not valid YAML: %v", path, err)
		}
		if doc != nil {
			out = append(out, doc)
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s holds no documents, so these checks would pass vacuously", path)
	}
	return out
}

// byKind returns the single document of a kind, failing if there is not
// exactly one.
func byKind(t *testing.T, docs []map[string]any, kind string) map[string]any {
	t.Helper()

	var found []map[string]any
	for _, d := range docs {
		if d["kind"] == kind {
			found = append(found, d)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %s in the manifest, found %d", kind, len(found))
	}
	return found[0]
}

// dig walks nested maps and lists, naming the step that failed.
func dig(t *testing.T, v any, path ...any) any {
	t.Helper()

	for i, step := range path {
		switch key := step.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%v is not a mapping, so %q cannot be read", path[:i], key)
			}
			v, ok = m[key]
			if !ok {
				t.Fatalf("%v has no %q", path[:i], key)
			}
		case int:
			l, ok := v.([]any)
			if !ok || key >= len(l) {
				t.Fatalf("%v has no element %d", path[:i], key)
			}
			v = l[key]
		}
	}
	return v
}

func TestTheStatefulSetStartsItsPodsInParallel(t *testing.T) {
	// OrderedReady starts pod N+1 only once pod N is ready, no pod is ready
	// before a leader exists, and no leader exists before a majority is
	// running. The first pod waits for a cluster that is waiting for it.
	docs := documents(t, manifestPath)
	sts := byKind(t, docs, "StatefulSet")

	if got := dig(t, sts, "spec", "podManagementPolicy"); got != "Parallel" {
		t.Errorf("podManagementPolicy = %v, want Parallel; ordered startup deadlocks on the first pod", got)
	}
}

func TestTheHeadlessServicePublishesNotReadyAddresses(t *testing.T) {
	// The same deadlock approached from DNS. Peers have to resolve each other
	// before any of them is ready, and without this the addresses they need
	// in order to become ready are exactly the ones withheld.
	docs := documents(t, manifestPath)

	var headless map[string]any
	for _, d := range docs {
		if d["kind"] != "Service" {
			continue
		}
		spec, _ := d["spec"].(map[string]any)
		if spec != nil && spec["clusterIP"] == "None" {
			headless = d
		}
	}
	if headless == nil {
		t.Fatal("the manifest has no headless service")
	}

	if got := dig(t, headless, "spec", "publishNotReadyAddresses"); got != true {
		t.Errorf("publishNotReadyAddresses = %v, want true; peers cannot resolve each other before they are ready", got)
	}
}

func TestTheProbesAskDifferentQuestions(t *testing.T) {
	// Readiness may depend on a leader. Liveness may not: during an election
	// no node has one, so a liveness probe that checked would fail on every
	// node at once and Kubernetes would restart the whole cluster.
	docs := documents(t, manifestPath)
	sts := byKind(t, docs, "StatefulSet")

	container := dig(t, sts, "spec", "template", "spec", "containers", 0)

	ready := dig(t, container, "readinessProbe", "httpGet", "path")
	live := dig(t, container, "livenessProbe", "httpGet", "path")

	if ready != "/ready" {
		t.Errorf("readinessProbe path = %v, want /ready", ready)
	}
	if live != "/health" {
		t.Errorf("livenessProbe path = %v, want /health; /ready would restart every node during an election", live)
	}
	if ready == live {
		t.Errorf("both probes ask %v, so they cannot be asking different questions", ready)
	}
}

func TestTheDisruptionBudgetHoldsAQuorum(t *testing.T) {
	// The one that is wrong the moment the replica count changes.
	docs := documents(t, manifestPath)

	replicas, ok := dig(t, byKind(t, docs, "StatefulSet"), "spec", "replicas").(int)
	if !ok {
		t.Fatal("the replica count is not a number")
	}
	minAvailable, ok := dig(t, byKind(t, docs, "PodDisruptionBudget"), "spec", "minAvailable").(int)
	if !ok {
		t.Fatal("minAvailable is not a number")
	}

	quorum := replicas/2 + 1
	if minAvailable != quorum {
		t.Errorf("minAvailable is %d for %d replicas, but a quorum is %d. "+
			"A drain would be allowed to take %d pods, which stops the cluster",
			minAvailable, replicas, quorum, replicas-minAvailable)
	}
}

func TestEveryAlertHasAnExpression(t *testing.T) {
	// An alert rule file that Prometheus refuses loads no rules at all, and
	// a cluster with no alerts looks exactly like a cluster with nothing
	// wrong. This does not evaluate the PromQL, which would need Prometheus
	// itself; it checks the structure the file must have to be loaded, and
	// that every rule carries an expression and a name.
	docs := documents(t, repoRoot+"/deploy/alerts.yml")
	if len(docs) != 1 {
		t.Fatalf("expected one document in alerts.yml, found %d", len(docs))
	}

	groups, ok := docs[0]["groups"].([]any)
	if !ok || len(groups) == 0 {
		t.Fatal("alerts.yml declares no rule groups")
	}

	total := 0
	for gi, g := range groups {
		rules, ok := dig(t, g, "rules").([]any)
		if !ok || len(rules) == 0 {
			t.Fatalf("group %d has no rules", gi)
		}
		for ri, r := range rules {
			rule, _ := r.(map[string]any)
			if name, _ := rule["alert"].(string); name == "" {
				t.Errorf("group %d rule %d has no alert name", gi, ri)
			}
			if expr, _ := rule["expr"].(string); expr == "" {
				t.Errorf("alert %v has no expression, so it can never fire", rule["alert"])
			}
			total++
		}
	}
	if total == 0 {
		t.Fatal("alerts.yml defines no alerts")
	}
	t.Logf("%d alerts checked", total)
}
