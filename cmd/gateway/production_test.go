//go:build !fakecluster

package main

import (
	"go/build"
	"strings"
	"testing"
)

// The production binary must never be able to serve fake data: the flag does
// not exist and the in-memory cluster is not even linked.
func TestProductionBuildHasNoFakeClusterFlag(t *testing.T) {
	dir := t.TempDir()
	admin := writeSecret(t, dir, "admin", "admin-token-aaaa\n")
	node := writeSecret(t, dir, "node", "node-token-bbbb\n")
	_, err := loadSettings([]string{
		"--fake-cluster",
		"--listen=127.0.0.1:0", "--admin-token-file=" + admin, "--node-token-file=" + node,
		"--state-dir=" + dir + "/state", "--dashboard-origin=https://dash.example.test",
	})
	if err == nil || !strings.Contains(err.Error(), "not defined") || !strings.Contains(err.Error(), "fake-cluster") {
		t.Fatalf("--fake-cluster in an untagged build = %v, want flag provided but not defined", err)
	}
}

func TestProductionBuildDoesNotLinkTheFakeCluster(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, imp := range pkg.Imports {
		if strings.Contains(imp, "fakecluster") {
			t.Fatalf("the untagged cmd/gateway imports %s", imp)
		}
	}
}

// Adding TLS must not add a way to the fake cluster: with every --tls-* flag in play the
// untagged binary still refuses --fake-cluster as an undefined flag.
func TestProductionBuildWithTLSFlagsStillHasNoFakeClusterFlag(t *testing.T) {
	args, _ := baseArgs(t)
	_, err := loadSettings(append(args, "--tls-domain=api.example.test", "--tls-staging", "--fake-cluster"))
	if err == nil || !strings.Contains(err.Error(), "not defined") || !strings.Contains(err.Error(), "fake-cluster") {
		t.Fatalf("--fake-cluster with TLS flags in an untagged build = %v, want flag provided but not defined", err)
	}
}
