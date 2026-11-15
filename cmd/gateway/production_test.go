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
