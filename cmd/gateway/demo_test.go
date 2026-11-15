//go:build fakecluster

package main

import "testing"

func TestFakeClusterFlagExistsOnlyWithTheBuildTag(t *testing.T) {
	dir := t.TempDir()
	admin := writeSecret(t, dir, "admin", "admin-token-aaaa\n")
	node := writeSecret(t, dir, "node", "node-token-bbbb\n")
	s, err := loadSettings([]string{
		"--fake-cluster",
		"--listen=127.0.0.1:0", "--admin-token-file=" + admin, "--node-token-file=" + node,
		"--state-dir=" + dir + "/state", "--dashboard-origin=https://dash.example.test",
	})
	if err != nil {
		t.Fatalf("a tagged build refused --fake-cluster without --nodes: %v", err)
	}
	if !s.fakeCluster {
		t.Fatal("--fake-cluster did not set the demo cluster")
	}
}
