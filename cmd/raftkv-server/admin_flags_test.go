package main

import (
	"flag"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/MenaceHecker/raftkv/internal/node"
	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/transport"
)

func parseOptions(t *testing.T, args ...string) options {
	t.Helper()
	var opt options
	fs := flag.NewFlagSet("raftkv-server", flag.ContinueOnError)
	defineFlags(fs, &opt)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parsing %v: %v", args, err)
	}
	return opt
}

// The admin API is off unless asked for: no listener, no token file, no chaos.
func TestAdminFlagsAreOffByDefault(t *testing.T) {
	opt := parseOptions(t)
	if opt.adminListen != "" || opt.adminTokenFile != "" || opt.adminChaos {
		t.Fatalf("admin options are not off by default: listen %q, token file %q, chaos %v",
			opt.adminListen, opt.adminTokenFile, opt.adminChaos)
	}
	token, err := validateAdminOptions(opt)
	if err != nil || token != "" {
		t.Fatalf("validateAdminOptions with the admin API off: %q, %v; want no token and no error", token, err)
	}
}

func TestAdminFlagsParseIntoOptions(t *testing.T) {
	opt := parseOptions(t, "-admin-listen=127.0.0.1:9200", "-admin-token-file=/run/token", "-admin-chaos")
	if opt.adminListen != "127.0.0.1:9200" || opt.adminTokenFile != "/run/token" || !opt.adminChaos {
		t.Fatalf("parsed %+v", opt)
	}
}

// Every unsafe combination is refused before anything starts, and no error
// message ever holds the token.
func TestAdminOptionsFailClosed(t *testing.T) {
	const secret = "s3cret-admin-token"
	dir := t.TempDir()
	write := func(name, body string, mode os.FileMode) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	good := write("good", secret+"\n", 0o600)

	refused := []struct {
		name string
		opt  options
	}{
		{"listener with no token file", options{adminListen: "127.0.0.1:0"}},
		{"listener with a missing token file", options{adminListen: "127.0.0.1:0", adminTokenFile: filepath.Join(dir, "absent")}},
		{"listener with an empty token file", options{adminListen: "127.0.0.1:0", adminTokenFile: write("empty", "", 0o600)}},
		{"listener with a group-readable token file", options{adminListen: "127.0.0.1:0", adminTokenFile: write("grp", secret, 0o640)}},
		{"listener with a world-readable token file", options{adminListen: "127.0.0.1:0", adminTokenFile: write("world", secret, 0o644)}},
		{"chaos without a listener", options{adminChaos: true, adminTokenFile: good}},
		{"token file without a listener", options{adminTokenFile: good}},
	}
	for _, tc := range refused {
		token, err := validateAdminOptions(tc.opt)
		if err == nil {
			t.Errorf("%s: accepted (token %q), want a refusal", tc.name, token)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: the error leaks the token: %v", tc.name, err)
		}
	}

	token, err := validateAdminOptions(options{adminListen: "127.0.0.1:0", adminTokenFile: good, adminChaos: true})
	if err != nil || token != secret {
		t.Fatalf("a valid configuration: %q, %v; want the token and no error", token, err)
	}
}

type discardTransport struct{}

func (discardTransport) Send([]raft.Message) {}

// buildServers builds the real servers without listening: GetServiceInfo says
// which services each one registered.
func buildServers(t *testing.T, adminOn bool) (shared, admin *grpc.Server) {
	t.Helper()
	n, err := node.Start(node.Config{
		ID:        1,
		Peers:     []raft.NodeID{1, 2, 3},
		DataDir:   t.TempDir(),
		Transport: discardTransport{},
		Sync:      storage.SyncNever,
	})
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	t.Cleanup(func() { _ = n.Stop() })

	raftServer, err := transport.NewRaftServer(n)
	if err != nil {
		t.Fatal(err)
	}
	kvServer, err := transport.NewKVServer(n, map[raft.NodeID]string{1: "a:1"})
	if err != nil {
		t.Fatal(err)
	}

	var faults *transport.FaultTransport
	if adminOn {
		faults = transport.NewFaultTransport(transport.FaultConfig{Self: 1, Inner: discardTransport{}})
	}
	shared = newSharedServer(n, raftServer, kvServer, faults)
	if adminOn {
		admin, err = newAdminServer(n, faults, "token", true)
		if err != nil {
			t.Fatal(err)
		}
	}
	return shared, admin
}

func serviceNames(srv *grpc.Server) []string {
	var names []string
	for name := range srv.GetServiceInfo() {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AdminService is registered on the admin listener only. The shared listener
// never serves it, with the admin API on or off, and the admin listener serves
// nothing else.
func TestAdminServiceIsRegisteredOnlyOnTheAdminListener(t *testing.T) {
	for _, adminOn := range []bool{false, true} {
		shared, admin := buildServers(t, adminOn)

		got := serviceNames(shared)
		want := []string{"raftkv.v1.ClusterService", "raftkv.v1.KVService", "raftkv.v1.RaftService"}
		if !slices.Equal(got, want) {
			t.Errorf("admin on=%v: the shared listener serves %v, want %v (and never AdminService)", adminOn, got, want)
		}

		if adminOn {
			if got := serviceNames(admin); !slices.Equal(got, []string{"raftkv.v1.AdminService"}) {
				t.Errorf("the admin listener serves %v, want only AdminService", got)
			}
		}
	}
}
