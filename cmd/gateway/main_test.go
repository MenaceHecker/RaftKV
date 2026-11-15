package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSecret(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const testNodes = "1=az-a=127.0.0.1:7001=127.0.0.1:9001,2=az-b=127.0.0.1:7002=127.0.0.1:9002,3=az-c=127.0.0.1:7003=127.0.0.1:9003"

func TestLoadSettingsFailsClosed(t *testing.T) {
	dir := t.TempDir()
	admin := writeSecret(t, dir, "admin", "admin-token-aaaa\n")
	node := writeSecret(t, dir, "node", "node-token-bbbb\n")
	empty := writeSecret(t, dir, "empty", "\n")
	state := filepath.Join(dir, "state")

	valid := func(mod map[string]string) []string {
		args := map[string]string{
			"--listen":           "127.0.0.1:0",
			"--nodes":            testNodes,
			"--admin-token-file": admin,
			"--node-token-file":  node,
			"--state-dir":        state,
			"--dashboard-origin": "https://dash.example.test",
		}
		for k, v := range mod {
			if v == "" {
				delete(args, k)
			} else {
				args[k] = v
			}
		}
		var out []string
		for k, v := range args {
			out = append(out, k+"="+v)
		}
		return out
	}

	if _, err := loadSettings(valid(nil)); err != nil {
		t.Fatalf("a complete command line was refused: %v", err)
	}
	cases := map[string]map[string]string{
		"no admin token flag": {"--admin-token-file": ""},
		"missing admin file":  {"--admin-token-file": filepath.Join(dir, "absent")},
		"empty admin file":    {"--admin-token-file": empty},
		"no node token flag":  {"--node-token-file": ""},
		"missing node file":   {"--node-token-file": filepath.Join(dir, "absent")},
		"empty node file":     {"--node-token-file": empty},
		"same token twice":    {"--node-token-file": admin},
		"no state dir":        {"--state-dir": ""},
		"no dashboard origin": {"--dashboard-origin": ""},
		"wildcard origin":     {"--dashboard-origin": "*"},
		"no nodes":            {"--nodes": ""},
		"malformed nodes":     {"--nodes": "1=az=nope"},
		"duplicate node ids":  {"--nodes": "1=a=127.0.0.1:7001=127.0.0.1:9001,1=b=127.0.0.1:7002=127.0.0.1:9002"},
	}
	for name, mod := range cases {
		_, err := loadSettings(valid(mod))
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), "admin-token-aaaa") || strings.Contains(err.Error(), "node-token-bbbb") {
			t.Errorf("%s: the error leaks a token: %v", name, err)
		}
	}
}

func TestHTTPServerHidesRemoteAddressesAndHasTimeouts(t *testing.T) {
	srv := newHTTPServer(":0", nil)
	if srv.ErrorLog == nil {
		t.Fatal("net/http error logs would include remote addresses; ErrorLog must be set")
	}
	if _, err := srv.ErrorLog.Writer().Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if srv.ErrorLog.Writer() != io.Discard {
		t.Fatal("ErrorLog must discard")
	}
	if srv.ReadHeaderTimeout == 0 || srv.ReadTimeout == 0 || srv.WriteTimeout == 0 || srv.IdleTimeout == 0 || srv.MaxHeaderBytes == 0 {
		t.Fatalf("the server has an unbounded setting: %+v", srv)
	}
}
