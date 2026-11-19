package infradrift

import (
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The deployment (infra/modules/compute) renders systemd units whose ExecStart lines name
// command line flags of cmd/raftkv-server and cmd/gateway. A flag that is renamed or removed in
// Go would otherwise only show up when a node fails to start on a real instance. These tests
// read the flag names from the Go source of the real binaries (so nothing here changes the
// binaries) and fail on drift that matters: a unit flag the binary does not define, or a token
// file flag the binary requires that the unit leaves out.

const (
	nodeUnitTemplate    = "infra/modules/compute/raftkvd.service.tftpl"
	gatewayUnitTemplate = "infra/modules/compute/gateway.service.tftpl"
	fetchScript         = "infra/scripts/fetch-gateway-tokens.sh"
	pushScript          = "infra/scripts/push-node-tokens.sh"
)

// requiredTokenFlags are the token file flags each binary insists on in this deployment. The
// gateway's two are required by loadSettings (it exits without them); the node's admin token is
// required by --admin-listen, and --cluster-token-file is optional in the binary but the
// deployment refuses to run without it (docs/gateway.md: the KV and Raft ports are
// unauthenticated, so AddNode and RemoveNode must be token protected).
var requiredTokenFlags = map[string][]string{
	"raftkv-server": {"admin-token-file", "cluster-token-file"},
	"gateway":       {"admin-token-file", "node-token-file"},
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %s has no go.mod: %v", root, err)
	}
	return root
}

// flagMethods maps the flag.FlagSet registration methods to the position of the flag name.
var flagMethods = map[string]int{
	"BoolVar": 1, "DurationVar": 1, "Float64Var": 1, "IntVar": 1, "Int64Var": 1,
	"StringVar": 1, "UintVar": 1, "Uint64Var": 1, "TextVar": 1, "Var": 1,
	"Bool": 0, "Duration": 0, "Float64": 0, "Int": 0, "Int64": 0,
	"String": 0, "Uint": 0, "Uint64": 0, "Func": 0, "BoolFunc": 0,
}

// definedFlags returns every flag name registered on a FlagSet (receiver fs, flags or flag) by
// the non-test Go files of dir that the default build includes (the gateway's fakecluster demo
// flag lives behind a build tag and is not part of the production binary).
func definedFlags(t *testing.T, dir string) map[string]bool {
	t.Helper()
	pkg, err := build.Default.ImportDir(dir, 0)
	if err != nil {
		t.Fatalf("reading package %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, name := range pkg.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, ok := sel.X.(*ast.Ident)
			if !ok || (recv.Name != "fs" && recv.Name != "flags" && recv.Name != "flag") {
				return true
			}
			idx, ok := flagMethods[sel.Sel.Name]
			if !ok || len(call.Args) <= idx {
				return true
			}
			lit, ok := call.Args[idx].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if s, err := strconv.Unquote(lit.Value); err == nil {
				out[s] = true
			}
			return true
		})
	}
	return out
}

// execStart returns the unit's ExecStart= command with line continuations joined.
func execStart(unit string) (string, error) {
	joined := strings.ReplaceAll(unit, "\\\n", " ")
	for _, line := range strings.Split(joined, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "ExecStart="); ok {
			return rest, nil
		}
	}
	return "", errors.New("no ExecStart= line")
}

// unitFlags returns flag name to value for every --flag on the ExecStart line. The value is the
// text after '=' or else the next word, unless that word is itself a flag.
func unitFlags(unit string) (map[string]string, error) {
	cmd, err := execStart(unit)
	if err != nil {
		return nil, err
	}
	words := strings.Fields(cmd)
	flags := map[string]string{}
	for i, w := range words {
		name, ok := strings.CutPrefix(w, "--")
		if !ok {
			continue
		}
		val := ""
		if n, v, hasEq := strings.Cut(name, "="); hasEq {
			name, val = n, v
		} else if i+1 < len(words) && !strings.HasPrefix(words[i+1], "--") {
			val = words[i+1]
		}
		flags[name] = val
	}
	return flags, nil
}

// checkUnit returns every drift problem between a unit and a binary's flag set.
func checkUnit(unit string, defined map[string]bool, required []string) []string {
	flags, err := unitFlags(unit)
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	for name := range flags {
		if !defined[name] {
			problems = append(problems, fmt.Sprintf("unit uses --%s, which the binary does not define", name))
		}
	}
	for _, name := range required {
		if !defined[name] {
			problems = append(problems, fmt.Sprintf("required flag --%s is not defined by the binary any more", name))
		}
		if v, ok := flags[name]; !ok || v == "" {
			problems = append(problems, fmt.Sprintf("unit does not pass the required --%s <file>", name))
		}
	}
	sort.Strings(problems)
	return problems
}

func readFile(t *testing.T, root, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, rel)) //nolint:gosec // the test reads fixed repo files named by constants
	if err != nil {
		t.Fatalf("reading %s: %v", rel, err)
	}
	return string(b)
}

func TestFlagSetDetectionSeesTheRealFlags(t *testing.T) {
	root := repoRoot(t)
	server := definedFlags(t, filepath.Join(root, "cmd", "raftkv-server"))
	gateway := definedFlags(t, filepath.Join(root, "cmd", "gateway"))
	for _, f := range []string{"id", "peers", "listen", "data-dir", "fsync", "admin-listen", "admin-token-file", "admin-chaos", "cluster-token-file", "metrics-listen"} {
		if !server[f] {
			t.Errorf("raftkv-server flag detection missed --%s: the AST scan is broken or the flag was removed", f)
		}
	}
	for _, f := range []string{"listen", "nodes", "admin-token-file", "node-token-file", "state-dir", "dashboard-origin", "tls-domain", "tls-http-listen", "tls-cache-dir", "tls-staging"} {
		if !gateway[f] {
			t.Errorf("gateway flag detection missed --%s: the AST scan is broken or the flag was removed", f)
		}
	}
	if gateway["fake-cluster"] {
		t.Error("the fakecluster-only --fake-cluster must not count as a production gateway flag")
	}
}

func TestNodeUnitFlagsExistInRaftkvServer(t *testing.T) {
	root := repoRoot(t)
	defined := definedFlags(t, filepath.Join(root, "cmd", "raftkv-server"))
	unit := readFile(t, root, nodeUnitTemplate)
	if problems := checkUnit(unit, defined, requiredTokenFlags["raftkv-server"]); len(problems) > 0 {
		t.Fatalf("%s drifted from cmd/raftkv-server:\n  %s", nodeUnitTemplate, strings.Join(problems, "\n  "))
	}
	flags, _ := unitFlags(unit)
	// The admin API fails closed without a token, and --admin-chaos means nothing without it.
	if _, ok := flags["admin-chaos"]; ok {
		if _, ok := flags["admin-listen"]; !ok {
			t.Error("--admin-chaos without --admin-listen: the binary refuses to start")
		}
	}
	if _, ok := flags["admin-listen"]; ok && flags["admin-token-file"] == "" {
		t.Error("--admin-listen without --admin-token-file: the binary refuses to start")
	}
	// Durability: never turn fsync off by accident (RUNBOOK 7.3).
	if v, ok := flags["fsync"]; ok && v != "true" {
		t.Errorf("--fsync=%q: the WAL must be fsynced", v)
	}
}

func TestGatewayUnitFlagsExistInGateway(t *testing.T) {
	root := repoRoot(t)
	defined := definedFlags(t, filepath.Join(root, "cmd", "gateway"))
	unit := readFile(t, root, gatewayUnitTemplate)
	if problems := checkUnit(unit, defined, requiredTokenFlags["gateway"]); len(problems) > 0 {
		t.Fatalf("%s drifted from cmd/gateway:\n  %s", gatewayUnitTemplate, strings.Join(problems, "\n  "))
	}
	flags, _ := unitFlags(unit)
	// validateTLS refuses --tls-* without --tls-domain, and the gateway needs --state-dir and --nodes.
	for _, need := range []string{"tls-domain", "state-dir", "nodes", "dashboard-origin"} {
		if flags[need] == "" {
			t.Errorf("gateway unit does not pass --%s", need)
		}
	}
}

// The token files named by the units, the files the scripts write, and the files a node unit
// waits for (ConditionPathExists) must be the same paths, or a node never starts, or starts
// without the token it was meant to have.
func TestTokenPathsAgreeBetweenUnitsAndScripts(t *testing.T) {
	root := repoRoot(t)
	nodeUnit := readFile(t, root, nodeUnitTemplate)
	nodeFlags, err := unitFlags(nodeUnit)
	if err != nil {
		t.Fatal(err)
	}
	var conditions []string
	for _, line := range strings.Split(nodeUnit, "\n") {
		if p, ok := strings.CutPrefix(strings.TrimSpace(line), "ConditionPathExists="); ok {
			conditions = append(conditions, p)
		}
	}
	sort.Strings(conditions)
	want := []string{nodeFlags["admin-token-file"], nodeFlags["cluster-token-file"]}
	sort.Strings(want)
	if strings.Join(conditions, ",") != strings.Join(want, ",") || want[0] == "" {
		t.Errorf("node unit ConditionPathExists %v must be exactly its two token files %v (fail closed, no crash loop)", conditions, want)
	}
	push := readFile(t, root, pushScript)
	for _, p := range want {
		if !strings.Contains(push, filepath.Base(p)) {
			t.Errorf("%s never writes %s", pushScript, filepath.Base(p))
		}
		if filepath.Dir(p) != "/etc/raftkv" {
			t.Errorf("token file %s is not under /etc/raftkv, where %s writes", p, pushScript)
		}
	}
	if !strings.Contains(push, "/etc/raftkv") {
		t.Errorf("%s does not default to /etc/raftkv", pushScript)
	}

	gwUnit := readFile(t, root, gatewayUnitTemplate)
	gwFlags, err := unitFlags(gwUnit)
	if err != nil {
		t.Fatal(err)
	}
	fetch := readFile(t, root, fetchScript)
	var runtimeDir string
	for _, line := range strings.Split(gwUnit, "\n") {
		if d, ok := strings.CutPrefix(strings.TrimSpace(line), "RuntimeDirectory="); ok {
			runtimeDir = "/run/" + d
		}
	}
	if runtimeDir == "" {
		t.Fatal("gateway unit has no RuntimeDirectory= (the tokens must live on tmpfs)")
	}
	for _, f := range requiredTokenFlags["gateway"] {
		p := gwFlags[f]
		if filepath.Dir(p) != runtimeDir {
			t.Errorf("gateway --%s %s is not inside its RuntimeDirectory %s (tmpfs, never on disk)", f, p, runtimeDir)
		}
		if !strings.Contains(fetch, filepath.Base(p)) {
			t.Errorf("%s never writes %s", fetchScript, filepath.Base(p))
		}
	}
	if !strings.Contains(fetch, runtimeDir) {
		t.Errorf("%s does not default to %s", fetchScript, runtimeDir)
	}
	if !strings.Contains(gwUnit, "ExecStartPre=+/usr/local/sbin/fetch-gateway-tokens.sh") {
		t.Error("gateway unit does not run the token fetch script as ExecStartPre=+")
	}
}

// The detector itself: it has to fail on drift, or the tests above prove nothing.
func TestDriftDetectorCatchesDrift(t *testing.T) {
	defined := map[string]bool{"id": true, "listen": true, "admin-token-file": true, "cluster-token-file": true}
	required := []string{"admin-token-file", "cluster-token-file"}
	good := "[Service]\nExecStart=/usr/local/bin/x \\\n  --id 1 \\\n  --listen :7000 \\\n  --admin-token-file /a \\\n  --cluster-token-file=/b\n"
	if p := checkUnit(good, defined, required); len(p) != 0 {
		t.Fatalf("clean unit reported drift: %v", p)
	}
	for name, tc := range map[string]struct{ unit, want string }{
		"unknown flag":        {strings.Replace(good, "--listen :7000", "--listen :7000 \\\n  --bogus 1", 1), "--bogus, which the binary does not define"},
		"renamed flag":        {strings.Replace(good, "--admin-token-file /a", "--admin-token /a", 1), "--admin-token, which the binary does not define"},
		"missing token flag":  {strings.Replace(good, " \\\n  --cluster-token-file=/b", "", 1), "does not pass the required --cluster-token-file"},
		"token flag no value": {strings.Replace(good, "--admin-token-file /a", "--admin-token-file", 1), "does not pass the required --admin-token-file"},
		"no ExecStart":        {"[Service]\nUser=x\n", "no ExecStart"},
	} {
		if joined := strings.Join(checkUnit(tc.unit, defined, required), "\n"); !strings.Contains(joined, tc.want) {
			t.Errorf("%s: want a problem containing %q, got %q", name, tc.want, joined)
		}
	}
	if p := checkUnit(good, map[string]bool{"id": true, "listen": true, "admin-token-file": true}, required); len(p) == 0 {
		t.Error("a required flag the binary no longer defines was not reported")
	}
}
