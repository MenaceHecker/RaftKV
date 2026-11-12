package determinism

import (
	"go/ast"
	"go/token"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/faultinject"
)

var injectionDirs = []string{
	"../raft",
	"../statemachine",
	"../storage",
}

// A call site is allowed to be exactly `faultinject.Active(faultinject.X)` used
// as a boolean, joined to the existing condition with && or ||, optionally
// negated. Anything else (a stored value, a call to Set, a branch that is only
// there for a mutation) would let the default build differ from the code it
// claims to be.
func TestCallSitesAreGuardedOnly(t *testing.T) {
	used := map[string]int{}

	for _, dir := range injectionDirs {
		files, fset := parsePackage(t, dir)
		if len(files) == 0 {
			t.Fatalf("%s: no source files were parsed, so this test checked nothing", dir)
		}

		for name, file := range files {
			var stack []ast.Node
			ast.Inspect(file, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				stack = append(stack, n)

				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := sel.X.(*ast.Ident)
				if !ok || pkg.Name != "faultinject" {
					return true
				}
				where := name + ":" + strconv.Itoa(fset.Position(sel.Pos()).Line)

				if sel.Sel.Name != "Active" {
					// The only other use is the switch constant, as Active's argument.
					if len(stack) < 3 {
						return true
					}
					call, ok := stack[len(stack)-2].(*ast.CallExpr)
					if !ok || len(call.Args) != 1 || call.Args[0] != n {
						t.Errorf("%s: faultinject.%s is used outside a call to Active", where, sel.Sel.Name)
						return true
					}
					known := false
					for _, s := range faultinject.All() {
						known = known || norm(s.Name()) == norm(sel.Sel.Name)
					}
					if !known {
						t.Errorf("%s: faultinject.%s is not one of the nine switches", where, sel.Sel.Name)
						return true
					}
					used[norm(sel.Sel.Name)]++
					return true
				}

				call, ok := stack[len(stack)-2].(*ast.CallExpr)
				if !ok || call.Fun != n || len(call.Args) != 1 {
					t.Errorf("%s: faultinject.Active is not called with exactly one switch", where)
					return true
				}
				if !guardedPosition(stack[:len(stack)-2]) {
					t.Errorf("%s: faultinject.Active is not used as a boolean joined to a condition with && or ||", where)
				}
				return true
			})
		}
	}

	for _, s := range faultinject.All() {
		if used[norm(s.Name())] == 0 {
			t.Errorf("switch %s has no call site, so the mutation can never be injected", s.Name())
		}
	}
}

// guardedPosition reports whether the call, whose ancestors are given from the
// outermost down, is a boolean operand in an if condition, an assignment or a
// return: parentheses, negation and && or || only.
func guardedPosition(ancestors []ast.Node) bool {
	for i := len(ancestors) - 1; i >= 0; i-- {
		switch a := ancestors[i].(type) {
		case *ast.ParenExpr:
			continue
		case *ast.UnaryExpr:
			if a.Op != token.NOT {
				return false
			}
		case *ast.BinaryExpr:
			if a.Op != token.LAND && a.Op != token.LOR {
				return false
			}
		case *ast.IfStmt, *ast.AssignStmt, *ast.ReturnStmt:
			return true
		default:
			return false
		}
	}
	return false
}

// norm lowercases a Go constant name or drops the dashes of a switch name, so
// that "SkipVoteUpToDate" and "skip-vote-uptodate" compare equal.
func norm(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "-", ""))
}

func TestDefaultBinaryHasNoMutationMachinery(t *testing.T) {
	dir := t.TempDir()

	symbols := func(tags, label string) string {
		t.Helper()
		out := filepath.Join(dir, "server-"+label)
		args := []string{"build", "-o", out}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		args = append(args, "./cmd/raftkv-server")
		build := exec.Command("go", args...) //nolint:gosec // fixed toolchain command built from constants
		build.Dir = repoRoot
		if b, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, b)
		}
		nm := exec.Command("go", "tool", "nm", out) //nolint:gosec // fixed toolchain command on a file this test built
		b, err := nm.CombinedOutput()
		if err != nil {
			t.Fatalf("go tool nm: %v\n%s", err, b)
		}
		return string(b)
	}

	// The linker drops what nothing references, so Set (called only by tests)
	// is absent even from the tagged binary; Enable is kept because the
	// package's init reads the environment with it.
	tagged := symbols("faultinject", "tagged")
	if !strings.Contains(tagged, "faultinject.Enable") {
		t.Fatal("the faultinject build has no faultinject.Enable symbol, so this test would pass vacuously")
	}

	plain := symbols("", "default")
	for _, sym := range []string{"faultinject.Set", "faultinject.Enable", "faultinject.Reset"} {
		if strings.Contains(plain, sym) {
			t.Errorf("the default server binary contains %s: the mutation machinery shipped", sym)
		}
	}
}
