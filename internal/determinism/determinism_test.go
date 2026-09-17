// Package determinism enforces the architectural rule the rest of the project
// is built on.
//
// The consensus core and the state machine are pure: given the same inputs in
// the same order they produce the same outputs, every time and on every
// replica. Two things depend on that and neither degrades gracefully without
// it.
//
// The first is replication itself. Every replica applies the same entries and
// must reach the same state; a state machine that consulted the clock, or
// iterated a map without sorting, would diverge between nodes while every
// individual node looked healthy. The second is the chaos suite, whose entire
// value is that a failing seed fails identically on the next run. A single
// time.Now() or goroutine in the core turns those runs into anecdotes.
//
// The rule is stated in several package comments and was, until this test,
// enforced by nothing but discipline. It is the kind of thing a plausible
// one-line fix breaks silently: nothing fails, the tests stay green, and the
// reproducibility everything else assumes is quietly gone.
package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// pureDirs are the packages that must contain no source of nondeterminism.
var pureDirs = []string{
	"../raft",
	"../statemachine",
}

// forbiddenImports must not appear in a pure package.
//
// math/rand is deliberately absent: the core takes a *rand.Rand from its
// caller so that election timeouts are random across nodes and identical
// across runs of the same seed. What matters is that it never creates its own
// source, which is checked separately.
var forbiddenImports = map[string]string{
	"time":         "reads the wall clock; logical time arrives through Tick",
	"net":          "does its own networking; messages must travel through the caller",
	"net/http":     "does its own networking",
	"os":           "touches the process environment",
	"math/rand/v2": "the global generator is not seedable per node",
	"crypto/rand":  "randomness must be reproducible from the run's seed",
	"runtime":      "exposes scheduling, which is not reproducible",
	"sync/atomic":  "implies concurrent access, which these packages must not have",
}

func TestPurePackagesImportNothingNondeterministic(t *testing.T) {
	for _, dir := range pureDirs {
		files, _ := parsePackage(t, dir)
		if len(files) == 0 {
			t.Fatalf("%s: no source files were parsed, so this test checked nothing", dir)
		}

		for name, file := range files {
			for _, imp := range file.Imports {
				path, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					t.Fatalf("%s: unparsable import %s", name, imp.Path.Value)
				}
				if why, bad := forbiddenImports[path]; bad {
					t.Errorf("%s imports %q, which %s", name, path, why)
				}
			}
		}
	}
}

func TestPurePackagesStartNoGoroutines(t *testing.T) {
	for _, dir := range pureDirs {
		files, fset := parsePackage(t, dir)
		if len(files) == 0 {
			t.Fatalf("%s: no source files were parsed, so this test checked nothing", dir)
		}
		for name, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				if g, ok := n.(*ast.GoStmt); ok {
					t.Errorf("%s starts a goroutine at line %d; these packages run entirely "+
						"on the caller's goroutine so that an interleaving is a property of "+
						"the test rather than of the scheduler",
						name, fset.Position(g.Pos()).Line)
				}
				return true
			})
		}
	}
}

func TestTheCoreNeverCreatesItsOwnRandomSource(t *testing.T) {
	// Randomized election timeouts are essential and must still be
	// reproducible, so the source comes from the caller. A node that made its
	// own would be random in a way no seed could reproduce, which is the one
	// thing the chaos suite cannot tolerate.
	files, fset := parsePackage(t, "../raft")
	if len(files) == 0 {
		t.Fatal("no source files were parsed, so this test checked nothing")
	}
	for name, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "rand" {
				return true
			}
			// Constructing a generator is fine, and both halves of
			// rand.New(rand.NewSource(seed)) are constructors. What must
			// never appear is the package-level generator: since Go 1.20 it
			// is seeded randomly at startup, so a run using it cannot be
			// reproduced from anything.
			//
			// The core seeds from the node ID when its caller supplies no
			// source, which is as reproducible as being handed one. An
			// earlier version of this test flagged that as a violation; the
			// rule was wrong, not the code.
			switch sel.Sel.Name {
			case "New", "NewSource":
				return true
			default:
				t.Errorf("%s calls rand.%s at line %d, which uses the package-level "+
					"generator; that is seeded randomly at startup, so a seeded run stops "+
					"being reproducible", name, sel.Sel.Name, fset.Position(sel.Pos()).Line)
			}
			return true
		})
	}
}

// parsePackage returns the non-test files of a package, keyed by base name,
// along with the file set needed to turn a node into a source position.
//
// Function bodies are parsed in full. Parsing imports only would be enough for
// the import check and would quietly make the goroutine and randomness checks
// examine nothing at all, which is the failure mode this whole file exists to
// guard other people against.
func parsePackage(t *testing.T, dir string) (map[string]*ast.File, *token.FileSet) {
	t.Helper()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", dir, err)
	}

	out := make(map[string]*ast.File)
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			out[filepath.Base(path)] = file
		}
	}
	return out, fset
}
