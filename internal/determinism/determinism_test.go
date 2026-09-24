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

var pureDirs = []string{
	"../raft",
	"../statemachine",
}

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
