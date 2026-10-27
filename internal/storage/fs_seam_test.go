package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Only fs.go and lock_*.go may reach the operating system. Everything else in
// the package goes through FS, so a fault-injecting FS sees every call.
func isOSBoundary(file string) bool {
	return file == "fs.go" || strings.HasPrefix(file, "lock_")
}

// os identifiers that carry no behavior: open flags and types in signatures.
func isInertOSName(name string) bool {
	switch name {
	case "FileMode", "FileInfo", "DirEntry":
		return true
	}
	return strings.HasPrefix(name, "O_")
}

var bannedFilepathFuncs = map[string]bool{"Glob": true, "Walk": true, "WalkDir": true}

func TestNoDirectFilesystemCallsInStorage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("listing the package: %v", err)
	}

	// Parsed per file, not per build: both lock_unix.go and lock_other.go are
	// seen whatever GOOS the test runs on.
	fset := token.NewFileSet()
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || isOSBoundary(name) {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		checked++

		for _, imp := range file.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: unparsable import %s", name, imp.Path.Value)
			}
			if p == "syscall" || p == "io/ioutil" {
				t.Errorf("%s imports %q; only fs.go and lock_*.go may", name, p)
			}
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkgIdent, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			line := fset.Position(sel.Pos()).Line
			switch {
			case pkgIdent.Name == "os" && !isInertOSName(sel.Sel.Name):
				t.Errorf("%s:%d calls os.%s directly; go through FS", name, line, sel.Sel.Name)
			case pkgIdent.Name == "filepath" && bannedFilepathFuncs[sel.Sel.Name]:
				t.Errorf("%s:%d calls filepath.%s, which reads the filesystem; go through FS",
					name, line, sel.Sel.Name)
			}
			return true
		})
	}

	if checked == 0 {
		t.Fatal("no source files were checked, so this test checked nothing")
	}
}
