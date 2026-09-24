package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

var storageWrites = map[string]bool{
	"Append":         true,
	"SetHardState":   true,
	"ApplySnapshot":  true,
	"CreateSnapshot": true,
	"TruncateBefore": true,
}

func TestEveryCoreStorageWriteMarksItsFailure(t *testing.T) {
	fset := token.NewFileSet()
	checked := 0

	err := filepath.WalkDir(filepath.Join(repoRoot, "internal", "raft"),
		func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") ||
				strings.HasSuffix(path, "_test.go") {
				return err
			}

			src := readFile(t, path)
			f, perr := parser.ParseFile(fset, path, src, 0)
			if perr != nil {
				t.Fatalf("parsing %s: %v", path, perr)
			}

			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || !writesStorage(fn.Body) {
					continue
				}
				checked++

				if !strings.Contains(nodeText(src, fset, fn), "ErrStorage") {
					t.Errorf("%s:%d: %s writes to storage but never names ErrStorage, "+
						"so the driver cannot tell its failure from a bad message",
						path, fset.Position(fn.Pos()).Line, fn.Name.Name)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("walking the core: %v", err)
	}

	if checked == 0 {
		t.Fatal("found no storage writes in the core, so this check would pass vacuously")
	}
	t.Logf("%d functions write to storage", checked)
}

func writesStorage(n ast.Node) bool {
	if n == nil {
		return false
	}
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !storageWrites[sel.Sel.Name] {
			return true
		}
		if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "storage" {
			found = true
		}
		return true
	})
	return found
}

func nodeText(src string, fset *token.FileSet, n ast.Node) string {
	start := fset.Position(n.Pos()).Offset
	end := fset.Position(n.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return ""
	}
	return src[start:end]
}
