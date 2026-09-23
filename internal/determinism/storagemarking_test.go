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

// Every write the consensus core makes has to be distinguishable from a bad
// message, because the driver stops the node for one and drops the other.
//
// The marker is applied by hand at each call, which is how one of them came
// to be missed. There are two Append calls in the log: the one a follower
// uses when it accepts entries from a leader, and the one a leader uses for
// its own. Marking the first and writing tests for the first left the second
// bare, and a leader whose disk had died went on holding the cluster,
// refusing every write and never standing down, because the failure reached
// the driver looking exactly like a malformed message.
//
// Behavioural tests cover the four calls that exist. They cannot cover a
// fifth that somebody adds later, which is what this is for.

// storageWrites are the Storage methods that change durable state. A failure
// from any of them means the write did not happen.
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

				// The function that performs the write must also name the
				// marker. This is scoped to the function rather than to the
				// branch so that it does not depend on how the call is
				// written, which is the kind of detail a later change moves
				// around. A function with two writes and one marker would
				// pass; none has two, and the behavioural tests cover the
				// calls that exist today.
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

// writesStorage reports whether a block calls a Storage method that changes
// durable state.
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
		// The receiver has to be a storage field, not any method that
		// happens to share the name.
		if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "storage" {
			found = true
		}
		return true
	})
	return found
}

// nodeText returns the source a node spans.
func nodeText(src string, fset *token.FileSet, n ast.Node) string {
	start := fset.Position(n.Pos()).Offset
	end := fset.Position(n.End()).Offset
	if start < 0 || end > len(src) || start >= end {
		return ""
	}
	return src[start:end]
}
