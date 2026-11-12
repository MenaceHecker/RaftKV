//go:build !faultinject

package faultinject

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestDefaultBuildSwitchesAreConstantFalse(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "off.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing off.go: %v", err)
	}

	var active *ast.FuncDecl
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.GenDecl:
			if d.Tok == token.VAR {
				t.Errorf("off.go declares a package-level var at line %d; the default build must have no mutable switch state",
					fset.Position(d.Pos()).Line)
			}
		case *ast.FuncDecl:
			if d.Name.Name == "Active" {
				active = d
			} else {
				t.Errorf("off.go declares %s; the default build may only declare Active", d.Name.Name)
			}
		}
	}
	if active == nil {
		t.Fatal("off.go has no Active function")
	}

	body := active.Body.List
	if len(body) != 1 {
		t.Fatalf("Active has %d statements, want a single return false", len(body))
	}
	ret, ok := body[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		t.Fatalf("Active does not consist of one return statement")
	}
	if id, ok := ret.Results[0].(*ast.Ident); !ok || id.Name != "false" {
		t.Fatalf("Active does not return the literal false")
	}

	for _, s := range All() {
		if Active(s) {
			t.Errorf("Active(%s) is true in the default build", s.Name())
		}
	}
}
