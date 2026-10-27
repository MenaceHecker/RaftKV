package storage

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Only fs.go and the two lock files may reach the operating system. Everything
// else in the package goes through FS, so a fault-injecting FS sees every call.
func isOSBoundary(file string) bool {
	return file == "fs.go" || file == "lock_unix.go" || file == "lock_other.go"
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

func isOSFSType(e ast.Expr) bool {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "osFS"
}

// isOSFSValue is osFS itself or an osFS literal, as the operand of a selector.
// Using osFS{} as a default FS value is fine; selecting a method off it is not.
func isOSFSValue(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.CompositeLit:
		return isOSFSType(x.Type)
	case *ast.Ident:
		return x.Name == "osFS" && x.Obj == nil
	case *ast.ParenExpr:
		return isOSFSValue(x.X)
	}
	return false
}

// seamViolations lists every way file reaches the operating system directly.
// Imports are resolved to their package paths, so an alias such as
// `import host "os"` is seen as os, and a dot import of a restricted package
// is rejected because its uses could not be traced.
func seamViolations(fset *token.FileSet, file *ast.File, name string) []string {
	var out []string
	local := map[string]string{} // local package name -> import path

	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: unparsable import %s", name, imp.Path.Value))
			continue
		}
		restricted := p == "os" || p == "path/filepath" || p == "syscall" || p == "io/ioutil"
		if imp.Name != nil && imp.Name.Name == "." && restricted {
			out = append(out, fmt.Sprintf("%s dot-imports %q; its uses cannot be checked", name, p))
			continue
		}
		if p == "syscall" || p == "io/ioutil" {
			out = append(out, fmt.Sprintf("%s imports %q; only fs.go and lock_*.go may", name, p))
			continue
		}
		if imp.Name != nil {
			local[imp.Name.Name] = p
		} else if p == "os" || p == "path/filepath" {
			local[p[strings.LastIndex(p, "/")+1:]] = p
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		if st, ok := n.(*ast.StructType); ok {
			for _, f := range st.Fields.List {
				if len(f.Names) == 0 && isOSFSType(f.Type) {
					out = append(out, fmt.Sprintf("%s:%d embeds osFS, which promotes its methods past the seam",
						name, fset.Position(f.Pos()).Line))
				}
			}
			return true
		}
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if isOSFSValue(sel.X) {
			out = append(out, fmt.Sprintf("%s:%d calls osFS.%s directly; go through FS",
				name, fset.Position(sel.Pos()).Line, sel.Sel.Name))
			return true
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		// A name bound in the file (parameter, local, ...) is not the package.
		if pkgIdent.Obj != nil {
			return true
		}
		line := fset.Position(sel.Pos()).Line
		switch local[pkgIdent.Name] {
		case "os":
			if !isInertOSName(sel.Sel.Name) {
				out = append(out, fmt.Sprintf("%s:%d calls os.%s directly; go through FS", name, line, sel.Sel.Name))
			}
		case "path/filepath":
			if bannedFilepathFuncs[sel.Sel.Name] {
				out = append(out, fmt.Sprintf("%s:%d calls filepath.%s, which reads the filesystem; go through FS",
					name, line, sel.Sel.Name))
			}
		}
		return true
	})
	return out
}

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
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		checked++

		for _, v := range seamViolations(fset, file, name) {
			t.Error(v)
		}
	}

	if checked == 0 {
		t.Fatal("no source files were checked, so this test checked nothing")
	}
}

// The guard above is only worth having if it fails on what it forbids, so each
// bypass the review found is fed to it as source and must be reported.
func TestSeamGuardRejectsBypasses(t *testing.T) {
	cases := []struct {
		name, src string
		want      int
	}{
		{"direct os call", `package p; import "os"; func f() { os.Remove("x") }`, 1},
		{"aliased os", `package p; import host "os"; func f() { host.Remove("x") }`, 1},
		{"aliased filepath", `package p; import paths "path/filepath"; func f() { paths.Glob("x") }`, 1},
		{"dot-imported os", `package p; import . "os"; func f() { Remove("x") }`, 1},
		{"dot-imported filepath", `package p; import . "path/filepath"; func f() { Glob("x") }`, 1},
		{"syscall", `package p; import "syscall"; var _ = syscall.EINTR`, 1},
		{"inert os names", `package p; import "os"; var _ os.FileMode; var _ = os.O_CREATE`, 0},
		{"aliased inert os names", `package p; import host "os"; var _ host.FileInfo; var _ = host.O_EXCL`, 0},
		{"filepath.Join", `package p; import "path/filepath"; var _ = filepath.Join("a", "b")`, 0},
		{"unrelated os identifier", `package p; import "fmt"; func f(os fmt.Stringer) { os.String() }`, 0},
		{"shadowing parameter with os imported", `package p; import "os"; var _ os.FileMode; type r interface{ Remove(string) error }; func f(os r) { _ = os.Remove("x") }`, 0},
		{"shadowing local with os imported", `package p; import "os"; var _ os.FileMode; type r struct{}; func (r) Remove(string) error { return nil }; func f() { os := r{}; _ = os.Remove("x") }`, 0},
		{"embedded osFS", `package p; type bypass struct{ osFS }; func f() { _ = (bypass{}).Remove("x") }`, 1},
		{"embedded *osFS", `package p; type bypass struct{ *osFS }`, 1},
		{"method call on osFS literal", `package p; func f() { _ = osFS{}.Remove("x") }`, 1},
		{"method expression on osFS", `package p; var remove = osFS.Remove`, 1},
		{"osFS as a default value", `package p; type FS interface{}; func f(fsys FS) FS { if fsys == nil { fsys = osFS{} }; return fsys }`, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", c.src, 0)
			if err != nil {
				t.Fatalf("parsing the fixture: %v", err)
			}
			got := seamViolations(fset, file, "fixture.go")
			if len(got) != c.want {
				t.Fatalf("violations = %v, want %d", got, c.want)
			}
		})
	}
}

func TestSeamBoundaryIsExactlyFSAndLockFiles(t *testing.T) {
	for _, name := range []string{"fs.go", "lock_unix.go", "lock_other.go"} {
		if !isOSBoundary(name) {
			t.Errorf("%s should be on the OS boundary", name)
		}
	}
	for _, name := range []string{"lock_extra.go", "lockfile.go", "wal.go", "snapshot.go", "disk.go", "fs_extra.go"} {
		if isOSBoundary(name) {
			t.Errorf("%s must not be on the OS boundary", name)
		}
	}
}
