package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// hunk is a changed line range of a file's post-image (inclusive, 1-based).
type hunk struct {
	file       string
	start, end int
}

// change is what the diff says about the Go files: which were touched and which lines of
// the post-image changed.
type change struct {
	files []string // every touched .go file (deleted ones too), in diff order
	hunks []hunk   // post-image ranges; deleted files have none
}

var hunkRe = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

// parseDiff reads `git diff -U0 --no-renames` output. A rename is therefore a delete plus an
// add, and the added file is all hunks, so selection never depends on a file's name.
func parseDiff(diff string) change {
	var c change
	var cur string
	sc := bufio.NewScanner(strings.NewReader(diff))
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "diff --git "):
			cur = ""
			if i := strings.LastIndex(line, " b/"); i >= 0 {
				p := line[i+3:]
				c.files = append(c.files, p)
				cur = p
			}
		case strings.HasPrefix(line, "+++ /dev/null"):
			cur = "" // deleted file: nothing in the post-image
		case strings.HasPrefix(line, "@@"):
			m := hunkRe.FindStringSubmatch(line)
			if m == nil || cur == "" {
				continue
			}
			start, _ := strconv.Atoi(m[1])
			n := 1
			if m[2] != "" {
				n, _ = strconv.Atoi(m[2])
			}
			if n == 0 { // pure deletion: it sits between line start and start+1
				c.hunks = append(c.hunks, hunk{cur, start, start + 1})
			} else {
				c.hunks = append(c.hunks, hunk{cur, start, start + n - 1})
			}
		}
	}
	return c
}

// source gives Select the post-image (HEAD) of the repository.
type source interface {
	Read(file string) ([]byte, error)
	TestFiles(dir string) ([]string, error) // *_test.go paths directly in dir
}

// Selection is the result: tests to re-run per package, and packages wanting a whole pass.
type Selection struct {
	Tests map[string][]string // "./dir" -> sorted test names
	Whole []string            // sorted "./dir"
}

// Select maps the change onto tests. A hunk inside a Test or Fuzz function selects it. A hunk
// inside any other top-level declaration of a test file (spy, driver, fake clock, shared
// var) selects every Test in the package that references it, directly or through another
// declaration. A hunk outside any declaration (imports, package clause) selects every test
// of that file; TestMain changing selects every test of the package. Selection is by
// post-image content, so renames and moves are seen.
func Select(c change, src source, wholeDirs []string) (Selection, error) {
	sel := Selection{Tests: map[string][]string{}}
	whole := map[string]bool{}
	isWhole := func(dir string) bool {
		for _, w := range wholeDirs {
			if dir == w {
				return true
			}
		}
		return false
	}
	hunksBy := map[string][]hunk{}
	for _, h := range c.hunks {
		hunksBy[h.file] = append(hunksBy[h.file], h)
	}
	dirs := map[string]bool{}
	for _, f := range c.files {
		dir := path.Dir(f)
		if skipDir(dir) {
			continue
		}
		if isWhole(dir) {
			whole["./"+dir] = true
		}
		if strings.HasSuffix(f, "_test.go") {
			dirs[dir] = true
		}
	}
	for dir := range dirs {
		names, err := selectDir(dir, hunksBy, src)
		if err != nil {
			return sel, err
		}
		if len(names) > 0 {
			sel.Tests["./"+dir] = names
		}
	}
	for d := range whole {
		sel.Whole = append(sel.Whole, d)
	}
	sort.Strings(sel.Whole)
	return sel, nil
}

func skipDir(dir string) bool {
	for _, seg := range strings.Split(dir, "/") {
		if seg == "testdata" || seg == "vendor" || strings.HasPrefix(seg, ".") && seg != "." {
			return true
		}
	}
	return false
}

type decl struct {
	names      []string
	start, end int
	isTest     bool
	file       string
	idents     map[string]bool
	changed    bool
}

func selectDir(dir string, hunksBy map[string][]hunk, src source) ([]string, error) {
	files, err := src.TestFiles(dir)
	if err != nil {
		return nil, err
	}
	var decls []*decl
	selected := map[string]bool{}
	allTests := map[string][]string{} // file -> test names
	selectAll := false
	for _, f := range files {
		body, err := src.Read(f)
		if err != nil {
			return nil, err
		}
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, f, body, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", f, err)
		}
		var fileDecls []*decl
		for _, d := range af.Decls {
			dd := &decl{file: f, idents: map[string]bool{}}
			dd.start = fset.Position(d.Pos()).Line
			dd.end = fset.Position(d.End()).Line
			switch x := d.(type) {
			case *ast.FuncDecl:
				if x.Doc != nil {
					dd.start = fset.Position(x.Doc.Pos()).Line
				}
				dd.names = append(dd.names, x.Name.Name)
				if x.Recv != nil && len(x.Recv.List) > 0 {
					dd.names = append(dd.names, recvName(x.Recv.List[0].Type))
				}
				dd.isTest = x.Recv == nil && isTestName(x.Name.Name) && x.Name.Name != "TestMain"
				if x.Recv == nil && x.Name.Name == "TestMain" {
					dd.names = nil
				}
			case *ast.GenDecl:
				if x.Doc != nil {
					dd.start = fset.Position(x.Doc.Pos()).Line
				}
				for _, s := range x.Specs {
					switch sp := s.(type) {
					case *ast.TypeSpec:
						dd.names = append(dd.names, sp.Name.Name)
					case *ast.ValueSpec:
						for _, n := range sp.Names {
							dd.names = append(dd.names, n.Name)
						}
					}
				}
			}
			ast.Inspect(d, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					dd.idents[id.Name] = true
				}
				return true
			})
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == "TestMain" {
				dd.idents = map[string]bool{}
			}
			fileDecls = append(fileDecls, dd)
			decls = append(decls, dd)
			if dd.isTest {
				allTests[f] = append(allTests[f], dd.names[0])
			}
		}
		for _, h := range hunksBy[f] {
			hit := false
			for _, dd := range fileDecls {
				if h.start <= dd.end && h.end >= dd.start {
					dd.changed = true
					hit = true
					if len(dd.names) == 0 { // TestMain
						selectAll = true
					}
				}
			}
			if !hit { // imports, package clause, a stray comment: be conservative
				for _, n := range allTests[f] {
					selected[n] = true
				}
			}
		}
	}
	helpers := map[string]bool{}
	for _, dd := range decls {
		if !dd.changed {
			continue
		}
		if dd.isTest {
			selected[dd.names[0]] = true
			continue
		}
		for _, n := range dd.names {
			if n != "_" && n != "init" && n != "" {
				helpers[n] = true
			}
		}
	}
	// A declaration that mentions a changed helper is itself changed (transitively).
	for grew := true; grew; {
		grew = false
		for _, dd := range decls {
			if dd.isTest || !mentions(dd, helpers) {
				continue
			}
			for _, n := range dd.names {
				if n != "_" && n != "init" && n != "" && !helpers[n] {
					helpers[n] = true
					grew = true
				}
			}
		}
	}
	var names []string
	for _, dd := range decls {
		if !dd.isTest {
			continue
		}
		if selectAll || selected[dd.names[0]] || mentions(dd, helpers) {
			names = append(names, dd.names[0])
		}
	}
	sort.Strings(names)
	return dedupe(names), nil
}

func mentions(d *decl, set map[string]bool) bool {
	for n := range set {
		if d.idents[n] {
			return true
		}
	}
	return false
}

func recvName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.StarExpr:
		return recvName(x.X)
	case *ast.Ident:
		return x.Name
	case *ast.IndexExpr:
		return recvName(x.X)
	case *ast.IndexListExpr:
		return recvName(x.X)
	}
	return ""
}

// isTestName reports whether name is a function `go test -run` can select: Test or Fuzz,
// followed by nothing or a non-lowercase rune.
func isTestName(name string) bool {
	for _, p := range []string{"Test", "Fuzz"} {
		if rest, ok := strings.CutPrefix(name, p); ok {
			if rest == "" {
				return true
			}
			r, _ := utf8.DecodeRuneInString(rest)
			return !unicode.IsLower(r)
		}
	}
	return false
}

func dedupe(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.TrimPrefix(path.Clean(p), "./"))
		}
	}
	return out
}

// Print writes the selection in the format documented on the package.
func (s Selection) Print(w io.Writer, format string) {
	pkgs := make([]string, 0, len(s.Tests))
	for p := range s.Tests {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)
	for _, p := range pkgs {
		names := s.Tests[p]
		if format == "regex" {
			q := make([]string, len(names))
			for i, n := range names {
				q[i] = regexp.QuoteMeta(n)
			}
			_, _ = fmt.Fprintf(w, "run\t%s\t^(%s)$\t%d\n", p, strings.Join(q, "|"), len(names))
			continue
		}
		for _, n := range names {
			_, _ = fmt.Fprintf(w, "test\t%s\t%s\n", p, n)
		}
	}
	for _, p := range s.Whole {
		_, _ = fmt.Fprintf(w, "whole\t%s\n", p)
	}
}
