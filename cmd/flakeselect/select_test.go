package main

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type memSource map[string]string

func (m memSource) Read(f string) ([]byte, error) {
	s, ok := m[f]
	if !ok {
		return nil, fmt.Errorf("no file %s", f)
	}
	return []byte(s), nil
}

func (m memSource) TestFiles(dir string) ([]string, error) {
	var out []string
	for f := range m {
		if strings.HasSuffix(f, "_test.go") && strings.TrimSuffix(f, f[strings.LastIndex(f, "/"):]) == dir {
			out = append(out, f)
		}
	}
	return out, nil
}

const sample = `package p

import "testing"

type spy struct{ n int }

func (s *spy) Hit() { s.n++ }

var shared = 1

func helper() int { return shared }

func wrapper() int { return helper() }

// TestA has a doc comment.
func TestA(t *testing.T) { _ = wrapper() }

func TestB(t *testing.T) { s := &spy{}; s.Hit() }

func TestC(t *testing.T) {}

func TestMain(m *testing.M) { m.Run() }
`

func line(t *testing.T, needle string) int {
	t.Helper()
	for i, l := range strings.Split(sample, "\n") {
		if strings.Contains(l, needle) {
			return i + 1
		}
	}
	t.Fatalf("no line with %q", needle)
	return 0
}

func sel(t *testing.T, hunks ...hunk) []string {
	t.Helper()
	for i := range hunks {
		hunks[i].file = "internal/x/x_test.go"
	}
	s, err := Select(change{files: []string{"internal/x/x_test.go"}, hunks: hunks},
		memSource{"internal/x/x_test.go": sample}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s.Tests["./internal/x"]
}

func TestSelectHunkInTestSelectsOnlyThatTest(t *testing.T) {
	n := line(t, "func TestC")
	if got := sel(t, hunk{start: n, end: n}); !reflect.DeepEqual(got, []string{"TestC"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectDocCommentBelongsToItsFunction(t *testing.T) {
	n := line(t, "// TestA has")
	if got := sel(t, hunk{start: n, end: n}); !reflect.DeepEqual(got, []string{"TestA"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectHelperChangeSelectsTransitiveDependents(t *testing.T) {
	n := line(t, "var shared")
	// shared -> helper -> wrapper -> TestA; TestB and TestC do not reach it.
	if got := sel(t, hunk{start: n, end: n}); !reflect.DeepEqual(got, []string{"TestA"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectMethodChangeSelectsUsersOfTheType(t *testing.T) {
	n := line(t, "func (s *spy) Hit")
	if got := sel(t, hunk{start: n, end: n}); !reflect.DeepEqual(got, []string{"TestB"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectImportChangeSelectsEveryTestInTheFile(t *testing.T) {
	if got := sel(t, hunk{start: 3, end: 3}); !reflect.DeepEqual(got, []string{"TestA", "TestB", "TestC"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectTestMainChangeSelectsEveryTest(t *testing.T) {
	n := line(t, "func TestMain")
	if got := sel(t, hunk{start: n, end: n}); !reflect.DeepEqual(got, []string{"TestA", "TestB", "TestC"}) {
		t.Fatalf("got %v", got)
	}
}

func TestSelectNonTestSourceInGatewaySelectsWholePackageOnly(t *testing.T) {
	c := change{files: []string{"internal/gateway/g.go", "internal/other/o.go"}}
	s, err := Select(c, memSource{}, []string{"internal/gateway", "internal/transport"})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Tests) != 0 || !reflect.DeepEqual(s.Whole, []string{"./internal/gateway"}) {
		t.Fatalf("got %+v", s)
	}
}

func TestSelectIgnoresTestdata(t *testing.T) {
	c := change{files: []string{"internal/x/testdata/y_test.go"}, hunks: []hunk{{"internal/x/testdata/y_test.go", 1, 1}}}
	s, err := Select(c, memSource{}, nil)
	if err != nil || len(s.Tests) != 0 {
		t.Fatalf("got %+v, %v", s, err)
	}
}

func TestParseDiffRenameIsDeletePlusAddAndDeletionHunkHasRange(t *testing.T) {
	diff := `diff --git a/a/old_test.go b/a/old_test.go
deleted file mode 100644
--- a/a/old_test.go
+++ /dev/null
@@ -1,3 +0,0 @@
-x
diff --git a/a/new_test.go b/a/new_test.go
new file mode 100644
--- /dev/null
+++ b/a/new_test.go
@@ -0,0 +1,3 @@
+x
diff --git a/b/b_test.go b/b/b_test.go
--- a/b/b_test.go
+++ b/b/b_test.go
@@ -5,2 +4,0 @@ func f()
-gone
`
	c := parseDiff(diff)
	if want := []string{"a/old_test.go", "a/new_test.go", "b/b_test.go"}; !reflect.DeepEqual(c.files, want) {
		t.Fatalf("files %v", c.files)
	}
	want := []hunk{{"a/new_test.go", 1, 3}, {"b/b_test.go", 4, 5}}
	if !reflect.DeepEqual(c.hunks, want) {
		t.Fatalf("hunks %v, want %v", c.hunks, want)
	}
}

func TestPrintRegexQuotesAndAnchors(t *testing.T) {
	var b bytes.Buffer
	Selection{Tests: map[string][]string{"./p": {"TestA", "TestA_B"}}, Whole: []string{"./internal/gateway"}}.Print(&b, "regex")
	want := "run\t./p\t^(TestA|TestA_B)$\t2\nwhole\t./internal/gateway\n"
	if b.String() != want {
		t.Fatalf("got %q", b.String())
	}
}

func TestIsTestName(t *testing.T) {
	for name, want := range map[string]bool{"TestA": true, "Test": true, "Test_x": true, "Testify": false, "FuzzX": true, "Fuzzy": false, "BenchmarkX": false} {
		if got := isTestName(name); got != want {
			t.Errorf("isTestName(%q) = %v, want %v", name, got, want)
		}
	}
}

// Regression: an import block has no names, and a new file's hunk covers its imports. That must
// select the tests of that file only, not every test of the package (found when one new test in
// internal/transport selected all 110).
func TestSelectNewFileSelectsOnlyItsOwnTests(t *testing.T) {
	newFile := "package p\n\nimport \"testing\"\n\nfunc TestNew(t *testing.T) {}\n"
	files := memSource{"internal/x/x_test.go": sample, "internal/x/new_test.go": newFile}
	c := change{files: []string{"internal/x/new_test.go"}, hunks: []hunk{{"internal/x/new_test.go", 1, 5}}}
	s, err := Select(c, files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Tests["./internal/x"]; !reflect.DeepEqual(got, []string{"TestNew"}) {
		t.Fatalf("got %v", got)
	}
}
