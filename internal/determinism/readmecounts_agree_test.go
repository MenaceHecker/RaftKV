package determinism

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/readmecounts"
)

// TestToolAgreesWithTheReadmeTest pins the generator to the oracle: the numbers
// cmd/readmecounts writes are the numbers readme_test.go checks, so the tool cannot drift.
func TestToolAgreesWithTheReadmeTest(t *testing.T) {
	wantTests, wantFuzz := countFunctions(t)
	wantImpl, wantTest := countLines(t)

	tests, fuzz, impl, testLines, err := readmecounts.Count(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	if tests != wantTests || fuzz != wantFuzz {
		t.Errorf("tool counts %d tests and %d fuzz; the README test counts %d and %d", tests, fuzz, wantTests, wantFuzz)
	}
	if impl != wantImpl || testLines != wantTest {
		t.Errorf("tool counts %d impl and %d test lines; the README test counts %d and %d", impl, testLines, wantImpl, wantTest)
	}
}

// TestRealReadmeIsFreshAfterRewrite: the committed README is what the tool would write.
func TestRealReadmeIsFreshAfterRewrite(t *testing.T) {
	readme := readFile(t, filepath.Join(repoRoot, "README.md"))
	tests, fuzz, impl, testLines, err := readmecounts.Count(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readmecounts.Rewrite(readme, readmecounts.Counts{Tests: tests, Fuzz: fuzz, ImplLines: impl, TestLines: testLines})
	if err != nil {
		t.Fatal(err)
	}
	if got != readme {
		t.Error("the committed README is stale: run `make readme-counts`")
	}
}

// TestCheckAndOracleAgreeOnLineDrift runs the real, unmodified TestReadmeLineCountsAreRoughlyRight
// in a scratch copy of the module whose README claims lines 9% and 11% out, and requires
// readmecounts.Check to accept and reject exactly the same two cases. The oracle reads its
// README from a fixed path, so the copy is how it is pointed at other claims.
func TestCheckAndOracleAgreeOnLineDrift(t *testing.T) {
	impl, test := countLines(t)
	tests, fuzz := countFunctions(t)
	counts := readmecounts.Counts{Tests: tests, Fuzz: fuzz, ImplLines: impl, TestLines: test}
	committed := readFile(t, filepath.Join(repoRoot, "README.md"))

	cases := []struct {
		name          string
		pct           int
		oracleAccepts bool
	}{{"9 percent out", 9, true}, {"11 percent out", 11, false}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			readme := committed
			readme = regexp.MustCompile(`Roughly [\d,]+ lines of implementation`).
				ReplaceAllString(readme, fmt.Sprintf("Roughly %d lines of implementation", impl*(100+c.pct)/100))
			readme = regexp.MustCompile(`and [\d,]+ of tests`).
				ReplaceAllString(readme, fmt.Sprintf("and %d of tests", test*(100+c.pct)/100))

			problems, err := readmecounts.Check(readme, counts)
			if err != nil {
				t.Fatal(err)
			}
			toolAccepts := len(problems) == 0
			oracleAccepts := oracleLineTestPasses(t, readme)
			t.Logf("%s: Check accepts=%v (%v); the oracle accepts=%v", c.name, toolAccepts, problems, oracleAccepts)
			if oracleAccepts != c.oracleAccepts {
				t.Fatalf("setup: the oracle's verdict is %v; expected %v", oracleAccepts, c.oracleAccepts)
			}
			if toolAccepts != oracleAccepts {
				t.Errorf("Check accepts=%v but the oracle accepts=%v", toolAccepts, oracleAccepts)
			}
		})
	}
}

// oracleLineTestPasses copies the module's Go sources to a temp dir, writes readme there, and
// runs TestReadmeLineCountsAreRoughlyRight unmodified, returning whether it passed.
func oracleLineTestPasses(t *testing.T, readme string) bool {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == ".claude") {
			return fs.SkipDir
		}
		rel, _ := filepath.Rel(repoRoot, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		if strings.HasSuffix(path, ".go") || rel == "go.mod" || rel == "go.sum" {
			return os.WriteFile(filepath.Join(dst, rel), []byte(readFile(t, path)), 0o600)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "README.md"), []byte(readme), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-count=1", "-run", "^TestReadmeLineCountsAreRoughlyRight$", "./internal/determinism")
	cmd.Dir = dst
	out, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(out), "FAIL") {
		t.Fatalf("running the oracle: %v\n%s", err, out)
	}
	return err == nil
}
