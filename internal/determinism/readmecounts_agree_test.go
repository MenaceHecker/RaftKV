package determinism

import (
	"path/filepath"
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
