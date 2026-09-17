package determinism

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The README states numbers as fact: how many tests there are, how many fuzz
// targets, how large the code is. Nobody recomputes those when they change, so
// they drift, and a document that is confidently wrong about something
// checkable invites doubt about the parts that are harder to check.
//
// They have drifted twice in this repository. At one point the README claimed
// 177 tests in one paragraph and 367 in another, when there were 434. The
// round that fixed those introduced a fresh error in the same sentence.
//
// So the counts are checked the way the determinism rules above are checked:
// mechanically, as part of the suite. Adding tests now means editing one
// number in the README, and the failure says which.

// repoRoot is where the module lives, relative to this package.
const repoRoot = "../.."

func TestReadmeTestCountIsCurrent(t *testing.T) {
	tests, fuzz := countFunctions(t)

	readme := readFile(t, filepath.Join(repoRoot, "README.md"))

	// "442 tests and seven fuzz targets, ..."
	claimed := extractInt(t, readme, `(\d+) tests and \w+ fuzz targets`)
	if claimed != tests {
		t.Errorf("the README opens by claiming %d tests; there are %d", claimed, tests)
	}

	// "... across 442 tests. The ratio is not an accident."
	alsoClaimed := extractInt(t, readme, `across ([\d,]+) tests`)
	if alsoClaimed != tests {
		t.Errorf("the layout section claims %d tests; there are %d", alsoClaimed, tests)
	}

	// The fuzz count is spelled out, so it is matched by word.
	words := map[int]string{
		5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
	}
	want, ok := words[fuzz]
	if !ok {
		t.Fatalf("there are %d fuzz targets, which this test has no word for; add it", fuzz)
	}
	if !strings.Contains(readme, want+" fuzz targets") {
		t.Errorf("the README does not say %q, but there are %d fuzz targets", want+" fuzz targets", fuzz)
	}
}

func TestReadmeLineCountsAreRoughlyRight(t *testing.T) {
	// These are hedged with "roughly" in the text, so they are held to a
	// tolerance rather than to the digit. The point is to catch a number that
	// has stopped describing the repository, not to force an edit for every
	// line added.
	const tolerance = 0.10

	impl, test := countLines(t)
	readme := readFile(t, filepath.Join(repoRoot, "README.md"))

	claimedImpl := extractInt(t, readme, `Roughly ([\d,]+) lines of implementation`)
	claimedTest := extractInt(t, readme, `and ([\d,]+) of tests`)

	check := func(what string, claimed, actual int) {
		t.Helper()
		if actual == 0 {
			t.Fatalf("counted zero %s lines, so this test checked nothing", what)
		}
		drift := float64(claimed-actual) / float64(actual)
		if drift < 0 {
			drift = -drift
		}
		if drift > tolerance {
			t.Errorf("the README says roughly %d %s lines; there are %d, which is %.0f%% out",
				claimed, what, actual, drift*100)
		}
	}
	check("implementation", claimedImpl, impl)
	check("test", claimedTest, test)
}

func TestChaosReportMatchesTheScenarioCount(t *testing.T) {
	// The report is generated, so its own two numbers should agree with each
	// other. A mismatch means a scenario errored in a way that still let the
	// report be written.
	report := readFile(t, filepath.Join(repoRoot, "docs", "chaos-report.md"))

	m := regexp.MustCompile(`(\d+) of (\d+) scenarios held`).FindStringSubmatch(report)
	if m == nil {
		t.Fatal("the chaos report does not state how many scenarios held")
	}
	held, total := m[1], m[2]
	if held != total {
		t.Errorf("the committed chaos report says %s of %s scenarios held", held, total)
	}

	// And the README should not claim more scenarios than the report ran.
	readme := readFile(t, filepath.Join(repoRoot, "README.md"))
	words := map[string]string{
		"Twenty-one": "21", "Twenty-two": "22", "Twenty-three": "23",
		"Twenty-four": "24", "Twenty-five": "25",
	}
	for word, n := range words {
		if strings.Contains(readme, word+" scenarios") && n != total {
			t.Errorf("the README says %s scenarios; the report ran %s", word, total)
		}
	}
}

// countFunctions counts test and fuzz functions across the module.
func countFunctions(t *testing.T) (tests, fuzz int) {
	t.Helper()

	testFn := regexp.MustCompile(`(?m)^func Test\w`)
	fuzzFn := regexp.MustCompile(`(?m)^func Fuzz\w`)

	walkGo(t, func(path string, isTest bool) {
		if !isTest {
			return
		}
		src := readFile(t, path)
		tests += len(testFn.FindAllString(src, -1))
		fuzz += len(fuzzFn.FindAllString(src, -1))
	})

	if tests == 0 {
		t.Fatal("counted zero tests, so this test checked nothing")
	}
	return tests, fuzz
}

// countLines counts implementation and test lines, excluding generated code.
func countLines(t *testing.T) (impl, test int) {
	t.Helper()

	walkGo(t, func(path string, isTest bool) {
		if strings.HasSuffix(path, ".pb.go") {
			return
		}
		n := strings.Count(readFile(t, path), "\n")
		if isTest {
			test += n
		} else {
			impl += n
		}
	})
	return impl, test
}

// walkGo visits every Go file in the module outside .git.
func walkGo(t *testing.T, visit func(path string, isTest bool)) {
	t.Helper()

	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		visit(path, strings.HasSuffix(path, "_test.go"))
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

// extractInt pulls the first capture group out of src and parses it, allowing
// thousands separators.
func extractInt(t *testing.T, src, pattern string) int {
	t.Helper()

	m := regexp.MustCompile(pattern).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("no text matching %q; the claim it checks has been reworded", pattern)
	}
	n, err := strconv.Atoi(strings.ReplaceAll(m[1], ",", ""))
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprintf("parsing %q", m[1]), err)
	}
	return n
}
