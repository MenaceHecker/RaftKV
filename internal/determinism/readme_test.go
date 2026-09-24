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

const repoRoot = "../.."

func TestReadmeTestCountIsCurrent(t *testing.T) {
	tests, fuzz := countFunctions(t)

	readme := readFile(t, filepath.Join(repoRoot, "README.md"))

	claimed := extractInt(t, readme, `(\d+) tests and \w+ fuzz targets`)
	if claimed != tests {
		t.Errorf("the README opens by claiming %d tests; there are %d", claimed, tests)
	}

	alsoClaimed := extractInt(t, readme, `across ([\d,]+) tests`)
	if alsoClaimed != tests {
		t.Errorf("the layout section claims %d tests; there are %d", alsoClaimed, tests)
	}

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
	report := readFile(t, filepath.Join(repoRoot, "docs", "chaos-report.md"))

	m := regexp.MustCompile(`(\d+) of (\d+) scenarios held`).FindStringSubmatch(report)
	if m == nil {
		t.Fatal("the chaos report does not state how many scenarios held")
	}
	held, total := m[1], m[2]
	if held != total {
		t.Errorf("the committed chaos report says %s of %s scenarios held", held, total)
	}

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
