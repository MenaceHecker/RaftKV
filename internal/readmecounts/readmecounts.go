// Package readmecounts counts what the README claims about the tree and rewrites
// those claims, so the exact-count rule in internal/determinism/readme_test.go can be
// satisfied by one command instead of by hand.
//
// It deliberately uses the same regexes and the same walk as that test (the test is
// the oracle; TestToolAgreesWithTheReadmeTest keeps the two identical). The one
// difference is that where the test's walk would silently double count, for example a
// second checkout nested inside the repository, this package refuses.
package readmecounts

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// ErrRefused means the tree cannot be counted the way internal/determinism counts it.
var ErrRefused = errors.New("readmecounts: refusing to count")

// Counts holds the four numbers the README states.
type Counts struct {
	Tests, Fuzz, ImplLines, TestLines int
}

// lineTolerance is the drift beyond which a line count is rewritten. The README test
// allows 10%; rewriting at 5% leaves headroom so the claim is not edited on every PR.
const lineTolerance = 0.05

var (
	testFn = regexp.MustCompile(`(?m)^func Test\w`)
	fuzzFn = regexp.MustCompile(`(?m)^func Fuzz\w`)

	// The patterns below are the ones readme_test.go matches, with the number (and for
	// the first, the word) captured so it can be replaced in place.
	openingRe = regexp.MustCompile(`(\d+) tests and (\w+) fuzz targets`)
	acrossRe  = regexp.MustCompile(`across ([\d,]+) tests`)
	implRe    = regexp.MustCompile(`Roughly ([\d,]+) lines of implementation`)
	testRe    = regexp.MustCompile(`and ([\d,]+) of tests`)

	// The same table as readme_test.go; a count outside it is an error rather than a new word.
	fuzzWords = map[int]string{5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"}
)

// Count walks root and returns the test and fuzz function counts and the implementation
// and test line counts, by the rules of readme_test.go: every *.go file under root except
// inside .git, tests counted from *_test.go only, *.pb.go excluded from line counts.
// It returns ErrRefused (wrapped, naming the path) for a nested .git entry or a .claude
// directory holding Go files, because those are other checkouts the oracle would count.
func Count(root string) (tests, fuzz, implLines, testLines int, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Name() == ".git" {
			if path != filepath.Join(root, ".git") {
				return fmt.Errorf("%w: %s is a nested checkout (a .git entry below the root); remove it or count from a clean clone", ErrRefused, path)
			}
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".claude" {
				return refuseGoUnder(path)
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path) //nolint:gosec // G304: path comes from walking the caller's root, as readme_test.go does
		if err != nil {
			return err
		}
		isTest := strings.HasSuffix(path, "_test.go")
		if isTest {
			tests += len(testFn.FindAll(src, -1))
			fuzz += len(fuzzFn.FindAll(src, -1))
		}
		if strings.HasSuffix(path, ".pb.go") {
			return nil
		}
		n := strings.Count(string(src), "\n")
		if isTest {
			testLines += n
		} else {
			implLines += n
		}
		return nil
	})
	if err == nil && tests == 0 {
		err = errors.New("readmecounts: counted zero tests, so this would count nothing")
	}
	return tests, fuzz, implLines, testLines, err
}

// refuseGoUnder returns ErrRefused if dir holds a Go file anywhere below it. A .claude
// directory without Go files (settings) is harmless; one with agent worktrees is not.
func refuseGoUnder(dir string) error {
	var found string
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".go") {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	if found != "" {
		return fmt.Errorf("%w: %s holds Go files under %s, so they would be counted twice; remove the nested worktree", ErrRefused, found, dir)
	}
	return nil
}

// Rewrite returns readme with the claimed numbers replaced by c. Only the digits (and the
// fuzz word) at the four sites change; line counts are touched only when more than 5% out,
// and then rounded to the nearest 100. It errors if a site is missing (the wording the
// README test matches has been changed) or if c.Fuzz has no word.
func Rewrite(readme string, c Counts) (string, error) {
	word, ok := fuzzWords[c.Fuzz]
	if !ok {
		return "", fmt.Errorf("there are %d fuzz targets, which readme_test.go has no word for (five to ten); extend its table first", c.Fuzz)
	}
	type edit struct {
		start, end int
		text       string
	}
	var edits []edit
	site := func(re *regexp.Regexp, what string) ([]int, error) {
		m := re.FindStringSubmatchIndex(readme)
		if m == nil {
			return nil, fmt.Errorf("no text matching %q: the README claim about %s has been reworded", re, what)
		}
		return m, nil
	}

	m, err := site(openingRe, "the test and fuzz counts")
	if err != nil {
		return "", err
	}
	edits = append(edits,
		edit{m[2], m[3], strconv.Itoa(c.Tests)},
		edit{m[4], m[5], word})

	if m, err = site(acrossRe, "the layout test count"); err != nil {
		return "", err
	}
	edits = append(edits, edit{m[2], m[3], formatLike(readme[m[2]:m[3]], c.Tests)})

	for _, l := range []struct {
		re     *regexp.Regexp
		what   string
		actual int
	}{
		{implRe, "implementation lines", c.ImplLines},
		{testRe, "test lines", c.TestLines},
	} {
		if m, err = site(l.re, l.what); err != nil {
			return "", err
		}
		old := readme[m[2]:m[3]]
		claimed, _ := strconv.Atoi(strings.ReplaceAll(old, ",", ""))
		if drift(claimed, l.actual) > lineTolerance {
			edits = append(edits, edit{m[2], m[3], formatLike(old, roundTo100(l.actual))})
		}
	}

	// Splice from the end so earlier offsets stay valid.
	for i := range edits {
		for j := i + 1; j < len(edits); j++ {
			if edits[j].start > edits[i].start {
				edits[i], edits[j] = edits[j], edits[i]
			}
		}
	}
	out := readme
	for _, e := range edits {
		out = out[:e.start] + e.text + out[e.end:]
	}
	return out, nil
}

// Mask replaces every number site (and the fuzz word) with a fixed, valid placeholder
// (0 tests, five fuzz targets, 0,000 lines), so three versions of a README can be merged
// without the counts conflicting and Rewrite can then fill the real numbers back in.
func Mask(readme string) string {
	readme = openingRe.ReplaceAllString(readme, "0 tests and five fuzz targets")
	readme = acrossRe.ReplaceAllString(readme, "across 0 tests")
	readme = implRe.ReplaceAllString(readme, "Roughly 0,000 lines of implementation")
	return testRe.ReplaceAllString(readme, "and 0,000 of tests")
}

func drift(claimed, actual int) float64 {
	if actual == 0 {
		return 1
	}
	d := float64(claimed-actual) / float64(actual)
	if d < 0 {
		d = -d
	}
	return d
}

func roundTo100(n int) int { return (n + 50) / 100 * 100 }

// formatLike formats n with thousands commas if old used them, as the README does for lines.
func formatLike(old string, n int) string {
	s := strconv.Itoa(n)
	if !strings.Contains(old, ",") || n < 1000 {
		return s
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}
