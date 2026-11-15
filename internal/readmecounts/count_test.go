package readmecounts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCountMatchesAKnownTree(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a.go", "package a\n\nfunc A() {}\n")                                                                                                                                          // 3 impl lines
	write(t, root, "a_test.go", "package a\n\nfunc TestOne(t *testing.T) {}\nfunc TestTwo(t *testing.T) {}\nfunc Test(t *testing.T) {}\nfunc FuzzX(f *testing.F) {}\n\tfunc TestIndented() {}\n") // 7 test lines, 2 tests, 1 fuzz
	write(t, root, "sub/b_test.go", "package sub\n\nfunc TestThree(t *testing.T) {}\n")                                                                                                           // 3 test lines, 1 test
	write(t, root, "gen.pb.go", "package a\n\n\n\n\n")                                                                                                                                            // excluded from lines
	write(t, root, ".git/hidden_test.go", "package h\n\nfunc TestHidden(t *testing.T) {}\n")                                                                                                      // .git is skipped
	write(t, root, "README.md", "not go\n\nfunc TestNotGo\n")

	tests, fuzz, impl, testLines, err := Count(root)
	if err != nil {
		t.Fatal(err)
	}
	if tests != 3 || fuzz != 1 || impl != 3 || testLines != 10 {
		t.Fatalf("Count = tests %d fuzz %d impl %d testLines %d; want 3 1 3 10", tests, fuzz, impl, testLines)
	}
}

const sample = `# T

300 tests and six fuzz targets, all green.

Prose with 12 numbers and 5 fuzz mentions that must not change.

Roughly 15,300 lines of implementation and 29,800 of tests, across 300 tests. Done.
`

func TestRewriteChangesOnlyTheNumberSites(t *testing.T) {
	c := Counts{Tests: 310, Fuzz: 7, ImplLines: 20000, TestLines: 29800}
	got, err := Rewrite(sample, c)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.NewReplacer(
		"300 tests and six fuzz targets", "310 tests and seven fuzz targets",
		"across 300 tests", "across 310 tests",
		"Roughly 15,300 lines", "Roughly 20,000 lines",
	).Replace(sample)
	if got != want {
		t.Fatalf("Rewrite changed more or less than the number sites\n got: %q\nwant: %q", got, want)
	}
	again, err := Rewrite(got, c)
	if err != nil {
		t.Fatal(err)
	}
	if again != got {
		t.Fatalf("second Rewrite is not a no-op\n got: %q\nwant: %q", again, got)
	}
}

func TestRewriteLeavesLineCountsWithinFivePercent(t *testing.T) {
	// 15,300 vs 15,700 is 2.5% out; 29,800 vs 30,500 is 2.3% out.
	got, err := Rewrite(sample, Counts{Tests: 300, Fuzz: 6, ImplLines: 15700, TestLines: 30500})
	if err != nil {
		t.Fatal(err)
	}
	if got != sample {
		t.Fatalf("line counts within 5%% were rewritten: %q", got)
	}
}

func TestRewriteUpdatesLineCountsBeyondFivePercent(t *testing.T) {
	// 15,300 vs 16,149 is 5.3% out; 29,800 vs 27,949 is 6.6% out. Both round to the nearest 100.
	got, err := Rewrite(sample, Counts{Tests: 300, Fuzz: 6, ImplLines: 16149, TestLines: 27949})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.NewReplacer("15,300", "16,100", "29,800", "27,900").Replace(sample)
	if got != want {
		t.Fatalf("line counts beyond 5%% were not rounded to 100\n got: %q\nwant: %q", got, want)
	}
}

func TestRefusesNestedWorktreeAndNestedGitFile(t *testing.T) {
	cases := map[string]func(root string){
		".claude worktree with Go files": func(root string) {
			write(t, root, ".claude/worktrees/x/a_test.go", "package a\n\nfunc TestX(t *testing.T) {}\n")
		},
		"nested .git file": func(root string) {
			write(t, root, "sub/.git", "gitdir: /elsewhere\n")
		},
		"nested .git directory": func(root string) {
			write(t, root, "sub/.git/HEAD", "ref: refs/heads/main\n")
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, "a_test.go", "package a\n\nfunc TestA(t *testing.T) {}\n")
			setup(root)
			_, _, _, _, err := Count(root)
			if !errors.Is(err, ErrRefused) {
				t.Fatalf("err = %v; want ErrRefused", err)
			}
			if !strings.Contains(err.Error(), filepath.Join(root, "sub")) && !strings.Contains(err.Error(), filepath.Join(root, ".claude")) {
				t.Fatalf("the refusal does not name the offending path: %v", err)
			}
		})
	}
}

func TestClaudeDirWithoutGoFilesAndRootGitFileAreFine(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a_test.go", "package a\n\nfunc TestA(t *testing.T) {}\n")
	write(t, root, ".claude/runner-settings.json", "{}\n")
	write(t, root, ".git", "gitdir: /elsewhere\n") // a worktree's own .git file
	tests, _, _, _, err := Count(root)
	if err != nil {
		t.Fatal(err)
	}
	if tests != 1 {
		t.Fatalf("tests = %d; want 1", tests)
	}
}

func TestFuzzWordOutsideTheTableIsAnError(t *testing.T) {
	for _, n := range []int{4, 11} {
		if _, err := Rewrite(sample, Counts{Tests: 300, Fuzz: n, ImplLines: 15300, TestLines: 29800}); err == nil {
			t.Fatalf("Rewrite accepted %d fuzz targets, which the README test has no word for", n)
		}
	}
	for n, word := range map[int]string{5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"} {
		got, err := Rewrite(sample, Counts{Tests: 300, Fuzz: n, ImplLines: 15300, TestLines: 29800})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got, word+" fuzz targets") {
			t.Fatalf("fuzz %d: README does not say %q: %q", n, word, got)
		}
	}
}

func TestRewriteErrorsWhenAClaimIsReworded(t *testing.T) {
	reworded := strings.Replace(sample, "across 300 tests", "over 300 tests", 1)
	if _, err := Rewrite(reworded, Counts{Tests: 300, Fuzz: 6, ImplLines: 15300, TestLines: 29800}); err == nil {
		t.Fatal("Rewrite did not notice that a claim the README test matches has been reworded")
	}
}

func TestMaskHidesOnlyTheNumberSites(t *testing.T) {
	other, err := Rewrite(sample, Counts{Tests: 310, Fuzz: 7, ImplLines: 20000, TestLines: 40000})
	if err != nil {
		t.Fatal(err)
	}
	if other == sample {
		t.Fatal("setup: Rewrite changed nothing")
	}
	if Mask(sample) != Mask(other) {
		t.Fatalf("masking does not hide the number sites\n%q\n%q", Mask(sample), Mask(other))
	}
	edited := strings.Replace(sample, "all green", "mostly green", 1)
	if Mask(edited) == Mask(sample) {
		t.Fatal("masking hid a prose change")
	}
}
