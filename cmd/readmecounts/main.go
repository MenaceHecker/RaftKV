// Command readmecounts keeps the README's test and line counts exact, the way
// internal/determinism/readme_test.go demands, without hand-editing them.
//
//	readmecounts           rewrite README.md in place (the default)
//	readmecounts -check    exit 1 and print the problems and the fix command if readme_test.go would fail
//	readmecounts -mask     filter a README on stdin, hiding the number sites (for merging)
//
// Exit codes: 0 fine, 1 stale (-check only), 2 usage error or the tree cannot be counted.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/MenaceHecker/raftkv/internal/readmecounts"
)

const fixCommand = "make readme-counts"

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("readmecounts", flag.ContinueOnError)
	fs.SetOutput(stderr)
	check := fs.Bool("check", false, "exit 1 and show the diff if the README counts are stale; write nothing")
	write := fs.Bool("write", false, "rewrite README.md in place (the default)")
	mask := fs.Bool("mask", false, "read a README on stdin and print it with the number sites masked")
	root := fs.String("root", ".", "repository root")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if (*check && *write) || (*mask && (*check || *write)) || fs.NArg() > 0 {
		say(stderr, "%s\n", "readmecounts: use at most one of -check, -write, -mask")
		return 2
	}

	if *mask {
		b, err := io.ReadAll(stdin)
		if err != nil {
			say(stderr, "readmecounts: %v\n", err)
			return 2
		}
		say(stdout, "%s", readmecounts.Mask(string(b)))
		return 0
	}

	tests, fuzz, impl, testLines, err := readmecounts.Count(*root)
	if err != nil {
		say(stderr, "%s\n", err)
		return 2
	}
	path := filepath.Join(*root, "README.md")
	old, err := os.ReadFile(path) //nolint:gosec // G304: the path is the -root flag joined with README.md; this is a developer CLI
	if err != nil {
		say(stderr, "readmecounts: %v\n", err)
		return 2
	}
	counts := readmecounts.Counts{Tests: tests, Fuzz: fuzz, ImplLines: impl, TestLines: testLines}
	updated, err := readmecounts.Rewrite(string(old), counts)
	if err != nil {
		say(stderr, "readmecounts: %v\n", err)
		return 2
	}

	if *check {
		// -check fails exactly when readme_test.go would: it is never stricter than the oracle.
		problems, err := readmecounts.Check(string(old), counts)
		if err != nil {
			say(stderr, "readmecounts: %v\n", err)
			return 2
		}
		if len(problems) == 0 {
			say(stdout, "README counts are current (%d tests, %d fuzz targets).\n", tests, fuzz)
			return 0
		}
		say(stdout, "README.md counts are stale (%d tests, %d fuzz targets, %d implementation and %d test lines actually):\n", tests, fuzz, impl, testLines)
		for _, p := range problems {
			say(stdout, "  %s\n", p)
		}
		say(stdout, "%s", changedLines(string(old), updated))
		say(stdout, "fix: run `%s` and commit README.md\n", fixCommand)
		return 1
	}
	if updated == string(old) {
		say(stdout, "README counts are current (%d tests, %d fuzz targets).\n", tests, fuzz)
		return 0
	}
	info, err := os.Stat(path)
	mode := os.FileMode(0o644)
	if err == nil {
		mode = info.Mode().Perm()
	}
	if err := os.WriteFile(path, []byte(updated), mode); err != nil { //nolint:gosec // G703: same developer-CLI path as the read above
		say(stderr, "readmecounts: %v\n", err)
		return 2
	}
	say(stdout, "README.md updated (%d tests, %d fuzz targets).\n", tests, fuzz)
	say(stdout, "%s", changedLines(string(old), updated))
	return 0
}

// changedLines shows the lines that differ. Rewrite only edits inside lines, so the two
// texts always have the same number of lines.
func changedLines(old, updated string) string {
	o, u := strings.Split(old, "\n"), strings.Split(updated, "\n")
	var b strings.Builder
	for i := range o {
		if i < len(u) && o[i] != u[i] {
			fmt.Fprintf(&b, "  README.md:%d\n    - %s\n    + %s\n", i+1, clip(o[i]), clip(u[i]))
		}
	}
	return b.String()
}

// say writes to a terminal stream; a failed write to stdout or stderr has nowhere to be reported.
func say(w io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }

func clip(s string) string {
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }
