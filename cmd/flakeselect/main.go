// Command flakeselect decides which tests scripts/flake-guard.sh re-runs, and summarises the
// results. It exists so selection is parsed Go (go/parser), not a regex over diff text.
//
//	flakeselect [-base REF] [-format tsv|regex]   selection from `git diff BASE...HEAD`
//	flakeselect report -mode M -pkg P [-rc N] [-whole] [-tail FILE] FILE.json
//
// Selection output, tab separated:
//
//	test <pkg> <TestName>               (-format tsv, one per test)
//	run  <pkg> <^(A|B)$> <count>        (-format regex, one per package, names QuoteMeta'd)
//	whole <pkg>                          a whole-package pass is wanted
//
// Exit codes: 0 ok, 2 usage or an unparsable file.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "report" {
		return runReport(args[1:], stdout, stderr)
	}
	fs := flag.NewFlagSet("flakeselect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	base := fs.String("base", "origin/main", "merge-base side of the diff (BASE...HEAD)")
	format := fs.String("format", "tsv", "tsv or regex")
	whole := fs.String("whole-pkgs", "internal/gateway,internal/transport", "comma separated directories that also get a whole-package pass when touched")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *format != "tsv" && *format != "regex" {
		_, _ = fmt.Fprintln(stderr, "flakeselect: -format must be tsv or regex")
		return 2
	}
	diff, err := git("diff", "-U0", "--no-renames", "--no-color", *base+"...HEAD", "--", "*.go")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "flakeselect:", err)
		return 2
	}
	sel, err := Select(parseDiff(diff), gitSource{}, splitList(*whole))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "flakeselect:", err)
		return 2
	}
	sel.Print(stdout, *format)
	return 0
}

func git(args ...string) (string, error) {
	cmd := exec.Command("git", args...) //nolint:gosec // G204: fixed program "git"; args are built by this tool from flags and fixed strings
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %v: %w: %s", args, err, stderr.String())
	}
	return string(out), nil
}
