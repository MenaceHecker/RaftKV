package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type event struct {
	Action  string
	Package string
	Test    string
	Output  string
}

type counts struct{ fails, runs int }

// runReport turns one `go test -json` file into rows:
//
//	row <mode> <pkg> <test> <fails> <runs>
//
// A test's runs and fails are counted per top-level test, once per -count iteration.
// With -whole the passing tests are folded into one "(whole package: N tests)" row and only
// failing tests are listed. If go test exited nonzero (-rc) but no test is recorded as failed
// (build error, panic, timeout), a "(package)" row with 1/1 records it. -tail FILE receives the
// output of the first failing run (at most -tail-lines lines).
func runReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("mode", "", "mode label")
	pkg := fs.String("pkg", "", "package label")
	rc := fs.Int("rc", 0, "exit status of go test")
	whole := fs.Bool("whole", false, "fold passing tests into one row")
	tail := fs.String("tail", "", "write the first failure's output here")
	tailLines := fs.Int("tail-lines", 30, "lines of failure output to keep")
	if err := fs.Parse(args); err != nil || fs.NArg() != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: flakeselect report -mode M -pkg P [-rc N] [-whole] [-tail FILE] FILE.json")
		return 2
	}
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "flakeselect:", err)
		return 2
	}
	defer func() { _ = f.Close() }() // read-only

	per := map[string]*counts{}
	cur := map[string][]string{} // output of the iteration in progress
	var firstFail []string
	var firstName string
	var pkgOut []string
	pkgFailed := false
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var ev event
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			pkgOut = append(pkgOut, sc.Text()) // non-JSON line (build output)
			continue
		}
		top := ev.Test != "" && !strings.Contains(ev.Test, "/")
		switch ev.Action {
		case "run":
			if top {
				cur[ev.Test] = nil
			}
		case "output":
			if top {
				cur[ev.Test] = append(cur[ev.Test], strings.TrimRight(ev.Output, "\n"))
			} else if ev.Test == "" {
				pkgOut = append(pkgOut, strings.TrimRight(ev.Output, "\n"))
			}
		case "pass", "fail":
			if ev.Test == "" {
				if ev.Action == "fail" {
					pkgFailed = true
				}
				continue
			}
			if !top {
				continue
			}
			c := per[ev.Test]
			if c == nil {
				c = &counts{}
				per[ev.Test] = c
			}
			c.runs++
			if ev.Action == "fail" {
				c.fails++
				if firstFail == nil {
					firstFail, firstName = cur[ev.Test], ev.Test
				}
			}
		}
	}
	names := make([]string, 0, len(per))
	totalFails := 0
	for n, c := range per {
		names = append(names, n)
		totalFails += c.fails
	}
	sort.Strings(names)
	row := func(test string, fails, runs int) {
		_, _ = fmt.Fprintf(stdout, "row\t%s\t%s\t%s\t%d\t%d\n", *mode, *pkg, test, fails, runs)
	}
	if *whole {
		row(fmt.Sprintf("(whole package: %d tests)", len(names)), totalFails, max(1, sumRuns(per)))
		for _, n := range names {
			if per[n].fails > 0 {
				row(n, per[n].fails, per[n].runs)
			}
		}
	} else {
		for _, n := range names {
			row(n, per[n].fails, per[n].runs)
		}
	}
	if (*rc != 0 || pkgFailed) && totalFails == 0 {
		row("(package)", 1, 1)
		firstName, firstFail = "(package)", pkgOut
	}
	if *tail != "" && firstFail != nil {
		if len(firstFail) > *tailLines {
			firstFail = firstFail[len(firstFail)-*tailLines:]
		}
		body := fmt.Sprintf("first failure: %s %s %s\n%s\n", *mode, *pkg, firstName, strings.Join(firstFail, "\n"))
		if err := os.WriteFile(*tail, []byte(body), 0o600); err != nil {
			_, _ = fmt.Fprintln(stderr, "flakeselect:", err)
			return 2
		}
	}
	return 0
}

func sumRuns(per map[string]*counts) int {
	n := 0
	for _, c := range per {
		n += c.runs
	}
	return n
}
