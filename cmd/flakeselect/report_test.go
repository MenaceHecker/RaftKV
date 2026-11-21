package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func report(t *testing.T, json string, args ...string) (rows, tail string) {
	t.Helper()
	dir := t.TempDir()
	in := filepath.Join(dir, "in.json")
	if err := os.WriteFile(in, []byte(json), 0o600); err != nil {
		t.Fatal(err)
	}
	tailFile := filepath.Join(dir, "tail")
	var out, errb bytes.Buffer
	a := append([]string{"report", "-mode", "race", "-pkg", "./p", "-tail", tailFile}, args...)
	if code := run(append(a, in), &out, &errb); code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	b, _ := os.ReadFile(tailFile) //nolint:gosec // G304: a path under t.TempDir()
	return out.String(), string(b)
}

const twoRuns = `{"Action":"run","Test":"TestX"}
{"Action":"output","Test":"TestX","Output":"WARNING: DATA RACE\n"}
{"Action":"fail","Test":"TestX"}
{"Action":"run","Test":"TestX"}
{"Action":"run","Test":"TestX/sub"}
{"Action":"pass","Test":"TestX/sub"}
{"Action":"pass","Test":"TestX"}
{"Action":"run","Test":"TestY"}
{"Action":"pass","Test":"TestY"}
{"Action":"fail"}
`

func TestReportCountsFailuresPerTopLevelRun(t *testing.T) {
	rows, tail := report(t, twoRuns, "-rc", "1")
	for _, want := range []string{"row\trace\t./p\tTestX\t1\t2\n", "row\trace\t./p\tTestY\t0\t1\n"} {
		if !strings.Contains(rows, want) {
			t.Errorf("rows lack %q:\n%s", want, rows)
		}
	}
	if strings.Contains(rows, "TestX/sub") || strings.Contains(rows, "(package)") {
		t.Errorf("subtests or a package row leaked:\n%s", rows)
	}
	if !strings.Contains(tail, "first failure: race ./p TestX") || !strings.Contains(tail, "DATA RACE") {
		t.Errorf("tail = %q", tail)
	}
}

func TestReportNonzeroExitWithoutFailedTestIsAPackageFailure(t *testing.T) {
	rows, tail := report(t, `{"Action":"output","Output":"panic: boom\n"}`+"\n", "-rc", "2")
	if !strings.Contains(rows, "row\trace\t./p\t(package)\t1\t1\n") || !strings.Contains(tail, "panic: boom") {
		t.Fatalf("rows %q tail %q", rows, tail)
	}
}

func TestReportWholeFoldsPassingTests(t *testing.T) {
	rows, _ := report(t, twoRuns, "-whole", "-rc", "1")
	if !strings.Contains(rows, "(whole package: 2 tests)\t1\t3\n") || !strings.Contains(rows, "\tTestX\t1\t2\n") || strings.Contains(rows, "\tTestY\t") {
		t.Fatalf("rows:\n%s", rows)
	}
}

func TestReportCleanRunHasNoTail(t *testing.T) {
	rows, tail := report(t, `{"Action":"run","Test":"TestY"}`+"\n"+`{"Action":"pass","Test":"TestY"}`+"\n")
	if rows != "row\trace\t./p\tTestY\t0\t1\n" || tail != "" {
		t.Fatalf("rows %q tail %q", rows, tail)
	}
}
