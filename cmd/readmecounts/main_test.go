package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture builds a tree with 3 tests, 5 fuzz targets, 2000 implementation and 1000 test lines.
func fixture(t *testing.T, claimedTests string) string {
	t.Helper()
	root := t.TempDir()
	put := func(rel, content string) {
		if err := os.WriteFile(filepath.Join(root, rel), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	put("a.go", "package a\n"+strings.Repeat("//\n", 1999))
	body := "package a\n"
	for _, n := range []string{"TestA", "TestB", "TestC", "FuzzA", "FuzzB", "FuzzC", "FuzzD", "FuzzE"} {
		body += "func " + n + "() {}\n"
	}
	put("a_test.go", body+strings.Repeat("//\n", 991))
	put("README.md", claimedTests+" tests and five fuzz targets.\n\nRoughly 2,000 lines of implementation and 1,000 of tests, across "+claimedTests+" tests.\n")
	return root
}

func TestCheckExitsOneWhenStaleZeroWhenFresh(t *testing.T) {
	stale := fixture(t, "4")
	var out, errb bytes.Buffer
	if code := run([]string{"-check", "-root", stale}, strings.NewReader(""), &out, &errb); code != 1 {
		t.Fatalf("stale: exit %d; want 1 (out %q err %q)", code, out.String(), errb.String())
	}
	if all := out.String() + errb.String(); !strings.Contains(all, "make readme-counts") || !strings.Contains(all, "4 tests") {
		t.Fatalf("stale: output does not show the diff and the fix command: %q", all)
	}

	fresh := fixture(t, "3")
	out.Reset()
	errb.Reset()
	if code := run([]string{"-check", "-root", fresh}, strings.NewReader(""), &out, &errb); code != 0 {
		t.Fatalf("fresh: exit %d; want 0 (out %q err %q)", code, out.String(), errb.String())
	}
}

func TestWriteThenCheckIsClean(t *testing.T) {
	root := fixture(t, "4")
	var out, errb bytes.Buffer
	if code := run([]string{"-root", root}, strings.NewReader(""), &out, &errb); code != 0 {
		t.Fatalf("write: exit %d (err %q)", code, errb.String())
	}
	b, err := os.ReadFile(filepath.Join(root, "README.md")) //nolint:gosec // G304: path under t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "3 tests and five fuzz targets") || !strings.Contains(string(b), "across 3 tests") {
		t.Fatalf("README not rewritten: %q", b)
	}
	if code := run([]string{"-check", "-root", root}, strings.NewReader(""), &out, &errb); code != 0 {
		t.Fatalf("check after write: exit %d", code)
	}
}

func TestRefusalAndUsageExitTwo(t *testing.T) {
	root := fixture(t, "3")
	if err := os.MkdirAll(filepath.Join(root, ".claude", "worktrees", "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".claude", "worktrees", "x", "a_test.go"), []byte("package a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	if code := run([]string{"-check", "-root", root}, strings.NewReader(""), &out, &errb); code != 2 {
		t.Fatalf("refusal: exit %d; want 2", code)
	}
	if code := run([]string{"-bogus"}, strings.NewReader(""), &out, &errb); code != 2 {
		t.Fatalf("usage: exit %d; want 2", code)
	}
}

func TestMaskModeFiltersStdin(t *testing.T) {
	var out, errb bytes.Buffer
	in := "10 tests and six fuzz targets\n"
	if code := run([]string{"-mask"}, strings.NewReader(in), &out, &errb); code != 0 {
		t.Fatalf("mask: exit %d (err %q)", code, errb.String())
	}
	if strings.Contains(out.String(), "10 tests") || !strings.Contains(out.String(), "fuzz targets") {
		t.Fatalf("mask output %q", out.String())
	}
}
