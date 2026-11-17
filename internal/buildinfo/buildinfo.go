// Package buildinfo carries the version stamped into release binaries (D3).
//
// The three variables are set at link time by scripts/release.sh:
//
//	-X github.com/MenaceHecker/raftkv/internal/buildinfo.Version=...
//
// A plain `go build` leaves the defaults below.
package buildinfo

import (
	"fmt"
	"io"
	"runtime"
)

var (
	// Version is the git describe string of the built commit (with -dirty when built from a dirty tree).
	Version = "dev"
	// Commit is the full commit hash.
	Commit = "unknown"
	// Date is the COMMIT date in UTC, never the build time, so builds are reproducible.
	Date = "unknown"
)

// Line returns the one-line version banner for prog. It never includes paths or hostnames.
func Line(prog string) string {
	return fmt.Sprintf("%s %s %s %s %s/%s %s", prog, Version, Commit, Date, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// HandleFlag prints Line(prog) and a newline to w and returns true when args[0] is
// exactly -version or --version. Only the first argument counts, so the check can run
// before any other flag or file is read and registers no flag of its own.
func HandleFlag(prog string, args []string, w io.Writer) bool {
	if len(args) == 0 || (args[0] != "-version" && args[0] != "--version") {
		return false
	}
	_, _ = fmt.Fprintln(w, Line(prog))
	return true
}
