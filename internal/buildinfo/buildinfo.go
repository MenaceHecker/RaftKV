// Package buildinfo carries the version stamped into release binaries (D3).
//
// The three variables are set at link time by scripts/release.sh:
//
//	-X github.com/MenaceHecker/raftkv/internal/buildinfo.Version=...
//
// A plain `go build` leaves the defaults below.
package buildinfo

import "io"

var (
	// Version is the git describe string of the built commit (with -dirty when built from a dirty tree).
	Version = "dev"
	// Commit is the full commit hash.
	Commit = "unknown"
	// Date is the COMMIT date in UTC, never the build time, so builds are reproducible.
	Date = "unknown"
)

// Line returns the one-line version banner for prog. STUB (D3 commit 1).
func Line(prog string) string { return "" }

// HandleFlag prints Line(prog) to w and returns true when args[0] is exactly
// -version or --version. STUB (D3 commit 1).
func HandleFlag(prog string, args []string, w io.Writer) bool { return false }
