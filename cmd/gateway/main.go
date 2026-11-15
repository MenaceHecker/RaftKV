// Command gateway is the public HTTP front of a RaftKV cluster.
//
// This is the stub (D5 commit 1).
package main

import (
	"fmt"
	"os"
)

// settings is the validated command line.
type settings struct{}

// loadSettings parses and validates the flags. It fails closed.
func loadSettings(_ []string) (*settings, error) { return &settings{}, nil }

func main() {
	if _, err := loadSettings(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, "gateway: not implemented")
	os.Exit(1)
}
