// Command chaosrun runs the chaos scenarios against the simulated cluster.
package main

import (
	"io"
	"os"

	"github.com/MenaceHecker/raftkv/chaos"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, chaos.AllScenarios()))
}

func run(_ []string, _, _ io.Writer, _ []chaos.Scenario) int {
	return 99
}
