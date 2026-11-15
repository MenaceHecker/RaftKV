// Command raftkvctl is a small command-line client for a RaftKV cluster. STUB.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/MenaceHecker/raftkv/internal/client"
)

type deps struct {
	connect func(addr string) (client.KV, io.Closer, error)
	clock   client.Clock
	rand    client.Rand
}

func run(_ []string, _ io.Reader, _, stderr io.Writer, d deps) int {
	_, _ = fmt.Fprintln(stderr, "raftkvctl: not implemented")
	if d.connect != nil || d.clock != nil || d.rand != nil {
		return 71
	}
	return 70
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, deps{}))
}
