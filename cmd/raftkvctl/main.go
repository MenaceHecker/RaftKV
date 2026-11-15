// Command raftkvctl is a small command-line client for a RaftKV cluster, built
// on internal/client.
//
//	raftkvctl --nodes a:7000,b:7000,c:7000 [flags] get KEY
//	raftkvctl --nodes ... put KEY VALUE      (VALUE "-" reads stdin)
//	raftkvctl --nodes ... delete KEY
//	raftkvctl --nodes ... status
//
// Exit status: 0 success, 1 key not found, 2 failure that cannot have changed
// anything (or a usage error), 3 a write whose outcome is unknown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/MenaceHecker/raftkv/internal/client"
)

const (
	exitOK       = 0
	exitNotFound = 1
	exitFailed   = 2
	exitUnknown  = 3
)

// deps are the seams tests replace; the zero value is the real world.
type deps struct {
	connect func(addr string) (client.KV, io.Closer, error)
	clock   client.Clock
	rand    client.Rand
}

const usageText = `usage: raftkvctl --nodes host:port,host:port [flags] COMMAND

commands:
  get KEY            print the value; exit 1 if the key does not exist
  put KEY VALUE      set a key (VALUE "-" reads the value from stdin)
  delete KEY         remove a key
  status             ask every node who it thinks leads

exit status: 0 ok, 1 not found, 2 failed or usage error, 3 outcome unknown
(a write was sent but no definite answer came back; it may have been applied).

Client ids: by default each run uses a fresh random id, so every run starts a
new session. If you set --client-id you also own --start-seq: reusing an id
with a sequence at or below one the cluster has already applied makes the
cluster silently ignore your writes while reporting success. Do not reuse a
fixed id across runs.

flags:
`

func run(args []string, stdin io.Reader, stdout, stderr io.Writer, d deps) int {
	fs := flag.NewFlagSet("raftkvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodes := fs.String("nodes", "", "comma-separated node addresses (required)")
	clientID := fs.Uint64("client-id", 0, "session id; 0 picks a random one (see above before setting)")
	startSeq := fs.Uint64("start-seq", 0, "first sequence number; only meaningful with --client-id")
	timeout := fs.Duration("timeout", 5*time.Second, "total time allowed for the command")
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, usageText)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitFailed
	}

	rest := fs.Args()
	usage := func(msg string) int {
		_, _ = fmt.Fprintf(stderr, "raftkvctl: %s\n", msg)
		fs.Usage()
		return exitFailed
	}
	if *nodes == "" {
		return usage("--nodes is required")
	}
	if len(rest) == 0 {
		return usage("missing command")
	}
	want := map[string]int{"get": 2, "put": 3, "delete": 2, "status": 1}
	n, known := want[rest[0]]
	if !known {
		return usage("unknown command " + rest[0])
	}
	if len(rest) != n {
		return usage(fmt.Sprintf("%s takes %d argument(s)", rest[0], n-1))
	}

	var addrs []string
	for _, a := range strings.Split(*nodes, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	cfg := client.Config{
		Addrs:    addrs,
		ClientID: *clientID,
		StartSeq: *startSeq,
		Connect:  d.connect,
		Clock:    d.clock,
		Rand:     d.rand,
	}
	c, err := client.New(cfg)
	if err != nil {
		return fail(stderr, err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch rest[0] {
	case "get":
		v, found, err := c.Get(ctx, rest[1])
		if err != nil {
			return fail(stderr, err)
		}
		if !found {
			_, _ = fmt.Fprintln(stderr, "not found")
			return exitNotFound
		}
		_, _ = fmt.Fprintf(stdout, "%s\n", v)
	case "put":
		value := []byte(rest[2])
		if rest[2] == "-" {
			if value, err = io.ReadAll(stdin); err != nil {
				return fail(stderr, fmt.Errorf("reading stdin: %w", err))
			}
		}
		if err := c.Put(ctx, rest[1], value); err != nil {
			return fail(stderr, err)
		}
	case "delete":
		if err := c.Delete(ctx, rest[1]); err != nil {
			return fail(stderr, err)
		}
	case "status":
		return printStatus(ctx, c, addrs, stdout, stderr)
	}
	return exitOK
}

// printStatus asks every node and prints one line each. It succeeds while at least
// one node answers.
func printStatus(ctx context.Context, c *client.Client, addrs []string, stdout, stderr io.Writer) int {
	answered := 0
	for _, a := range addrs {
		st, err := c.StatusFrom(ctx, a)
		if err != nil {
			_, _ = fmt.Fprintf(stdout, "%s\tunreachable: %v\n", a, err)
			continue
		}
		answered++
		_, _ = fmt.Fprintf(stdout, "%s\tid=%d state=%s term=%d leader=%d leader_addr=%s commit=%d applied=%d\n",
			a, st.ID, st.State, st.Term, st.LeaderID, st.LeaderAddr, st.CommitIndex, st.AppliedIndex)
	}
	if answered == 0 {
		_, _ = fmt.Fprintln(stderr, "raftkvctl: no node answered")
		return exitFailed
	}
	return exitOK
}

func fail(stderr io.Writer, err error) int {
	if client.IsIndeterminate(err) {
		_, _ = fmt.Fprintf(stderr, "raftkvctl: outcome unknown: the write may or may not have been applied: %v\n", err)
		return exitUnknown
	}
	_, _ = fmt.Fprintf(stderr, "raftkvctl: %v\n", err)
	return exitFailed
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, deps{}))
}
