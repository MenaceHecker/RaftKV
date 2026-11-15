// Command gateway is the public HTTP front of a RaftKV cluster: HTTP+JSON for
// a dashboard, with the abuse controls of RUNBOOK 10.6, in front of the
// cluster behind the gateway.Cluster interface.
//
// It serves plain HTTP; TLS is D6. This build has no live cluster adapter yet
// (it needs internal/client, card T4.6), so only --fake-cluster, an in-memory
// demo cluster, can serve.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
)

// settings is the validated command line.
type settings struct {
	listen          string
	nodes           string
	adminToken      string // HTTP admin Bearer
	nodeToken       string // machine token for node admin RPCs; used by the live adapter
	stateDir        string
	dashboardOrigin string
	keyspaceReset   time.Duration
	fakeCluster     bool
}

// loadSettings parses and validates the flags. It fails closed: both token
// files must exist, be mode 0600 and be non-empty, and the two tokens must
// differ, so a token typed into a browser is never the machine token.
func loadSettings(args []string) (*settings, error) {
	var s settings
	var adminFile, nodeFile string
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&s.listen, "listen", ":8080", "HTTP listen address (TLS is D6)")
	fs.StringVar(&s.nodes, "nodes", "", "nodes as id=az=addr=adminaddr,... (live adapter; not in this build)")
	fs.StringVar(&adminFile, "admin-token-file", "", "file holding the HTTP admin Bearer token (mode 0600)")
	fs.StringVar(&nodeFile, "node-token-file", "", "file holding the machine token for node admin RPCs (mode 0600)")
	fs.StringVar(&s.stateDir, "state-dir", "", "directory for the key registry")
	fs.StringVar(&s.dashboardOrigin, "dashboard-origin", "", "the one origin allowed by CORS, e.g. https://dash.example.com")
	fs.DurationVar(&s.keyspaceReset, "public-keyspace-reset", gateway.DefaultReset, "how often the public keyspace is wiped")
	fs.BoolVar(&s.fakeCluster, "fake-cluster", false, "serve an in-memory DEMO cluster instead of a real one")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	var err error
	if s.adminToken, err = gateway.LoadTokenFile(adminFile); err != nil {
		return nil, fmt.Errorf("--admin-token-file: %w", err)
	}
	if s.nodeToken, err = gateway.LoadTokenFile(nodeFile); err != nil {
		return nil, fmt.Errorf("--node-token-file: %w", err)
	}
	if s.adminToken == s.nodeToken {
		return nil, errors.New("the admin token and the node token must differ")
	}
	if s.stateDir == "" {
		return nil, errors.New("--state-dir is required")
	}
	if err := os.MkdirAll(s.stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("--state-dir: %w", err)
	}
	if s.dashboardOrigin, err = gateway.ParseOrigin(s.dashboardOrigin); err != nil {
		return nil, fmt.Errorf("--dashboard-origin: %w", err)
	}
	if s.keyspaceReset <= 0 {
		return nil, errors.New("--public-keyspace-reset must be positive")
	}
	if s.nodes == "" && !s.fakeCluster {
		return nil, errors.New("--nodes is required")
	}
	return &s, nil
}

// newHTTPServer bounds every phase of a connection and silences net/http's own
// error log, which prints remote addresses.
func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type systemRand struct{}

func (systemRand) Uint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return binary.LittleEndian.Uint64(b[:])
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	s, err := loadSettings(args)
	if err != nil {
		return err
	}
	if !s.fakeCluster {
		return errors.New("the live cluster adapter is not part of this build (D5 step 6, after card T4.6); use --fake-cluster for a demo")
	}
	logger.Warn("serving an in-memory DEMO cluster, not a real one")

	g, err := gateway.New(gateway.Config{
		Cluster:         fakecluster.New(),
		Clock:           systemClock{},
		Rand:            systemRand{},
		Logger:          logger,
		StateDir:        s.stateDir,
		DashboardOrigin: s.dashboardOrigin,
		AdminToken:      s.adminToken,
		KeyspaceReset:   s.keyspaceReset,
	})
	if err != nil {
		return err
	}
	defer func() { _ = g.Close() }()

	poll := time.NewTicker(gateway.DefaultPollEvery)
	defer poll.Stop()
	reset := time.NewTicker(time.Minute)
	defer reset.Stop()
	go g.RunPoller(ctx, poll.C)
	go g.RunReset(ctx, reset.C)
	g.PollOnce(ctx)

	srv := newHTTPServer(s.listen, g.Handler())
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("listening", "addr", s.listen)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	}
}

func main() {
	if err := runMain(); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}

func runMain() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, os.Args[1:], slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}
