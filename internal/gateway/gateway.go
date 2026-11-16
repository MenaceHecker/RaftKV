package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Gateway is the public HTTP front of the cluster.
type Gateway struct {
	cfg         Config
	cl          Cluster
	clk         Clock
	rnd         Rand
	log         *slog.Logger
	adminDigest [sha256.Size]byte
	topology    []NodeInfo

	reg  *registry
	lim  *limiter
	pool chan KV

	mu        sync.Mutex // guards the three fields below
	verifying int
	resetting bool
	lastReset time.Time

	pollMu sync.RWMutex
	cache  map[uint64]nodeCache

	chaos chaosCtl
	hub   *hub
	ver   verifier

	base context.Context // ends at Close; everything in the background hangs off it
	stop context.CancelFunc
	wg   sync.WaitGroup

	handler http.Handler
}

// New builds a Gateway. It fails closed: a missing cluster, clock, state
// directory, admin token or a bad dashboard origin is an error.
func New(cfg Config) (*Gateway, error) {
	switch {
	case cfg.Cluster == nil:
		return nil, errors.New("gateway: no cluster")
	case cfg.Clock == nil:
		return nil, errors.New("gateway: no clock")
	case cfg.Rand == nil:
		return nil, errors.New("gateway: no random source")
	case cfg.StateDir == "":
		return nil, errors.New("gateway: no state directory")
	case cfg.AdminToken == "":
		return nil, errors.New("gateway: no admin token")
	}
	if _, err := ParseOrigin(cfg.DashboardOrigin); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.KeyspaceReset <= 0 {
		cfg.KeyspaceReset = DefaultReset
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultTimeout
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 2 * time.Second
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = DefaultPoolSize
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	if cfg.RatePerSecond <= 0 {
		cfg.RatePerSecond = DefaultRate
	}
	if cfg.Burst <= 0 {
		cfg.Burst = DefaultBurst
	}
	if cfg.MaxLimiterIPs <= 0 {
		cfg.MaxLimiterIPs = 10000
	}

	reg, err := openRegistry(cfg.StateDir, cfg.MaxKeys)
	if err != nil {
		return nil, err
	}
	g := &Gateway{
		cfg: cfg, cl: cfg.Cluster, clk: cfg.Clock, rnd: cfg.Rand, log: cfg.Logger,
		adminDigest: sha256.Sum256([]byte(cfg.AdminToken)),
		topology:    cfg.Cluster.Topology(),
		reg:         reg,
		lim:         newLimiter(cfg.RatePerSecond, cfg.Burst, cfg.MaxLimiterIPs),
		pool:        make(chan KV, cfg.PoolSize),
		lastReset:   cfg.Clock.Now(),
		cache:       map[uint64]nodeCache{},
		hub:         newHub(),
	}
	g.base, g.stop = context.WithCancel(context.Background())
	if err := g.loadVerify(); err != nil {
		_ = reg.close()
		return nil, err
	}
	for i := 0; i < cfg.PoolSize; i++ {
		g.pool <- cfg.Cluster.NewKV()
	}
	g.handler = g.routes()
	return g, nil
}

// Handler is the HTTP surface.
func (g *Gateway) Handler() http.Handler { return g.handler }

// Close stops the background work (a verification run is aborted and the
// cluster healed first) and releases the state files. It is safe to call twice.
func (g *Gateway) Close() error {
	g.stop()
	g.wg.Wait()
	return g.reg.close()
}

// LimiterSize is the number of client addresses the rate limiter remembers.
func (g *Gateway) LimiterSize() int { return g.lim.size() }

// Keys lists the registered public keys.
func (g *Gateway) Keys() []string { return g.reg.list() }

// Start begins the work that needs a running process: the safe start (heal
// every node, count the public chaos bucket as just used, hold new faults off
// until every node answers) and one upstream event stream per node. It ends
// when ctx ends or the gateway is closed.
func (g *Gateway) Start(ctx context.Context) {
	sctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-g.base.Done():
		case <-sctx.Done():
		}
		cancel()
	}()
	g.safeStart(sctx)
	g.startEvents(sctx)
}
