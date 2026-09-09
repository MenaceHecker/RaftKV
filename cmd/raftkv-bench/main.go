// Command raftkv-bench measures what a running RaftKV cluster actually does.
//
// It exists because published throughput numbers that nobody can reproduce are
// worse than no numbers at all. Everything it reports is measured against real
// nodes over real gRPC, with real fsyncs underneath, and it prints the
// conditions alongside the results so a number can be argued with.
//
// It is a closed-loop benchmark: each client sends one request, waits for the
// reply, and sends the next. That measures latency honestly and throughput
// conservatively. An open-loop generator would produce larger throughput
// figures by queueing work the cluster has not agreed to yet, which says more
// about the queue than the cluster.
//
//	raftkv-bench --endpoints 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
//	  --clients 32 --duration 30s --workload mixed
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/MenaceHecker/raftkv/internal/transport/raftkv/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv-bench:", err)
		os.Exit(1)
	}
}

type options struct {
	endpoints string
	clients   int
	duration  time.Duration
	warmup    time.Duration
	keys      int
	valueSize int
	workload  string
	readRatio float64
	timeout   time.Duration
}

func run() error {
	var opt options
	flag.StringVar(&opt.endpoints, "endpoints", "127.0.0.1:9001",
		"comma-separated node addresses; any one is enough, redirects find the leader")
	flag.IntVar(&opt.clients, "clients", 16,
		"concurrent clients, each with its own session and sequence numbers")
	flag.DurationVar(&opt.duration, "duration", 30*time.Second, "how long to measure for")
	flag.DurationVar(&opt.warmup, "warmup", 3*time.Second,
		"time to run before measuring, so an election or a cold cache is not counted")
	flag.IntVar(&opt.keys, "keys", 1000, "size of the key space")
	flag.IntVar(&opt.valueSize, "value-size", 128, "value size in bytes")
	flag.StringVar(&opt.workload, "workload", "mixed", "write, read, or mixed")
	flag.Float64Var(&opt.readRatio, "read-ratio", 0.9, "fraction of reads when workload is mixed")
	flag.DurationVar(&opt.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.Parse()

	if opt.clients < 1 {
		return errors.New("--clients must be at least 1")
	}
	switch opt.workload {
	case "write", "read", "mixed":
	default:
		return fmt.Errorf("unknown --workload %q", opt.workload)
	}

	addrs := strings.Split(opt.endpoints, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	pool, err := newPool(addrs)
	if err != nil {
		return err
	}
	defer pool.close()

	fmt.Printf("raftkv-bench: %s workload, %d clients, %d byte values, %d keys\n",
		opt.workload, opt.clients, opt.valueSize, opt.keys)
	fmt.Printf("              %v warmup then %v measured against %d endpoints\n\n",
		opt.warmup, opt.duration, len(addrs))

	// A read-only workload needs keys to exist, or it measures the cost of
	// answering "not found" and nothing else.
	if opt.workload != "write" {
		fmt.Print("seeding the key space... ")
		if err := seed(pool, &opt); err != nil {
			return fmt.Errorf("seeding: %w", err)
		}
		fmt.Println("done")
	}

	res := measure(pool, &opt)
	res.print(&opt)
	return nil
}

// pool holds one connection per node and remembers which is the leader.
//
// The leader is discovered from redirects rather than configuration, which is
// what a real client has to do anyway: membership can change underneath it.
type pool struct {
	mu      sync.RWMutex
	conns   map[string]raftkvv1.KVServiceClient
	all     []*grpc.ClientConn
	leader  string
	addrs   []string
	dialing sync.Mutex
}

func newPool(addrs []string) (*pool, error) {
	p := &pool{conns: make(map[string]raftkvv1.KVServiceClient), addrs: addrs}
	for _, a := range addrs {
		if err := p.dial(a); err != nil {
			p.close()
			return nil, err
		}
	}
	p.leader = addrs[0]
	return p, nil
}

func (p *pool) dial(addr string) error {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dialing %s: %w", addr, err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[addr] = raftkvv1.NewKVServiceClient(cc)
	p.all = append(p.all, cc)
	return nil
}

func (p *pool) client() (raftkvv1.KVServiceClient, string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conns[p.leader], p.leader
}

// redirect points the pool at a new leader. An address that was not in the
// original endpoint list is dialed on demand, which is how a client follows
// the cluster through a membership change.
func (p *pool) redirect(addr string) {
	if addr == "" {
		return
	}
	p.mu.RLock()
	_, known := p.conns[addr]
	p.mu.RUnlock()

	if !known {
		p.dialing.Lock()
		p.mu.RLock()
		_, known = p.conns[addr]
		p.mu.RUnlock()
		if !known {
			if err := p.dial(addr); err != nil {
				p.dialing.Unlock()
				return
			}
		}
		p.dialing.Unlock()
	}

	p.mu.Lock()
	p.leader = addr
	p.mu.Unlock()
}

// rotate moves to some other node, for when the current one names no leader.
// During an election nobody knows who leads, so trying elsewhere is all a
// client can do.
func (p *pool) rotate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, a := range p.addrs {
		if a == p.leader {
			p.leader = p.addrs[(i+1)%len(p.addrs)]
			return
		}
	}
}

func (p *pool) close() {
	for _, cc := range p.all {
		cc.Close()
	}
}

// notLeader extracts the redirect detail, if the error carries one.
func notLeader(err error) (*raftkvv1.NotLeader, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, d := range st.Details() {
		if nl, ok := d.(*raftkvv1.NotLeader); ok {
			return nl, true
		}
	}
	return nil, false
}

// result is what one measured run produced.
type result struct {
	latencies []time.Duration
	reads     int
	writes    int
	redirects int
	errors    map[string]int
	elapsed   time.Duration
}

func seed(p *pool, opt *options) error {
	value := make([]byte, opt.valueSize)
	seq := uint64(0)
	for i := range opt.keys {
		seq++
		ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
		err := putOnce(ctx, p, 1, seq, fmt.Sprintf("key%06d", i), value)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// putOnce writes one key, following redirects until it lands on the leader.
func putOnce(ctx context.Context, p *pool, clientID, seq uint64, key string, value []byte) error {
	for attempt := 0; attempt < 20; attempt++ {
		kv, _ := p.client()
		_, err := kv.Put(ctx, &raftkvv1.PutRequest{
			Client: &raftkvv1.ClientRequest{ClientId: clientID, Sequence: seq},
			Key:    key,
			Value:  value,
		})
		if err == nil {
			return nil
		}
		nl, ok := notLeader(err)
		if !ok {
			return err
		}
		if nl.LeaderAddress == "" {
			p.rotate()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		p.redirect(nl.LeaderAddress)
	}
	return errors.New("gave up following redirects")
}

// measure runs the workload and collects per-request latencies.
func measure(p *pool, opt *options) result {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		merged    = result{errors: map[string]int{}}
		measuring window
	)

	value := make([]byte, opt.valueSize)
	stop := make(chan struct{})

	for c := range opt.clients {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()

			local := result{errors: map[string]int{}}
			rng := rand.New(rand.NewSource(int64(c)*7919 + 1))
			// Client IDs start above the seeder's so sequence numbers never
			// collide with it in the dedup table.
			clientID := uint64(c) + 100
			seq := uint64(0)

			for {
				select {
				case <-stop:
					mu.Lock()
					merged.latencies = append(merged.latencies, local.latencies...)
					merged.reads += local.reads
					merged.writes += local.writes
					merged.redirects += local.redirects
					for k, v := range local.errors {
						merged.errors[k] += v
					}
					mu.Unlock()
					return
				default:
				}

				isRead := opt.workload == "read" ||
					(opt.workload == "mixed" && rng.Float64() < opt.readRatio)
				key := fmt.Sprintf("key%06d", rng.Intn(opt.keys))

				start := time.Now()
				var err error
				if isRead {
					err = getOnce(p, opt, key, &local)
				} else {
					seq++
					ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
					err = putOnce(ctx, p, clientID, seq, key, value)
					cancel()
				}
				took := time.Since(start)

				// Only requests issued during the measured window count.
				// Warmup traffic still runs, so the cluster is in steady
				// state when measurement begins.
				if measuring.on() {
					if err != nil {
						local.errors[classify(err)]++
					} else {
						local.latencies = append(local.latencies, took)
						if isRead {
							local.reads++
						} else {
							local.writes++
						}
					}
				}
			}
		}(c)
	}

	time.Sleep(opt.warmup)
	measuring.set(true)
	began := time.Now()
	time.Sleep(opt.duration)
	merged.elapsed = time.Since(began)
	measuring.set(false)
	close(stop)
	wg.Wait()

	return merged
}

// window is a flag marking whether the measured period is open. Warmup
// traffic runs through the same code path but is not recorded, so the cluster
// is in steady state by the time anything counts.
type window struct {
	mu sync.RWMutex
	v  bool
}

func (w *window) set(v bool) { w.mu.Lock(); w.v = v; w.mu.Unlock() }
func (w *window) on() bool   { w.mu.RLock(); defer w.mu.RUnlock(); return w.v }

func getOnce(p *pool, opt *options, key string, local *result) error {
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), opt.timeout)
		kv, _ := p.client()
		_, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: key})
		cancel()
		if err == nil {
			return nil
		}
		nl, ok := notLeader(err)
		if !ok {
			return err
		}
		local.redirects++
		if nl.LeaderAddress == "" {
			p.rotate()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		p.redirect(nl.LeaderAddress)
	}
	return errors.New("gave up following redirects")
}

func classify(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Code().String()
	}
	return "unknown"
}

func (r *result) print(opt *options) {
	total := r.reads + r.writes
	if total == 0 {
		fmt.Println("\nno successful operations")
		for k, v := range r.errors {
			fmt.Printf("  %s: %d\n", k, v)
		}
		return
	}

	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })

	pct := func(f float64) time.Duration {
		if len(r.latencies) == 0 {
			return 0
		}
		i := int(f * float64(len(r.latencies)))
		if i >= len(r.latencies) {
			i = len(r.latencies) - 1
		}
		return r.latencies[i]
	}

	var sum time.Duration
	for _, d := range r.latencies {
		sum += d
	}

	fmt.Printf("\n%-22s %v\n", "duration", r.elapsed.Round(time.Millisecond))
	fmt.Printf("%-22s %d (%d reads, %d writes)\n", "operations", total, r.reads, r.writes)
	fmt.Printf("%-22s %.0f ops/s\n", "throughput", float64(total)/r.elapsed.Seconds())
	fmt.Println()
	fmt.Printf("%-22s %v\n", "latency mean", (sum / time.Duration(len(r.latencies))).Round(time.Microsecond))
	for _, p := range []struct {
		name string
		f    float64
	}{{"p50", 0.50}, {"p95", 0.95}, {"p99", 0.99}, {"p99.9", 0.999}} {
		fmt.Printf("%-22s %v\n", "latency "+p.name, pct(p.f).Round(time.Microsecond))
	}
	fmt.Printf("%-22s %v\n", "latency max", r.latencies[len(r.latencies)-1].Round(time.Microsecond))

	if r.redirects > 0 {
		fmt.Printf("\n%-22s %d\n", "redirects followed", r.redirects)
	}
	if len(r.errors) > 0 {
		fmt.Println("\nerrors:")
		for k, v := range r.errors {
			fmt.Printf("  %-20s %d\n", k, v)
		}
	}
}
