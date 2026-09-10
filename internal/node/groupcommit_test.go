package node

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Tests for group commit.
//
// The benchmark that motivated this found write throughput pinned at one
// fsync per write, flat from 1 client to 64. The fix is to append the writes
// that are already waiting as a single durable write, so what these tests
// assert is not that concurrent writes succeed, which they always did, but
// that they stop paying for a disk write each.

// batchRecorder captures the size of every durable log write.
type batchRecorder struct {
	mu      sync.Mutex
	persist []int
}

func (r *batchRecorder) ObservePersist(entries int, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Hard state writes carry no entries and are not log appends.
	if entries > 0 {
		r.persist = append(r.persist, entries)
	}
}

func (r *batchRecorder) ObserveProposal(string, time.Duration) {}
func (r *batchRecorder) ObserveRead(string, time.Duration)     {}
func (r *batchRecorder) ObserveApply(int, time.Duration)       {}
func (r *batchRecorder) SnapshotCreated(uint64, time.Duration) {}
func (r *batchRecorder) SnapshotReceived()                     {}
func (r *batchRecorder) LeaderChanged(uint64, uint64, bool)    {}

// stats returns the biggest batch written, the total entries, and how many
// durable writes it took to store them.
func (r *batchRecorder) stats() (max, total, calls int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.persist {
		if n > max {
			max = n
		}
		total += n
	}
	return max, total, len(r.persist)
}

// concurrentWrites issues n writes at once from separate goroutines and
// returns once all have completed, failing on the first error.
func concurrentWrites(t *testing.T, n *Node, count int) {
	t.Helper()

	var wg sync.WaitGroup
	errs := make([]error, count)
	// A start gate makes the writes genuinely simultaneous. Launching
	// goroutines in a loop lets the first few finish before the last are
	// created, which would leave nothing to batch.
	gate := make(chan struct{})

	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			errs[i] = put(t, n, uint64(i+1), 1, fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
}

func TestConcurrentWritesShareOneDurableWrite(t *testing.T) {
	rec := &batchRecorder{}
	c := newTunedTestCluster(t, 3, func(cfg *Config) { cfg.Metrics = rec })
	leader := c.awaitLeader()

	const writers = 32
	concurrentWrites(t, leader, writers)

	max, total, calls := rec.stats()
	if total < writers {
		t.Fatalf("only %d entries were persisted, want at least %d", total, writers)
	}

	// The threshold is deliberately well below what this actually does.
	// Measured over twelve runs the smallest batch seen was 6 and the median
	// 10, so 4 leaves room for a loaded machine scheduling the writers badly
	// while still failing loudly if batching degenerates. Requiring merely
	// "more than one" would pass an implementation that grouped writes in
	// pairs and left almost all of the benefit on the table.
	const wantBatch = 4
	if max < wantBatch {
		t.Fatalf("the largest durable write covered %d entries; %d writes issued at once should "+
			"share far more than that, so group commit is barely working", max, writers)
	}

	// The economic claim: this many writes must not cost this many fsyncs.
	if calls >= writers {
		t.Fatalf("%d concurrent writes produced %d durable writes, one each", writers, calls)
	}
	t.Logf("%d concurrent writes became %d durable writes, largest batch %d entries",
		writers, calls, max)
}

func TestBatchedWritesEachGetTheirOwnResult(t *testing.T) {
	// Batching moves several clients' entries into one append, and each
	// client is waiting on a specific index. Mapping a client to the wrong
	// index would tell it about somebody else's write, so every value must
	// come back readable and correct.
	c := newTunedTestCluster(t, 3, nil)
	leader := c.awaitLeader()

	const writers = 24
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			if err := put(t, leader, uint64(i+1), 1,
				fmt.Sprintf("key%02d", i), fmt.Sprintf("value%02d", i)); err != nil {
				t.Errorf("write %d: %v", i, err)
			}
		}(i)
	}
	close(gate)
	wg.Wait()

	for i := range writers {
		want := fmt.Sprintf("value%02d", i)
		if got := mustGet(t, leader, fmt.Sprintf("key%02d", i)); got != want {
			t.Errorf("key%02d = %q, want %q", i, got, want)
		}
	}
}

func TestBatchSizeIsCapped(t *testing.T) {
	// The cap bounds how much one fsync is made to cover. Without it a burst
	// could produce an arbitrarily large single write, and the unlucky client
	// that started the batch waits for all of it.
	const maxBatch = 4
	rec := &batchRecorder{}
	c := newTunedTestCluster(t, 3, func(cfg *Config) {
		cfg.Metrics = rec
		cfg.MaxProposalBatch = maxBatch
	})
	leader := c.awaitLeader()

	concurrentWrites(t, leader, 40)

	if max, _, _ := rec.stats(); max > maxBatch {
		t.Errorf("a durable write covered %d entries with MaxProposalBatch=%d", max, maxBatch)
	}
}

func TestSingleWriterStillCommits(t *testing.T) {
	// Batching must never wait for writes that have not arrived. A lone
	// client has nobody to batch with, and if the loop paused hoping for
	// company its write would hang.
	rec := &batchRecorder{}
	c := newTunedTestCluster(t, 3, func(cfg *Config) { cfg.Metrics = rec })
	leader := c.awaitLeader()

	for i := range 5 {
		mustPut(t, leader, 1, uint64(i+1), "k", fmt.Sprintf("v%d", i))
	}
	if got := mustGet(t, leader, "k"); got != "v4" {
		t.Fatalf("k = %q, want v4", got)
	}
}
