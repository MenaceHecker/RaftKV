package node

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type batchRecorder struct {
	mu      sync.Mutex
	persist []int
}

func (r *batchRecorder) ObservePersist(entries int, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

func concurrentWrites(t *testing.T, n *Node, count int) {
	t.Helper()

	var wg sync.WaitGroup
	errs := make([]error, count)
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
	const (
		writers = 32
		rounds  = 3
	)

	var (
		bestBatch int
		bestCalls = writers * 10
	)
	for round := range rounds {
		rec := &batchRecorder{}
		c := newTunedTestCluster(t, 3, func(cfg *Config) { cfg.Metrics = rec })
		leader := c.awaitLeader()
		concurrentWrites(t, leader, writers)

		max, total, calls := rec.stats()
		if total < writers {
			t.Fatalf("round %d persisted %d entries, want at least %d", round, total, writers)
		}
		if max > bestBatch {
			bestBatch = max
		}
		if calls < bestCalls {
			bestCalls = calls
		}
		c.stopAll()
	}

	const wantBatch = 4
	if bestBatch < wantBatch {
		t.Errorf("the largest durable write across %d bursts covered %d entries; %d writes "+
			"issued at once should share far more than that, so group commit is barely working",
			rounds, bestBatch, writers)
	}

	if bestCalls >= writers {
		t.Errorf("the best of %d bursts turned %d concurrent writes into %d durable writes, "+
			"one each", rounds, writers, bestCalls)
	}
	t.Logf("best burst: %d writes became %d durable writes, largest batch %d",
		writers, bestCalls, bestBatch)
}

func TestBatchedWritesEachGetTheirOwnResult(t *testing.T) {
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
