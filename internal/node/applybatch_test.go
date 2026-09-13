package node

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Tests for bounding how much is applied in one pass of the loop.
//
// The driver's loop is the node's only goroutine: it ticks the clock, reads
// messages, and applies entries. Applying a large backlog in one go therefore
// stops the node doing anything else for the duration. Measured before this
// was capped, a 20,000 entry replay applied in a single batch and blocked the
// loop for 478ms, which is half a default election timeout spent unable to
// send a heartbeat, answer one, or notice its own timer.

// applyRecorder tracks the largest batch and the longest single apply.
type applyRecorder struct {
	mu       sync.Mutex
	maxCount int
	maxDur   time.Duration
	batches  int
}

func (r *applyRecorder) ObserveApply(entries int, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches++
	if entries > r.maxCount {
		r.maxCount = entries
	}
	if d > r.maxDur {
		r.maxDur = d
	}
}

func (r *applyRecorder) ObserveProposal(string, time.Duration) {}
func (r *applyRecorder) ObserveRead(string, time.Duration)     {}
func (r *applyRecorder) ObservePersist(int, time.Duration)     {}
func (r *applyRecorder) SnapshotCreated(uint64, time.Duration) {}
func (r *applyRecorder) SnapshotReceived()                     {}
func (r *applyRecorder) LeaderChanged(uint64, uint64, bool)    {}

func (r *applyRecorder) read() (maxCount int, maxDur time.Duration, batches int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxCount, r.maxDur, r.batches
}

// fillLog writes n entries through the leader, concurrently so the writes are
// not serialized by one round trip each.
func fillLog(t *testing.T, leader *Node, n int) {
	t.Helper()

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			put(t, leader, uint64(i+1), 1, fmt.Sprintf("k%d", i%500), "value")
		}(i)
	}
	wg.Wait()
}

func TestReplayIsAppliedInBoundedBatches(t *testing.T) {
	const (
		entries = 5000
		cap     = 200
	)

	rec := &applyRecorder{}
	c := newTunedTestCluster(t, 3, func(cfg *Config) {
		cfg.Metrics = rec
		cfg.MaxCommittedEntries = cap
		// Never snapshot, so a restart has the whole log to replay.
		cfg.SnapshotThreshold = 1 << 40
	})
	leader := c.awaitLeader()
	fillLog(t, leader, entries)

	victim := c.ids[0]
	if victim == leader.Status().ID {
		victim = c.ids[1]
	}
	c.stop(victim)

	replay := &applyRecorder{}
	c.tune = func(cfg *Config) {
		cfg.Metrics = replay
		cfg.MaxCommittedEntries = cap
		cfg.SnapshotThreshold = 1 << 40
	}
	c.start(victim)

	want := leader.Status().Applied
	c.eventually("the restarted node to replay its log", func() bool {
		return c.node(victim).Status().Applied >= want
	})

	maxCount, maxDur, batches := replay.read()
	if maxCount > cap {
		t.Errorf("a single apply batch held %d entries, over the cap of %d", maxCount, cap)
	}
	if batches < 5 {
		t.Errorf("the replay took %d batches; it was not split, so the cap is not "+
			"being exercised", batches)
	}
	t.Logf("replayed %d entries in %d batches, largest %d, longest apply %v",
		want, batches, maxCount, maxDur)
}

func TestReplayContinuesWithoutWaitingForAnEvent(t *testing.T) {
	// Capping the batch is only half of it. The remainder has to be picked up
	// without waiting for something else to happen, or capping turns a short
	// stall into a long crawl of one batch per tick.
	//
	// Two things make this measurable. A single node has no peers, so no
	// heartbeats or appends arrive to wake the loop incidentally, leaving the
	// ticker as the only other thing that would. And the clock is slowed
	// right down, so "one batch per tick" is unmistakable.
	//
	// The applied index is read exactly once, at the end. Polling it would
	// send on the status channel and wake the loop each time, which is itself
	// the event this test is checking the loop does not need. An earlier
	// version of this test polled every two milliseconds and therefore passed
	// whether or not the mechanism existed.
	const (
		entries  = 2000
		cap      = 40
		tick     = 200 * time.Millisecond
		settling = 1500 * time.Millisecond
	)

	c := newTunedTestCluster(t, 1, func(cfg *Config) {
		cfg.MaxCommittedEntries = cap
		cfg.SnapshotThreshold = 1 << 40
	})
	leader := c.awaitLeader()
	fillLog(t, leader, entries)

	only := c.ids[0]
	want := leader.Status().Applied
	c.stop(only)

	c.tune = func(cfg *Config) {
		cfg.MaxCommittedEntries = cap
		cfg.SnapshotThreshold = 1 << 40
		cfg.TickInterval = tick
		cfg.ElectionTick = 2
	}
	c.start(only)

	time.Sleep(settling)

	got := c.node(only).Status().Applied
	if got < want {
		// One batch per tick would manage roughly this much in the time
		// allowed, which is the failure this is distinguishing.
		paced := int(settling/tick) * cap
		t.Errorf("applied %d of %d entries in %v; about %d is what one batch per tick "+
			"would reach, so the loop is waiting for an event between batches",
			got, want, settling, paced)
	}
}
