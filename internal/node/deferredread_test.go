package node

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func (c *testCluster) currentLeader() *Node {
	for _, n := range c.running() {
		if n.Status().State == raft.Leader {
			return n
		}
	}
	return nil
}

func TestReadsNeverSurfaceLeaderNotReady(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	mustPut(t, leader, 1, 1, "k", "v")

	var (
		mu      sync.Mutex
		results = map[string]int{}
		stop    = make(chan struct{})
		wg      sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			n := c.currentLeader()
			if n == nil {
				continue
			}
			ctx, cancel := testContext(t)
			_, _, err := n.Get(ctx, "k")
			cancel()

			mu.Lock()
			switch {
			case err == nil:
				results["ok"]++
			case errors.Is(err, raft.ErrLeaderNotReady):
				results["leader not ready"]++
			case errors.Is(err, ErrNotLeader):
				results["not leader"]++
			case errors.Is(err, ErrStopped):
				results["stopped"]++
			default:
				results[err.Error()]++
			}
			mu.Unlock()
		}
	}()

	for range 3 {
		old := c.awaitLeader().Status().ID
		c.stop(old)
		c.awaitLeaderOtherThan(old)
		c.start(old)
	}

	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	t.Logf("read outcomes: %v", results)

	if results["ok"] == 0 {
		t.Fatal("no read ever succeeded, so this test exercised nothing")
	}
	if got := results["leader not ready"]; got != 0 {
		t.Errorf("%d reads failed with ErrLeaderNotReady; that is an internal condition "+
			"a client cannot act on and should have been retried inside the driver", got)
	}
	for reason, n := range results {
		if strings.Contains(reason, "leader has not yet committed") {
			t.Errorf("%d reads surfaced the not-ready condition as %q", n, reason)
		}
	}
}

func TestDeferredReadsAreFailedWhenLeadershipMoves(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.awaitLeader()
	mustPut(t, leader, 1, 1, "k", "v")

	leaderID := leader.Status().ID
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := testContext(t)
			defer cancel()
			_, _, err := leader.Get(ctx, "k")
			errs <- err
		}(i)
	}

	for _, id := range c.ids {
		if id != leaderID {
			c.stop(id)
		}
	}
	wg.Wait()
	close(errs)

	var stuck int
	for err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, raft.ErrLeaderNotReady) {
			stuck++
		}
	}
	if stuck > 0 {
		t.Errorf("%d reads came back with ErrLeaderNotReady after leadership was lost", stuck)
	}
}
