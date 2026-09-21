package node

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for reads that arrive before a new leader can serve them.
//
// A leader may not hand out a read index until it has committed an entry in
// its own term (§5.4.2): until then it cannot tell which entries inherited
// from earlier terms are really committed, so any index it produced could
// point at one that is later overwritten. The window is brief, a heartbeat or
// so after each election, and it is entirely an internal condition.
//
// A client has no way to act on it. ErrLeaderNotReady is not "ask someone
// else", it is "ask again in a moment", so surfacing it would push a retry
// loop into every caller for a state that resolves on its own. The driver
// holds those reads instead and starts them once the leader is ready.
//
// This is the guarantee rather than the mechanism: reads taken across
// repeated elections must either succeed or fail for a reason the client can
// do something about.

// currentLeader returns whichever running node believes it leads, or nil.
// Unlike awaitLeader it does not wait, because the point here is to keep
// reading through the moments when there is no leader at all.
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

	// Read continuously against whichever node currently leads, through
	// several elections.
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

	// Force leadership to move repeatedly, which is what opens the window.
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
	// The other half: a read held back is not held forever. If the node
	// stops leading while the read waits, it has to be released with an
	// error the client can act on, because the answer is now somewhere else.
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

	// Take the majority away from under it.
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
