package gateway

import (
	"context"
	"net/http"
	"time"
)

type nodeCache struct {
	up     bool
	status NodeStatus
	lastOK time.Time
	hasOK  bool
}

// PollOnce asks every node for its status and updates the cache that serves
// /v1/cluster. Public requests only ever read the cache, so they never fan out
// to the nodes and a hung node cannot hold one up. The whole poll is bounded
// by PollTimeout; a node that has not answered by then counts as down.
func (g *Gateway) PollOnce(parent context.Context) {
	g.pollOnce(parent)
	g.chaosObserve()
}

func (g *Gateway) pollOnce(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// The timer is created before any call starts, so a test that sees a call
	// begin knows the timer exists.
	timeout := g.clk.After(g.cfg.PollTimeout)

	type result struct {
		id  uint64
		st  NodeStatus
		err error
	}
	results := make(chan result, len(g.topology))
	for _, n := range g.topology {
		go func() {
			st, err := g.cl.Status(ctx, n.ID)
			results <- result{n.ID, st, err}
		}()
	}

	answered := map[uint64]bool{}
	for len(answered) < len(g.topology) {
		select {
		case r := <-results:
			answered[r.id] = true
			g.record(r.id, r.st, r.err == nil)
		case <-timeout:
			for _, n := range g.topology {
				if !answered[n.ID] {
					g.record(n.ID, NodeStatus{}, false)
				}
			}
			return
		case <-parent.Done():
			return
		}
	}
}

func (g *Gateway) record(id uint64, st NodeStatus, ok bool) {
	g.pollMu.Lock()
	prev := g.cache[id]
	c := prev
	c.up = ok
	now := g.clk.Now()
	if ok {
		c.status = st
		c.lastOK = now
		c.hasOK = true
	}
	g.cache[id] = c
	g.pollMu.Unlock()
	g.observePoll(id, prev, st, ok, now)
}

// RunPoller polls on every tick until ctx ends.
func (g *Gateway) RunPoller(ctx context.Context, tick <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			g.PollOnce(ctx)
		}
	}
}

type nodeView struct {
	ID            uint64 `json:"id"`
	AZ            string `json:"az"`
	Up            bool   `json:"up"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	CommitIndex   uint64 `json:"commitIndex"`
	LastApplied   uint64 `json:"lastApplied"`
	LogLen        uint64 `json:"logLen"`
	SnapshotIndex uint64 `json:"snapshotIndex"`
	// LastContactMs is the time since the gateway last polled this node
	// successfully (D5 card, Q5), or null if it never has.
	LastContactMs *int64 `json:"lastContactMs"`
	Paused        bool   `json:"paused"`
}

// clusterView is the document served at /v1/cluster, from the poll cache.
func (g *Gateway) clusterView() map[string]any {
	now := g.clk.Now()
	g.pollMu.RLock()
	nodes := make([]nodeView, 0, len(g.topology))
	var leader uint64
	for _, n := range g.topology {
		c := g.cache[n.ID]
		v := nodeView{ID: n.ID, AZ: n.AZ, Up: c.up, Role: "unknown"}
		if c.hasOK {
			ms := now.Sub(c.lastOK).Milliseconds()
			v.LastContactMs = &ms
		}
		if c.up {
			s := c.status
			v.Role, v.Term, v.CommitIndex, v.LastApplied = s.Role, s.Term, s.CommitIndex, s.AppliedIndex
			v.SnapshotIndex, v.Paused = s.SnapshotIndex, s.Paused
			if s.LastIndex > 0 && s.LastIndex >= s.FirstIndex {
				v.LogLen = s.LastIndex - s.FirstIndex + 1
			}
			if s.Role == "leader" {
				leader = n.ID
			}
		}
		nodes = append(nodes, v)
	}
	g.pollMu.RUnlock()
	return map[string]any{"leaderId": leader, "nodes": nodes, "chaos": g.chaosView()}
}

func (g *Gateway) handleCluster(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, g.clusterView())
}
