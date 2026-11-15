package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// /v1/events: one upstream Events stream per node feeds a hub, and the hub
// fans out to browsers. Browsers never hold a node's subscriber slot (a node
// allows 16); three streams are enough whatever the audience.
const (
	ringSize       = 256
	clientBuffer   = 64
	maxBrowsers    = 100
	maxBrowsersPer = 3
	keepaliveEvery = 15 * time.Second
	commitEvery    = time.Second
	backoffStart   = time.Second
	backoffMax     = 30 * time.Second
)

type hubEvent struct {
	id   uint64
	kind string
	data []byte
}

type browser struct {
	ch      chan hubEvent
	dropped chan struct{} // closed when the hub gives up on a slow browser
	ip      string
}

type nodeLife int

const (
	lifeUnknown nodeLife = iota
	lifeUp
	lifeDown
)

type hub struct {
	mu      sync.Mutex
	ring    [ringSize]hubEvent
	next    uint64 // id of the next event; ids start at 1
	clients map[*browser]struct{}
	perIP   map[string]int

	life       map[uint64]nodeLife
	lastCommit map[uint64]time.Time
	warned     map[uint64]bool
}

func newHub() *hub {
	return &hub{
		next:       1,
		clients:    map[*browser]struct{}{},
		perIP:      map[string]int{},
		life:       map[uint64]nodeLife{},
		lastCommit: map[uint64]time.Time{},
		warned:     map[uint64]bool{},
	}
}

// publish stores the event in the ring and offers it to every browser without
// ever blocking: a browser whose buffer is full is dropped, so a slow browser
// can hurt neither the others nor a node stream.
func (h *hub) publish(kind string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ev := hubEvent{id: h.next, kind: kind, data: data}
	h.ring[ev.id%ringSize] = ev
	h.next++
	for b := range h.clients {
		select {
		case b.ch <- ev:
		default:
			h.dropLocked(b)
		}
	}
}

func (h *hub) dropLocked(b *browser) {
	if _, ok := h.clients[b]; !ok {
		return
	}
	delete(h.clients, b)
	if h.perIP[b.ip]--; h.perIP[b.ip] <= 0 {
		delete(h.perIP, b.ip)
	}
	close(b.dropped)
}

func (h *hub) remove(b *browser) {
	h.mu.Lock()
	h.dropLocked(b)
	h.mu.Unlock()
}

// join registers a browser and returns the events it missed. resync is true
// when Last-Event-ID is older than the ring (or not an id this hub issued), so
// the caller must send a full snapshot instead.
func (h *hub) join(ip, lastID string) (b *browser, replay []hubEvent, resync bool, code int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case len(h.clients) >= maxBrowsers:
		return nil, nil, false, http.StatusServiceUnavailable
	case h.perIP[ip] >= maxBrowsersPer:
		return nil, nil, false, http.StatusTooManyRequests
	}
	if lastID != "" {
		id, err := strconv.ParseUint(lastID, 10, 64)
		oldest := uint64(1)
		if h.next > ringSize {
			oldest = h.next - ringSize
		}
		switch {
		case err != nil || id >= h.next || id+1 < oldest:
			resync = true
		default:
			for i := id + 1; i < h.next; i++ {
				replay = append(replay, h.ring[i%ringSize])
			}
		}
	}
	b = &browser{ch: make(chan hubEvent, clientBuffer), dropped: make(chan struct{}), ip: ip}
	h.clients[b] = struct{}{}
	h.perIP[ip]++
	return b, replay, resync, 0
}

// EventClients is the number of browsers connected to /v1/events.
func (g *Gateway) EventClients() int {
	g.hub.mu.Lock()
	defer g.hub.mu.Unlock()
	return len(g.hub.clients)
}

// --- node liveness, from both the streams and the poller ---

func (g *Gateway) nodeUp(id uint64) {
	h := g.hub
	h.mu.Lock()
	prev := h.life[id]
	h.life[id] = lifeUp
	h.mu.Unlock()
	if prev == lifeDown {
		h.publish("node_up", map[string]uint64{"node": id})
	}
}

func (g *Gateway) nodeDown(id uint64) {
	h := g.hub
	h.mu.Lock()
	prev := h.life[id]
	h.life[id] = lifeDown
	h.mu.Unlock()
	if prev == lifeUp {
		h.publish("node_down", map[string]uint64{"node": id})
	}
}

// observePoll turns what the poller sees into events the nodes do not emit:
// liveness, snapshot index rising, and commit progress (at most one event per
// node per second, never per entry).
func (g *Gateway) observePoll(id uint64, prev nodeCache, st NodeStatus, ok bool, now time.Time) {
	if !ok {
		g.nodeDown(id)
		return
	}
	g.nodeUp(id)
	if !prev.hasOK {
		return
	}
	if st.SnapshotIndex > prev.status.SnapshotIndex {
		g.hub.publish("snapshot", map[string]uint64{"node": id, "snapshotIndex": st.SnapshotIndex})
	}
	if st.CommitIndex != prev.status.CommitIndex {
		g.hub.mu.Lock()
		due := now.Sub(g.hub.lastCommit[id]) >= commitEvery
		if due {
			g.hub.lastCommit[id] = now
		}
		g.hub.mu.Unlock()
		if due {
			g.hub.publish("commit", map[string]uint64{"node": id, "commitIndex": st.CommitIndex})
		}
	}
}

func (g *Gateway) publishChaos(kind, action string, target uint64) {
	v := g.chaosView()
	g.hub.publish("chaos", map[string]any{"kind": kind, "action": action, "target": target, "state": v.State})
}

// --- upstream ---

// startEvents opens one Events stream per node. Each reconnects with jittered
// backoff, after ResourceExhausted, Unauthenticated, or a node crash alike.
func (g *Gateway) startEvents(ctx context.Context) {
	for _, n := range g.topology {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.nodeStream(ctx, n.ID)
		}()
	}
}

func (g *Gateway) nodeStream(ctx context.Context, id uint64) {
	backoff := backoffStart
	wait := func() bool {
		d := backoff + time.Duration(g.rnd.Uint64()%uint64(backoff/2+1)) //nolint:gosec // backoff is positive
		select {
		case <-g.clk.After(d):
		case <-ctx.Done():
			return false
		}
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
		return true
	}
	for ctx.Err() == nil {
		sctx, cancel := context.WithCancel(ctx)
		ch, err := g.cl.Events(sctx, id)
		if err != nil {
			cancel()
			if status.Code(err) == codes.Unauthenticated {
				g.warnOnce(id) // a wrong machine token will not fix itself
			}
			g.nodeDown(id)
			if !wait() {
				return
			}
			continue
		}
		backoff = backoffStart
		g.nodeUp(id)
		g.readStream(ctx, id, ch)
		cancel()
		if ctx.Err() != nil {
			return
		}
		g.nodeDown(id) // the stream ended: the node crashed or restarted
		if !wait() {
			return
		}
	}
}

func (g *Gateway) warnOnce(id uint64) {
	g.hub.mu.Lock()
	first := !g.hub.warned[id]
	g.hub.warned[id] = true
	g.hub.mu.Unlock()
	if first {
		g.log.Warn("events stream unauthenticated", "node", id)
	}
}

type stateEvent struct {
	Node     uint64 `json:"node"`
	Term     uint64 `json:"term"`
	Role     string `json:"role"`
	LeaderID uint64 `json:"leaderId"`
	Seq      uint64 `json:"seq"`
	UnixNano int64  `json:"unixNano"`
}

func (g *Gateway) readStream(ctx context.Context, id uint64, ch <-chan NodeEvent) {
	var last uint64
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if !first && ev.Seq != last+1 {
				// An event was lost between the node and here: browsers must
				// reload instead of trusting the stream.
				g.hub.publish("resync", g.clusterView())
			}
			first, last = false, ev.Seq
			g.hub.publish("state", stateEvent{Node: id, Term: ev.Term, Role: ev.Role, LeaderID: ev.LeaderID, Seq: ev.Seq, UnixNano: ev.UnixNano})
		}
	}
}

// --- HTTP ---

func (g *Gateway) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, "GET")
		return
	}
	b, replay, resync, code := g.hub.join(clientKey(r.RemoteAddr), r.Header.Get("Last-Event-ID"))
	if code != 0 {
		w.Header().Set("Retry-After", "5")
		writeError(w, code, "too many event streams")
		return
	}
	defer g.hub.remove(b)

	// A stream outlives the server's write timeout.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flush := func() { _ = rc.Flush() }

	write := func(format string, a ...any) bool {
		if _, err := fmt.Fprintf(w, format, a...); err != nil {
			return false
		}
		flush()
		return true
	}
	if !write(": connected\n\n") {
		return
	}
	if resync {
		snap, _ := json.Marshal(g.clusterView())
		if !write("event: resync\ndata: %s\n\n", snap) {
			return
		}
	}
	for _, ev := range replay {
		if !write("id: %d\nevent: %s\ndata: %s\n\n", ev.id, ev.kind, ev.data) {
			return
		}
	}

	keepalive := g.clk.After(keepaliveEvery)
	for {
		select {
		case ev := <-b.ch:
			if !write("id: %d\nevent: %s\ndata: %s\n\n", ev.id, ev.kind, ev.data) {
				return
			}
		case <-keepalive:
			if !write(": keepalive\n\n") {
				return
			}
			keepalive = g.clk.After(keepaliveEvery)
		case <-b.dropped:
			return
		case <-r.Context().Done():
			return
		case <-g.base.Done():
			return
		}
	}
}
