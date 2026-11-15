package gateway_test

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustID(t *testing.T, s string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("event id %q: %v", s, err)
	}
	return n
}

func isState(term float64, node float64) func(sseEvent) bool {
	return func(ev sseEvent) bool {
		if ev.Event != "state" {
			return false
		}
		m := ev.json()
		return m["term"] == term && m["node"] == node
	}
}

func TestOneUpstreamStreamPerNodeRegardlessOfBrowsers(t *testing.T) {
	e := newEnv(t)
	e.start()
	eventually(t, "three upstream streams", func() bool { return e.fc.EventStreams() == 3 })

	accepted := 0
	for i := 0; i < 200; i++ {
		c := e.connect(ip(i))
		if c.status() == http.StatusOK {
			accepted++
		}
	}
	if accepted != 100 {
		t.Fatalf("%d browsers accepted, want exactly the cap of 100", accepted)
	}
	if n := e.fc.EventStreams(); n != 3 {
		t.Fatalf("%d upstream streams for 200 browser attempts, want 3", n)
	}
	if n := e.g.EventClients(); n != 100 {
		t.Fatalf("EventClients = %d, want 100", n)
	}
}

func TestSlowBrowserDroppedOthersUnaffected(t *testing.T) {
	e := newEnv(t)
	e.start()
	eventually(t, "streams", func() bool { return e.fc.EventStreams() == 3 })
	slow := e.connect(ip(1))
	good := e.connect(ip(2))
	if e.g.EventClients() != 2 {
		t.Fatalf("EventClients = %d, want 2", e.g.EventClients())
	}
	slow.blockWrites()

	term := 1.0
	for i := 0; i < 100; i++ {
		e.fc.SetLeader(uint64(i%3 + 1))
		term++
		good.wait(t, "event of term "+strconv.Itoa(int(term)), isState(term, 3))
	}
	eventually(t, "the slow browser to be dropped", func() bool { return e.g.EventClients() == 1 })
	slow.unblockWrites()
	select {
	case <-slow.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the dropped browser's connection was not closed")
	}
	e.fc.SetLeader(1)
	good.wait(t, "an event after the drop", isState(term+1, 3))
}

func TestMaxBrowsersAndPerIP(t *testing.T) {
	e := newEnv(t)
	e.start()
	const same = "198.51.100.50:1"
	var first *sseConn
	for i := 0; i < 3; i++ {
		c := e.connect(same)
		if c.status() != http.StatusOK {
			t.Fatalf("browser %d from one address got %d", i+1, c.status())
		}
		if first == nil {
			first = c
		}
	}
	if c := e.connect(same); c.status() != http.StatusTooManyRequests {
		t.Fatalf("a 4th browser from one address got %d, want 429", c.status())
	}
	first.cancel()
	<-first.done
	eventually(t, "the slot to free", func() bool { return e.g.EventClients() == 2 })
	if c := e.connect(same); c.status() != http.StatusOK {
		t.Fatalf("a connection after one closed got %d, want 200", c.status())
	}

	for i := 0; i < 97; i++ { // 3 already connected, 97 more fill the cap
		if c := e.connect(ip(i + 10)); c.status() != http.StatusOK {
			t.Fatalf("browser %d got %d", i+4, c.status())
		}
	}
	if n := e.g.EventClients(); n != 100 {
		t.Fatalf("EventClients = %d, want 100", n)
	}
	if c := e.connect(ip(500)); c.status() != http.StatusServiceUnavailable {
		t.Fatalf("browser 101 got %d, want 503", c.status())
	}
}

func TestSSEHeadersAndKeepalive(t *testing.T) {
	e := newEnv(t)
	e.start()
	c := e.connect(ip(1))
	c.mu.Lock()
	ct, cc := c.hdr.Get("Content-Type"), c.hdr.Get("Cache-Control")
	c.mu.Unlock()
	if ct != "text/event-stream" || cc != "no-cache" {
		t.Fatalf("Content-Type %q, Cache-Control %q", ct, cc)
	}
	eventually(t, "the keepalive timer", func() bool { e.clk.Advance(15 * time.Second); c.mu.Lock(); defer c.mu.Unlock(); return c.comments >= 2 })
	if rec := e.do(http.MethodPost, "/v1/events", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/events = %d, want 405", rec.Code)
	}
}

func TestLastEventIDReplayAndResync(t *testing.T) {
	e := newEnv(t)
	e.start()
	eventually(t, "streams", func() bool { return e.fc.EventStreams() == 3 })
	a := e.connect(ip(1))
	e.fc.SetLeader(2)
	a.wait(t, "term 2", isState(2, 3))
	e.fc.SetLeader(3)
	a.wait(t, "term 3", isState(3, 3))

	all := a.all()
	if len(all) < 6 {
		t.Fatalf("A saw %d events, want at least 6 (3 nodes, 2 changes): %+v", len(all), all)
	}
	for i, ev := range all {
		if ev.ID != strconv.FormatUint(mustID(t, all[0].ID)+uint64(i), 10) { //nolint:gosec // small index
			t.Fatalf("ids are not consecutive: %+v", all)
		}
	}

	// A browser that last saw the second event gets exactly the rest, with the
	// same ids and data.
	b := e.connect(ip(2), "Last-Event-ID", all[1].ID)
	b.wait(t, "the replay", isState(3, 3))
	// The start-up heal publishes a chaos event at an unpredictable moment, so
	// compare what both browsers have in common.
	got, now := b.all(), a.all()
	if len(got) < len(all)-2 {
		t.Fatalf("replay after id %s gave %d events, want at least %d: %+v", all[1].ID, len(got), len(all)-2, got)
	}
	for i := 0; i < len(got) && i+2 < len(now); i++ {
		if got[i] != now[i+2] {
			t.Fatalf("replayed event %d = %+v, want %+v", i, got[i], now[i+2])
		}
	}

	// Push more events than the ring of 256 holds, one change at a time so no
	// browser falls behind.
	term := 3.0
	for i := 0; i < 100; i++ {
		e.fc.SetLeader(uint64(i%3 + 1))
		term++
		a.wait(t, "term "+strconv.Itoa(int(term)), isState(term, 3))
	}
	c := e.connect(ip(3), "Last-Event-ID", "1")
	c.wait(t, "a resync", func(ev sseEvent) bool { return ev.Event == "resync" })
	first := c.all()[0]
	if first.Event != "resync" {
		t.Fatalf("first event after a too old Last-Event-ID is %q, want resync", first.Event)
	}
	if m := first.json(); m["nodes"] == nil {
		t.Fatalf("resync carries no cluster snapshot: %s", first.Data)
	}
	// A current id replays just what is missing; an id from the future resyncs.
	last := a.all()[len(a.all())-1].ID
	d := e.connect(ip(4), "Last-Event-ID", last)
	if n := len(d.all()); n != 0 {
		t.Fatalf("nothing to replay, got %+v", d.all())
	}
	f := e.connect(ip(5), "Last-Event-ID", "99999999")
	f.wait(t, "a resync for an id from the future", func(ev sseEvent) bool { return ev.Event == "resync" })
}

func TestUpstreamResourceExhaustedReconnects(t *testing.T) {
	e := newEnv(t)
	e.fc.FailEvents(2, status.Error(codes.ResourceExhausted, "too many subscribers"), 2)
	e.start()
	eventually(t, "streams of nodes 1 and 3", func() bool { return e.fc.EventStreams() == 2 })
	advanceUntil(t, e, time.Second, "node 2's stream", func() bool { return e.fc.EventStreams() == 3 })
	c := e.connect(ip(1))
	e.fc.SetLeader(2)
	c.wait(t, "an event from node 2 after the reconnect", isState(2, 2))
}

func TestUpstreamUnauthenticatedKeepsRetryingAndLogsOnce(t *testing.T) {
	e := newEnv(t)
	e.fc.FailEvents(1, status.Error(codes.Unauthenticated, "bad token"), 3)
	e.start()
	eventually(t, "the other streams", func() bool { return e.fc.EventStreams() == 2 })
	advanceUntil(t, e, time.Second, "node 1's stream", func() bool { return e.fc.EventStreams() == 3 })
	if n := countLogs(e.log.String(), "events stream unauthenticated"); n != 1 {
		t.Fatalf("the unauthenticated stream was logged %d times, want once:\n%s", n, e.log.String())
	}
}

func TestNodeSeqGapEmitsResync(t *testing.T) {
	e := newEnv(t)
	e.start()
	eventually(t, "streams", func() bool { return e.fc.EventStreams() == 3 })
	c := e.connect(ip(1))
	e.fc.SetLeader(2) // seq 1 on every node: no gap
	c.wait(t, "term 2", isState(2, 3))
	if c.seen(func(ev sseEvent) bool { return ev.Event == "resync" }) {
		t.Fatalf("a resync without a gap: %+v", c.all())
	}
	e.fc.SkipEventSeq(1)
	e.fc.SetLeader(3)
	c.wait(t, "a resync after the lost event", func(ev sseEvent) bool { return ev.Event == "resync" })
}

func TestCrashedNodeYieldsNodeDownThenUp(t *testing.T) {
	e := newEnv(t)
	e.start()
	eventually(t, "streams", func() bool { return e.fc.EventStreams() == 3 })
	// The start-up heal polls the nodes once; let that poll finish, so a late
	// answer from before the crash cannot be mistaken for the node returning.
	eventually(t, "the start-up poll", func() bool { return e.chaosState() == "idle" })
	c := e.connect(ip(1))
	_ = e.fc.Crash(background(), 1)
	isNode := func(kind string) func(sseEvent) bool {
		return func(ev sseEvent) bool { return ev.Event == kind && ev.json()["node"] == float64(1) }
	}
	c.wait(t, "node_down for node 1", isNode("node_down"))
	if c.seen(isNode("node_up")) {
		t.Fatal("node_up before the node came back")
	}
	e.fc.Restart(1)
	advanceUntil(t, e, time.Second, "node_up for node 1", func() bool { return c.seen(isNode("node_up")) })
	// Its stream is back, so a new event from it arrives again.
	e.fc.SetLeader(2)
	c.wait(t, "an event from node 1 after the restart", func(ev sseEvent) bool {
		return ev.Event == "state" && ev.json()["node"] == float64(1) && ev.json()["leaderId"] == float64(2)
	})
}

func TestPollerSynthesizesSnapshotAndCommitEvents(t *testing.T) {
	e := newEnv(t)
	e.start()
	c := e.connect(ip(1))
	e.pollAll()
	e.fc.SetSnapshotIndex(7)
	e.pollAll()
	c.wait(t, "a snapshot event", func(ev sseEvent) bool {
		return ev.Event == "snapshot" && ev.json()["node"] == float64(1) && ev.json()["snapshotIndex"] == float64(7)
	})
	if r := e.put("a", "1"); r.Code != http.StatusOK {
		t.Fatalf("put: %d", r.Code)
	}
	e.clk.Advance(2 * time.Second)
	e.pollAll()
	c.wait(t, "a commit event", func(ev sseEvent) bool { return ev.Event == "commit" })
	// At most one commit event per node per second.
	for i := 0; i < 5; i++ {
		e.put("a", strconv.Itoa(i))
		e.pollAll()
	}
	n := 0
	for _, ev := range c.all() {
		if ev.Event == "commit" && ev.json()["node"] == float64(1) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d commit events for node 1 within one second, want exactly 1", n)
	}
}
