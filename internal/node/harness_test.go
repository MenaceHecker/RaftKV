package node

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
	"github.com/MenaceHecker/raftkv/internal/storage"
)

// waitLimit bounds every wait for something that must happen. It is a failure
// ceiling, not a pacing sleep: passing runs return as soon as the event arrives.
const waitLimit = 5 * time.Second

// fakeNodeClock lets a test deliver ticks and fire timers by hand.
type fakeNodeClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeNodeTimer
	tickers []*fakeNodeTicker
}

type fakeNodeTimer struct {
	c       *fakeNodeClock
	when    time.Time
	f       func()
	stopped bool
	fired   bool
}

type fakeNodeTicker struct {
	ch      chan time.Time
	stopc   chan struct{}
	stopOne sync.Once
}

func newFakeNodeClock() *fakeNodeClock { return &fakeNodeClock{now: time.Unix(2_000_000, 0)} }

func (c *fakeNodeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeNodeClock) NewTicker(time.Duration) Ticker {
	t := &fakeNodeTicker{ch: make(chan time.Time), stopc: make(chan struct{})}
	c.mu.Lock()
	c.tickers = append(c.tickers, t)
	c.mu.Unlock()
	return t
}

func (t *fakeNodeTicker) C() <-chan time.Time { return t.ch }
func (t *fakeNodeTicker) Stop()               { t.stopOne.Do(func() { close(t.stopc) }) }

func (c *fakeNodeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeNodeTimer{c: c, when: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

func (t *fakeNodeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}

// Advance moves time forward and runs the timers that fall due, in order.
func (c *fakeNodeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var next *fakeNodeTimer
		for _, t := range c.timers {
			if t.stopped || t.fired || t.when.After(target) {
				continue
			}
			if next == nil || t.when.Before(next.when) {
				next = t
			}
		}
		if next == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		next.fired = true
		if next.when.After(c.now) {
			c.now = next.when
		}
		f := next.f
		c.mu.Unlock()
		f()
	}
}

// sinkTransport records what the node sends.
type sinkTransport struct {
	mu   sync.Mutex
	msgs []raft.Message
}

func (s *sinkTransport) Send(msgs []raft.Message) {
	s.mu.Lock()
	s.msgs = append(s.msgs, msgs...)
	s.mu.Unlock()
}

func (s *sinkTransport) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// since returns the messages recorded after mark.
func (s *sinkTransport) since(mark int) []raft.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]raft.Message(nil), s.msgs[mark:]...)
}

// recorder counts applied entries and can stall the apply step.
type recorder struct {
	nopRecorder
	applied atomic.Int64
	onApply func() // set before Start
}

func (r *recorder) ObserveApply(entries int, _ time.Duration) {
	r.applied.Add(int64(entries))
	if r.onApply != nil {
		r.onApply()
	}
}

// harness is a node (ID 1 of 1,2,3) whose ticks, peers and clock the test owns.
type harness struct {
	t     *testing.T
	n     *Node
	clock *fakeNodeClock
	sink  *sinkTransport
	rec   *recorder
}

func newHarness(t *testing.T, tune func(*Config, *recorder)) *harness {
	t.Helper()
	h := &harness{t: t, clock: newFakeNodeClock(), sink: &sinkTransport{}, rec: &recorder{}}
	cfg := Config{
		ID:            1,
		Peers:         []raft.NodeID{1, 2, 3},
		DataDir:       t.TempDir(),
		Transport:     h.sink,
		Clock:         h.clock,
		TickInterval:  time.Hour, // ignored: the fake clock delivers ticks
		ElectionTick:  10,
		HeartbeatTick: 1,
		Sync:          storage.SyncNever,
		Metrics:       h.rec,
	}
	if tune != nil {
		tune(&cfg, h.rec)
	}
	n, err := Start(cfg)
	if err != nil {
		t.Fatalf("starting node: %v", err)
	}
	h.n = n
	t.Cleanup(func() { _ = n.Stop() })
	return h
}

// tick delivers one tick and returns once the run loop has fully handled it.
func (h *harness) tick() {
	h.t.Helper()
	h.clock.mu.Lock()
	tickers := append([]*fakeNodeTicker(nil), h.clock.tickers...)
	h.clock.mu.Unlock()
	for _, tk := range tickers {
		select {
		case tk.ch <- h.clock.Now():
		case <-tk.stopc:
		case <-time.After(waitLimit):
			h.t.Fatal("the run loop did not take a tick")
		}
	}
	h.n.Status() // the loop serves this only after it finished the tick
}

// await polls Status until cond holds.
func (h *harness) await(what string, cond func(Status) bool) Status {
	h.t.Helper()
	deadline := time.Now().Add(waitLimit)
	for {
		st := h.n.Status()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s; status %+v", what, st)
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitSent waits for a message recorded after mark that match accepts.
func (h *harness) awaitSent(mark int, what string, match func(raft.Message) bool) raft.Message {
	h.t.Helper()
	deadline := time.Now().Add(waitLimit)
	for {
		for _, m := range h.sink.since(mark) {
			if match(m) {
				return m
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// electLeader makes node 1 the leader of {1,2,3} by feeding it node 2's votes.
// It returns the leader's term.
func (h *harness) electLeader() raft.Term {
	h.t.Helper()
	mark := h.sink.len()
	h.n.Step(raft.Message{Type: raft.MsgCampaign, From: 1, To: 1})
	pv := h.awaitSent(mark, "a pre-vote request", func(m raft.Message) bool {
		return m.Type == raft.MsgPreVoteRequest && m.To == 2
	})

	mark = h.sink.len()
	h.n.Step(raft.Message{Type: raft.MsgPreVoteResponse, From: 2, To: 1, Term: pv.Term, Granted: true})
	vr := h.awaitSent(mark, "a vote request", func(m raft.Message) bool {
		return m.Type == raft.MsgVoteRequest && m.To == 2
	})

	h.n.Step(raft.Message{Type: raft.MsgVoteResponse, From: 2, To: 1, Term: vr.Term, Granted: true})
	h.await("node 1 to lead", func(s Status) bool { return s.State == raft.Leader })
	return vr.Term
}

// ackAll has node 2 acknowledge everything the leader has sent it.
func (h *harness) ackAll(term raft.Term) {
	h.t.Helper()
	var last raft.Index
	for _, m := range h.sink.since(0) {
		if m.Type == raft.MsgAppendRequest && m.To == 2 {
			if idx := m.PrevLogIndex + raft.Index(len(m.Entries)); idx > last {
				last = idx
			}
		}
	}
	if last == 0 {
		h.t.Fatal("the leader has sent node 2 nothing to acknowledge")
	}
	h.n.Step(raft.Message{Type: raft.MsgAppendResponse, From: 2, To: 1, Term: term, Success: true, MatchIndex: last})
}

func putCmd(seq uint64, key string, val byte) []byte {
	return statemachine.Command{ClientID: 1, Seq: seq, Op: statemachine.OpPut, Key: key, Value: []byte{val}}.Encode()
}
