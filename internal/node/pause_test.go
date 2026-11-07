package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
	"github.com/MenaceHecker/raftkv/internal/statemachine"
)

// requirePaused is the precondition of every pause test: the pause must be in
// effect, or the test proves nothing about it.
func requirePaused(t *testing.T, n *Node, why string) {
	t.Helper()
	if !n.Paused() {
		t.Fatalf("%s: the node is not paused", why)
	}
}

// 18: committed entries and a snapshot that are pending when the pause starts are
// applied exactly once, with no incoming traffic and no resume. The apply step
// is held open on entry 1 while four more committed entries wait
// (MaxCommittedEntries = 1) and a snapshot sits in the receive queue.
func TestPauseWithCommittedEntriesAndPendingSnapshotAppliesOnce(t *testing.T) {
	applying := make(chan struct{})
	release := make(chan struct{})
	var once, relOnce sync.Once
	h := newHarness(t, func(cfg *Config, rec *recorder) {
		cfg.MaxCommittedEntries = 1
		rec.onApply = func() {
			once.Do(func() { close(applying); <-release })
		}
	})
	releaseApply := func() { relOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseApply) // runs before the node's own cleanup, which would block on the held loop

	var entries []raft.Entry
	for i := 1; i <= 5; i++ {
		entries = append(entries, raft.Entry{Term: 1, Index: raft.Index(i), Type: raft.EntryNormal,
			Data: putCmd(uint64(i), "k"+string(rune('0'+i)), byte(i))})
	}
	h.n.Step(raft.Message{Type: raft.MsgAppendRequest, From: 2, To: 1, Term: 1, CommitIndex: 5, Entries: entries})
	select {
	case <-applying:
	case <-time.After(waitLimit):
		t.Fatal("the node never started applying")
	}

	src := statemachine.New()
	if err := src.Apply(raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: putCmd(1, "snap", 9)}); err != nil {
		t.Fatalf("building the snapshot image: %v", err)
	}
	for i := raft.Index(2); i <= 10; i++ { // the image must be applied through the snapshot's index
		if err := src.Apply(raft.Entry{Term: 1, Index: i, Type: raft.EntryNoOp}); err != nil {
			t.Fatalf("building the snapshot image: %v", err)
		}
	}
	image, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snapshot image: %v", err)
	}
	h.n.Step(raft.Message{Type: raft.MsgInstallSnapshot, From: 2, To: 1, Term: 1,
		Snapshot: &raft.Snapshot{Index: 10, Term: 1, Data: image, Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}})

	h.n.Pause(time.Minute)
	requirePaused(t, h.n, "after Pause")
	releaseApply()

	st := h.await("the pending entries and snapshot to be applied", func(s Status) bool { return s.Applied >= 10 })
	if st.SnapshotsReceived != 1 {
		t.Fatalf("snapshot installed %d times, want exactly once", st.SnapshotsReceived)
	}
	if got := h.rec.applied.Load(); got < 1 || got > 5 {
		t.Fatalf("%d entries applied through the apply step, want between 1 and 5 and none twice", got)
	}
	if _, ok := h.n.kv.Get("snap"); !ok {
		t.Fatal("the snapshot's key is missing after the install")
	}
	requirePaused(t, h.n, "after the backlog drained, with no resume and no clock movement")
}

// 19: the node stays live across a pause with no incoming traffic. A backlog of
// committed entries drains by itself while paused (the apply wakeup must not be
// lost), ticks do nothing while paused, and after Resume the node times out and
// starts a pre-vote on its own. All messages arrive before the pause.
func TestResumeWithNoIncomingTrafficIsLive(t *testing.T) {
	applying := make(chan struct{})
	release := make(chan struct{})
	var once, relOnce sync.Once
	h := newHarness(t, func(cfg *Config, rec *recorder) {
		cfg.MaxCommittedEntries = 1
		rec.onApply = func() { once.Do(func() { close(applying); <-release }) }
	})
	releaseApply := func() { relOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseApply)

	var entries []raft.Entry
	for i := 1; i <= 5; i++ {
		entries = append(entries, raft.Entry{Term: 1, Index: raft.Index(i), Type: raft.EntryNormal,
			Data: putCmd(uint64(i), "k"+string(rune('0'+i)), byte(i))})
	}
	h.n.Step(raft.Message{Type: raft.MsgAppendRequest, From: 2, To: 1, Term: 1, CommitIndex: 5, Entries: entries})
	select {
	case <-applying:
	case <-time.After(waitLimit):
		t.Fatal("the node never started applying")
	}

	h.n.Pause(time.Minute)
	requirePaused(t, h.n, "after Pause")
	releaseApply()

	// Wait on the apply counter, not on Status: serving Status runs processReady
	// too, so polling it would supply the very wakeup this test is about.
	deadline := time.Now().Add(waitLimit)
	for h.rec.applied.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 5 committed entries were applied while paused with no traffic: the apply wakeup was lost",
				h.rec.applied.Load())
		}
		time.Sleep(time.Millisecond)
	}
	requirePaused(t, h.n, "after the backlog drained")

	for i := 0; i < 100; i++ {
		h.tick()
	}
	if st := h.n.Status(); st.State != raft.Follower {
		t.Fatalf("after 100 ticks while paused the node is %s, want Follower: ticks must stop", st.State)
	}

	h.n.Resume()
	if h.n.Paused() {
		t.Fatal("still paused after Resume")
	}
	for i := 0; i < 40 && h.n.Status().State != raft.PreCandidate; i++ {
		h.tick()
	}
	if st := h.n.Status(); st.State != raft.PreCandidate {
		t.Fatalf("after Resume and 40 ticks with no traffic the node is %s, want PreCandidate", st.State)
	}
	if got := h.rec.applied.Load(); got != 5 {
		t.Fatalf("%d entries applied through the pause and resume, want exactly 5, each once", got)
	}
}

// 22: a request in flight when the pause starts neither hangs the node nor is
// applied twice: it completes once after the resume.
func TestRequestsPendingAtPauseDoNotHangOrApplyTwice(t *testing.T) {
	h := newHarness(t, nil)
	term := h.electLeader()

	result := make(chan error, 1)
	go func() {
		result <- h.n.Propose(context.Background(), statemachine.Command{
			ClientID: 7, Seq: 1, Op: statemachine.OpPut, Key: "pending", Value: []byte("v")})
	}()
	h.await("the proposal to reach the log", func(_ Status) bool { return len(h.sink.since(0)) > 0 && h.lastIndexSent() >= 2 })

	h.n.Pause(time.Minute)
	requirePaused(t, h.n, "after Pause")

	done := make(chan struct{})
	go func() { h.n.Status(); close(done) }()
	select {
	case <-done:
	case <-time.After(waitLimit):
		t.Fatal("Status hung while paused")
	}
	// Ticks while paused must do nothing. If they ran, the leader would send
	// heartbeats and, with no acknowledgements for two election timeouts,
	// CheckQuorum would depose it and fail the proposal.
	sent := h.sink.len()
	for i := 0; i < 40; i++ {
		h.tick()
	}
	if got := h.sink.len(); got != sent {
		t.Fatalf("a paused leader sent %d messages on ticks, want none", got-sent)
	}
	if st := h.n.Status(); st.State != raft.Leader || st.Term != term {
		t.Fatalf("a paused leader changed to %s at term %d (was Leader at %d)", st.State, st.Term, term)
	}
	select {
	case err := <-result:
		t.Fatalf("the proposal finished while nothing could acknowledge it: %v", err)
	default:
	}

	h.n.Resume()
	h.ackAll(term)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("the pending proposal failed after the resume: %v", err)
		}
	case <-time.After(waitLimit):
		t.Fatal("the pending proposal never completed after the resume")
	}
	h.await("everything applied", func(s Status) bool { return s.Applied == s.Commit && s.Commit >= 2 })
	if got := h.rec.applied.Load(); got != 2 { // the leader's no-op and the proposal
		t.Fatalf("%d entries applied, want 2 (no-op and one proposal), each once", got)
	}
}

func (h *harness) lastIndexSent() raft.Index {
	var last raft.Index
	for _, m := range h.sink.since(0) {
		if m.Type == raft.MsgAppendRequest && m.To == 2 {
			if idx := m.PrevLogIndex + raft.Index(len(m.Entries)); idx > last {
				last = idx
			}
		}
	}
	return last
}

// 23: a pause is clamped to MaxPause and lifts by itself exactly then; pausing
// again replaces the earlier deadline.
func TestPauseAutoResumesAtCap(t *testing.T) {
	h := newHarness(t, nil)

	if got := h.n.Pause(time.Hour); got != MaxPause {
		t.Fatalf("Pause(1h) applied %v, want it clamped to %v", got, MaxPause)
	}
	requirePaused(t, h.n, "after Pause(1h)")
	h.clock.Advance(MaxPause - time.Nanosecond)
	requirePaused(t, h.n, "1ns before the cap")
	h.clock.Advance(time.Nanosecond)
	if h.n.Paused() {
		t.Fatal("still paused at the cap: the node must resume by itself")
	}

	h.n.Pause(10 * time.Second)
	h.n.Pause(30 * time.Second) // replaces the 10s deadline
	h.clock.Advance(10 * time.Second)
	requirePaused(t, h.n, "at 10s after the second Pause, whose deadline is 30s")
	h.clock.Advance(20 * time.Second)
	if h.n.Paused() {
		t.Fatal("still paused at the second Pause's deadline")
	}
}
