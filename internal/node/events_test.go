package node

import (
	"errors"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func (h *harness) subscribe() *Subscription {
	h.t.Helper()
	sub, err := h.n.Subscribe()
	if err != nil {
		h.t.Fatalf("Subscribe: %v", err)
	}
	h.t.Cleanup(sub.Close)
	return sub
}

func recvEvent(t *testing.T, sub *Subscription) Event {
	t.Helper()
	select {
	case ev, ok := <-sub.C:
		if !ok {
			t.Fatalf("subscription closed early: %v", sub.Err())
		}
		return ev
	case <-time.After(waitLimit):
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

func expectNoEvent(t *testing.T, sub *Subscription, why string) {
	t.Helper()
	select {
	case ev := <-sub.C:
		t.Fatalf("%s: unexpected event %+v", why, ev)
	default:
	}
}

// transitions drives node 1 through pre-candidate, candidate, leader and then
// down to follower under a higher-term heartbeat. Each step is one transition.
func (h *harness) transitions(sub *Subscription, check func(step string, ev Event)) {
	h.t.Helper()
	mark := h.sink.len()
	h.n.Step(raft.Message{Type: raft.MsgCampaign, From: 1, To: 1})
	check("pre-candidate", recvEvent(h.t, sub))
	pv := h.awaitSent(mark, "a pre-vote request", func(m raft.Message) bool {
		return m.Type == raft.MsgPreVoteRequest && m.To == 2
	})

	mark = h.sink.len()
	h.n.Step(raft.Message{Type: raft.MsgPreVoteResponse, From: 2, To: 1, Term: pv.Term, Granted: true})
	check("candidate", recvEvent(h.t, sub))
	vr := h.awaitSent(mark, "a vote request", func(m raft.Message) bool {
		return m.Type == raft.MsgVoteRequest && m.To == 2
	})

	h.n.Step(raft.Message{Type: raft.MsgVoteResponse, From: 2, To: 1, Term: vr.Term, Granted: true})
	check("leader", recvEvent(h.t, sub))

	h.n.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: vr.Term + 4})
	check("follower", recvEvent(h.t, sub))
}

// 10: Seq starts at 1 and never skips or repeats.
func TestEventsAreMonotonicAndGapFree(t *testing.T) {
	h := newHarness(t, nil)
	sub := h.subscribe()

	var seqs []uint64
	h.transitions(sub, func(_ string, ev Event) { seqs = append(seqs, ev.Seq) })
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("event %d has Seq %d; all Seqs %v, want 1,2,3,...", i, s, seqs)
		}
	}
}

// 11: each event equals an independent Status sample taken right after the
// transition. The test drives one transition at a time, so nothing else can
// change in between and the comparison claims only observed transitions.
func TestObservedTransitionsMatchIndependentStatus(t *testing.T) {
	h := newHarness(t, nil)
	sub := h.subscribe()

	h.transitions(sub, func(step string, ev Event) {
		st := h.n.Status()
		if ev.Term != st.Term || ev.Role != st.State || ev.Leader != st.Leader {
			t.Errorf("%s: event (term %d, %s, leader %d) != Status (term %d, %s, leader %d)",
				step, ev.Term, ev.Role, ev.Leader, st.Term, st.State, st.Leader)
		}
		if ev.Commit != st.Commit || ev.Applied != st.Applied || ev.Node != st.ID {
			t.Errorf("%s: event node %d commit %d applied %d != Status node %d commit %d applied %d",
				step, ev.Node, ev.Commit, ev.Applied, st.ID, st.Commit, st.Applied)
		}
	})
}

// 12: a role change inside one term, with no change of term or leader, is an
// event. A fresh follower becoming a pre-candidate is exactly that: term 0 and
// no leader before and after, which a (term, leader) comparison cannot see.
func TestSameTermRoleChangeIsAnEvent(t *testing.T) {
	h := newHarness(t, nil)
	before := h.n.Status()
	sub := h.subscribe()

	for i := 0; i < 40 && h.n.Status().State != raft.PreCandidate; i++ {
		h.tick()
	}
	if h.n.Status().State != raft.PreCandidate {
		t.Fatal("the node never became a pre-candidate")
	}

	ev := recvEvent(t, sub)
	if ev.Role != raft.PreCandidate || ev.Term != before.Term || ev.Leader != before.Leader {
		t.Fatalf("event (term %d, %s, leader %d), want a role-only change to PreCandidate at term %d leader %d",
			ev.Term, ev.Role, ev.Leader, before.Term, before.Leader)
	}
}

// 13: term, role and leader all change in one step; that is one event.
func TestSimultaneousTermAndRoleChange(t *testing.T) {
	h := newHarness(t, nil)
	term := h.electLeader()
	sub := h.subscribe()

	h.n.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: term + 5})
	ev := recvEvent(t, sub)
	if ev.Term != term+5 || ev.Role != raft.Follower || ev.Leader != 2 {
		t.Fatalf("event (term %d, %s, leader %d), want (term %d, Follower, leader 2)", ev.Term, ev.Role, ev.Leader, term+5)
	}
	h.tick() // ordered after the step
	expectNoEvent(t, sub, "one step is one event")
}

// 14: an unchanged observation emits nothing, and a transition seen by both
// observation hooks in one loop turn is emitted once.
func TestUnchangedObservationEmitsNothing(t *testing.T) {
	h := newHarness(t, nil)
	sub := h.subscribe()

	term := h.electLeader()
	for i, want := range []raft.State{raft.PreCandidate, raft.Candidate, raft.Leader} {
		ev := recvEvent(t, sub)
		if ev.Role != want {
			t.Fatalf("event %d is %s, want %s", i, ev.Role, want)
		}
	}

	// Inputs that change none of (term, role, leader).
	h.tick()
	h.tick()
	mark := h.sink.len()
	h.n.Step(raft.Message{Type: raft.MsgVoteRequest, From: 3, To: 1, Term: term - 1}) // stale: refused
	h.awaitSent(mark, "the refusal of a stale vote request", func(m raft.Message) bool {
		return m.Type == raft.MsgVoteResponse && m.To == 3 && !m.Granted
	})
	h.n.Status()
	expectNoEvent(t, sub, "no transition happened; two hooks must not emit twice")
}

// 15: past MaxSubscribers a new subscriber is refused, and a closed one frees a slot.
func TestSubscriberCountIsBounded(t *testing.T) {
	h := newHarness(t, nil)

	var subs []*Subscription
	for i := 0; i < MaxSubscribers; i++ {
		sub, err := h.n.Subscribe()
		if err != nil {
			t.Fatalf("subscriber %d of %d refused: %v", i+1, MaxSubscribers, err)
		}
		subs = append(subs, sub)
	}
	if _, err := h.n.Subscribe(); !errors.Is(err, ErrTooManySubscribers) {
		t.Fatalf("subscriber %d: err = %v, want ErrTooManySubscribers", MaxSubscribers+1, err)
	}

	subs[0].Close()
	again, err := h.n.Subscribe()
	if err != nil {
		t.Fatalf("after Close freed a slot, Subscribe: %v", err)
	}
	again.Close()
	for _, s := range subs[1:] {
		s.Close()
	}
}

// 16: a subscriber that stops reading is ended with ErrSubscriberOverflow once
// SubscriberBuffer events pile up; the run loop and a fast subscriber are unaffected.
func TestSlowSubscriberIsDroppedNotBlocking(t *testing.T) {
	h := newHarness(t, nil)
	slow := h.subscribe()
	fast := h.subscribe()

	const total = SubscriberBuffer + 50
	var last uint64
	for i := 1; i <= total; i++ {
		// Each higher term from node 2 is one (term, leader) change.
		h.n.Step(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: raft.Term(i)})
		ev := recvEvent(t, fast)
		if ev.Seq != last+1 {
			t.Fatalf("fast subscriber: Seq %d after %d", ev.Seq, last)
		}
		last = ev.Seq
	}
	if st := h.n.Status(); st.Term != total {
		t.Fatalf("the run loop did not reach term %d: %+v", total, st)
	}

	got := 0
	deadline := time.After(waitLimit)
drain:
	for {
		select {
		case _, ok := <-slow.C:
			if !ok {
				break drain
			}
			got++
		case <-deadline:
			t.Fatalf("the slow subscriber was never ended; it received %d events", got)
		}
	}
	if got > SubscriberBuffer {
		t.Fatalf("slow subscriber received %d events, more than its buffer of %d", got, SubscriberBuffer)
	}
	if err := slow.Err(); !errors.Is(err, ErrSubscriberOverflow) {
		t.Fatalf("slow subscriber ended with %v, want ErrSubscriberOverflow", err)
	}
}
