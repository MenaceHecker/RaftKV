package raft

import (
	"bytes"
	"testing"
)

// Tests for bounding AppendEntries.
//
// A follower can fall arbitrarily far behind, and the leader's reply to that
// is a message containing everything it is missing. Every transport has a
// maximum message size, so without a bound there is a backlog past which the
// message cannot be delivered at all and the follower never recovers. These
// tests pin the bound and, just as importantly, that catching up still
// finishes once it is in place.

func TestLimitEntriesStopsAtTheBudget(t *testing.T) {
	entries := make([]Entry, 10)
	for i := range entries {
		entries[i] = Entry{Index: Index(i + 1), Data: bytes.Repeat([]byte("x"), 100)}
	}

	// Four entries of 100 bytes plus overhead fit in 600; the fifth does not.
	got := limitEntries(entries, 4*(100+entryOverheadBytes))
	if len(got) != 4 {
		t.Errorf("limitEntries returned %d entries, want 4", len(got))
	}
	// The prefix must start at the beginning: entries are only meaningful in
	// order, and skipping any would leave a hole the follower cannot fill.
	if got[0].Index != 1 {
		t.Errorf("prefix starts at index %d, want 1", got[0].Index)
	}
}

func TestLimitEntriesAlwaysSendsAtLeastOne(t *testing.T) {
	// A single entry larger than the entire budget still has to go. The
	// alternative is a follower that can never be given it, and so never
	// catches up, because of a limit that exists to help it.
	huge := []Entry{{Index: 1, Data: bytes.Repeat([]byte("x"), 10_000)}}
	if got := limitEntries(huge, 10); len(got) != 1 {
		t.Errorf("limitEntries returned %d entries for an oversized single entry, want 1", len(got))
	}
}

func TestLimitEntriesHandlesAnEmptyLog(t *testing.T) {
	if got := limitEntries(nil, 1000); len(got) != 0 {
		t.Errorf("limitEntries(nil) returned %d entries", len(got))
	}
}

func TestAppendMessagesStayWithinTheBudget(t *testing.T) {
	// The end to end property: no matter how far behind a follower is, no
	// single append carries more than the budget allows.
	const budget = 256
	c := newCluster(t, 3, clusterOpts{seed: 5, maxAppendBytes: budget})
	leader := c.awaitLeader(defaultElectionTick * 3)

	// Cut one follower off so a real backlog accumulates.
	victim := otherNodes(c.ids, leader)[0]
	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))

	value := bytes.Repeat([]byte("x"), 60)
	for i := range 40 {
		if err := c.node(leader).Propose(append([]byte{byte(i)}, value...)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		c.deliverAll()
	}

	// Watch every message from here on, then let the follower back in.
	var largest int
	c.filter = func(m Message) bool {
		if m.Type == MsgAppendRequest {
			size := 0
			for _, e := range m.Entries {
				size += len(e.Data) + entryOverheadBytes
			}
			if size > largest {
				largest = size
			}
		}
		return true
	}
	c.tickN(defaultElectionTick * 4)

	if largest == 0 {
		t.Fatal("no appends carrying entries were observed, so nothing was measured")
	}
	if largest > budget {
		t.Errorf("an append carried %d bytes of entries, over the %d byte budget", largest, budget)
	}

	// And the backlog must actually have been delivered, in pieces.
	want := c.node(leader).log.lastIndex()
	if got := c.node(victim).log.lastIndex(); got < want {
		t.Errorf("follower reached index %d of %d; bounding the append stalled catch-up\n%s",
			got, want, c.dump())
	}
}

func TestCatchUpNeedsManyAppendsAndStillCompletes(t *testing.T) {
	// A guard on the test above. If the budget were large enough to hold the
	// whole backlog, that test would pass without ever splitting anything,
	// and would be asserting nothing about the bound.
	const budget = 256
	c := newCluster(t, 3, clusterOpts{seed: 9, maxAppendBytes: budget})
	leader := c.awaitLeader(defaultElectionTick * 3)
	victim := otherNodes(c.ids, leader)[0]

	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))
	value := bytes.Repeat([]byte("x"), 60)
	for i := range 30 {
		if err := c.node(leader).Propose(append([]byte{byte(i)}, value...)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		c.deliverAll()
	}

	var appendsToVictim int
	c.filter = func(m Message) bool {
		if m.Type == MsgAppendRequest && m.To == victim && len(m.Entries) > 0 {
			appendsToVictim++
		}
		return true
	}
	c.tickN(defaultElectionTick * 4)

	if appendsToVictim < 3 {
		t.Errorf("catching up took %d appends carrying entries; the backlog was not "+
			"split, so the budget is not being exercised", appendsToVictim)
	}
	want := c.node(leader).log.lastIndex()
	if got := c.node(victim).log.lastIndex(); got < want {
		t.Errorf("follower reached index %d of %d\n%s", got, want, c.dump())
	}
}

func TestCatchUpDoesNotWaitForAHeartbeatPerSlice(t *testing.T) {
	// Bounding the append means a backlog now takes several messages. If the
	// leader only sent the next slice when a heartbeat came round, catching
	// up would cost one heartbeat interval per slice, turning a brief absence
	// into a long recovery. It should instead follow each acknowledgement
	// straight away, so the whole backlog drains at network speed.
	//
	// The harness runs the network to quiescence within a tick, so "does not
	// wait for a heartbeat" is measurable as "finishes in very few ticks".
	const budget = 256
	c := newCluster(t, 3, clusterOpts{seed: 31, maxAppendBytes: budget})
	leader := c.awaitLeader(defaultElectionTick * 3)
	victim := otherNodes(c.ids, leader)[0]

	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))
	value := bytes.Repeat([]byte("x"), 60)
	for i := range 40 {
		if err := c.node(leader).Propose(append([]byte{byte(i)}, value...)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		c.deliverAll()
	}

	want := c.node(leader).log.lastIndex()
	behind := want - c.node(victim).log.lastIndex()
	slices := int(behind) / (budget / (60 + entryOverheadBytes))
	if slices < 5 {
		t.Fatalf("the backlog needs only %d slices; the test is not measuring anything", slices)
	}

	c.heal()

	// A handful of ticks, not one per slice.
	const allowed = 3
	for i := range allowed {
		c.tick()
		if c.node(victim).log.lastIndex() >= want {
			t.Logf("caught up %d entries in %d tick(s), in slices of about %d",
				behind, i+1, budget/(60+entryOverheadBytes))
			return
		}
	}
	t.Errorf("follower reached index %d of %d after %d ticks; catch-up appears to be "+
		"waiting for a heartbeat between slices\n%s",
		c.node(victim).log.lastIndex(), want, allowed, c.dump())
}

func TestSteadyStateSendsNoExtraAppends(t *testing.T) {
	// Chasing a follower after every acknowledgement is the obvious way to
	// make catch-up fast, and it costs real throughput. Under load a
	// follower is nearly always an entry or two behind, so "still behind"
	// fires constantly and produces an extra message per response, carrying
	// entries the next proposal was about to send anyway. Measured at about
	// 25% of write throughput at 64 clients before it was narrowed.
	//
	// The rule is therefore: follow up only when the size budget actually
	// held entries back.
	//
	// Five nodes rather than three, because with three a single follower's
	// acknowledgement commits the entry and the leader broadcasts for that
	// reason instead, which never reaches the branch under test. With five,
	// one acknowledgement is not a majority.
	c := newCluster(t, 5, clusterOpts{seed: 41})
	leader := c.awaitLeader(defaultElectionTick * 3)
	l := c.node(leader)
	follower := otherNodes(c.ids, leader)[0]
	c.tickN(defaultElectionTick)

	// Two small writes, both well within the budget, so nothing is ever
	// held back.
	if err := l.Propose([]byte("first")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	firstIdx := l.log.lastIndex()
	if err := l.Propose([]byte("second")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	if pr := l.progress[follower]; pr.heldBack {
		t.Fatal("the budget held entries back for a tiny write; the test is not set up as intended")
	}

	// The follower acknowledges only the first write, so it is genuinely
	// behind by one entry, exactly as it would be under sustained load.
	l.msgs = nil
	if err := l.Step(Message{
		Type: MsgAppendResponse, From: follower, To: leader,
		Term: l.term, Success: true, MatchIndex: firstIdx,
	}); err != nil {
		t.Fatalf("stepping response: %v", err)
	}

	for _, m := range l.msgs {
		if m.Type == MsgAppendRequest && m.To == follower && len(m.Entries) > 0 {
			t.Errorf("the leader chased follower %d after an acknowledgement even though "+
				"nothing was held back; this costs a message per response under load", follower)
			break
		}
	}
}
