package raft

import (
	"bytes"
	"testing"
)

func TestLimitEntriesStopsAtTheBudget(t *testing.T) {
	entries := make([]Entry, 10)
	for i := range entries {
		entries[i] = Entry{Index: Index(i + 1), Data: bytes.Repeat([]byte("x"), 100)}
	}

	got := limitEntries(entries, 4*(100+entryOverheadBytes))
	if len(got) != 4 {
		t.Errorf("limitEntries returned %d entries, want 4", len(got))
	}
	if got[0].Index != 1 {
		t.Errorf("prefix starts at index %d, want 1", got[0].Index)
	}
}

func TestLimitEntriesAlwaysSendsAtLeastOne(t *testing.T) {
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
	const budget = 256
	c := newCluster(t, 3, clusterOpts{seed: 5, maxAppendBytes: budget})
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

	want := c.node(leader).log.lastIndex()
	if got := c.node(victim).log.lastIndex(); got < want {
		t.Errorf("follower reached index %d of %d; bounding the append stalled catch-up\n%s",
			got, want, c.dump())
	}
}

func TestCatchUpNeedsManyAppendsAndStillCompletes(t *testing.T) {
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
	c := newCluster(t, 5, clusterOpts{seed: 41})
	leader := c.awaitLeader(defaultElectionTick * 3)
	l := c.node(leader)
	follower := otherNodes(c.ids, leader)[0]
	c.tickN(defaultElectionTick)

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
