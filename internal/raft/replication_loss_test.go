package raft

import (
	"bytes"
	"testing"
)

// behindCluster returns a 3-node cluster whose leader holds `entries` proposals
// that the returned victim has not seen, with a small append budget so catching
// up takes many slices. The victim is left partitioned.
func behindCluster(t *testing.T, entries, budget int) (c *cluster, leader, victim NodeID) {
	t.Helper()
	c = newCluster(t, 3, clusterOpts{seed: 11, maxAppendBytes: budget})
	leader = c.awaitLeader(defaultElectionTick * 3)
	victim = otherNodes(c.ids, leader)[0]
	c.partition([]NodeID{victim}, otherNodes(c.ids, victim))

	value := bytes.Repeat([]byte("x"), 60)
	for i := range entries {
		if err := c.node(leader).Propose(append([]byte{byte(i)}, value...)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		c.deliverAll()
	}
	if got := c.node(victim).LastIndex(); got >= c.node(leader).LastIndex() {
		t.Fatalf("victim is not behind (last=%d, leader last=%d)", got, c.node(leader).LastIndex())
	}
	return c, leader, victim
}

// slicesFor is the number of appends needed to carry n entries of the size
// behindCluster proposes under the given budget.
func slicesFor(n, budget int) int {
	per := max(budget/(61+entryOverheadBytes), 1)
	return (n + per - 1) / per
}

// Raft paper section 5.3: a leader retries AppendEntries indefinitely, so a
// dropped response must leave no state that only a response can clear.
func TestFollowerCatchesUpAfterEveryAppendResponseIsDropped(t *testing.T) {
	const entries, budget = 40, 256
	c, leader, victim := behindCluster(t, entries, budget)

	c.filter = func(m Message) bool {
		return m.Type != MsgAppendResponse || m.From != victim
	}
	c.tickN(5)

	var lastMatch Index
	check := func() {
		t.Helper()
		pr := c.node(leader).progress[victim]
		if pr.match < lastMatch {
			t.Fatalf("pr.match for node %d went backwards: %d -> %d", victim, lastMatch, pr.match)
		}
		lastMatch = pr.match
	}
	check()
	if pr := c.node(leader).progress[victim]; pr.match >= c.node(leader).LastIndex() {
		t.Fatalf("leader already counts the victim caught up (match=%d); nothing was left to recover", pr.match)
	}

	c.heal()
	k := slicesFor(entries, budget) + 1
	for range k {
		c.tick()
		check()
	}

	if got, want := c.node(victim).LastIndex(), c.node(leader).LastIndex(); got != want {
		t.Fatalf("follower at %d, leader at %d after %d heartbeat ticks\n%s", got, want, k, c.dump())
	}
	if got, want := c.node(leader).progress[victim].match, c.node(leader).LastIndex(); got != want {
		t.Fatalf("leader pr.match=%d, want %d\n%s", got, want, c.dump())
	}
}

// Duplicates of one append, delivered late and in order, must not regress
// progress or truncate the follower's log (Raft paper section 5.3).
func TestStaleDuplicateAppendsDoNotRegressProgress(t *testing.T) {
	const entries, budget = 40, 256
	c, leader, victim := behindCluster(t, entries, budget)
	c.heal()

	for range 20 {
		if err := c.node(leader).Tick(); err != nil {
			t.Fatalf("leader tick: %v", err)
		}
		c.collect()
	}
	var dups int
	for _, m := range c.inflight {
		if m.Type == MsgAppendRequest && m.To == victim && len(m.Entries) > 0 {
			dups++
		}
	}
	if dups < 20 {
		t.Fatalf("only %d appends to the victim in flight, want 20", dups)
	}

	var lastNext, lastMatch Index
	var lastCommit Index
	c.filter = func(_ Message) bool {
		pr := c.node(leader).progress[victim]
		if pr.next < lastNext || pr.match < lastMatch {
			t.Fatalf("progress regressed: next %d->%d match %d->%d", lastNext, pr.next, lastMatch, pr.match)
		}
		lastNext, lastMatch = pr.next, pr.match
		if ci := c.node(victim).CommitIndex(); ci < lastCommit {
			t.Fatalf("victim commit went backwards: %d -> %d", lastCommit, ci)
		} else {
			lastCommit = ci
		}
		return true
	}
	c.deliverAll()
	c.tickN(slicesFor(entries, budget) + 1)

	if got, want := c.node(victim).LastIndex(), c.node(leader).LastIndex(); got != want {
		t.Fatalf("follower at %d, leader at %d\n%s", got, want, c.dump())
	}
	le, ve := c.logEntries(leader), c.logEntries(victim)
	if len(le) != len(ve) {
		t.Fatalf("log lengths differ: leader %d follower %d", len(le), len(ve))
	}
	for i := range le {
		if le[i].Index != ve[i].Index || le[i].Term != ve[i].Term || !bytes.Equal(le[i].Data, ve[i].Data) {
			t.Fatalf("entry %d differs: %+v vs %+v", i, le[i], ve[i])
		}
	}
}
