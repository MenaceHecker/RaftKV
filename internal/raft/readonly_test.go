package raft

import (
	"errors"
	"fmt"
	"testing"
)

func (c *cluster) readIndex(id NodeID, context string) error {
	c.t.Helper()
	err := c.node(id).ReadIndex([]byte(context))
	c.deliverAll()
	return err
}

func (c *cluster) completedReads(id NodeID) []string {
	out := []string{}
	for _, rs := range c.readStates[id] {
		out = append(out, string(rs.Context))
	}
	return out
}

func (c *cluster) readIndexFor(id NodeID, context string) (Index, bool) {
	for _, rs := range c.readStates[id] {
		if string(rs.Context) == context {
			return rs.Index, true
		}
	}
	return 0, false
}

func TestReadIndexCompletesOnHealthyLeader(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 200})
	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.readIndex(leader, "r1"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	idx, ok := c.readIndexFor(leader, "r1")
	if !ok {
		t.Fatalf("the read did not complete\n%s", c.dump())
	}
	if idx != c.node(leader).CommitIndex() {
		t.Fatalf("read index = %d, want the commit index %d", idx, c.node(leader).CommitIndex())
	}
}

func TestReadIndexRequiresLeadership(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 201})
	leader := c.awaitLeader(defaultElectionTick * 2)

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if err := c.readIndex(id, "nope"); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("ReadIndex on follower %d gave %v, want ErrNotLeader", id, err)
		}
		if reads := c.completedReads(id); len(reads) != 0 {
			t.Fatalf("follower %d completed reads %v", id, reads)
		}
	}
}

func TestPartitionedLeaderCannotCompleteRead(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 202})
	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.propose(leader, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	var rest []NodeID
	for _, id := range c.ids {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.partition([]NodeID{leader}, rest)

	if err := c.readIndex(leader, "stale"); err != nil {
		t.Fatalf("ReadIndex was rejected outright with %v; it should be accepted "+
			"and simply never complete", err)
	}

	if c.node(leader).State() != Leader {
		t.Fatalf("the isolated node stopped believing it was leader, so this test " +
			"no longer exercises the dangerous case")
	}
	if _, ok := c.readIndexFor(leader, "stale"); ok {
		t.Fatalf("an isolated leader completed a read index; the read would be stale\n%s", c.dump())
	}

	c.tickN(defaultElectionTick * 3)
	if _, ok := c.readIndexFor(leader, "stale"); ok {
		t.Fatalf("an isolated leader eventually completed a read index\n%s", c.dump())
	}
}

func TestReadCompletesAgainAfterHealing(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 203})
	leader := c.awaitLeader(defaultElectionTick * 2)

	var rest []NodeID
	for _, id := range c.ids {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.partition([]NodeID{leader}, rest)
	if err := c.readIndex(leader, "doomed"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	c.heal()
	next := c.awaitLeader(defaultElectionTick * 4)

	if err := c.readIndex(next, "fresh"); err != nil {
		t.Fatalf("ReadIndex on the healed leader: %v", err)
	}

	c.tickN(defaultHeartbeatTick * 2)

	if _, ok := c.readIndexFor(next, "fresh"); !ok {
		t.Fatalf("a leader with a full majority could not complete a read\n%s", c.dump())
	}
}

func TestReadIndexNeedsAMajority(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 204})
	leader := c.awaitLeader(defaultElectionTick * 2)

	minority := []NodeID{leader}
	var rest []NodeID
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
			continue
		}
		rest = append(rest, id)
	}
	c.partition(minority, rest)

	if err := c.readIndex(leader, "two-of-five"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if _, ok := c.readIndexFor(leader, "two-of-five"); ok {
		t.Fatalf("a read completed with only 2 of 5 nodes reachable\n%s", c.dump())
	}
}

func TestReadIndexSucceedsWithExactlyAMajority(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 205})
	leader := c.awaitLeader(defaultElectionTick * 2)

	group := []NodeID{leader}
	var rest []NodeID
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if len(group) < 3 {
			group = append(group, id)
			continue
		}
		rest = append(rest, id)
	}
	c.partition(group, rest)

	if err := c.readIndex(leader, "three-of-five"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if _, ok := c.readIndexFor(leader, "three-of-five"); !ok {
		t.Fatalf("a read failed with exactly a majority reachable\n%s", c.dump())
	}
}

func TestFreshLeaderCannotServeReadsYet(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 206})

	n := c.node(1)
	if err := n.becomeCandidate(); err != nil {
		t.Fatalf("becomeCandidate: %v", err)
	}
	if err := n.becomeLeader(); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}

	if err := n.ReadIndex([]byte("early")); !errors.Is(err, ErrLeaderNotReady) {
		t.Fatalf("ReadIndex on a leader with nothing committed in its term gave %v, "+
			"want ErrLeaderNotReady", err)
	}
}

func TestLeaderCanServeReadsOnceNoOpCommits(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 207})
	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.readIndex(leader, "after-noop"); err != nil {
		t.Fatalf("ReadIndex after the no-op committed: %v", err)
	}
	if _, ok := c.readIndexFor(leader, "after-noop"); !ok {
		t.Fatalf("read did not complete once the no-op was committed\n%s", c.dump())
	}
}

func TestConcurrentReadsShareOneRound(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 208})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	contexts := []string{"a", "b", "c", "d", "e"}
	for _, ctx := range contexts {
		if err := n.ReadIndex([]byte(ctx)); err != nil {
			t.Fatalf("ReadIndex %q: %v", ctx, err)
		}
	}
	c.deliverAll()

	for _, ctx := range contexts {
		if _, ok := c.readIndexFor(leader, ctx); !ok {
			t.Fatalf("read %q did not complete; got %v\n%s",
				ctx, c.completedReads(leader), c.dump())
		}
	}
	if got := len(c.readStates[leader]); got != len(contexts) {
		t.Fatalf("%d read states for %d reads", got, len(contexts))
	}
}

func TestReadIndexRejectsDuplicateContext(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 209})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	if err := n.ReadIndex([]byte("dup")); err != nil {
		t.Fatalf("first ReadIndex: %v", err)
	}
	if err := n.ReadIndex([]byte("dup")); !errors.Is(err, ErrReadIndexInFlight) {
		t.Fatalf("reusing an in-flight context gave %v, want ErrReadIndexInFlight", err)
	}

	c.deliverAll()
	if err := n.ReadIndex([]byte("dup")); err != nil {
		t.Fatalf("reusing a completed context: %v", err)
	}
}

func TestReadIndexRejectsEmptyContext(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 210})
	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.node(leader).ReadIndex(nil); err == nil {
		t.Fatal("a read with no context was accepted")
	}
}

func TestReadIndexOnSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{seed: 211})
	c.campaign(1)
	c.tickN(2)

	if err := c.readIndex(1, "solo"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if _, ok := c.readIndexFor(1, "solo"); !ok {
		t.Fatalf("a single-node cluster did not complete a read\n%s", c.dump())
	}
}

func TestReadIndexReflectsCommittedWrites(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 212})
	leader := c.awaitLeader(defaultElectionTick * 2)

	for i := range 5 {
		if err := c.propose(leader, fmt.Sprintf("set k%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
		ctx := fmt.Sprintf("read-%d", i)
		if err := c.readIndex(leader, ctx); err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}

		idx, ok := c.readIndexFor(leader, ctx)
		if !ok {
			t.Fatalf("read %q did not complete\n%s", ctx, c.dump())
		}
		if want := c.node(leader).LastIndex(); idx < want {
			t.Fatalf("read index %d is below the just-committed index %d; a client "+
				"could miss its own write", idx, want)
		}
	}
}

func TestDeposedLeaderAbandonsInFlightReads(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 213})
	leader := c.awaitLeader(defaultElectionTick * 2)
	n := c.node(leader)

	var rest []NodeID
	for _, id := range c.ids {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.partition([]NodeID{leader}, rest)

	if err := n.ReadIndex([]byte("orphan")); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if err := n.Step(Message{
		Type: MsgAppendRequest,
		From: rest[0],
		To:   leader,
		Term: n.Term() + 5,
	}); err != nil {
		t.Fatalf("stepping a higher term: %v", err)
	}
	if n.State() != Follower {
		t.Fatalf("state = %s, want Follower", n.State())
	}

	c.heal()
	c.tickN(defaultElectionTick * 2)

	if _, ok := c.readIndexFor(leader, "orphan"); ok {
		t.Fatalf("a read registered before the node was deposed completed anyway\n%s", c.dump())
	}
}

func TestHeartbeatDoesNotDisturbReplication(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 214})
	leader := c.awaitLeader(defaultElectionTick * 2)

	for i := range 3 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}

	before := c.node(leader).CommitIndex()
	beforeLog := c.logEntries(leader)

	for i := range 5 {
		if err := c.readIndex(leader, fmt.Sprintf("r%d", i)); err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}
	}

	if got := c.node(leader).CommitIndex(); got != before {
		t.Fatalf("commit index moved from %d to %d during reads", before, got)
	}
	if got := c.logEntries(leader); len(got) != len(beforeLog) {
		t.Fatalf("the log grew from %d to %d entries during reads; reads must not "+
			"append anything", len(beforeLog), len(got))
	}

	if err := c.propose(leader, "after-reads"); err != nil {
		t.Fatalf("propose after reads: %v", err)
	}
	c.assertCommitted(leader, c.node(leader).LastIndex())
	c.assertAppliedConsistent()
}

func TestHeartbeatAdvancesFollowerCommitIndex(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 215})
	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.propose(leader, "committed"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if err := c.readIndex(leader, "hb"); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	want := c.node(leader).CommitIndex()
	for _, id := range c.ids {
		if got := c.node(id).CommitIndex(); got != want {
			t.Fatalf("node %d commit index = %d, want %d\n%s", id, got, want, c.dump())
		}
	}
}

func TestReadContextIsCopied(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{seed: 216})
	leader := c.awaitLeader(defaultElectionTick * 2)

	ctx := []byte("original")
	if err := c.node(leader).ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	for i := range ctx {
		ctx[i] = 'x'
	}
	c.deliverAll()

	if _, ok := c.readIndexFor(leader, "original"); !ok {
		t.Fatalf("mutating the caller's context buffer lost the read; got %v",
			c.completedReads(leader))
	}
}

func TestAQueuedReadIsNotConfirmedByAnEarlierRound(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{seed: 211})
	leader := c.awaitLeader(defaultElectionTick * 2)

	c.filter = func(m Message) bool {
		return !(m.Type == MsgHeartbeat && string(m.Context) == "second")
	}

	n := c.node(leader)
	if err := n.ReadIndex([]byte("first")); err != nil {
		t.Fatalf("first ReadIndex: %v", err)
	}
	if err := n.ReadIndex([]byte("second")); err != nil {
		t.Fatalf("second ReadIndex: %v", err)
	}
	c.deliverAll()

	if _, ok := c.readIndexFor(leader, "first"); !ok {
		t.Fatalf("the first read was never confirmed, so the test proves nothing\n%s", c.dump())
	}
	if _, ok := c.readIndexFor(leader, "second"); ok {
		t.Errorf("a read registered after the round began was confirmed by that round's "+
			"acknowledgements, which were collected before it existed\n%s", c.dump())
	}

	c.heal()
	c.tickN(defaultHeartbeatTick * 3)
	if _, ok := c.readIndexFor(leader, "second"); !ok {
		t.Errorf("the queued read never completed even with a reachable majority\n%s", c.dump())
	}
}
