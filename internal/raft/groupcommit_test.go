package raft

import (
	"fmt"
	"testing"
)

// Tests for batched proposals.
//
// The property that matters is not that ProposeBatch accepts a slice, it is
// that the whole batch reaches storage in a single call. A write cannot be
// acknowledged until it is durable, and the durable write is one fsync
// regardless of how many entries it covers, so "one storage call per batch"
// is the entire point. A version that looped and appended one at a time would
// pass every correctness check here and deliver none of the benefit, which is
// why the call is counted rather than assumed.

// countingStorage records how the log reaches storage.
type countingStorage struct {
	Storage
	appends int
	entries int
}

func (c *countingStorage) Append(e []Entry) error {
	c.appends++
	c.entries += len(e)
	return c.Storage.Append(e)
}

// newLeader returns a single-voter node that has already won its election,
// with storage counters reset so only the test's own writes are counted.
func newLeader(t *testing.T) (*Node, *countingStorage) {
	t.Helper()

	st := &countingStorage{Storage: NewMemoryStorage()}
	n, err := NewNode(Config{
		ID:            1,
		Peers:         []NodeID{1},
		Storage:       st,
		ElectionTick:  10,
		HeartbeatTick: 1,
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}
	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if n.State() != Leader {
		t.Fatalf("node is %v, want Leader", n.State())
	}

	st.appends, st.entries = 0, 0
	return n, st
}

func TestProposeBatchIsOneStorageWrite(t *testing.T) {
	n, st := newLeader(t)

	const size = 8
	datas := make([][]byte, size)
	for i := range datas {
		datas[i] = fmt.Appendf(nil, "cmd-%d", i)
	}

	if err := n.ProposeBatch(datas); err != nil {
		t.Fatalf("ProposeBatch: %v", err)
	}

	if st.appends != 1 {
		t.Errorf("storage saw %d append calls for one batch, want 1; the batch is not being written together", st.appends)
	}
	if st.entries != size {
		t.Errorf("storage received %d entries, want %d", st.entries, size)
	}
}

func TestProposeBatchAppendsContiguouslyInOrder(t *testing.T) {
	n, _ := newLeader(t)

	before := n.LastIndex()
	const size = 5
	datas := make([][]byte, size)
	for i := range datas {
		datas[i] = fmt.Appendf(nil, "cmd-%d", i)
	}

	if err := n.ProposeBatch(datas); err != nil {
		t.Fatalf("ProposeBatch: %v", err)
	}

	if got, want := n.LastIndex(), before+size; got != want {
		t.Fatalf("last index is %d, want %d", got, want)
	}

	// The driver locates a batch by assuming it occupies the final
	// len(batch) indexes, in order. If that assumption ever broke, clients
	// would be told about somebody else's write.
	for i := range size {
		idx := before + Index(i) + 1
		entries, err := n.log.entries(idx, idx+1)
		if err != nil {
			t.Fatalf("reading index %d: %v", idx, err)
		}
		if got, want := string(entries[0].Data), fmt.Sprintf("cmd-%d", i); got != want {
			t.Errorf("index %d holds %q, want %q", idx, got, want)
		}
		if entries[0].Term != n.Term() {
			t.Errorf("index %d has term %d, want %d", idx, entries[0].Term, n.Term())
		}
	}
}

func TestProposeBatchOfOneMatchesPropose(t *testing.T) {
	// Propose delegates to ProposeBatch, so the single-write path must stay
	// exactly as cheap as it was before batching existed.
	n, st := newLeader(t)

	if err := n.Propose([]byte("only")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if st.appends != 1 || st.entries != 1 {
		t.Errorf("one write produced %d appends of %d entries, want 1 and 1", st.appends, st.entries)
	}
}

func TestProposeBatchEmptyWritesNothing(t *testing.T) {
	n, st := newLeader(t)

	if err := n.ProposeBatch(nil); err != nil {
		t.Fatalf("ProposeBatch(nil): %v", err)
	}
	if st.appends != 0 {
		t.Errorf("an empty batch produced %d storage writes, want 0", st.appends)
	}
}

func TestProposeBatchOnFollowerAppendsNothing(t *testing.T) {
	// A follower must reject the whole batch rather than write part of it.
	st := &countingStorage{Storage: NewMemoryStorage()}
	n, err := NewNode(Config{
		ID:            1,
		Peers:         []NodeID{1, 2, 3},
		Storage:       st,
		ElectionTick:  10,
		HeartbeatTick: 1,
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}
	if n.State() != Follower {
		t.Fatalf("node is %v, want Follower", n.State())
	}

	before := n.LastIndex()
	err = n.ProposeBatch([][]byte{[]byte("a"), []byte("b")})
	if err == nil {
		t.Fatal("a follower accepted a batch of proposals")
	}
	if st.appends != 0 {
		t.Errorf("a rejected batch still wrote to storage %d times", st.appends)
	}
	if n.LastIndex() != before {
		t.Errorf("last index moved from %d to %d on a rejected batch", before, n.LastIndex())
	}
}
