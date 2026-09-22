package raft

import (
	"errors"
	"testing"
)

// What the core does when the disk refuses.
//
// The Storage comment states the contract: the core calls into it
// synchronously and treats a returned error as fatal, because Raft's safety
// argument assumes that state reported as persisted really is. Nothing
// checked that the core keeps its side of that, and the interface makes it
// checkable without touching the disk at all.
//
// The vote is where it matters most. A node votes at most once per term, and
// that rule is only worth anything if the vote outlives a crash. A node that
// answered a candidate and then died before the write landed would come back
// with no record of having voted, grant a second vote in the same term, and
// two leaders could be elected in it. So the answer must not leave until the
// write has.

var errDiskGone = errors.New("the disk is gone")

// brokenStorage fails hard state writes from the moment it is armed, and
// counts the attempts so a test can tell "refused" from "never tried".
type brokenStorage struct {
	Storage
	armed       bool
	attempts    int
	appendsFail bool
	appends     int

	snapshotWritesFail bool
	snapshotWrites     int
	snapshotReadsFail  bool
}

func (b *brokenStorage) SetHardState(hs HardState) error {
	b.attempts++
	if b.armed {
		return errDiskGone
	}
	return b.Storage.SetHardState(hs)
}

func (b *brokenStorage) Append(e []Entry) error {
	b.appends++
	if b.appendsFail {
		return errDiskGone
	}
	return b.Storage.Append(e)
}

func (b *brokenStorage) ApplySnapshot(snap Snapshot) error {
	b.snapshotWrites++
	if b.snapshotWritesFail {
		return errDiskGone
	}
	return b.Storage.ApplySnapshot(snap)
}

func (b *brokenStorage) Snapshot() (Snapshot, error) {
	if b.snapshotReadsFail {
		return Snapshot{}, errDiskGone
	}
	return b.Storage.Snapshot()
}

// newFragileNode returns a follower in a three-node cluster whose storage can
// be made to fail on demand.
func newFragileNode(t *testing.T) (*Node, *brokenStorage) {
	t.Helper()

	st := &brokenStorage{Storage: NewMemoryStorage()}
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
	return n, st
}

// atTerm moves the node into a term with a working disk, so that a later vote
// request does not trip the term rules on its way in.
//
// This matters more than it looks. A vote request carrying a higher term
// makes the node a follower in that term first, and that step persists too.
// Arming the failure before it means the request never reaches the code that
// decides about the vote, and a test written that way passes whatever the
// vote path does. This one did, until the mutation that removed the check
// left it green.
func atTerm(t *testing.T, n *Node, term Term) {
	t.Helper()

	if err := n.Step(Message{Type: MsgAppendRequest, From: 2, To: 1, Term: term}); err != nil {
		t.Fatalf("moving to term %d: %v", term, err)
	}
	if n.term != term {
		t.Fatalf("node is in term %d, want %d", n.term, term)
	}
	n.Ready() // drain, so only what follows is examined
}

// voteResponses returns the vote answers the node is trying to send.
func voteResponses(n *Node) []Message {
	var out []Message
	for _, m := range n.Ready().Messages {
		if m.Type == MsgVoteResponse {
			out = append(out, m)
		}
	}
	return out
}

func TestNoVoteIsAnsweredUntilItIsDurable(t *testing.T) {
	// The two-leaders bug, from the one direction a test can reach: if the
	// answer can go out while the write is failing, then it can go out
	// before the write lands.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)
	st.armed = true

	err := n.Step(Message{Type: MsgVoteRequest, From: 2, To: 1, Term: 1})
	if err == nil {
		t.Fatal("a vote request was handled although the vote could not be persisted")
	}
	if !errors.Is(err, errDiskGone) {
		t.Errorf("error is %v, which does not report the storage failure", err)
	}
	if st.attempts == 0 {
		t.Fatal("the vote was never written, so this test proves nothing about failing to write it")
	}

	if sent := voteResponses(n); len(sent) != 0 {
		t.Fatalf("the node answered the candidate anyway: %+v", sent)
	}
}

func TestAVoteThatCouldNotBeWrittenIsNotRemembered(t *testing.T) {
	// persist updates memory only after the write succeeds, so that the two
	// can never disagree in the dangerous direction. The dangerous direction
	// is believing a vote was recorded when it was not: this node would
	// refuse to vote again in the term, having never actually voted, which
	// costs an election rather than safety. Believing the opposite costs
	// safety, and this is the check that the code is on the right side.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)
	st.armed = true

	if err := n.Step(Message{Type: MsgVoteRequest, From: 2, To: 1, Term: 1}); err == nil {
		t.Fatal("the vote request was accepted")
	}

	if n.vote != None {
		t.Errorf("the node recorded a vote for %d although the write failed", n.vote)
	}
	if n.term != 1 {
		t.Errorf("the node is in term %d, want the one it was already in", n.term)
	}
}

func TestAVoteIsAnsweredOnceItIsDurable(t *testing.T) {
	// The same path with a working disk, so the test above is known to be
	// failing for the reason it claims rather than never getting that far.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)

	if err := n.Step(Message{Type: MsgVoteRequest, From: 2, To: 1, Term: 1}); err != nil {
		t.Fatalf("stepping a vote request: %v", err)
	}
	if st.attempts == 0 {
		t.Fatal("the vote was never written")
	}

	sent := voteResponses(n)
	if len(sent) != 1 {
		t.Fatalf("got %d vote responses, want 1", len(sent))
	}
	if !sent[0].Granted {
		t.Error("the vote was refused by a node that had not voted and had an empty log")
	}
	if n.vote != 2 {
		t.Errorf("the vote was recorded as %d, want 2", n.vote)
	}
}

func TestAnElectionIsNotStartedIfTheTermCannotBePersisted(t *testing.T) {
	// Campaigning raises the term and votes for itself, both of which have to
	// be durable first. A node that campaigned on an unwritten term could
	// come back after a crash in an earlier term and vote again in the one it
	// had already voted in.
	n, st := newFragileNode(t)
	st.armed = true

	var err error
	for i := 0; i < 100 && err == nil; i++ {
		err = n.Tick()
	}
	if err == nil {
		t.Fatal("the node campaigned without persisting its new term")
	}
	if !errors.Is(err, errDiskGone) {
		t.Errorf("error is %v, which does not report the storage failure", err)
	}
	if n.state == Candidate || n.state == Leader {
		t.Errorf("the node became %v on a term it could not write", n.state)
	}
	if n.term != 0 {
		t.Errorf("the node is in term %d although the write failed", n.term)
	}
}

func TestSteppingUpATermIsNotRememberedIfItCannotBePersisted(t *testing.T) {
	// A message from a later term makes this node a follower in that term,
	// which clears its vote. Doing that in memory alone would let it vote in
	// the new term, crash, come back in the old one, and vote again.
	n, st := newFragileNode(t)
	st.armed = true

	err := n.Step(Message{Type: MsgAppendRequest, From: 2, To: 1, Term: 5})
	if err == nil {
		t.Fatal("a later term was adopted although it could not be persisted")
	}
	if n.term != 0 {
		t.Errorf("the node is in term %d although the write failed", n.term)
	}
}

// appendResponses returns the acknowledgements the node is trying to send.
func appendResponses(n *Node) []Message {
	var out []Message
	for _, m := range n.Ready().Messages {
		if m.Type == MsgAppendResponse {
			out = append(out, m)
		}
	}
	return out
}

// oneEntry is an append a leader in term 1 would send to an empty follower.
func oneEntry() Message {
	return Message{
		Type: MsgAppendRequest, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:     []Entry{{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("x")}},
		CommitIndex: 1,
	}
}

func TestAnAppendIsNotAcknowledgedUntilItIsDurable(t *testing.T) {
	// The counterpart to the vote. A leader commits an entry once a majority
	// has acknowledged it, and then tells clients it is safe. An
	// acknowledgement from a follower that did not write the entry is a
	// promise the follower cannot keep: if it restarts, the entry is gone
	// from a majority that was counted as holding it.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)
	st.appendsFail = true

	err := n.Step(oneEntry())
	if err == nil {
		t.Fatal("an append was handled although it could not be written")
	}
	if !errors.Is(err, errDiskGone) {
		t.Errorf("error is %v, which does not report the storage failure", err)
	}
	if st.appends == 0 {
		t.Fatal("no write was attempted, so this test proves nothing about failing to write")
	}

	if sent := appendResponses(n); len(sent) != 0 {
		t.Fatalf("the follower acknowledged an entry it did not write: %+v", sent)
	}
}

func TestAFailedAppendDoesNotAdvanceTheCommitIndex(t *testing.T) {
	// The leader's commit index travels with the append. Adopting it while
	// the entries it refers to were not written would leave this node
	// reporting as applied a prefix it does not hold.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)
	before := n.log.committed
	st.appendsFail = true

	if err := n.Step(oneEntry()); err == nil {
		t.Fatal("the append was accepted")
	}

	if n.log.committed != before {
		t.Errorf("commit index moved from %d to %d on an append that was never written",
			before, n.log.committed)
	}
	if got := n.log.lastIndex(); got != 0 {
		t.Errorf("the log ends at %d, so an unwritten entry was counted as held", got)
	}
}

func TestAnAppendIsAcknowledgedOnceItIsDurable(t *testing.T) {
	// With a working disk, so the two above are known to be failing for the
	// reason they claim rather than never reaching the write at all.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)

	if err := n.Step(oneEntry()); err != nil {
		t.Fatalf("stepping an append: %v", err)
	}
	if st.appends == 0 {
		t.Fatal("the entry was never written")
	}

	sent := appendResponses(n)
	if len(sent) != 1 {
		t.Fatalf("got %d append responses, want 1", len(sent))
	}
	if !sent[0].Success {
		t.Error("a valid append was rejected")
	}
	if got := n.log.lastIndex(); got != 1 {
		t.Errorf("the log ends at %d, want 1", got)
	}
}

// snapshotResponses returns the acknowledgements for an installed image.
//
// Not appendResponses: a snapshot is answered with its own message type, and
// filtering for the wrong one makes "nothing was acknowledged" true no matter
// what the code does. The paired test on a working disk is what caught that.
func snapshotResponses(n *Node) []Message {
	var out []Message
	for _, m := range n.Ready().Messages {
		if m.Type == MsgInstallSnapshotResponse {
			out = append(out, m)
		}
	}
	return out
}

// anImage is a state machine image a leader in term 1 would install on a
// follower that has fallen too far behind to catch up from the log.
func anImage() Snapshot {
	return Snapshot{
		Index: 5, Term: 1,
		Conf: ConfState{Voters: []NodeID{1, 2, 3}},
		Data: []byte("state"),
	}
}

func TestASnapshotIsNotAcknowledgedUntilItIsStored(t *testing.T) {
	// A snapshot moves everything at once: the log is replaced and the commit
	// and applied cursors jump to its index. Acknowledging one that was not
	// stored would tell the leader this node holds a prefix it would lose on
	// restart, and the leader would then send only what follows it.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)
	st.snapshotWritesFail = true

	snap := anImage()
	err := n.Step(Message{Type: MsgInstallSnapshot, From: 2, To: 1, Term: 1, Snapshot: &snap})
	if err == nil {
		t.Fatal("a snapshot was handled although it could not be stored")
	}
	if !errors.Is(err, errDiskGone) {
		t.Errorf("error is %v, which does not report the storage failure", err)
	}
	if st.snapshotWrites == 0 {
		t.Fatal("no write was attempted, so this test proves nothing about failing to write")
	}

	if sent := snapshotResponses(n); len(sent) != 0 {
		t.Fatalf("the follower acknowledged a snapshot it did not store: %+v", sent)
	}
	if n.log.committed != 0 || n.log.applied != 0 {
		t.Errorf("cursors moved to commit %d applied %d on a snapshot that was never stored",
			n.log.committed, n.log.applied)
	}
}

func TestASnapshotIsAcknowledgedOnceItIsStored(t *testing.T) {
	// The same path with a working disk, so the test above is known to be
	// reaching the write rather than failing earlier.
	n, st := newFragileNode(t)
	atTerm(t, n, 1)

	snap := anImage()
	if err := n.Step(Message{Type: MsgInstallSnapshot, From: 2, To: 1, Term: 1, Snapshot: &snap}); err != nil {
		t.Fatalf("stepping a snapshot: %v", err)
	}
	if st.snapshotWrites == 0 {
		t.Fatal("the snapshot was never written")
	}

	sent := snapshotResponses(n)
	if len(sent) != 1 {
		t.Fatalf("got %d acknowledgements, want 1", len(sent))
	}
	if !sent[0].Success || sent[0].MatchIndex != snap.Index {
		t.Errorf("acknowledgement is success=%v match=%d, want success at %d",
			sent[0].Success, sent[0].MatchIndex, snap.Index)
	}
	if n.log.committed != snap.Index {
		t.Errorf("commit index is %d, want %d", n.log.committed, snap.Index)
	}
}

// electLeader wins an election for node 1 through the ordinary path, so the
// leader state under test is one the code actually produces.
func electLeader(t *testing.T, n *Node) {
	t.Helper()

	if err := n.becomeCandidate(); err != nil {
		t.Fatalf("campaigning: %v", err)
	}
	if err := n.Step(Message{
		Type: MsgVoteResponse, From: 2, To: 1, Term: n.term, Granted: true,
	}); err != nil {
		t.Fatalf("counting a vote: %v", err)
	}
	if n.state != Leader {
		t.Fatalf("node is %v after a majority granted, want Leader", n.state)
	}
	n.Ready()
}

func TestASnapshotThatCannotBeReadIsNotCountedAsSent(t *testing.T) {
	// The leader assumes a snapshot it sends will be installed and moves the
	// follower's next index past it. If the image could not be read, nothing
	// was sent, and moving the index anyway would have the leader resume from
	// a point the follower never reached. The entries in between would never
	// be sent again: a hole, in the one direction the log-matching check
	// cannot see.
	n, st := newFragileNode(t)
	electLeader(t, n)

	pr := n.progress[2]
	if pr == nil {
		t.Fatal("the leader has no progress for node 2")
	}
	before := pr.next
	st.snapshotReadsFail = true

	n.sendSnapshot(2)

	if pr.next != before {
		t.Errorf("next index for node 2 moved from %d to %d although nothing was sent",
			before, pr.next)
	}
	for _, m := range n.Ready().Messages {
		if m.Type == MsgInstallSnapshot {
			t.Errorf("a snapshot message went out although the image could not be read: %+v", m)
		}
	}
}

func TestStorageFailuresAreMarkedAsSuch(t *testing.T) {
	// The marker is what lets a caller tell "this message was nonsense" from
	// "this node can no longer store anything". Every path that touches the
	// disk has to carry it, or the caller silently makes the wrong choice on
	// whichever one was missed.
	cases := []struct {
		name string
		arm  func(*brokenStorage)
		step func(*Node) error
	}{
		{
			"a vote that cannot be persisted",
			func(b *brokenStorage) { b.armed = true },
			func(n *Node) error {
				return n.Step(Message{Type: MsgVoteRequest, From: 2, To: 1, Term: 1})
			},
		},
		{
			"an append that cannot be written",
			func(b *brokenStorage) { b.appendsFail = true },
			func(n *Node) error { return n.Step(oneEntry()) },
		},
		{
			"a snapshot that cannot be stored",
			func(b *brokenStorage) { b.snapshotWritesFail = true },
			func(n *Node) error {
				snap := anImage()
				return n.Step(Message{
					Type: MsgInstallSnapshot, From: 2, To: 1, Term: 1, Snapshot: &snap,
				})
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, st := newFragileNode(t)
			atTerm(t, n, 1)
			c.arm(st)

			err := c.step(n)
			if err == nil {
				t.Fatal("the step succeeded, so there is no error to classify")
			}
			if !errors.Is(err, ErrStorage) {
				t.Errorf("error %v is not marked as a storage failure, so a caller "+
					"cannot tell it from a bad message", err)
			}
			if !errors.Is(err, errDiskGone) {
				t.Errorf("error %v lost the underlying cause", err)
			}
		})
	}
}

func TestAnUnusableMessageIsNotAStorageFailure(t *testing.T) {
	// The other half of the distinction. If everything were marked, a caller
	// acting on the marker would stop the node over one bad message, which is
	// exactly what a hostile peer would want.
	n, _ := newFragileNode(t)
	atTerm(t, n, 1)

	empty := Snapshot{}
	err := n.Step(Message{Type: MsgInstallSnapshot, From: 2, To: 1, Term: 1, Snapshot: &empty})
	if err == nil {
		t.Fatal("an empty snapshot was accepted")
	}
	if errors.Is(err, ErrStorage) {
		t.Errorf("a malformed message was reported as a storage failure: %v", err)
	}
}
