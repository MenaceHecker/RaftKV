package raft

import (
	"fmt"
	"testing"
)

// Tests for log replication and commitment (§5.3, §5.4.2).
//
// Together with the election tests these cover the paper's five safety
// properties:
//
//   - Election Safety — asserted continuously by the harness.
//   - Leader Append-Only — TestLeaderNeverOverwritesItsOwnLog.
//   - Log Matching — TestFollowerWithConflictingLogIsRepaired.
//   - Leader Completeness — TestCommittedEntrySurvivesLeaderChange.
//   - State Machine Safety — assertAppliedConsistent, called throughout.

func TestFiveNodeClusterReplicatesAndCommits(t *testing.T) {
	// Phase 1's exit criterion: a five-node cluster elects a leader,
	// replicates an entry to a majority, and commits it — verified through
	// state, not logs.
	c := newCluster(t, 5, clusterOpts{seed: 100})

	leader := c.awaitLeader(defaultElectionTick * 2)

	if err := c.propose(leader, "set x=1"); err != nil {
		t.Fatalf("propose to leader %d: %v", leader, err)
	}

	// The entry is at the end of the leader's log, after the no-op the leader
	// appended when it took office.
	idx := c.node(leader).LastIndex()

	c.assertCommitted(leader, idx)

	// Committed means a majority stores it — the leader's word alone is not
	// the property being claimed.
	if got := c.countCommitted(idx); got < 3 {
		t.Fatalf("%d of 5 nodes committed through index %d, want at least 3 (a majority)\n%s",
			got, idx, c.dump())
	}

	// Every node must have applied the command, and applied the same one.
	for _, id := range c.ids {
		if got := c.commands(id); len(got) != 1 || got[0] != "set x=1" {
			t.Fatalf("node %d applied %v, want [set x=1]\n%s", id, got, c.dump())
		}
	}
	c.assertAppliedConsistent()
}

func TestMultipleEntriesCommitInOrder(t *testing.T) {
	// The log is an ordered sequence, not a set. Every node must apply the
	// same commands in the same order, which is what makes the replicated
	// state machines converge.
	c := newCluster(t, 5, clusterOpts{seed: 101})
	leader := c.awaitLeader(defaultElectionTick * 2)

	want := make([]string, 0, 10)
	for i := range 10 {
		cmd := fmt.Sprintf("set k%d=%d", i, i)
		if err := c.propose(leader, cmd); err != nil {
			t.Fatalf("propose %q: %v", cmd, err)
		}
		want = append(want, cmd)
	}

	for _, id := range c.ids {
		got := c.commands(id)
		if len(got) != len(want) {
			t.Fatalf("node %d applied %d commands, want %d\n%s", id, len(got), len(want), c.dump())
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("node %d applied %q at position %d, want %q\n%s",
					id, got[i], i, want[i], c.dump())
			}
		}
	}
	c.assertAppliedConsistent()
}

func TestProposeToFollowerIsRejected(t *testing.T) {
	// Only the leader may append. A follower must refuse rather than accept
	// and hope, since accepting would create an entry no one else has agreed
	// to order. Phase 3 turns this error into a redirect.
	c := newCluster(t, 3, clusterOpts{seed: 102})
	leader := c.awaitLeader(defaultElectionTick * 2)

	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	before := c.node(follower).LastIndex()

	if err := c.propose(follower, "set x=1"); err != ErrNotLeader {
		t.Fatalf("proposing to follower %d returned %v, want ErrNotLeader", follower, err)
	}
	if after := c.node(follower).LastIndex(); after != before {
		t.Fatalf("follower %d log grew from %d to %d on a rejected proposal", follower, before, after)
	}
	if got := c.node(follower).Leader(); got != leader {
		t.Fatalf("follower %d names leader %d, want %d; a redirect needs this", follower, got, leader)
	}
}

func TestEntryNotCommittedWithoutMajority(t *testing.T) {
	// A leader that cannot reach a majority must not commit. This is the
	// safety half of the partition story: the minority side stays available
	// for proposals but those proposals never take effect.
	c := newCluster(t, 5, clusterOpts{seed: 103})
	leader := c.awaitLeader(defaultElectionTick * 2)

	// Strand the leader with a single follower — two of five, one short of a
	// majority.
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

	committedBefore := c.node(leader).CommitIndex()

	if err := c.propose(leader, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// The entry is in the leader's log — it accepted the proposal — but it
	// must not have advanced the commit index.
	if got := c.node(leader).CommitIndex(); got != committedBefore {
		t.Fatalf("leader %d advanced commit from %d to %d without a majority\n%s",
			leader, committedBefore, got, c.dump())
	}
	if got := c.commands(leader); len(got) != 0 {
		t.Fatalf("leader %d applied %v without a majority\n%s", leader, got, c.dump())
	}
	c.assertAppliedConsistent()
}

func TestEntryCommitsAfterPartitionHeals(t *testing.T) {
	// The liveness counterpart: once a majority is reachable again, an entry
	// stranded by a partition must commit without a client retry.
	c := newCluster(t, 5, clusterOpts{seed: 104})
	leader := c.awaitLeader(defaultElectionTick * 2)

	// Isolate one follower, propose, and confirm the rest still commit — a
	// single unreachable node cannot block a five-node cluster.
	var isolated NodeID
	var majority []NodeID
	for _, id := range c.ids {
		if id != leader && isolated == None {
			isolated = id
			continue
		}
		majority = append(majority, id)
	}
	c.partition(majority, []NodeID{isolated})

	if err := c.propose(leader, "set x=1"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	idx := c.node(leader).LastIndex()
	c.assertCommitted(leader, idx)

	if got := c.node(isolated).CommitIndex(); got >= idx {
		t.Fatalf("isolated node %d committed index %d while partitioned\n%s", isolated, idx, c.dump())
	}

	// Healing must bring the straggler up to date from the leader's
	// heartbeats alone.
	c.heal()
	c.tickN(defaultElectionTick)

	c.assertCommitted(isolated, idx)
	if got := c.commands(isolated); len(got) != 1 || got[0] != "set x=1" {
		t.Fatalf("node %d applied %v after healing, want [set x=1]\n%s", isolated, got, c.dump())
	}
	c.assertAppliedConsistent()
}

func TestFollowerWithConflictingLogIsRepaired(t *testing.T) {
	// The Log Matching Property (§5.3). A follower that accepted entries from
	// a leader which then lost its term holds a divergent suffix. The new
	// leader must overwrite it, and the follower must end up byte-identical.
	c := newCluster(t, 3, clusterOpts{seed: 105})

	leader := c.awaitLeader(defaultElectionTick * 2)
	if err := c.propose(leader, "shared"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	var others []NodeID
	for _, id := range c.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	victim, survivor := others[0], others[1]

	// Strand the leader with one follower and let it append entries that can
	// never commit. Both keep them in their logs.
	c.partition([]NodeID{leader, victim}, []NodeID{survivor})
	for i := range 3 {
		if err := c.propose(leader, fmt.Sprintf("doomed-%d", i)); err != nil {
			t.Fatalf("propose doomed-%d: %v", i, err)
		}
	}

	divergedAt := c.node(victim).LastIndex()
	if divergedAt <= c.node(survivor).LastIndex() {
		t.Fatalf("test setup failed to diverge the logs: victim last=%d, survivor last=%d",
			divergedAt, c.node(survivor).LastIndex())
	}

	// Heal, then force the node that never saw the doomed entries to take
	// over. Its log is at least as up-to-date as a majority's, since the
	// doomed entries only ever reached a minority.
	c.heal()
	c.campaign(survivor)
	c.tickN(defaultElectionTick * 2)

	newLeader := c.mustLeader()

	// Whatever the outcome of the election, every log must converge: same
	// length, same terms, same data at every index.
	ref := c.logEntries(newLeader)
	for _, id := range c.ids {
		got := c.logEntries(id)
		if len(got) != len(ref) {
			t.Fatalf("node %d log has %d entries, leader %d has %d\n%s",
				id, len(got), newLeader, len(ref), c.dump())
		}
		for i := range ref {
			if got[i].Index != ref[i].Index || got[i].Term != ref[i].Term ||
				string(got[i].Data) != string(ref[i].Data) {
				t.Fatalf("node %d differs from leader %d at position %d: %+v vs %+v\n%s",
					id, newLeader, i, got[i], ref[i], c.dump())
			}
		}
	}
	c.assertAppliedConsistent()
}

func TestCommittedEntrySurvivesLeaderChange(t *testing.T) {
	// Leader Completeness (§5.4). An entry committed in one term must be
	// present in every future leader's log. The election restriction is what
	// enforces it; this checks the consequence.
	c := newCluster(t, 5, clusterOpts{seed: 106})

	leader := c.awaitLeader(defaultElectionTick * 2)
	if err := c.propose(leader, "durable"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	committedIdx := c.node(leader).LastIndex()
	c.assertCommitted(leader, committedIdx)

	// Force a leader change by campaigning elsewhere.
	var next NodeID
	for _, id := range c.ids {
		if id != leader {
			next = id
			break
		}
	}
	c.campaign(next)
	c.tickN(defaultElectionTick * 2)

	newLeader := c.mustLeader()
	if newLeader == leader {
		t.Fatalf("leadership did not change; test needs a new leader")
	}

	// The committed entry must still be there, at the same index and term.
	entries := c.logEntries(newLeader)
	var found *Entry
	for i := range entries {
		if entries[i].Index == committedIdx {
			found = &entries[i]
		}
	}
	if found == nil {
		t.Fatalf("new leader %d is missing committed index %d\n%s", newLeader, committedIdx, c.dump())
	}
	if string(found.Data) != "durable" {
		t.Fatalf("new leader %d holds %q at index %d, want %q\n%s",
			newLeader, found.Data, committedIdx, "durable", c.dump())
	}
	c.assertAppliedConsistent()
}

func TestNewLeaderAppendsNoOp(t *testing.T) {
	// §5.4.2. A new leader may not commit an entry from an earlier term just
	// because a majority stores it, so it appends a no-op in its own term.
	// Committing that commits the inherited prefix along with it.
	c := newCluster(t, 3, clusterOpts{seed: 107})
	c.campaign(1)

	entries := c.logEntries(1)
	if len(entries) != 1 {
		t.Fatalf("new leader log has %d entries, want 1 (the no-op)\n%s", len(entries), c.dump())
	}
	if entries[0].Type != EntryNoOp {
		t.Fatalf("first entry type = %d, want EntryNoOp", entries[0].Type)
	}
	if entries[0].Term != c.node(1).Term() {
		t.Fatalf("no-op term = %d, want the leader's own term %d; committing an entry from an "+
			"earlier term by replica count is exactly what §5.4.2 forbids",
			entries[0].Term, c.node(1).Term())
	}

	// The no-op is in the leader's own term, so it commits on its own and
	// carries any inherited prefix with it.
	c.tickN(defaultHeartbeatTick * 2)
	c.assertCommitted(1, entries[0].Index)
}

func TestCommitRequiresEntryFromCurrentTerm(t *testing.T) {
	// §5.4.2, stated directly and the single most important rule in this file:
	// replica count alone does not commit. An entry from an earlier term can
	// sit on a majority and still be overwritten by a future leader, so a
	// leader must not commit it on the strength of the count.
	//
	// This is white-box on purpose. Reproducing the paper's Figure 8 through
	// the message layer takes an elaborate sequence of partitions and leader
	// changes; setting the replication state directly reaches the same
	// condition and states the rule far more legibly.

	// A node inheriting three entries from term 1, now in term 5.
	storage := NewMemoryStorage()
	inherited := []Entry{
		{Term: 1, Index: 1, Type: EntryNormal, Data: []byte("a")},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("b")},
		{Term: 1, Index: 3, Type: EntryNormal, Data: []byte("c")},
	}
	if err := storage.Append(inherited); err != nil {
		t.Fatalf("seeding log: %v", err)
	}
	if err := storage.SetHardState(HardState{Term: 5}); err != nil {
		t.Fatalf("seeding hard state: %v", err)
	}

	ids := []NodeID{1, 2, 3, 4, 5}
	n, err := NewNode(Config{
		ID:            1,
		Peers:         ids,
		ElectionTick:  defaultElectionTick,
		HeartbeatTick: defaultHeartbeatTick,
		Storage:       storage,
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}

	if err := n.becomeCandidate(); err != nil {
		t.Fatalf("becomeCandidate: %v", err)
	}
	if err := n.becomeLeader(); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}
	n.Ready() // discard the election's outbound traffic

	noopIdx := n.LastIndex()
	if noopIdx != 4 {
		t.Fatalf("no-op landed at index %d, want 4", noopIdx)
	}
	if n.CommitIndex() != 0 {
		t.Fatalf("commit index = %d before any acknowledgement, want 0", n.CommitIndex())
	}

	// Three of five now store the inherited entry at index 3 — a clear
	// majority. Under a naive count-the-replicas rule this would commit.
	n.progress[2].match = 3
	n.progress[3].match = 3

	if n.maybeCommit() {
		t.Fatalf("committed index 3 on replica count alone; it is from term 1, not the "+
			"leader's term %d, and a future leader could still overwrite it", n.Term())
	}
	if got := n.CommitIndex(); got != 0 {
		t.Fatalf("commit index advanced to %d on a previous-term entry, want 0", got)
	}

	// Once a majority stores the no-op — an entry from the leader's own term —
	// commitment is safe, and it carries the inherited prefix with it.
	n.progress[2].match = noopIdx
	n.progress[3].match = noopIdx

	if !n.maybeCommit() {
		t.Fatalf("failed to commit index %d, which is from the leader's own term %d",
			noopIdx, n.Term())
	}
	if got := n.CommitIndex(); got != noopIdx {
		t.Fatalf("commit index = %d, want %d; committing an own-term entry must carry the "+
			"whole inherited prefix with it", got, noopIdx)
	}
}

func TestLeaderNeverOverwritesItsOwnLog(t *testing.T) {
	// Leader Append-Only (§5.2). A leader only ever appends; entries already
	// in its log keep their index, term, and data for as long as it leads.
	c := newCluster(t, 3, clusterOpts{seed: 109})
	leader := c.awaitLeader(defaultElectionTick * 2)

	snapshots := make([][]Entry, 0, 5)
	for i := range 5 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose cmd-%d: %v", i, err)
		}
		snapshots = append(snapshots, c.logEntries(leader))
	}

	// Every earlier snapshot must be an exact prefix of every later one.
	for i := 1; i < len(snapshots); i++ {
		prev, curr := snapshots[i-1], snapshots[i]
		if len(curr) <= len(prev) {
			t.Fatalf("log did not grow between proposals: %d then %d entries", len(prev), len(curr))
		}
		for j := range prev {
			if curr[j].Index != prev[j].Index || curr[j].Term != prev[j].Term ||
				string(curr[j].Data) != string(prev[j].Data) {
				t.Fatalf("leader rewrote entry %d: %+v became %+v", j, prev[j], curr[j])
			}
		}
	}
}

func TestLogSurvivesRestart(t *testing.T) {
	// Phase 1's persistence requirement for the log itself. A restarted node
	// must come back with its entries intact and rejoin without losing
	// committed data.
	c := newCluster(t, 3, clusterOpts{seed: 110})
	leader := c.awaitLeader(defaultElectionTick * 2)

	for i := range 3 {
		if err := c.propose(leader, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatalf("propose cmd-%d: %v", i, err)
		}
	}

	var follower NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	before := c.logEntries(follower)
	c.restart(follower, clusterOpts{seed: 110})
	after := c.logEntries(follower)

	if len(after) != len(before) {
		t.Fatalf("log length changed across restart: %d then %d", len(before), len(after))
	}
	for i := range before {
		if after[i].Index != before[i].Index || after[i].Term != before[i].Term ||
			string(after[i].Data) != string(before[i].Data) {
			t.Fatalf("entry %d changed across restart: %+v became %+v", i, before[i], after[i])
		}
	}

	// It must also rejoin cleanly and catch back up to the leader.
	c.tickN(defaultElectionTick)
	c.assertCommitted(follower, c.node(leader).CommitIndex())
	c.assertAppliedConsistent()
}

// compactedStorage is a Storage whose log begins above index 1, as a real one
// does after a snapshot has compacted the entries below that point.
//
// MemoryStorage cannot be compacted, so this stands in for the disk-backed
// storage the raft package deliberately does not depend on. It exists to cover
// a gap the package's own tests could not reach: every other test here runs
// against an uncompacted log, so the behaviour of a restarting node whose
// storage starts part-way through the log was never exercised.
type compactedStorage struct {
	*MemoryStorage
	// boundary is the last compacted index. Entries at or below it are gone;
	// its own term is still answerable, as a snapshot point must be.
	boundary     Index
	boundaryTerm Term
}

func (s *compactedStorage) FirstIndex() Index { return s.boundary + 1 }

func (s *compactedStorage) Term(i Index) (Term, error) {
	if i == s.boundary {
		return s.boundaryTerm, nil
	}
	if i < s.boundary {
		return 0, ErrCompacted
	}
	return s.MemoryStorage.Term(i)
}

func (s *compactedStorage) Entries(lo, hi Index) ([]Entry, error) {
	if lo <= s.boundary && lo < hi {
		return nil, ErrCompacted
	}
	return s.MemoryStorage.Entries(lo, hi)
}

func TestRestartOnCompactedStorageDoesNotRereadCompactedEntries(t *testing.T) {
	// A node restarting on compacted storage must not start its committed and
	// applied cursors at zero. Everything a snapshot covers is committed and
	// applied by definition, and the entries are gone — so a cursor left at
	// zero sends the log looking for entries that no longer exist as soon as
	// it reports what is newly committed.
	//
	// This was found through the node driver, where a snapshot and a restart
	// meet. Neither this package's tests nor the storage package's could see
	// it alone: these run on an uncompacted log, and those never run the
	// consensus core.
	const boundary = 50

	mem := NewMemoryStorage()
	storage := &compactedStorage{MemoryStorage: mem, boundary: boundary, boundaryTerm: 3}

	// Seed the whole log, then let the wrapper hide everything at or below the
	// boundary. Hiding rather than deleting is what compaction looks like from
	// the core's side: the entries are simply no longer answerable.
	all := make([]Entry, 0, boundary+5)
	for i := range boundary + 5 {
		all = append(all, Entry{Term: 3, Index: Index(i + 1), Type: EntryNormal})
	}
	if err := mem.Append(all); err != nil {
		t.Fatalf("seeding the log: %v", err)
	}
	if err := mem.SetHardState(HardState{Term: 3}); err != nil {
		t.Fatalf("seeding hard state: %v", err)
	}

	n, err := NewNode(Config{
		ID:            1,
		Peers:         []NodeID{1},
		ElectionTick:  defaultElectionTick,
		HeartbeatTick: defaultHeartbeatTick,
		Storage:       storage,
	})
	if err != nil {
		t.Fatalf("creating node: %v", err)
	}

	if got := n.CommitIndex(); got < boundary {
		t.Fatalf("commit index = %d after restarting on storage compacted through %d; "+
			"everything a snapshot covers is already committed", got, boundary)
	}

	// A single-node cluster commits immediately on election, so this is where
	// the log would go looking for compacted entries.
	if err := n.Step(Message{Type: MsgCampaign}); err != nil {
		t.Fatalf("campaign: %v", err)
	}

	rd := n.Ready()
	for _, e := range rd.CommittedEntries {
		if e.Index <= boundary {
			t.Fatalf("Ready reported committed entry %d, which is below the compaction "+
				"boundary %d and no longer exists", e.Index, boundary)
		}
	}
	n.Advance(rd)
}
