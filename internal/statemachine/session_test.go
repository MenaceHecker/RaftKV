package statemachine

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for client session tracking and request deduplication (§6.3).
//
// The interesting failure is not a duplicated write — Put and Delete are
// idempotent, so applying one twice changes nothing. It is a *reordered*
// retry: an old request appended after a newer one from the same client,
// quietly undoing work the client already believes is done. Most of these
// tests are built around that shape.
//
// The second theme is determinism. The session table is part of the state
// machine, so every decision it makes — including which client to forget when
// it is full — has to come out the same on every replica.

// clientPut builds a Put carrying a client session tag.
func clientPut(index raft.Index, clientID, seq uint64, key, value string) raft.Entry {
	return raft.Entry{
		Term:  1,
		Index: index,
		Type:  raft.EntryNormal,
		Data: Command{
			ClientID: clientID,
			Seq:      seq,
			Op:       OpPut,
			Key:      key,
			Value:    []byte(value),
		}.Encode(),
	}
}

// clientDelete builds a Delete carrying a client session tag.
func clientDelete(index raft.Index, clientID, seq uint64, key string) raft.Entry {
	return raft.Entry{
		Term:  1,
		Index: index,
		Type:  raft.EntryNormal,
		Data: Command{
			ClientID: clientID,
			Seq:      seq,
			Op:       OpDelete,
			Key:      key,
		}.Encode(),
	}
}

func TestStaleRetryDoesNotClobberNewerWrite(t *testing.T) {
	// The hazard deduplication exists for. A client writes 1, times out and
	// retries, but has already written 2 in the meantime. If the retry is
	// applied after the newer write, the client's value silently reverts and
	// nothing anywhere reports a problem.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 7, 1, "x", "1"),
		clientPut(2, 7, 2, "x", "2"),
		clientPut(3, 7, 1, "x", "1"), // the delayed retry of seq 1
	})

	if got := mustGet(t, kv, "x"); got != "2" {
		t.Fatalf("x = %q after a stale retry, want 2; the retry undid a newer write", got)
	}
}

func TestDuplicateStillConsumesItsIndex(t *testing.T) {
	// A duplicate is committed like any other entry, so every replica must
	// advance past it. Leaving the cursor behind would make the next entry
	// look like a gap and stall the state machine.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 1, 1, "k", "v"),
		clientPut(2, 1, 1, "k", "ignored"), // duplicate
		clientPut(3, 1, 2, "k", "next"),
	})

	if got := kv.Applied(); got != 3 {
		t.Fatalf("applied = %d, want 3; a duplicate must still move the cursor", got)
	}
	if got := mustGet(t, kv, "k"); got != "next" {
		t.Fatalf("k = %q, want next", got)
	}
}

func TestExactDuplicateIsIgnored(t *testing.T) {
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 5, 1, "k", "original"),
		clientPut(2, 5, 1, "k", "resent"),
	})

	if got := mustGet(t, kv, "k"); got != "original" {
		t.Fatalf("k = %q, want original; the resent request was applied", got)
	}
}

func TestSequencesNeedNotBeContiguous(t *testing.T) {
	// A client that gives up on a request and moves on leaves a gap. The next
	// request is still newer than everything applied, so it must take effect.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 3, 1, "k", "one"),
		clientPut(2, 3, 9, "k", "nine"), // 2..8 abandoned
	})

	if got := mustGet(t, kv, "k"); got != "nine" {
		t.Fatalf("k = %q, want nine; a non-contiguous sequence was rejected", got)
	}

	// And anything below the new high-water mark is still a duplicate.
	applyAll(t, kv, []raft.Entry{clientPut(3, 3, 5, "k", "five")})
	if got := mustGet(t, kv, "k"); got != "nine" {
		t.Fatalf("k = %q, want nine; a sequence below the high-water mark was applied", got)
	}
}

func TestClientsAreIndependent(t *testing.T) {
	// One client's sequence numbers say nothing about another's. Two clients
	// both starting at 1 must both be served.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 100, 1, "a", "from-100"),
		clientPut(2, 200, 1, "b", "from-200"),
		clientPut(3, 300, 1, "c", "from-300"),
	})

	for key, want := range map[string]string{
		"a": "from-100", "b": "from-200", "c": "from-300",
	} {
		if got := mustGet(t, kv, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if got := kv.Sessions(); got != 3 {
		t.Fatalf("tracking %d sessions, want 3", got)
	}
}

func TestNoClientIsNeverDeduplicated(t *testing.T) {
	// Commands with no session are not claiming exactly-once, so identical
	// ones must all take effect. Deduplicating them would silently drop
	// legitimate writes.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		{Term: 1, Index: 1, Type: raft.EntryNormal,
			Data: Command{Op: OpPut, Key: "k", Value: []byte("first")}.Encode()},
		{Term: 1, Index: 2, Type: raft.EntryNormal,
			Data: Command{Op: OpPut, Key: "k", Value: []byte("second")}.Encode()},
	})

	if got := mustGet(t, kv, "k"); got != "second" {
		t.Fatalf("k = %q, want second; sessionless commands were deduplicated", got)
	}
	if got := kv.Sessions(); got != 0 {
		t.Fatalf("tracking %d sessions for sessionless commands, want 0", got)
	}
}

func TestDeleteIsDeduplicated(t *testing.T) {
	// Deletes need the same protection: a retried delete arriving after the
	// key has been rewritten would remove data the client never asked to
	// remove.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 4, 1, "k", "value"),
		clientDelete(2, 4, 2, "k"),
		clientPut(3, 4, 3, "k", "rewritten"),
		clientDelete(4, 4, 2, "k"), // stale retry of the delete
	})

	if got := mustGet(t, kv, "k"); got != "rewritten" {
		t.Fatalf("k = %q after a stale delete retry, want rewritten", got)
	}
}

func TestSessionsSurviveSnapshotAndRestore(t *testing.T) {
	// A replica that restored without the session table would have forgotten
	// every client's progress and would re-apply the next retry it saw.
	kv := New()
	applyAll(t, kv, []raft.Entry{
		clientPut(1, 7, 1, "x", "1"),
		clientPut(2, 7, 2, "x", "2"),
	})

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	seq, ok := restored.LastSeq(7)
	if !ok {
		t.Fatal("the restored replica has no session for client 7")
	}
	if seq != 2 {
		t.Fatalf("restored last sequence = %d, want 2", seq)
	}

	// The restored replica must still reject the stale retry.
	applyAll(t, restored, []raft.Entry{clientPut(3, 7, 1, "x", "1")})
	if got := mustGet(t, restored, "x"); got != "2" {
		t.Fatalf("x = %q on the restored replica, want 2; the retry was re-applied", got)
	}
}

func TestSnapshotWithSessionsIsDeterministic(t *testing.T) {
	// Sessions are encoded in client-ID order for the same reason keys are
	// encoded in key order: without it, identical state would produce
	// different bytes and convergence could not be checked by comparison.
	kv := New()
	for i := range 100 {
		if err := kv.Apply(clientPut(raft.Index(i+1), uint64(i%20+1), uint64(i/20+1), "k", "v")); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}

	first, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for i := range 50 {
		again, err := kv.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("snapshot %d differs; the session encoding is not deterministic", i)
		}
	}
}

func TestReplicasWithSessionsConverge(t *testing.T) {
	// Two replicas fed the same entries, including duplicates, must reach
	// byte-identical state — session tables included.
	entries := []raft.Entry{
		clientPut(1, 1, 1, "a", "1"),
		clientPut(2, 2, 1, "b", "2"),
		clientPut(3, 1, 1, "a", "duplicate"),
		clientPut(4, 1, 2, "a", "3"),
		clientDelete(5, 2, 2, "b"),
		clientPut(6, 2, 1, "b", "stale"),
	}

	x, y := New(), New()
	applyAll(t, x, entries)
	applyAll(t, y, entries)

	sx, _ := x.Snapshot()
	sy, _ := y.Snapshot()
	if !bytes.Equal(sx, sy) {
		t.Fatalf("two replicas diverged\nx: keys=%v sessions=%d\ny: keys=%v sessions=%d",
			x.Keys(), x.Sessions(), y.Keys(), y.Sessions())
	}
}

func TestSessionTableIsBounded(t *testing.T) {
	// The table is written into every snapshot, so it cannot grow with every
	// client that has ever connected.
	const max = 8
	kv := NewWithMaxSessions(max)

	for i := range 100 {
		if err := kv.Apply(clientPut(raft.Index(i+1), uint64(i+1), 1, "k", "v")); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}

	if got := kv.Sessions(); got > max {
		t.Fatalf("tracking %d sessions with a limit of %d", got, max)
	}
}

func TestEvictionIsDeterministicAcrossReplicas(t *testing.T) {
	// Eviction is a state machine transition like any other. If two replicas
	// chose different victims — because one walked a map in a different order,
	// say — their session tables would diverge and, from the next retry
	// onward, so would their data.
	const max = 4
	entries := make([]raft.Entry, 0, 60)
	for i := range 60 {
		// Revisit some clients so the tables have varied last-seen indexes
		// rather than a simple increasing sequence.
		clientID := uint64(i%17 + 1)
		entries = append(entries, clientPut(raft.Index(i+1), clientID, uint64(i/17+1), "k", "v"))
	}

	x, y := NewWithMaxSessions(max), NewWithMaxSessions(max)
	applyAll(t, x, entries)
	applyAll(t, y, entries)

	sx, err := x.Snapshot()
	if err != nil {
		t.Fatalf("snapshotting x: %v", err)
	}
	sy, err := y.Snapshot()
	if err != nil {
		t.Fatalf("snapshotting y: %v", err)
	}

	if !bytes.Equal(sx, sy) {
		t.Fatal("two replicas evicted different sessions; the choice is not deterministic")
	}
	if got := x.Sessions(); got > max {
		t.Fatalf("tracking %d sessions with a limit of %d", got, max)
	}
}

func TestEvictionKeepsTheMostRecentlyUsed(t *testing.T) {
	// Evicting the least recently used client is what makes the bound
	// tolerable: an active client keeps its protection, and only a client
	// that has been quiet for a long time loses it.
	const max = 3
	kv := NewWithMaxSessions(max)

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 1, 1, "k", "v"),
		clientPut(2, 2, 1, "k", "v"),
		clientPut(3, 3, 1, "k", "v"),
		// Client 1 stays active, so client 2 is now the oldest.
		clientPut(4, 1, 2, "k", "v"),
		// A fourth client forces an eviction.
		clientPut(5, 4, 1, "k", "v"),
	})

	if _, ok := kv.LastSeq(1); !ok {
		t.Fatal("the most recently active client was evicted")
	}
	if _, ok := kv.LastSeq(2); ok {
		t.Fatal("the least recently used client survived eviction")
	}
	for _, id := range []uint64{3, 4} {
		if _, ok := kv.LastSeq(id); !ok {
			t.Fatalf("client %d was evicted unexpectedly", id)
		}
	}
}

func TestEvictedClientLosesDeduplication(t *testing.T) {
	// The cost of the bound, stated as a test rather than left implicit. A
	// client evicted from the table is indistinguishable from a new one, so
	// its next retry is applied as though it were fresh.
	const max = 2
	kv := NewWithMaxSessions(max)

	applyAll(t, kv, []raft.Entry{
		clientPut(1, 1, 1, "x", "original"),
		clientPut(2, 2, 1, "k", "v"),
		clientPut(3, 3, 1, "k", "v"), // evicts client 1
	})

	if _, ok := kv.LastSeq(1); ok {
		t.Fatal("client 1 was not evicted; the test no longer exercises the case")
	}

	// The retry is now indistinguishable from a first request.
	applyAll(t, kv, []raft.Entry{clientPut(4, 1, 1, "x", "replayed")})
	if got := mustGet(t, kv, "x"); got != "replayed" {
		t.Fatalf("x = %q; an evicted client's retry was still deduplicated, which "+
			"contradicts the documented limitation", got)
	}
}

func TestCommandWithSessionRoundTrips(t *testing.T) {
	cases := []Command{
		{ClientID: 0, Seq: 0, Op: OpPut, Key: "k", Value: []byte("v")},
		{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("v")},
		{ClientID: ^uint64(0), Seq: ^uint64(0), Op: OpDelete, Key: "k"},
		{ClientID: 42, Seq: 7, Op: OpPut, Key: "", Value: nil},
	}

	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("decoding %+v: %v", want, err)
		}
		if got.ClientID != want.ClientID || got.Seq != want.Seq ||
			got.Op != want.Op || got.Key != want.Key {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("value = %q, want %q", got.Value, want.Value)
		}
	}
}

func TestMalformedSessionFieldsAreRejected(t *testing.T) {
	// A command truncated inside the session fields must be reported, not
	// silently decoded as client 0 — which would turn a deduplicated request
	// into a sessionless one and reintroduce the hazard.
	full := Command{ClientID: 9, Seq: 3, Op: OpPut, Key: "k", Value: []byte("v")}.Encode()

	for cut := 1; cut < 17; cut++ {
		if _, err := DecodeCommand(full[:cut]); err == nil {
			t.Fatalf("a command truncated to %d bytes was accepted", cut)
		}
	}
}

func TestMalformedSessionTableIsRejected(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{clientPut(1, 1, 1, "k", "v")})
	valid, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Cutting anywhere inside the session table must fail rather than restore
	// a partial table, which would silently forget some clients.
	for cut := range len(valid) {
		target := New()
		if err := target.Restore(valid[:cut]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes was accepted", cut, len(valid))
		}
	}
}

func TestSessionsRestoreOntoAPopulatedStore(t *testing.T) {
	// Restore replaces the session table as well as the data. A replica that
	// merged the two would keep sessions no other node has.
	stale := New()
	applyAll(t, stale, []raft.Entry{
		clientPut(1, 111, 5, "old", "value"),
	})

	source := New()
	applyAll(t, source, []raft.Entry{
		clientPut(1, 222, 3, "new", "value"),
	})
	snap, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if err := stale.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := stale.LastSeq(111); ok {
		t.Fatal("a session absent from the snapshot survived the restore")
	}
	seq, ok := stale.LastSeq(222)
	if !ok || seq != 3 {
		t.Fatalf("restored session for client 222 = (%d, %v), want (3, true)", seq, ok)
	}
}

func TestManyClientsUnderTheLimit(t *testing.T) {
	// A realistic mix: many clients, several requests each, with retries
	// scattered through. Every client's final value must reflect its highest
	// sequence and nothing older.
	kv := New()

	const clients = 50
	const perClient = 10

	index := raft.Index(0)
	next := func() raft.Index { index++; return index }

	for seq := uint64(1); seq <= perClient; seq++ {
		for id := uint64(1); id <= clients; id++ {
			key := fmt.Sprintf("client-%d", id)
			if err := kv.Apply(clientPut(next(), id, seq, key, fmt.Sprintf("seq-%d", seq))); err != nil {
				t.Fatalf("applying: %v", err)
			}
			// Every third request is retried immediately.
			if seq%3 == 0 {
				if err := kv.Apply(clientPut(next(), id, seq, key, "RETRY")); err != nil {
					t.Fatalf("applying retry: %v", err)
				}
			}
		}
	}

	for id := uint64(1); id <= clients; id++ {
		key := fmt.Sprintf("client-%d", id)
		want := fmt.Sprintf("seq-%d", perClient)
		if got := mustGet(t, kv, key); got != want {
			t.Fatalf("%s = %q, want %q; a retry overwrote the final value", key, got, want)
		}
		seq, ok := kv.LastSeq(id)
		if !ok || seq != perClient {
			t.Fatalf("client %d last sequence = (%d, %v), want (%d, true)", id, seq, ok, perClient)
		}
	}
}
