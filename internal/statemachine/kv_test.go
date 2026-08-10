package statemachine

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the key-value state machine.
//
// The store itself is simple; what these tests are really checking is that it
// is a deterministic function of the entries applied to it. A replicated state
// machine that agrees on the log but not on the resulting state has failed in
// the worst possible way, because nothing in Raft detects it — so determinism
// is tested directly and repeatedly rather than assumed.

// putEntry builds a Put entry at a given index.
func putEntry(index raft.Index, key, value string) raft.Entry {
	return raft.Entry{
		Term:  1,
		Index: index,
		Type:  raft.EntryNormal,
		Data:  Command{Op: OpPut, Key: key, Value: []byte(value)}.Encode(),
	}
}

// deleteEntry builds a Delete entry at a given index.
func deleteEntry(index raft.Index, key string) raft.Entry {
	return raft.Entry{
		Term:  1,
		Index: index,
		Type:  raft.EntryNormal,
		Data:  Command{Op: OpDelete, Key: key}.Encode(),
	}
}

// applyAll applies entries in order, failing the test on error.
func applyAll(t *testing.T, kv *KV, entries []raft.Entry) {
	t.Helper()
	for _, e := range entries {
		if err := kv.Apply(e); err != nil {
			t.Fatalf("applying entry %d: %v", e.Index, err)
		}
	}
}

// mustGet returns a key's value, failing the test if it is absent.
func mustGet(t *testing.T, kv *KV, key string) string {
	t.Helper()
	v, ok := kv.Get(key)
	if !ok {
		t.Fatalf("key %q is absent", key)
	}
	return string(v)
}

func TestApplyPutAndGet(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "a", "1"),
		putEntry(2, "b", "2"),
	})

	if got := mustGet(t, kv, "a"); got != "1" {
		t.Fatalf("a = %q, want 1", got)
	}
	if got := mustGet(t, kv, "b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
	if _, ok := kv.Get("missing"); ok {
		t.Fatal("a key that was never written is present")
	}
	if got := kv.Applied(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
}

func TestPutOverwrites(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "k", "first"),
		putEntry(2, "k", "second"),
	})

	if got := mustGet(t, kv, "k"); got != "second" {
		t.Fatalf("k = %q, want second", got)
	}
	if got := kv.Len(); got != 1 {
		t.Fatalf("store holds %d keys, want 1", got)
	}
}

func TestDelete(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "k", "v"),
		deleteEntry(2, "k"),
	})

	if _, ok := kv.Get("k"); ok {
		t.Fatal("key is still present after a delete")
	}
	if got := kv.Len(); got != 0 {
		t.Fatalf("store holds %d keys, want 0", got)
	}
}

func TestDeleteMissingKeySucceeds(t *testing.T) {
	// A command has to be applicable on every replica whatever that replica
	// holds. A delete that errored where the key was absent would diverge the
	// cluster, so it must succeed everywhere.
	kv := New()

	if err := kv.Apply(deleteEntry(1, "never-existed")); err != nil {
		t.Fatalf("deleting a missing key: %v", err)
	}
	if got := kv.Applied(); got != 1 {
		t.Fatalf("applied = %d, want 1", got)
	}
}

func TestEmptyValueIsDistinctFromAbsent(t *testing.T) {
	// An empty value is a value. Conflating it with a missing key would make
	// Get ambiguous and break any client storing empty strings.
	kv := New()
	applyAll(t, kv, []raft.Entry{putEntry(1, "k", "")})

	v, ok := kv.Get("k")
	if !ok {
		t.Fatal("a key with an empty value reads as absent")
	}
	if len(v) != 0 {
		t.Fatalf("value = %q, want empty", v)
	}
}

func TestNoOpEntriesAdvanceAppliedIndex(t *testing.T) {
	// A no-op is not a command, but it occupies an index. Failing to advance
	// past it would put the state machine one behind the log for every leader
	// election, and the gap check would then reject everything after it.
	kv := New()

	applyAll(t, kv, []raft.Entry{
		putEntry(1, "a", "1"),
		{Term: 1, Index: 2, Type: raft.EntryNoOp},
		putEntry(3, "b", "2"),
	})

	if got := kv.Applied(); got != 3 {
		t.Fatalf("applied = %d, want 3; the no-op did not advance the cursor", got)
	}
	if got := mustGet(t, kv, "b"); got != "2" {
		t.Fatalf("b = %q, want 2", got)
	}
}

func TestConfChangeEntriesAdvanceAppliedIndex(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "a", "1"),
		{Term: 1, Index: 2, Type: raft.EntryConfChange, Data: []byte("membership")},
	})

	if got := kv.Applied(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
}

func TestAlreadyAppliedEntriesAreIgnored(t *testing.T) {
	// After a crash a node restores a snapshot and replays the log from
	// before it, so re-delivery is the normal path rather than an error. The
	// replayed entry must not take effect a second time.
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "k", "current"),
		putEntry(2, "other", "x"),
	})

	stale := putEntry(1, "k", "stale")
	if err := kv.Apply(stale); err != nil {
		t.Fatalf("re-applying entry 1: %v", err)
	}

	if got := mustGet(t, kv, "k"); got != "current" {
		t.Fatalf("k = %q after replaying an old entry, want current", got)
	}
	if got := kv.Applied(); got != 2 {
		t.Fatalf("applied = %d, want 2; a replayed entry moved the cursor backwards", got)
	}
}

func TestGapInIndexesIsRejected(t *testing.T) {
	// The Raft core delivers committed entries in order, so a gap means the
	// caller is broken. Applying across it would silently skip commands.
	kv := New()
	applyAll(t, kv, []raft.Entry{putEntry(1, "a", "1")})

	err := kv.Apply(putEntry(5, "b", "2"))
	if !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("applying entry 5 after entry 1 gave %v, want ErrOutOfOrder", err)
	}
	if got := kv.Applied(); got != 1 {
		t.Fatalf("applied = %d after a rejected entry, want 1", got)
	}
}

func TestMalformedCommandIsRejected(t *testing.T) {
	cases := map[string][]byte{
		"empty":         {},
		"unknown op":    {99},
		"truncated key": {byte(OpPut), 0xff},
		"missing value": append([]byte{byte(OpPut)}, appendBytes(nil, []byte("k"))...),
		"key longer than buffer": append([]byte{byte(OpPut)},
			append(appendUint64(nil, 1000), []byte("short")...)...),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			kv := New()
			err := kv.Apply(raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: data})
			if !errors.Is(err, ErrMalformedCommand) {
				t.Fatalf("applying %q gave %v, want ErrMalformedCommand", name, err)
			}
			if got := kv.Applied(); got != 0 {
				t.Fatalf("applied advanced to %d despite a malformed command", got)
			}
		})
	}
}

func TestUnknownEntryTypeIsRejected(t *testing.T) {
	kv := New()
	err := kv.Apply(raft.Entry{Term: 1, Index: 1, Type: raft.EntryType(200)})
	if err == nil {
		t.Fatal("an unknown entry type was accepted")
	}
}

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: "k", Value: []byte("v")},
		{Op: OpPut, Key: "", Value: nil},
		{Op: OpPut, Key: "unicode-ключ-🔑", Value: []byte{0x00, 0xff}},
		{Op: OpDelete, Key: "gone"},
		{Op: OpPut, Key: "big", Value: bytes.Repeat([]byte("x"), 100_000)},
	}

	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("decoding %+v: %v", want.Op, err)
		}
		if got.Op != want.Op || got.Key != want.Key {
			t.Fatalf("decoded op %s key %q, want op %s key %q", got.Op, got.Key, want.Op, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("value length %d, want %d", len(got.Value), len(want.Value))
		}
	}
}

func TestSnapshotIsDeterministicAcrossCalls(t *testing.T) {
	// Go randomizes map iteration, so an encoding that walks the map directly
	// produces different bytes every time for identical state. Enough keys
	// are used here that a non-deterministic encoding could not pass by luck.
	kv := New()
	for i := range 200 {
		if err := kv.Apply(putEntry(raft.Index(i+1), fmt.Sprintf("key-%03d", i), fmt.Sprintf("value-%d", i))); err != nil {
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
			t.Fatalf("snapshot %d differs from the first; the encoding is not deterministic", i)
		}
	}
}

func TestReplicasConvergeToIdenticalSnapshots(t *testing.T) {
	// The property the whole package exists for: two nodes applying the same
	// entries in the same order must reach byte-identical state. Comparing
	// snapshots is the cheapest way to check it, and only works because the
	// encoding is deterministic.
	entries := []raft.Entry{
		putEntry(1, "zebra", "1"),
		putEntry(2, "apple", "2"),
		{Term: 1, Index: 3, Type: raft.EntryNoOp},
		putEntry(4, "mango", "3"),
		deleteEntry(5, "apple"),
		putEntry(6, "zebra", "updated"),
		putEntry(7, "banana", "4"),
	}

	a, b := New(), New()
	applyAll(t, a, entries)
	applyAll(t, b, entries)

	sa, err := a.Snapshot()
	if err != nil {
		t.Fatalf("snapshotting a: %v", err)
	}
	sb, err := b.Snapshot()
	if err != nil {
		t.Fatalf("snapshotting b: %v", err)
	}

	if !bytes.Equal(sa, sb) {
		t.Fatalf("two replicas applying identical entries produced different snapshots\n"+
			"a: keys=%v applied=%d\nb: keys=%v applied=%d",
			a.Keys(), a.Applied(), b.Keys(), b.Applied())
	}
}

func TestInsertionOrderDoesNotAffectSnapshot(t *testing.T) {
	// The same final state reached by different paths must encode identically,
	// or a node that took a different route through the log would look
	// divergent when it is not.
	forward := New()
	applyAll(t, forward, []raft.Entry{
		putEntry(1, "a", "1"),
		putEntry(2, "b", "2"),
		putEntry(3, "c", "3"),
	})

	// Same destination, opposite insertion order, plus a key added and
	// removed along the way.
	backward := New()
	applyAll(t, backward, []raft.Entry{
		putEntry(1, "c", "3"),
		putEntry(2, "temp", "x"),
		putEntry(3, "b", "2"),
		deleteEntry(4, "temp"),
		putEntry(5, "a", "1"),
	})

	// The applied index is part of the snapshot and legitimately differs, so
	// compare the key-value portion by restoring both into fresh stores at a
	// common index.
	sf, _ := forward.Snapshot()
	sb, _ := backward.Snapshot()

	if bytes.Equal(sf, sb) {
		t.Fatal("snapshots matched despite different applied indexes; the index is not being recorded")
	}

	x, y := New(), New()
	if err := x.Restore(sf); err != nil {
		t.Fatalf("restoring forward: %v", err)
	}
	if err := y.Restore(sb); err != nil {
		t.Fatalf("restoring backward: %v", err)
	}

	for _, k := range []string{"a", "b", "c"} {
		if mustGet(t, x, k) != mustGet(t, y, k) {
			t.Fatalf("key %q differs between the two paths", k)
		}
	}
	if x.Len() != y.Len() {
		t.Fatalf("stores hold %d and %d keys", x.Len(), y.Len())
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "a", "1"),
		putEntry(2, "b", ""),
		putEntry(3, "c", "value with \x00 bytes"),
		deleteEntry(4, "a"),
	})

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got := restored.Applied(); got != kv.Applied() {
		t.Fatalf("restored applied index %d, want %d", got, kv.Applied())
	}
	if got := restored.Len(); got != kv.Len() {
		t.Fatalf("restored %d keys, want %d", got, kv.Len())
	}

	again, err := restored.Snapshot()
	if err != nil {
		t.Fatalf("re-snapshotting: %v", err)
	}
	if !bytes.Equal(snap, again) {
		t.Fatal("a restored store does not re-snapshot identically")
	}
}

func TestRestoreReplacesExistingState(t *testing.T) {
	// Restore is a replacement, not a merge. Keys present before but absent
	// from the snapshot must be gone, or a restored replica would carry
	// state no other node has.
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "old", "1"),
		putEntry(2, "shared", "old value"),
	})

	other := New()
	applyAll(t, other, []raft.Entry{
		putEntry(1, "new", "1"),
		putEntry(2, "shared", "new value"),
	})
	snap, _ := other.Snapshot()

	if err := kv.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := kv.Get("old"); ok {
		t.Fatal("a key absent from the snapshot survived the restore")
	}
	if got := mustGet(t, kv, "shared"); got != "new value" {
		t.Fatalf("shared = %q, want the snapshot's value", got)
	}
	if got := mustGet(t, kv, "new"); got != "1" {
		t.Fatalf("new = %q, want 1", got)
	}
}

func TestRestoreAfterSnapshotResumesApplying(t *testing.T) {
	// The recovery path end to end: restore a snapshot, then continue
	// applying the entries that came after it.
	source := New()
	applyAll(t, source, []raft.Entry{
		putEntry(1, "a", "1"),
		putEntry(2, "b", "2"),
	})
	snap, _ := source.Snapshot()

	kv := New()
	if err := kv.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if err := kv.Apply(putEntry(3, "c", "3")); err != nil {
		t.Fatalf("applying after restore: %v", err)
	}
	if got := mustGet(t, kv, "c"); got != "3" {
		t.Fatalf("c = %q, want 3", got)
	}
	if got := kv.Applied(); got != 3 {
		t.Fatalf("applied = %d, want 3", got)
	}
}

func TestCorruptSnapshotLeavesStateIntact(t *testing.T) {
	// Restore is all-or-nothing. A snapshot that fails to decode part-way
	// through must not leave the store holding a mixture of old and new.
	kv := New()
	applyAll(t, kv, []raft.Entry{
		putEntry(1, "a", "1"),
		putEntry(2, "b", "2"),
	})
	before, _ := kv.Snapshot()

	source := New()
	applyAll(t, source, []raft.Entry{
		putEntry(1, "x", "9"),
		putEntry(2, "y", "8"),
		putEntry(3, "z", "7"),
	})
	snap, _ := source.Snapshot()

	// Truncate part-way through, so decoding fails only after several keys
	// have been read.
	if err := kv.Restore(snap[:len(snap)-5]); err == nil {
		t.Fatal("a truncated snapshot was accepted")
	}

	after, _ := kv.Snapshot()
	if !bytes.Equal(before, after) {
		t.Fatalf("a failed restore modified the store\nbefore: %v\nafter:  %v",
			before, after)
	}
}

func TestMalformedSnapshotIsRejected(t *testing.T) {
	kv := New()
	applyAll(t, kv, []raft.Entry{putEntry(1, "a", "1")})
	valid, _ := kv.Snapshot()

	cases := map[string][]byte{
		"empty":            {},
		"header only":      valid[:8],
		"truncated":        valid[:len(valid)-1],
		"trailing garbage": append(bytes.Clone(valid), 0x00),
		"implausible count": append(appendUint64(nil, 1),
			appendUint64(nil, ^uint64(0))...),
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			target := New()
			if err := target.Restore(data); !errors.Is(err, ErrMalformedSnapshot) {
				t.Fatalf("restoring %q gave %v, want ErrMalformedSnapshot", name, err)
			}
		})
	}
}

func TestEveryTruncationOfASnapshotIsRejected(t *testing.T) {
	// A snapshot cut at any point must fail to decode. Accepting a prefix
	// would silently drop keys and leave the replica quietly divergent.
	kv := New()
	for i := range 20 {
		if err := kv.Apply(putEntry(raft.Index(i+1), fmt.Sprintf("k%02d", i), "v")); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	full, _ := kv.Snapshot()

	for cut := range len(full) {
		target := New()
		if err := target.Restore(full[:cut]); err == nil {
			t.Fatalf("a snapshot truncated to %d of %d bytes was accepted", cut, len(full))
		}
	}
}

func TestGetReturnsACopy(t *testing.T) {
	// A caller holding the returned slice must not be able to mutate
	// committed state through it.
	kv := New()
	applyAll(t, kv, []raft.Entry{putEntry(1, "k", "original")})

	v, _ := kv.Get("k")
	for i := range v {
		v[i] = 'x'
	}

	if got := mustGet(t, kv, "k"); got != "original" {
		t.Fatalf("k = %q after a caller mutated the returned slice, want original", got)
	}
}

func TestApplyDoesNotAliasEntryData(t *testing.T) {
	// Entry payloads come from decoded log records whose buffers the caller
	// may reuse. The store must own what it holds.
	kv := New()
	e := putEntry(1, "k", "original")
	if err := kv.Apply(e); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for i := range e.Data {
		e.Data[i] = 0xff
	}

	if got := mustGet(t, kv, "k"); got != "original" {
		t.Fatalf("k = %q after the source entry buffer was overwritten, want original", got)
	}
}

func TestConcurrentReadsAndApplies(t *testing.T) {
	// Raft applies from one goroutine while clients read from others. Run
	// under -race, this is what proves the locking is right.
	kv := New()

	const writes = 500
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range writes {
			if err := kv.Apply(putEntry(raft.Index(i+1), fmt.Sprintf("key-%d", i%20), "v")); err != nil {
				panic(err)
			}
		}
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(1))
			for range writes {
				kv.Get(fmt.Sprintf("key-%d", rng.Intn(20)))
				kv.Len()
				kv.Applied()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			if _, err := kv.Snapshot(); err != nil {
				panic(err)
			}
		}
	}()

	wg.Wait()

	if got := kv.Applied(); got != writes {
		t.Fatalf("applied = %d, want %d", got, writes)
	}
}
