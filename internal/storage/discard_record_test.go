package storage

// T3.11: the DiscardSuffix record (type 4). Decoding, replay, torn and corrupt
// records, and how it interacts with segment compaction and segment naming.

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func entryRec(e raft.Entry) []byte { return appendRecord(nil, recordEntry, encodeEntry(nil, e)) }
func metaRec(m SnapshotMeta) []byte {
	return appendRecord(nil, recordSnapshotMeta, encodeSnapshotMeta(nil, m))
}
func discardRec(from raft.Index) []byte {
	return appendRecord(nil, recordDiscardSuffix, encodeDiscardSuffix(nil, from))
}

func entryRecs(es []raft.Entry) []byte {
	var b []byte
	for _, e := range es {
		b = append(b, entryRec(e)...)
	}
	return b
}

func concat(parts ...[]byte) []byte {
	var b []byte
	for _, p := range parts {
		b = append(b, p...)
	}
	return b
}

func writeNamedSegment(t *testing.T, dir string, seq uint64, first raft.Index, data []byte) string {
	t.Helper()
	if err := os.MkdirAll(dir, dirMode); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	path := filepath.Join(dir, segmentName(seq, first))
	writeSegment(t, path, data)
	return path
}

func TestDecodeDiscardSuffix(t *testing.T) {
	if got, err := decodeDiscardSuffix(encodeDiscardSuffix(nil, 6)); err != nil || got != 6 {
		t.Fatalf("round trip of 6 = %d, %v", got, err)
	}
	for name, payload := range map[string][]byte{
		"empty":        nil,
		"short":        make([]byte, 7),
		"long":         make([]byte, 9),
		"from index 0": make([]byte, 8),
	} {
		if _, err := decodeDiscardSuffix(payload); !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("%s: error = %v, want ErrCorruptRecord", name, err)
		}
	}
	if recordDiscardSuffix.String() != "DiscardSuffix" {
		t.Errorf("String() = %q", recordDiscardSuffix.String())
	}
}

// TestDiscardSuffixReplay: the record cuts the log at replay, in log order.
func TestDiscardSuffixReplay(t *testing.T) {
	stale := entries(1, 1, 10)
	for _, tc := range []struct {
		name string
		data []byte
		want []raft.Entry
	}{
		{"cuts the tail", concat(entryRecs(stale), discardRec(6)), stale[:5]},
		{"from the first index empties the log", concat(entryRecs(stale), discardRec(1)), nil},
		{"above the last entry is a no-op", concat(entryRecs(stale), discardRec(11)), stale},
		{"far above the last entry is a no-op", concat(entryRecs(stale), discardRec(1<<40)), stale},
		{"on an empty log is a no-op", discardRec(6), nil},
		{"entries appended after it re-extend the log",
			concat(entryRecs(stale), discardRec(6), entryRecs(entries(2, 6, 2))),
			append(append([]raft.Entry(nil), stale[:5]...), entries(2, 6, 2)...)},
		{"twice is idempotent", concat(entryRecs(stale), discardRec(6), discardRec(6)), stale[:5]},
		{"a later, lower discard wins", concat(entryRecs(stale), discardRec(8), discardRec(4)), stale[:3]},
		{"an old boundary entry after it is just an entry",
			concat(entryRecs(stale), discardRec(6), entryRec(stale[4])), stale[:5]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeNamedSegment(t, dir, 1, 1, tc.data)
			w, rep := openWAL(t, dir, Options{})
			assertEntries(t, rep.Entries, tc.want)
			closeWAL(t, w)

			// A second open replays the same thing.
			_, rep2 := openWAL(t, dir, Options{})
			assertEntries(t, rep2.Entries, tc.want)
		})
	}

	t.Run("does not touch the snapshot meta or the hard state", func(t *testing.T) {
		dir := t.TempDir()
		hs := raft.HardState{Term: 3, VotedFor: 2}
		meta := SnapshotMeta{Index: 5, Term: 2}
		writeNamedSegment(t, dir, 1, 1, concat(entryRecs(stale),
			appendRecord(nil, recordHardState, encodeHardState(nil, hs)), metaRec(meta), discardRec(6)))
		_, rep := openWAL(t, dir, Options{})
		if rep.HardState != hs || rep.Snapshot != meta {
			t.Fatalf("hard state %+v, snapshot %+v; want %+v, %+v", rep.HardState, rep.Snapshot, hs, meta)
		}
	})
}

// TestDiscardWriterThenRestarts: SaveSnapshotMetaAndDiscardSuffix, appends after
// it, and any number of restarts. The discarded entries never return and the
// appended ones always survive.
func TestDiscardWriterThenRestarts(t *testing.T) {
	dir := t.TempDir()
	meta := SnapshotMeta{Index: 5, Term: 2}
	w, _ := openWAL(t, dir, Options{})
	appendEach(t, w, entries(1, 1, 10))
	if err := w.SaveSnapshotMetaAndDiscardSuffix(meta, 6); err != nil {
		t.Fatalf("SaveSnapshotMetaAndDiscardSuffix: %v", err)
	}
	if err := w.AppendEntries(entries(2, 6, 2)); err != nil {
		t.Fatalf("append after the discard: %v", err)
	}
	closeWAL(t, w)

	want := append(append([]raft.Entry(nil), entries(1, 1, 5)...), entries(2, 6, 2)...)
	for restart := 1; restart <= 3; restart++ {
		w, rep := openWAL(t, dir, Options{})
		assertEntries(t, rep.Entries, want)
		if rep.Snapshot != meta {
			t.Fatalf("restart %d: snapshot %+v, want %+v", restart, rep.Snapshot, meta)
		}
		closeWAL(t, w)
	}

	w, _ = openWAL(t, dir, Options{})
	if err := w.SaveSnapshotMetaAndDiscardSuffix(meta, 0); err == nil {
		t.Error("a discard from index 0 was accepted")
	}
}

// TestTornDiscardIsRepairedAsUnacknowledged: a crash inside the combined write
// leaves the snapshot meta (or nothing) and the old tail (states b and c of the
// card); Open succeeds, repairs the torn record and keeps the stale entries.
func TestTornDiscardIsRepairedAsUnacknowledged(t *testing.T) {
	meta := SnapshotMeta{Index: 5, Term: 2}
	discardLen := len(discardRec(6))

	for cut := 1; cut <= discardLen; cut++ {
		t.Run(fmt.Sprintf("last %d of %d bytes lost", cut, discardLen), func(t *testing.T) {
			dir := t.TempDir()
			w, _ := openWAL(t, dir, Options{})
			appendEach(t, w, entries(1, 1, 10))
			if err := w.SaveSnapshotMetaAndDiscardSuffix(meta, 6); err != nil {
				t.Fatalf("writing: %v", err)
			}
			closeWAL(t, w)
			truncateTail(t, dir, int64(cut))

			w, rep := openWAL(t, dir, Options{})
			defer closeWAL(t, w)
			if cut < discardLen && !rep.Repaired {
				t.Error("a torn discard was not reported as repaired")
			}
			if rep.Snapshot != meta {
				t.Errorf("snapshot %+v, want the meta that precedes the discard, %+v", rep.Snapshot, meta)
			}
			assertEntries(t, rep.Entries, entries(1, 1, 10))
		})
	}
}

func TestCorruptDiscardIsRefused(t *testing.T) {
	t.Run("CRC-valid but undecodable (from index 0)", func(t *testing.T) {
		dir := t.TempDir()
		bad := appendRecord(nil, recordDiscardSuffix, make([]byte, 8))
		path := writeNamedSegment(t, dir, 1, 1, concat(entryRecs(entries(1, 1, 3)), bad))
		assertRefused(t, dir, path, readSegment(t, path))
	})

	t.Run("CRC-valid but wrong length", func(t *testing.T) {
		dir := t.TempDir()
		bad := appendRecord(nil, recordDiscardSuffix, make([]byte, 9))
		path := writeNamedSegment(t, dir, 1, 1, concat(entryRecs(entries(1, 1, 3)), bad))
		assertRefused(t, dir, path, readSegment(t, path))
	})

	t.Run("bad CRC with a valid record after it", func(t *testing.T) {
		dir := t.TempDir()
		data := concat(entryRecs(entries(1, 1, 3)), discardRec(2), entryRec(entries(2, 2, 1)[0]))
		path := writeNamedSegment(t, dir, 1, 1, data)
		starts := recordStarts(t, data)
		mutateSegment(t, path, func(d []byte) { d[starts[3]+headerSize+typeSize] ^= 0xff })
		assertRefused(t, dir, path, readSegment(t, path))
	})

	t.Run("bad CRC at the very tail is a tear", func(t *testing.T) {
		dir := t.TempDir()
		data := concat(entryRecs(entries(1, 1, 3)), discardRec(2))
		path := writeNamedSegment(t, dir, 1, 1, data)
		mutateSegment(t, path, func(d []byte) { d[len(d)-1] ^= 0xff })
		w, rep := openWAL(t, dir, Options{})
		defer closeWAL(t, w)
		if !rep.Repaired {
			t.Error("a corrupt discard with nothing after it was not repaired")
		}
		assertEntries(t, rep.Entries, entries(1, 1, 3))
	})
}

// compactionLayout is a WAL after a mismatching install at 5, as segments: the
// stale tail in the oldest, the discard and new entries after it, naming each
// segment the way roll would (lastIndex+1).
//
//	seq 1  first 1  entries 1..10 at term 1 (6..10 stale)
//	seq 2  first 6  SnapshotMeta(5,2), DiscardSuffix(6), entries 6..7 at term 2
//	seq 3  first 8  entries 8..9 at term 2
func compactionLayout(t *testing.T, dir string) {
	t.Helper()
	writeNamedSegment(t, dir, 1, 1, entryRecs(entries(1, 1, 10)))
	writeNamedSegment(t, dir, 2, 6, concat(metaRec(SnapshotMeta{Index: 5, Term: 2}), discardRec(6), entryRecs(entries(2, 6, 2))))
	writeNamedSegment(t, dir, 3, 8, entryRecs(entries(2, 8, 2)))
}

func segmentSeqs(t *testing.T, dir string) []uint64 {
	t.Helper()
	var seqs []uint64
	for _, p := range segmentPaths(t, dir) {
		s, err := parseSegmentName(filepath.Base(p))
		if err != nil {
			t.Fatalf("parsing %s: %v", p, err)
		}
		seqs = append(seqs, s.seq)
	}
	return seqs
}

// TestDiscardSurvivesCompactionOrdering (T3.11 condition A). TruncateBefore removes
// a prefix of the segments in sequence order and stops at the first one it
// cannot remove, so the segment holding the discard is never removed while an
// older one holding the stale tail remains. Pins the exact scenario (older
// segment 6..10 stale, newer segment the discard plus new 6..7, TruncateBefore(8))
// and then every cut point.
func TestDiscardSurvivesCompactionOrdering(t *testing.T) {
	t.Run("stale 6..10 older, discard and new 6..7 newer, TruncateBefore(8)", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSegment(t, dir, 1, 6, entryRecs(entries(1, 6, 5)))
		writeNamedSegment(t, dir, 2, 6, concat(metaRec(SnapshotMeta{Index: 5, Term: 2}), discardRec(6), entryRecs(entries(2, 6, 2))))

		w, rep := openWAL(t, dir, Options{})
		assertEntries(t, rep.Entries, entries(2, 6, 2))
		if err := w.TruncateBefore(8); err != nil {
			t.Fatalf("TruncateBefore: %v", err)
		}
		if got := segmentSeqs(t, dir); len(got) != 1 || got[0] != 2 {
			t.Fatalf("segments left %v, want only the newer one (seq 2)", got)
		}
		closeWAL(t, w)

		_, rep = openWAL(t, dir, Options{})
		for _, e := range rep.Entries {
			if e.Term == 1 {
				t.Fatalf("a stale term-1 entry came back: %s", formatEntries(rep.Entries))
			}
		}
		assertEntries(t, rep.Entries, entries(2, 6, 2))
	})

	t.Run("every cut point", func(t *testing.T) {
		for cut := raft.Index(0); cut <= 12; cut++ {
			t.Run(fmt.Sprintf("TruncateBefore(%d)", cut), func(t *testing.T) {
				dir := t.TempDir()
				compactionLayout(t, dir)
				w, _ := openWAL(t, dir, Options{})
				if err := w.TruncateBefore(cut); err != nil {
					t.Fatalf("TruncateBefore: %v", err)
				}
				closeWAL(t, w)

				seqs := segmentSeqs(t, dir)
				if len(seqs) == 0 || seqs[len(seqs)-1] != 3 {
					t.Fatalf("segments left %v: the last segment must stay", seqs)
				}
				for i := 1; i < len(seqs); i++ {
					if seqs[i] != seqs[i-1]+1 {
						t.Fatalf("segments left %v are not a contiguous suffix in sequence order", seqs)
					}
				}
				_, rep := openWAL(t, dir, Options{})
				for _, e := range rep.Entries {
					if e.Term == 1 && e.Index > 5 {
						t.Fatalf("a stale term-1 entry %d/%d came back: %s", e.Index, e.Term, formatEntries(rep.Entries))
					}
				}
			})
		}
	})

	t.Run("non-monotonic first indexes: removal still stops at the first segment it cannot remove", func(t *testing.T) {
		dir := t.TempDir()
		// seq 2 is named for a high first index, as after a rewrite of a long tail.
		writeNamedSegment(t, dir, 1, 1, entryRecs(entries(1, 1, 10)))
		writeNamedSegment(t, dir, 2, 20, concat(metaRec(SnapshotMeta{Index: 5, Term: 2}), discardRec(6)))
		writeNamedSegment(t, dir, 3, 6, entryRecs(entries(2, 6, 2)))
		writeNamedSegment(t, dir, 4, 8, entryRecs(entries(2, 8, 2)))

		w, _ := openWAL(t, dir, Options{})
		if err := w.TruncateBefore(7); err != nil {
			t.Fatalf("TruncateBefore: %v", err)
		}
		closeWAL(t, w)
		if got := segmentSeqs(t, dir); len(got) != 4 {
			t.Fatalf("segments left %v, want all four: seq 1 cannot be removed (seq 2 starts at 20), so nothing after it may be", got)
		}
		_, rep := openWAL(t, dir, Options{})
		for _, e := range rep.Entries {
			if e.Term == 1 && e.Index > 5 {
				t.Fatalf("a stale entry came back: %s", formatEntries(rep.Entries))
			}
		}
	})
}

// TestSegmentOrderIsBySequenceNotFirstIndex (T3.11 condition B, a HYPOTHESIS
// test: "after a suffix rewrite the next segment is named S+1 while an older
// segment still holds 6..10, and Open orders segments by sequence, not by first
// index"). Built through the real writer with a tiny segment size so rolls happen
// on both sides of the discard.
func TestSegmentOrderIsBySequenceNotFirstIndex(t *testing.T) {
	dir := t.TempDir()
	meta := SnapshotMeta{Index: 5, Term: 2}

	w, _ := openWAL(t, dir, Options{SegmentSize: 100})
	appendEach(t, w, entries(1, 1, 10))
	if err := w.SaveSnapshotMetaAndDiscardSuffix(meta, 6); err != nil {
		t.Fatalf("discard: %v", err)
	}
	appendEach(t, w, entries(2, 6, 6))
	closeWAL(t, w)

	var firsts []raft.Index
	for _, p := range segmentPaths(t, dir) {
		s, err := parseSegmentName(filepath.Base(p))
		if err != nil {
			t.Fatalf("parsing %s: %v", p, err)
		}
		firsts = append(firsts, s.firstIndex)
	}
	monotonic := true
	for i := 1; i < len(firsts); i++ {
		if firsts[i] < firsts[i-1] {
			monotonic = false
		}
	}
	if monotonic {
		t.Fatalf("precondition: first indexes %v are monotonic, the test would prove nothing", firsts)
	}
	t.Logf("segment first indexes in sequence order: %v", firsts)

	want := append(append([]raft.Entry(nil), entries(1, 1, 5)...), entries(2, 6, 6)...)
	for restart := 1; restart <= 2; restart++ {
		w, rep := openWAL(t, dir, Options{SegmentSize: 100})
		assertEntries(t, rep.Entries, want)
		if rep.Snapshot != meta {
			t.Fatalf("restart %d: snapshot %+v, want %+v", restart, rep.Snapshot, meta)
		}
		// The active segment is the highest sequence number, whatever its first index.
		if err := w.AppendEntries(entries(2, raft.Index(len(want))+1, 1)); err != nil {
			t.Fatalf("restart %d: appending: %v", restart, err)
		}
		want = append(want, entries(2, raft.Index(len(want))+1, 1)...)
		closeWAL(t, w)
	}

	// Compaction on the non-monotonic names still drops only a prefix and still
	// never revives the stale tail.
	w, _ = openWAL(t, dir, Options{SegmentSize: 100})
	if err := w.TruncateBefore(9); err != nil {
		t.Fatalf("TruncateBefore: %v", err)
	}
	closeWAL(t, w)
	_, rep := openWAL(t, dir, Options{SegmentSize: 100})
	for _, e := range rep.Entries {
		if e.Term == 1 && e.Index > 5 {
			t.Fatalf("a stale entry came back after compaction: %s", formatEntries(rep.Entries))
		}
	}
}

// TestReplayModelWithDiscardRecords: 500 random sequences of Entry, HardState,
// SnapshotMeta and DiscardSuffix records, split over one to three segments,
// replayed by Open and compared with tailModel, the fuzz oracle that applies the
// same records one by one.
func TestReplayModelWithDiscardRecords(t *testing.T) {
	for seed := int64(1); seed <= 500; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test
		// below returns a value in [0, n), as the unsigned type the log indexes use.
		below := func(n uint64) uint64 { return rng.Uint64() % n }

		var model tailModel
		var recs [][]byte
		last := raft.Index(0)
		for range 4 + rng.Intn(30) {
			var rec []byte
			var typ recordType
			var payload []byte
			switch rng.Intn(6) {
			case 0, 1, 2: // an entry, extending the log or rewriting from the middle
				idx := last + 1
				if last > 0 && rng.Intn(3) == 0 {
					idx = 1 + raft.Index(below(uint64(last)))
				}
				e := raft.Entry{Term: raft.Term(1 + below(4)), Index: idx, Type: raft.EntryNormal,
					Data: []byte(fmt.Sprintf("s%d-%d", seed, len(recs)))}
				typ, payload = recordEntry, encodeEntry(nil, e)
				last = idx
			case 3:
				typ, payload = recordHardState, encodeHardState(nil, raft.HardState{
					Term: raft.Term(below(9)), VotedFor: raft.NodeID(below(4))})
			case 4:
				typ, payload = recordSnapshotMeta, encodeSnapshotMeta(nil, SnapshotMeta{
					Index: raft.Index(below(12)), Term: raft.Term(below(5))})
			default: // a discard: inside the log, at its end, or far above it
				from := raft.Index(1 + below(14))
				if rng.Intn(5) == 0 {
					from = last + raft.Index(1+below(1000))
				}
				typ, payload = recordDiscardSuffix, encodeDiscardSuffix(nil, from)
				if from <= last {
					last = from - 1
				}
			}
			rec = appendRecord(nil, typ, payload)
			if err := model.apply(byte(typ), payload); err != nil {
				t.Fatalf("seed %d: the model rejected a record it was given: %v", seed, err)
			}
			recs = append(recs, rec)
		}

		dir := t.TempDir()
		segs := 1 + rng.Intn(3)
		per := (len(recs) + segs - 1) / segs
		seq := uint64(1)
		for i := 0; i < len(recs); i += per {
			end := min(i+per, len(recs))
			writeNamedSegment(t, dir, seq, raft.Index(1+below(20)), concat(recs[i:end]...)) // names need not be monotonic
			seq++
		}

		w, rep, err := Open(Options{Dir: dir, FS: noSyncFS{osFS{}}})
		if err != nil {
			t.Fatalf("seed %d: Open: %v", seed, err)
		}
		if !sameEntries(rep.Entries, model.entries) || rep.HardState != model.hs || rep.Snapshot != model.snap {
			t.Fatalf("seed %d: Open replayed entries [%s] hard state %+v snapshot %+v\nmodel:    entries [%s] hard state %+v snapshot %+v",
				seed, formatEntries(rep.Entries), rep.HardState, rep.Snapshot,
				formatEntries(model.entries), model.hs, model.snap)
		}
		closeWAL(t, w)
	}
}
