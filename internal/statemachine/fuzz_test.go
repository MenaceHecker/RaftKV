package statemachine

import (
	"runtime"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Fuzz targets for the decoders.
//
// These parse bytes that came from somewhere else: a log entry replayed off a
// disk that may have been damaged, a snapshot sent by another node. The
// contract for all of them is the same and is worth stating plainly, because
// it is the thing being fuzzed: malformed input must produce an error, never a
// panic and never a half-applied state. A node that crashes on a corrupt
// record turns a recoverable single-file problem into an outage, and the
// storage layer goes to some trouble to tell a torn write from a corrupt one
// precisely so that it can carry on.

func FuzzDecodeCommand(f *testing.F) {
	// Seeds are real encodings, so the fuzzer starts from the shape of a
	// valid command and mutates outward rather than guessing the format.
	f.Add(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("v")}.Encode())
	f.Add(Command{ClientID: 0, Seq: 0, Op: OpDelete, Key: "", Value: nil}.Encode())
	f.Add(Command{ClientID: ^uint64(0), Seq: ^uint64(0), Op: OpPut,
		Key: "a longer key", Value: make([]byte, 64)}.Encode())
	f.Add([]byte{})
	f.Add([]byte{0})

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := DecodeCommand(data)
		if err != nil {
			return
		}
		// Anything that decodes cleanly must re-encode to the same bytes.
		// A decoder that accepted two spellings of one command would let two
		// replicas disagree about what a log entry says while both believing
		// they had read it correctly.
		again := cmd.Encode()
		if string(again) != string(data) {
			t.Fatalf("command round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

func FuzzRestore(f *testing.F) {
	// A snapshot arrives from another node and replaces this one's entire
	// state, so a decoder that panicked here would take down a follower that
	// was merely trying to catch up.
	kv := New()
	empty, err := kv.Snapshot()
	if err != nil {
		f.Fatalf("snapshotting an empty store: %v", err)
	}
	f.Add(empty)

	kv2 := New()
	for i := range 4 {
		cmd := Command{ClientID: uint64(i + 1), Seq: 1, Op: OpPut,
			Key: string(rune('a' + i)), Value: []byte{byte(i)}}
		if err := kv2.Apply(raft.Entry{Index: raft.Index(i + 1), Term: 1, Data: cmd.Encode()}); err != nil {
			f.Fatalf("applying: %v", err)
		}
	}
	populated, err := kv2.Snapshot()
	if err != nil {
		f.Fatalf("snapshotting: %v", err)
	}
	f.Add(populated)
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		target := New()
		// Put something in first, so a failed restore has state it could
		// damage. Restore is documented as all-or-nothing.
		before := Command{ClientID: 9, Seq: 1, Op: OpPut, Key: "keep", Value: []byte("me")}
		if err := target.Apply(raft.Entry{Index: 1, Term: 1, Data: before.Encode()}); err != nil {
			t.Fatalf("seeding: %v", err)
		}

		if err := target.Restore(data); err != nil {
			// A rejected snapshot must leave the store exactly as it was.
			got, ok := target.Get("keep")
			if !ok || string(got) != "me" {
				t.Fatalf("a rejected snapshot damaged the existing state: got %q, present=%v",
					got, ok)
			}
			return
		}

		// A snapshot that restored cleanly must itself be re-snapshottable to
		// the same bytes, since the encoding is deterministic by design and
		// that is what lets two replicas be compared directly.
		again, err := target.Snapshot()
		if err != nil {
			t.Fatalf("re-snapshotting a restored store: %v", err)
		}
		if string(again) != string(data) {
			t.Fatalf("snapshot round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

// The regression tests below pin what fuzzing found. Each one is a property
// the decoders should always have had, and each was violated in a way no
// existing test noticed.

func TestDecodeCommandRejectsTrailingBytes(t *testing.T) {
	// Two byte strings must not decode to the same command. Beyond costing
	// the encoding its canonical form, ignoring a suffix throws away a free
	// corruption check: damage that leaves the prefix intact would be applied
	// as though the entry were sound.
	good := Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "k", Value: []byte("v")}.Encode()
	if _, err := DecodeCommand(good); err != nil {
		t.Fatalf("a well-formed command was rejected: %v", err)
	}
	if _, err := DecodeCommand(append(good, 0x00)); err == nil {
		t.Error("a command with a trailing byte was accepted")
	}
}

func TestRestoreRejectsUnsortedKeys(t *testing.T) {
	// Snapshot writes keys in order so that two replicas holding the same
	// state produce identical bytes, which is how convergence is checked.
	// Accepting any other order would mean several encodings of one state.
	buf := appendUint64(nil, 0) // applied
	buf = appendUint64(buf, 2)  // two keys
	buf = appendBytes(buf, []byte("b"))
	buf = appendBytes(buf, []byte("1"))
	buf = appendBytes(buf, []byte("a")) // out of order
	buf = appendBytes(buf, []byte("2"))
	buf = (&sessions{entries: map[uint64]*session{}}).encode(buf)

	if err := New().Restore(buf); err == nil {
		t.Error("a snapshot with descending keys was accepted")
	}
}

func TestRestoreRejectsRepeatedKeys(t *testing.T) {
	// A repeated key would otherwise be resolved silently by whichever copy
	// was decoded last.
	buf := appendUint64(nil, 0)
	buf = appendUint64(buf, 2)
	buf = appendBytes(buf, []byte("a"))
	buf = appendBytes(buf, []byte("1"))
	buf = appendBytes(buf, []byte("a"))
	buf = appendBytes(buf, []byte("2"))
	buf = (&sessions{entries: map[uint64]*session{}}).encode(buf)

	if err := New().Restore(buf); err == nil {
		t.Error("a snapshot with a repeated key was accepted")
	}
}

func TestRestoreRejectsACountTheDataCannotHold(t *testing.T) {
	// The count decides how large a map to allocate and arrives from a peer
	// or off a disk, so it has to be checked against the bytes that follow
	// before anything is sized from it. This sixteen byte payload allocated
	// three gigabytes before the fix.
	buf := appendUint64(nil, 0)
	buf = appendUint64(buf, 50_000_000)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	if err := New().Restore(buf); err == nil {
		t.Fatal("a snapshot declaring fifty million keys in sixteen bytes was accepted")
	}

	runtime.ReadMemStats(&after)
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Errorf("rejecting a %d byte payload allocated %.1f MB; the declared count is "+
			"still being trusted", len(buf), float64(grew)/(1<<20))
	}
}

func TestRestoreRejectsASessionCountTheDataCannotHold(t *testing.T) {
	buf := appendUint64(nil, 0)    // applied
	buf = appendUint64(buf, 0)     // no keys
	buf = appendUint64(buf, 1<<20) // but a million sessions
	if err := New().Restore(buf); err == nil {
		t.Error("a snapshot declaring a million sessions in eight bytes was accepted")
	}
}
