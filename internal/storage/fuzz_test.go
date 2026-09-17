package storage

import (
	"bytes"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Fuzz targets for the write-ahead log's decoders.
//
// This is where a torn write actually lands. The log is read back after a
// crash, by definition from a file whose tail may be half-written, and it is
// the layer that has to tell a record cut short by a kill from one whose bytes
// have been altered: the first is expected and recoverable, the second means
// something is wrong. Both paths run on input nobody chose.
//
// Two properties are checked throughout. Malformed input must produce an
// error, never a panic. And anything that decodes cleanly must re-encode to
// exactly the bytes it came from, because a decoder that accepts input its own
// encoder could not produce is accepting corruption that happens to parse.

func FuzzReadRecord(f *testing.F) {
	f.Add(appendRecord(nil, recordEntry, []byte("payload")))
	f.Add(appendRecord(nil, recordHardState, nil))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	// A truncated record, which is what a crash mid-write leaves behind.
	full := appendRecord(nil, recordEntry, []byte("cut short"))
	f.Add(full[:len(full)-3])

	f.Fuzz(func(t *testing.T, data []byte) {
		typ, payload, n, err := readRecord(data)
		if err != nil {
			return
		}
		if n <= 0 || n > len(data) {
			t.Fatalf("readRecord consumed %d bytes of %d", n, len(data))
		}

		again := appendRecord(nil, typ, payload)
		if !bytes.Equal(again, data[:n]) {
			t.Fatalf("record round-tripped to different bytes:\n in: %x\nout: %x",
				data[:n], again)
		}
	})
}

func FuzzDecodeEntry(f *testing.F) {
	f.Add(encodeEntry(nil, raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("x")}))
	f.Add(encodeEntry(nil, raft.Entry{}))
	f.Add(encodeEntry(nil, raft.Entry{Term: raft.Term(^uint64(0) >> 1), Index: 99, Data: make([]byte, 32)}))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		e, err := decodeEntry(data)
		if err != nil {
			return
		}
		again := encodeEntry(nil, e)
		if !bytes.Equal(again, data) {
			t.Fatalf("entry round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

func FuzzDecodeHardState(f *testing.F) {
	f.Add(encodeHardState(nil, raft.HardState{Term: 7, VotedFor: 3}))
	f.Add(encodeHardState(nil, raft.HardState{}))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		hs, err := decodeHardState(data)
		if err != nil {
			return
		}
		again := encodeHardState(nil, hs)
		if !bytes.Equal(again, data) {
			t.Fatalf("hard state round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

func FuzzDecodeSnapshotMeta(f *testing.F) {
	f.Add(encodeSnapshotMeta(nil, SnapshotMeta{Index: 12, Term: 3}))
	f.Add(encodeSnapshotMeta(nil, SnapshotMeta{}))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := decodeSnapshotMeta(data)
		if err != nil {
			return
		}
		again := encodeSnapshotMeta(nil, m)
		if !bytes.Equal(again, data) {
			t.Fatalf("snapshot meta round-tripped to different bytes:\n in: %x\nout: %x",
				data, again)
		}
	})
}

// Regression tests for what fuzzing found in the record decoders.

func TestRecordDecodersRejectTrailingBytes(t *testing.T) {
	// The framing establishes each payload's length exactly, so anything
	// left over means the payload does not match the shape it claims. All
	// three accepted a suffix and silently ignored it, which let a record
	// this encoder could never have written decode as though it had.
	entry := encodeEntry(nil, raft.Entry{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("x")})
	if _, err := decodeEntry(append(entry, 0)); err == nil {
		t.Error("an entry with a trailing byte was accepted")
	}

	hs := encodeHardState(nil, raft.HardState{Term: 4, VotedFor: 2})
	if _, err := decodeHardState(append(hs, 0)); err == nil {
		t.Error("a hard state with a trailing byte was accepted")
	}

	meta := encodeSnapshotMeta(nil, SnapshotMeta{Index: 9, Term: 2})
	if _, err := decodeSnapshotMeta(append(meta, 0)); err == nil {
		t.Error("snapshot metadata with a trailing byte was accepted")
	}
}

func TestDecodeEntryRejectsAnUnknownType(t *testing.T) {
	// The type occupies eight bytes but is a single byte wide, so a damaged
	// field used to be truncated into whatever the low byte happened to be.
	// A corrupt entry would then decode as an ordinary one, of a type it
	// never had.
	valid := encodeEntry(nil, raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: nil})
	if _, err := decodeEntry(valid); err != nil {
		t.Fatalf("a well-formed entry was rejected: %v", err)
	}

	// Same low byte as EntryNormal, garbage above it.
	corrupt := appendUint64(nil, 1)
	corrupt = appendUint64(corrupt, 1)
	corrupt = appendUint64(corrupt, 0x3030303030303000)
	corrupt = appendBytes(corrupt, nil)
	if _, err := decodeEntry(corrupt); err == nil {
		t.Error("an entry whose type field was corrupt above its low byte was accepted")
	}

	// A type that fits in a byte but names nothing.
	unknown := appendUint64(nil, 1)
	unknown = appendUint64(unknown, 1)
	unknown = appendUint64(unknown, 200)
	unknown = appendBytes(unknown, nil)
	if _, err := decodeEntry(unknown); err == nil {
		t.Error("an entry with an undefined type was accepted")
	}
}
