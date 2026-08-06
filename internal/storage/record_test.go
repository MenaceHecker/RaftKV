package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Tests for the on-disk record format.
//
// The framing is what makes crash recovery possible, so these tests are less
// about round-tripping happy-path values and more about the two ways a file
// can end badly: a record that was never finished (torn), and a record whose
// bytes reached disk wrong (corrupt). Recovery treats those differently, so
// the reader has to tell them apart reliably.

func TestRecordRoundTrip(t *testing.T) {
	payloads := map[string][]byte{
		"empty":      {},
		"small":      []byte("hello"),
		"binary":     {0x00, 0xff, 0x00, 0xde, 0xad, 0xbe, 0xef},
		"with nulls": make([]byte, 1024),
	}

	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			buf := appendRecord(nil, recordEntry, payload)

			typ, got, n, err := readRecord(buf)
			if err != nil {
				t.Fatalf("readRecord: %v", err)
			}
			if typ != recordEntry {
				t.Fatalf("type = %s, want %s", typ, recordEntry)
			}
			if n != len(buf) {
				t.Fatalf("consumed %d bytes, want %d", n, len(buf))
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload = %x, want %x", got, payload)
			}
		})
	}
}

func TestRecordsReadBackInSequence(t *testing.T) {
	// A file is a bare sequence of records with no index, so the reader must
	// be able to walk it by consuming one record at a time.
	var buf []byte
	want := []string{"first", "second", "third"}
	for _, s := range want {
		buf = appendRecord(buf, recordEntry, []byte(s))
	}

	var got []string
	for len(buf) > 0 {
		_, payload, n, err := readRecord(buf)
		if err != nil {
			t.Fatalf("readRecord at %d records in: %v", len(got), err)
		}
		got = append(got, string(payload))
		buf = buf[n:]
	}

	if len(got) != len(want) {
		t.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestTruncatedRecordIsTorn(t *testing.T) {
	// The crash case. A process killed mid-write leaves a partial record at
	// the tail, and every prefix of a record must be reported as torn rather
	// than corrupt — recovery keeps the good prefix of the file and truncates
	// there, so misclassifying this would discard valid committed entries.
	full := appendRecord(nil, recordEntry, []byte("a reasonably sized payload"))

	for cut := range len(full) {
		_, _, _, err := readRecord(full[:cut])
		if !errors.Is(err, ErrTornRecord) {
			t.Fatalf("a %d-byte prefix of a %d-byte record gave %v, want ErrTornRecord",
				cut, len(full), err)
		}
	}
}

func TestCorruptPayloadIsDetected(t *testing.T) {
	// Bytes reached disk but are not what was written. Every single-bit flip
	// in the body must be caught, otherwise a silently corrupted entry gets
	// applied to the state machine as though it were real.
	payload := []byte("the quick brown fox jumps over the lazy dog")
	original := appendRecord(nil, recordEntry, payload)

	for i := headerSize; i < len(original); i++ {
		for bit := range 8 {
			corrupt := bytes.Clone(original)
			corrupt[i] ^= 1 << bit

			_, _, _, err := readRecord(corrupt)
			if !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("flipping bit %d of byte %d gave %v, want ErrCorruptRecord",
					bit, i, err)
			}
		}
	}
}

func TestCorruptChecksumIsDetected(t *testing.T) {
	// The mirror case: the body is intact but the stored checksum is not.
	original := appendRecord(nil, recordEntry, []byte("payload"))

	corrupt := bytes.Clone(original)
	corrupt[lenSize] ^= 0xff

	_, _, _, err := readRecord(corrupt)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("corrupting the checksum gave %v, want ErrCorruptRecord", err)
	}
}

func TestOversizedLengthIsRejectedBeforeAllocating(t *testing.T) {
	// The length field is read before the CRC can prove it is garbage, so a
	// corrupt length would otherwise drive an enormous allocation. The bound
	// has to be enforced on the declared size, not the real one.
	buf := make([]byte, headerSize+16)
	binary.LittleEndian.PutUint32(buf[0:lenSize], math.MaxUint32)

	_, _, _, err := readRecord(buf)
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("a record claiming %d bytes gave %v, want ErrRecordTooLarge",
			uint32(math.MaxUint32), err)
	}
}

func TestZeroLengthRecordIsRejected(t *testing.T) {
	// Every record carries at least a type byte, so a body length below that
	// is impossible and means the header is garbage. A run of zero bytes —
	// what a sparse or preallocated file reads as — hits this path.
	buf := make([]byte, headerSize+16)

	_, _, _, err := readRecord(buf)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("a zero-length record gave %v, want ErrCorruptRecord", err)
	}
}

func TestEntryRoundTrip(t *testing.T) {
	cases := []raft.Entry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("set x=1")},
		{Term: 0, Index: 0, Type: raft.EntryNoOp, Data: nil},
		{Term: 9, Index: 4, Type: raft.EntryNoOp, Data: []byte{}},
		{Term: 100, Index: 5000, Type: raft.EntryConfChange, Data: []byte{0x00, 0xff}},
		{
			Term:  math.MaxUint64,
			Index: math.MaxUint64,
			Type:  raft.EntryNormal,
			Data:  bytes.Repeat([]byte("x"), 4096),
		},
	}

	for _, want := range cases {
		payload := encodeEntry(nil, want)
		got, err := decodeEntry(payload)
		if err != nil {
			t.Fatalf("decodeEntry(%+v): %v", want, err)
		}

		if got.Term != want.Term || got.Index != want.Index || got.Type != want.Type {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
		if !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("data = %x, want %x", got.Data, want.Data)
		}
	}
}

func TestDecodedEntryDoesNotAliasItsBuffer(t *testing.T) {
	// Payloads point into the buffer a file was read into. If a decoded entry
	// aliased that buffer, reusing it for the next read would silently rewrite
	// entries already handed to the Raft core.
	want := []byte("original data")
	payload := encodeEntry(nil, raft.Entry{Term: 1, Index: 1, Data: want})

	got, err := decodeEntry(payload)
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}

	for i := range payload {
		payload[i] = 0xaa
	}

	if !bytes.Equal(got.Data, want) {
		t.Fatalf("entry data changed to %q when its source buffer was overwritten; "+
			"decodeEntry must copy, not alias", got.Data)
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	cases := []raft.HardState{
		{},
		{Term: 1, VotedFor: 0},
		{Term: 1, VotedFor: 3},
		{Term: math.MaxUint64, VotedFor: math.MaxUint64},
	}

	for _, want := range cases {
		got, err := decodeHardState(encodeHardState(nil, want))
		if err != nil {
			t.Fatalf("decodeHardState(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
	}
}

func TestSnapshotMetaRoundTrip(t *testing.T) {
	cases := []SnapshotMeta{
		{},
		{Index: 1, Term: 1},
		{Index: 999999, Term: 42},
	}

	for _, want := range cases {
		got, err := decodeSnapshotMeta(encodeSnapshotMeta(nil, want))
		if err != nil {
			t.Fatalf("decodeSnapshotMeta(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("decoded %+v, want %+v", got, want)
		}
	}
}

func TestTruncatedPayloadIsRejected(t *testing.T) {
	// A payload can be short without the record framing noticing — the CRC
	// covers what was written, so a record framing a truncated payload is
	// internally consistent. The field decoders are the last line of defence
	// and must report the shortfall instead of reading past the end.
	full := encodeEntry(nil, raft.Entry{Term: 1, Index: 2, Data: []byte("data")})

	for cut := range len(full) {
		if _, err := decodeEntry(full[:cut]); err == nil {
			t.Fatalf("decodeEntry accepted a %d-byte prefix of a %d-byte payload",
				cut, len(full))
		}
	}
}

func TestEntryDataLengthIsBounded(t *testing.T) {
	// The same allocation guard one level down: a corrupt length prefix
	// inside an otherwise valid record must not drive a huge allocation.
	payload := appendUint64(nil, 1)                 // term
	payload = appendUint64(payload, 1)              // index
	payload = appendUint64(payload, 0)              // type
	payload = appendUint64(payload, math.MaxUint64) // data length
	payload = append(payload, 'x')

	_, err := decodeEntry(payload)
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("a data length of %d gave %v, want ErrRecordTooLarge",
			uint64(math.MaxUint64), err)
	}
}

func TestRecordTypesAreDistinct(t *testing.T) {
	// The type tag is what recovery dispatches on. Two records sharing a tag
	// would have their payloads decoded by the wrong function.
	seen := map[recordType]string{}
	for _, tc := range []struct {
		typ  recordType
		name string
	}{
		{recordEntry, "entry"},
		{recordHardState, "hard state"},
		{recordSnapshotMeta, "snapshot meta"},
	} {
		if prev, dup := seen[tc.typ]; dup {
			t.Fatalf("record types %q and %q share the tag %d", prev, tc.name, tc.typ)
		}
		seen[tc.typ] = tc.name
	}
}
