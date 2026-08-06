// Package storage provides the durable backing for the Raft log: a
// write-ahead log, snapshots, and the recovery path that reconstructs a node's
// state after a crash.
//
// It implements the raft.Storage interface, so the consensus core is unaware
// of whether it is running against memory or disk.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// The on-disk record format.
//
// Every durable file in this package — the WAL and the snapshot — is a
// sequence of self-describing records:
//
//	 0      4      8      9              9+len
//	+------+------+------+----------------+
//	| len  | crc  | type |    payload     |
//	+------+------+------+----------------+
//	  u32    u32    u8      len-1 bytes
//
// len covers the type byte and the payload, so a reader knows how far to skip
// before validating anything. crc is computed over that same range.
//
// The format is designed around one question: what does a reader do with the
// tail of a file that was being written when the process was killed? Both
// failure modes are detectable. A record whose declared length runs past the
// end of the file was never finished, and a record whose CRC does not match
// was written partially or scrambled. In both cases recovery truncates at that
// point and keeps everything before it, which is sound because the log is
// append-only — a torn record is always the last one.
const (
	lenSize    = 4
	crcSize    = 4
	typeSize   = 1
	headerSize = lenSize + crcSize

	// maxRecordSize bounds how much a single record may claim. Without it, a
	// corrupt length field would make the reader allocate arbitrarily much
	// memory before discovering the CRC does not match.
	maxRecordSize = 64 << 20 // 64 MiB
)

// recordType tags what a record's payload holds.
type recordType uint8

const (
	// recordEntry carries one raft.Entry.
	recordEntry recordType = 1
	// recordHardState carries a raft.HardState — the term and vote that must
	// survive a crash.
	recordHardState recordType = 2
	// recordSnapshotMeta carries the index and term a snapshot was taken at.
	recordSnapshotMeta recordType = 3
)

func (t recordType) String() string {
	switch t {
	case recordEntry:
		return "Entry"
	case recordHardState:
		return "HardState"
	case recordSnapshotMeta:
		return "SnapshotMeta"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(t))
	}
}

var (
	// ErrTornRecord means a record was not fully written, which is the
	// expected result of a crash mid-append. Recovery truncates the file
	// here rather than treating it as corruption.
	ErrTornRecord = errors.New("storage: record is incomplete")

	// ErrCorruptRecord means a record's CRC does not match its contents. The
	// bytes reached disk but are not what was written.
	ErrCorruptRecord = errors.New("storage: record failed its checksum")

	// ErrRecordTooLarge means a record declares a length beyond the sane
	// bound, which in practice means the length field itself is garbage.
	ErrRecordTooLarge = errors.New("storage: record length exceeds the maximum")
)

// crcTable uses the Castagnoli polynomial, which has hardware support on the
// architectures this runs on, so checksumming does not dominate append cost.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// appendRecord frames payload as a record of type t and appends it to dst,
// returning the extended slice. Building into a caller-supplied buffer lets
// the WAL batch several records into one write.
func appendRecord(dst []byte, t recordType, payload []byte) []byte {
	body := make([]byte, 0, typeSize+len(payload))
	body = append(body, byte(t))
	body = append(body, payload...)

	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:lenSize], uint32(len(body)))
	binary.LittleEndian.PutUint32(header[lenSize:headerSize], crc32.Checksum(body, crcTable))

	dst = append(dst, header[:]...)
	return append(dst, body...)
}

// readRecord decodes the record at the front of b. It returns the record's
// type, its payload, and the total number of bytes consumed, so the caller can
// walk a file by repeatedly re-slicing.
//
// The payload aliases b; callers that retain it past the life of the buffer
// must copy.
func readRecord(b []byte) (recordType, []byte, int, error) {
	if len(b) < headerSize {
		return 0, nil, 0, ErrTornRecord
	}

	bodyLen := int(binary.LittleEndian.Uint32(b[0:lenSize]))
	want := binary.LittleEndian.Uint32(b[lenSize:headerSize])

	if bodyLen < typeSize {
		// Even an empty payload has a type byte, so this length is garbage.
		return 0, nil, 0, ErrCorruptRecord
	}
	if bodyLen > maxRecordSize {
		return 0, nil, 0, ErrRecordTooLarge
	}

	total := headerSize + bodyLen
	if len(b) < total {
		// The record was still being written when the process died.
		return 0, nil, 0, ErrTornRecord
	}

	body := b[headerSize:total]
	if got := crc32.Checksum(body, crcTable); got != want {
		return 0, nil, 0, fmt.Errorf("%w: computed %08x, expected %08x", ErrCorruptRecord, got, want)
	}

	return recordType(body[0]), body[typeSize:], total, nil
}

// appendUint64 writes v in little-endian order.
func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

// appendBytes writes a length-prefixed byte slice.
func appendBytes(dst []byte, b []byte) []byte {
	dst = appendUint64(dst, uint64(len(b)))
	return append(dst, b...)
}

// reader walks a decoded payload, tracking position so each field decoder can
// report a short buffer rather than panicking on a slice bound.
type reader struct {
	b   []byte
	pos int
}

func (r *reader) uint64() (uint64, error) {
	if len(r.b)-r.pos < 8 {
		return 0, fmt.Errorf("%w: wanted 8 bytes at offset %d, have %d",
			ErrCorruptRecord, r.pos, len(r.b)-r.pos)
	}
	v := binary.LittleEndian.Uint64(r.b[r.pos : r.pos+8])
	r.pos += 8
	return v, nil
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.uint64()
	if err != nil {
		return nil, err
	}
	if n > maxRecordSize {
		return nil, ErrRecordTooLarge
	}
	if uint64(len(r.b)-r.pos) < n {
		return nil, fmt.Errorf("%w: wanted %d bytes at offset %d, have %d",
			ErrCorruptRecord, n, r.pos, len(r.b)-r.pos)
	}
	// Copy, because the payload aliases the file buffer and entries outlive
	// the read that produced them.
	out := make([]byte, n)
	copy(out, r.b[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

// encodeEntry serializes a log entry.
//
// The encoding is hand-rolled rather than reflective (gob) or generated
// (protobuf) for two reasons: the Raft log is the part of this system whose
// on-disk representation should be fully understood rather than delegated, and
// a fixed layout makes a corrupt record diagnosable by reading hex.
func encodeEntry(dst []byte, e raft.Entry) []byte {
	dst = appendUint64(dst, uint64(e.Term))
	dst = appendUint64(dst, uint64(e.Index))
	dst = appendUint64(dst, uint64(e.Type))
	return appendBytes(dst, e.Data)
}

// decodeEntry deserializes a log entry.
func decodeEntry(payload []byte) (raft.Entry, error) {
	r := &reader{b: payload}

	term, err := r.uint64()
	if err != nil {
		return raft.Entry{}, fmt.Errorf("decoding entry term: %w", err)
	}
	index, err := r.uint64()
	if err != nil {
		return raft.Entry{}, fmt.Errorf("decoding entry index: %w", err)
	}
	typ, err := r.uint64()
	if err != nil {
		return raft.Entry{}, fmt.Errorf("decoding entry type: %w", err)
	}
	data, err := r.bytes()
	if err != nil {
		return raft.Entry{}, fmt.Errorf("decoding entry data: %w", err)
	}

	return raft.Entry{
		Term:  raft.Term(term),
		Index: raft.Index(index),
		Type:  raft.EntryType(typ),
		Data:  data,
	}, nil
}

// encodeHardState serializes the term and vote.
func encodeHardState(dst []byte, hs raft.HardState) []byte {
	dst = appendUint64(dst, uint64(hs.Term))
	return appendUint64(dst, uint64(hs.VotedFor))
}

// decodeHardState deserializes the term and vote.
func decodeHardState(payload []byte) (raft.HardState, error) {
	r := &reader{b: payload}

	term, err := r.uint64()
	if err != nil {
		return raft.HardState{}, fmt.Errorf("decoding hard state term: %w", err)
	}
	vote, err := r.uint64()
	if err != nil {
		return raft.HardState{}, fmt.Errorf("decoding hard state vote: %w", err)
	}

	return raft.HardState{
		Term:     raft.Term(term),
		VotedFor: raft.NodeID(vote),
	}, nil
}

// SnapshotMeta identifies the point in the log a snapshot was taken at. The
// index and term are what let a restarted node splice the snapshot together
// with the log entries that follow it, and what a leader sends a follower that
// has fallen too far behind to catch up from the log alone.
type SnapshotMeta struct {
	// Index is the last log index included in the snapshot.
	Index raft.Index
	// Term is the term of the entry at Index.
	Term raft.Term
}

// encodeSnapshotMeta serializes snapshot metadata.
func encodeSnapshotMeta(dst []byte, m SnapshotMeta) []byte {
	dst = appendUint64(dst, uint64(m.Index))
	return appendUint64(dst, uint64(m.Term))
}

// decodeSnapshotMeta deserializes snapshot metadata.
func decodeSnapshotMeta(payload []byte) (SnapshotMeta, error) {
	r := &reader{b: payload}

	index, err := r.uint64()
	if err != nil {
		return SnapshotMeta{}, fmt.Errorf("decoding snapshot index: %w", err)
	}
	term, err := r.uint64()
	if err != nil {
		return SnapshotMeta{}, fmt.Errorf("decoding snapshot term: %w", err)
	}

	return SnapshotMeta{Index: raft.Index(index), Term: raft.Term(term)}, nil
}
