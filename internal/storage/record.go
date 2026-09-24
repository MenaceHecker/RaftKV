package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

const (
	lenSize    = 4
	crcSize    = 4
	typeSize   = 1
	headerSize = lenSize + crcSize

	maxRecordSize = 64 << 20
)

type recordType uint8

const (
	recordEntry        recordType = 1
	recordHardState    recordType = 2
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
	ErrTornRecord = errors.New("storage: record is incomplete")

	ErrCorruptRecord = errors.New("storage: record failed its checksum")

	ErrRecordTooLarge = errors.New("storage: record length exceeds the maximum")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

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

func readRecord(b []byte) (recordType, []byte, int, error) {
	if len(b) < headerSize {
		return 0, nil, 0, ErrTornRecord
	}

	bodyLen := int(binary.LittleEndian.Uint32(b[0:lenSize]))
	want := binary.LittleEndian.Uint32(b[lenSize:headerSize])

	if bodyLen < typeSize {
		return 0, nil, 0, ErrCorruptRecord
	}
	if bodyLen > maxRecordSize {
		return 0, nil, 0, ErrRecordTooLarge
	}

	total := headerSize + bodyLen
	if len(b) < total {
		return 0, nil, 0, ErrTornRecord
	}

	body := b[headerSize:total]
	if got := crc32.Checksum(body, crcTable); got != want {
		return 0, nil, 0, fmt.Errorf("%w: computed %08x, expected %08x", ErrCorruptRecord, got, want)
	}

	return recordType(body[0]), body[typeSize:], total, nil
}

func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = appendUint64(dst, uint64(len(b)))
	return append(dst, b...)
}

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

func (r *reader) atEnd(what string) error {
	if r.pos != len(r.b) {
		return fmt.Errorf("%w: %d trailing bytes after the %s",
			ErrCorruptRecord, len(r.b)-r.pos, what)
	}
	return nil
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
	out := make([]byte, n)
	copy(out, r.b[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

func encodeEntry(dst []byte, e raft.Entry) []byte {
	dst = appendUint64(dst, uint64(e.Term))
	dst = appendUint64(dst, uint64(e.Index))
	dst = appendUint64(dst, uint64(e.Type))
	return appendBytes(dst, e.Data)
}

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
	if err := r.atEnd("entry"); err != nil {
		return raft.Entry{}, err
	}

	if typ > math.MaxUint8 || !raft.EntryType(typ).Valid() {
		return raft.Entry{}, fmt.Errorf("%w: entry type %d is not a known type",
			ErrCorruptRecord, typ)
	}

	return raft.Entry{
		Term:  raft.Term(term),
		Index: raft.Index(index),
		Type:  raft.EntryType(typ),
		Data:  data,
	}, nil
}

func encodeHardState(dst []byte, hs raft.HardState) []byte {
	dst = appendUint64(dst, uint64(hs.Term))
	return appendUint64(dst, uint64(hs.VotedFor))
}

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
	if err := r.atEnd("hard state"); err != nil {
		return raft.HardState{}, err
	}

	return raft.HardState{
		Term:     raft.Term(term),
		VotedFor: raft.NodeID(vote),
	}, nil
}

type SnapshotMeta struct {
	Index raft.Index
	Term  raft.Term
}

func encodeSnapshotMeta(dst []byte, m SnapshotMeta) []byte {
	dst = appendUint64(dst, uint64(m.Index))
	return appendUint64(dst, uint64(m.Term))
}

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
	if err := r.atEnd("snapshot meta"); err != nil {
		return SnapshotMeta{}, err
	}

	return SnapshotMeta{Index: raft.Index(index), Term: raft.Term(term)}, nil
}
