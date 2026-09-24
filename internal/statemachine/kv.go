package statemachine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(o))
	}
}

var (
	ErrMalformedCommand = errors.New("statemachine: malformed command")

	ErrOutOfOrder = errors.New("statemachine: entry is out of order")

	ErrMalformedSnapshot = errors.New("statemachine: malformed snapshot")
)

const maxFieldSize = 64 << 20

type Command struct {
	ClientID uint64
	Seq      uint64

	Op    Op
	Key   string
	Value []byte
}

func (c Command) Encode() []byte {
	buf := make([]byte, 0, 1+16+8+len(c.Key)+8+len(c.Value))
	buf = append(buf, byte(c.Op))
	buf = appendUint64(buf, c.ClientID)
	buf = appendUint64(buf, c.Seq)
	buf = appendBytes(buf, []byte(c.Key))
	buf = appendBytes(buf, c.Value)
	return buf
}

func DecodeCommand(b []byte) (Command, error) {
	if len(b) == 0 {
		return Command{}, fmt.Errorf("%w: empty payload", ErrMalformedCommand)
	}

	op := Op(b[0])
	switch op {
	case OpPut, OpDelete:
	default:
		return Command{}, fmt.Errorf("%w: unknown operation %d", ErrMalformedCommand, b[0])
	}

	r := &reader{b: b, pos: 1}

	clientID, err := r.uint64()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading client ID: %w", ErrMalformedCommand, err)
	}
	seq, err := r.uint64()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading sequence: %w", ErrMalformedCommand, err)
	}
	key, err := r.bytes()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading key: %w", ErrMalformedCommand, err)
	}
	value, err := r.bytes()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading value: %w", ErrMalformedCommand, err)
	}

	if r.pos != len(r.b) {
		return Command{}, fmt.Errorf("%w: %d trailing bytes after the value",
			ErrMalformedCommand, len(r.b)-r.pos)
	}

	return Command{
		ClientID: clientID,
		Seq:      seq,
		Op:       op,
		Key:      string(key),
		Value:    value,
	}, nil
}

type KV struct {
	mu   sync.RWMutex
	data map[string][]byte

	sessions *sessions

	applied raft.Index
}

func New() *KV {
	return NewWithMaxSessions(DefaultMaxSessions)
}

func NewWithMaxSessions(max int) *KV {
	return &KV{
		data:     make(map[string][]byte),
		sessions: newSessions(max),
	}
}

func (kv *KV) Apply(e raft.Entry) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if e.Index <= kv.applied {
		return nil
	}
	if e.Index != kv.applied+1 {
		return fmt.Errorf("%w: entry %d follows %d", ErrOutOfOrder, e.Index, kv.applied)
	}

	switch e.Type {
	case raft.EntryNormal:
		cmd, err := DecodeCommand(e.Data)
		if err != nil {
			return fmt.Errorf("applying entry %d: %w", e.Index, err)
		}
		if kv.sessions.shouldApply(cmd.ClientID, cmd.Seq, e.Index) {
			kv.applyCommand(cmd)
		}

	case raft.EntryNoOp, raft.EntryConfChange:

	default:
		return fmt.Errorf("applying entry %d: %w: unknown entry type %d",
			e.Index, ErrMalformedCommand, e.Type)
	}

	kv.applied = e.Index
	return nil
}

func (kv *KV) applyCommand(cmd Command) {
	switch cmd.Op {
	case OpPut:
		v := make([]byte, len(cmd.Value))
		copy(v, cmd.Value)
		kv.data[cmd.Key] = v

	case OpDelete:
		delete(kv.data, cmd.Key)
	}
}

func (kv *KV) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	v, ok := kv.data[key]
	if !ok {
		return nil, false
	}

	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (kv *KV) LastSeq(clientID uint64) (uint64, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.sessions.lastSeq(clientID)
}

func (kv *KV) Sessions() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.sessions.len()
}

func (kv *KV) Applied() raft.Index {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.applied
}

func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

func (kv *KV) Keys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.sortedKeysLocked()
}

func (kv *KV) sortedKeysLocked() []string {
	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (kv *KV) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	keys := kv.sortedKeysLocked()

	buf := make([]byte, 0, kv.snapshotSizeLocked(keys))
	buf = appendUint64(buf, uint64(kv.applied))
	buf = appendUint64(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendBytes(buf, []byte(k))
		buf = appendBytes(buf, kv.data[k])
	}

	buf = kv.sessions.encode(buf)
	return buf, nil
}

func (kv *KV) snapshotSizeLocked(keys []string) int {
	size := 8 + 8
	for _, k := range keys {
		size += 8 + len(k) + 8 + len(kv.data[k])
	}
	size += 8 + len(kv.sessions.entries)*24
	return size
}

func (kv *KV) Restore(b []byte) error {
	r := &reader{b: b}

	applied, err := r.uint64()
	if err != nil {
		return fmt.Errorf("%w: reading applied index: %w", ErrMalformedSnapshot, err)
	}
	count, err := r.uint64()
	if err != nil {
		return fmt.Errorf("%w: reading key count: %w", ErrMalformedSnapshot, err)
	}
	if count > maxFieldSize {
		return fmt.Errorf("%w: implausible key count %d", ErrMalformedSnapshot, count)
	}

	const minBytesPerPair = 16
	if remaining := uint64(len(r.b) - r.pos); count > remaining/minBytesPerPair {
		return fmt.Errorf("%w: %d keys declared but only %d bytes remain",
			ErrMalformedSnapshot, count, remaining)
	}

	data := make(map[string][]byte, count)
	var previous string
	for i := uint64(0); i < count; i++ {
		key, err := r.bytes()
		if err != nil {
			return fmt.Errorf("%w: reading key %d: %w", ErrMalformedSnapshot, i, err)
		}
		value, err := r.bytes()
		if err != nil {
			return fmt.Errorf("%w: reading value for key %q: %w", ErrMalformedSnapshot, key, err)
		}

		if i > 0 && string(key) <= previous {
			return fmt.Errorf("%w: key %q follows %q, but keys must ascend",
				ErrMalformedSnapshot, key, previous)
		}
		previous = string(key)

		data[string(key)] = value
	}

	restored, err := decodeSessions(r, kv.sessions.max)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMalformedSnapshot, err)
	}

	if r.pos != len(r.b) {
		return fmt.Errorf("%w: %d trailing bytes after %d keys",
			ErrMalformedSnapshot, len(r.b)-r.pos, count)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = data
	kv.applied = raft.Index(applied)
	kv.sessions = restored
	return nil
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
		return 0, fmt.Errorf("wanted 8 bytes at offset %d, have %d", r.pos, len(r.b)-r.pos)
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
	if n > maxFieldSize {
		return nil, fmt.Errorf("length %d exceeds the %d-byte maximum", n, maxFieldSize)
	}
	if uint64(len(r.b)-r.pos) < n {
		return nil, fmt.Errorf("wanted %d bytes at offset %d, have %d", n, r.pos, len(r.b)-r.pos)
	}
	out := make([]byte, n)
	copy(out, r.b[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}
