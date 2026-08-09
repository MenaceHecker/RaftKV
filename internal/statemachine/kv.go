// Package statemachine implements the replicated state machine that sits on
// top of the Raft log: an in-memory key-value store rebuilt by applying
// committed entries in order.
//
// The one property everything here is built around is determinism. Every node
// applies the same entries in the same order, so every node must arrive at
// byte-identical state — otherwise the cluster agrees on the log but disagrees
// on what the log means, which is a worse failure than not agreeing at all
// because nothing detects it.
package statemachine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// Op is the kind of mutation a command performs.
//
// Reads are absent on purpose. A linearizable Get is served by confirming
// leadership and reading local state, not by appending to the log — putting
// reads in the log would make every read a full round of replication for no
// gain in correctness.
type Op uint8

const (
	// OpPut sets a key to a value, creating it if absent.
	OpPut Op = 1
	// OpDelete removes a key. Deleting a key that does not exist is not an
	// error: commands must be applicable on every replica regardless of what
	// that replica happens to hold, and a delete that failed on some nodes
	// and succeeded on others would diverge the cluster.
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
	// ErrMalformedCommand means an entry's payload could not be decoded. On a
	// follower this means the log itself is damaged, since the leader only
	// ever replicates commands it encoded.
	ErrMalformedCommand = errors.New("statemachine: malformed command")

	// ErrOutOfOrder means an entry arrived that does not follow the last one
	// applied. The Raft core delivers committed entries in index order, so a
	// gap is a bug in the caller rather than a recoverable condition.
	ErrOutOfOrder = errors.New("statemachine: entry is out of order")

	// ErrMalformedSnapshot means a snapshot could not be decoded.
	ErrMalformedSnapshot = errors.New("statemachine: malformed snapshot")
)

// maxFieldSize bounds any length prefix read from an encoded command or
// snapshot, so a corrupt length cannot drive an unbounded allocation before
// the rest of the decode discovers something is wrong.
const maxFieldSize = 64 << 20 // 64 MiB

// Command is a single mutation of the store, the thing carried in an entry's
// opaque Data field.
type Command struct {
	Op    Op
	Key   string
	Value []byte // ignored for OpDelete
}

// Encode serializes a command for the log.
//
// The layout is fixed and hand-rolled rather than reflective, for the same
// reason the log records are: an encoding whose bytes are decided by struct
// field order or map iteration would make the same logical command serialize
// differently on different nodes or Go versions, and the whole point of the
// state machine is that every replica does exactly the same thing.
func (c Command) Encode() []byte {
	buf := make([]byte, 0, 1+8+len(c.Key)+8+len(c.Value))
	buf = append(buf, byte(c.Op))
	buf = appendBytes(buf, []byte(c.Key))
	buf = appendBytes(buf, c.Value)
	return buf
}

// DecodeCommand deserializes a command from an entry's payload.
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

	key, err := r.bytes()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading key: %w", ErrMalformedCommand, err)
	}
	value, err := r.bytes()
	if err != nil {
		return Command{}, fmt.Errorf("%w: reading value: %w", ErrMalformedCommand, err)
	}

	return Command{Op: op, Key: string(key), Value: value}, nil
}

// KV is the replicated key-value store.
//
// It is safe for concurrent use: Raft applies entries from one goroutine while
// clients read from others.
type KV struct {
	mu   sync.RWMutex
	data map[string][]byte

	// applied is the index of the last entry incorporated into this state.
	// It travels with the snapshot, so a restored replica knows where in the
	// log to resume.
	applied raft.Index
}

// New returns an empty store, as a node with no snapshot and no log starts.
func New() *KV {
	return &KV{data: make(map[string][]byte)}
}

// Apply incorporates one committed entry.
//
// Entries must arrive in index order. An entry at or below the last applied
// index is ignored rather than rejected: after a crash the node replays from
// the log, and re-delivering entries the snapshot already covers is the normal
// path, not an error.
func (kv *KV) Apply(e raft.Entry) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if e.Index <= kv.applied {
		// Already reflected in this state. Applying it again would be
		// harmless for Put and Delete, which are idempotent, but silently
		// double-applying is a habit that stops being safe the moment a
		// non-idempotent command type is added.
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
		kv.applyCommand(cmd)

	case raft.EntryNoOp, raft.EntryConfChange:
		// Not state machine commands. They still occupy an index, so the
		// applied cursor must advance past them or the log and the state
		// machine drift apart by one for every leader election.

	default:
		return fmt.Errorf("applying entry %d: %w: unknown entry type %d",
			e.Index, ErrMalformedCommand, e.Type)
	}

	kv.applied = e.Index
	return nil
}

// applyCommand performs the mutation. The caller must hold the write lock.
func (kv *KV) applyCommand(cmd Command) {
	switch cmd.Op {
	case OpPut:
		// Copy the value: it came from a decoded log entry whose buffer the
		// caller may reuse, and the store must own what it holds.
		v := make([]byte, len(cmd.Value))
		copy(v, cmd.Value)
		kv.data[cmd.Key] = v

	case OpDelete:
		delete(kv.data, cmd.Key)
	}
}

// Get returns the value for a key.
//
// This reads local state, which is only linearizable once the caller has
// confirmed the node is still leader. That confirmation belongs to the layer
// above; the store deliberately does not pretend to provide it.
func (kv *KV) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	v, ok := kv.data[key]
	if !ok {
		return nil, false
	}

	// Copy on the way out too, so a caller cannot mutate committed state by
	// holding on to the returned slice.
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Applied returns the index of the last entry applied.
func (kv *KV) Applied() raft.Index {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.applied
}

// Len returns the number of keys held.
func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// Keys returns every key, sorted. Intended for tests and debugging.
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

// Snapshot serializes the entire store.
//
// Keys are written in sorted order, which matters more than it looks. Go
// randomizes map iteration, so an unsorted encoding would produce different
// bytes on every call and on every node for identical state. Sorting makes a
// snapshot a deterministic function of the state, which in turn makes two
// replicas' snapshots directly comparable — the cheapest possible check that
// they really did converge, and the basis for verifying it in tests.
func (kv *KV) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	keys := kv.sortedKeysLocked()

	buf := make([]byte, 0, 16+len(keys)*32)
	buf = appendUint64(buf, uint64(kv.applied))
	buf = appendUint64(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendBytes(buf, []byte(k))
		buf = appendBytes(buf, kv.data[k])
	}
	return buf, nil
}

// Restore replaces the store's contents with a snapshot.
//
// It is all-or-nothing: the new state is built separately and swapped in only
// once it has decoded cleanly, so a corrupt snapshot leaves the existing state
// untouched rather than half-overwritten.
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

	data := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		key, err := r.bytes()
		if err != nil {
			return fmt.Errorf("%w: reading key %d: %w", ErrMalformedSnapshot, i, err)
		}
		value, err := r.bytes()
		if err != nil {
			return fmt.Errorf("%w: reading value for key %q: %w", ErrMalformedSnapshot, key, err)
		}
		data[string(key)] = value
	}

	if r.pos != len(r.b) {
		return fmt.Errorf("%w: %d trailing bytes after %d keys",
			ErrMalformedSnapshot, len(r.b)-r.pos, count)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = data
	kv.applied = raft.Index(applied)
	return nil
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

// reader walks an encoded buffer, reporting a shortfall rather than panicking
// on a slice bound.
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
