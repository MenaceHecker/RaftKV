package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// oracleCRC is the oracle's own checksum table, so the oracle does not share
// crcTable with the code under test.
var oracleCRC = crc32.MakeTable(crc32.Castagnoli)

// oracleFrame is an independent frame reader: it does not call readRecord.
func oracleFrame(data []byte, off int) (typ byte, payload []byte, n int, ok bool) {
	if len(data)-off < 8 {
		return 0, nil, 0, false
	}
	l := int(binary.LittleEndian.Uint32(data[off:]))
	want := binary.LittleEndian.Uint32(data[off+4:])
	if l < 1 || l > 64<<20 || len(data)-off-8 < l {
		return 0, nil, 0, false
	}
	body := data[off+8 : off+8+l]
	if crc32.Checksum(body, oracleCRC) != want {
		return 0, nil, 0, false
	}
	return body[0], body[1:], 8 + l, true
}

// tailModel is the reference model of what replaying a sequence of valid
// records produces.
type tailModel struct {
	entries []raft.Entry
	hs      raft.HardState
	snap    SnapshotMeta
}

// apply applies one CRC-valid record with its real effect. It is the oracle's
// one table of known record types: a new type is added here and nowhere else
// in this file.
func (m *tailModel) apply(typ byte, payload []byte) error {
	switch recordType(typ) {
	case recordEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		// An entry at or below the current last index cuts the log first.
		cut := len(m.entries)
		for i, have := range m.entries {
			if have.Index >= e.Index {
				cut = i
				break
			}
		}
		m.entries = append(m.entries[:cut:cut], e)
	case recordHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return err
		}
		m.hs = hs
	case recordSnapshotMeta:
		sm, err := decodeSnapshotMeta(payload)
		if err != nil {
			return err
		}
		m.snap = sm
	default:
		return errors.New("unknown record type")
	}
	return nil
}

// noSyncFS is the real filesystem with fsync turned into a no-op. The fuzz
// target checks replay logic, not durability (the power-loss cases are T3.8),
// and a real fsync per input would cut the exec rate to a few per second.
type noSyncFS struct{ FS }

type noSyncFile struct{ File }

func (noSyncFile) Sync() error { return nil }

func (n noSyncFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := n.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return noSyncFile{f}, nil
}

func (noSyncFS) SyncDir(string) error { return nil }

func tailPrefix() []byte {
	var b []byte
	b = appendRecord(b, recordEntry, encodeEntry(nil, raft.Entry{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("a")}))
	b = appendRecord(b, recordHardState, encodeHardState(nil, raft.HardState{Term: 1, VotedFor: 2}))
	b = appendRecord(b, recordEntry, encodeEntry(nil, raft.Entry{Term: 1, Index: 2, Type: raft.EntryNormal, Data: []byte("b")}))
	return b
}

func tailEntryRecord(term raft.Term, index raft.Index) []byte {
	return appendRecord(nil, recordEntry, encodeEntry(nil, raft.Entry{Term: term, Index: index, Type: raft.EntryNormal, Data: []byte("tail-data")}))
}

func sameEntries(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || a[i].Index != b[i].Index || a[i].Type != b[i].Type ||
			!bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

func FuzzWALReplayTail(f *testing.F) {
	rec := tailEntryRecord(1, 3)

	f.Add([]byte{})
	f.Add(rec)
	for i := range rec {
		flipped := bytes.Clone(rec)
		flipped[i] ^= 0xff
		f.Add(flipped)
	}
	for n := 0; n <= len(rec); n++ {
		f.Add(bytes.Clone(rec[:n]))
	}
	for _, n := range []int{1, 8, len(rec)} {
		zeroed := bytes.Clone(rec)
		clear(zeroed[len(zeroed)-n:])
		f.Add(zeroed)
	}
	oversize := bytes.Clone(rec)
	binary.LittleEndian.PutUint32(oversize, 0xFFFFFFFF)
	f.Add(oversize)

	// A damaged record followed by an intact one: the oracle expects refusal.
	damaged := bytes.Clone(rec)
	damaged[headerSize+typeSize+2] ^= 0xff
	f.Add(append(bytes.Clone(damaged), tailEntryRecord(1, 4)...))
	grown := bytes.Clone(rec)
	binary.LittleEndian.PutUint32(grown, binary.LittleEndian.Uint32(grown)+1000)
	f.Add(append(grown, tailEntryRecord(1, 4)...))

	// Several intact records, a conflicting one, and CRC-valid undecodable ones.
	f.Add(append(bytes.Clone(rec), tailEntryRecord(1, 4)...))
	f.Add(append(bytes.Clone(rec), tailEntryRecord(2, 2)...))
	f.Add(appendRecord(nil, recordType(99), []byte("x")))
	f.Add(appendRecord(nil, recordEntry, []byte{1, 2, 3}))
	f.Add(append(appendRecord(nil, recordEntry, []byte{1, 2, 3}), rec...))

	f.Fuzz(func(t *testing.T, tail []byte) {
		data := append(tailPrefix(), tail...)

		// The oracle: walk records in file order until one fails to frame or
		// to decode.
		var want tailModel
		refuse := false
		good := 0
		for good < len(data) {
			typ, payload, n, ok := oracleFrame(data, good)
			if !ok {
				break
			}
			if err := want.apply(typ, payload); err != nil {
				refuse = true
				break
			}
			good += n
		}
		torn := !refuse && good < len(data)
		if torn {
			for p := good + 1; p < len(data) && !refuse; p++ {
				typ, payload, _, ok := oracleFrame(data, p)
				if !ok {
					continue
				}
				var scratch tailModel
				if scratch.apply(typ, payload) == nil {
					refuse = true
				}
			}
		}

		dir := t.TempDir()
		path := filepath.Join(dir, segmentName(1, 1))
		if err := os.WriteFile(path, data, fileMode); err != nil {
			t.Fatalf("writing segment: %v", err)
		}

		w, rep, err := Open(Options{Dir: dir, FS: noSyncFS{osFS{}}})
		if refuse {
			if err == nil {
				_ = w.Close()
				t.Fatalf("opened a log the oracle says is corrupt (tail %x)", tail)
			}
			if !errors.Is(err, ErrCorruptWAL) {
				t.Fatalf("open error = %v, want ErrCorruptWAL (tail %x)", err, tail)
			}
			if after, rerr := os.ReadFile(path); rerr != nil || !bytes.Equal(after, data) { //nolint:gosec // path under t.TempDir()
				t.Fatalf("a refused open changed the segment (tail %x)", tail)
			}
			return
		}
		if err != nil {
			t.Fatalf("open failed: %v (tail %x)", err, tail)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}

		if rep.Repaired != torn {
			t.Fatalf("Repaired = %v, want %v (tail %x)", rep.Repaired, torn, tail)
		}
		if !sameEntries(rep.Entries, want.entries) || rep.HardState != want.hs || rep.Snapshot != want.snap {
			t.Fatalf("replayed state differs from the model (tail %x):\n got entries %v hs %v snap %v\nwant entries %v hs %v snap %v",
				tail, rep.Entries, rep.HardState, rep.Snapshot, want.entries, want.hs, want.snap)
		}
		onDisk, rerr := os.ReadFile(path) //nolint:gosec // path under t.TempDir()
		if rerr != nil {
			t.Fatalf("reading segment: %v", rerr)
		}
		if !bytes.Equal(onDisk, data[:good]) {
			t.Fatalf("segment is %d bytes after open, want the first %d (tail %x)", len(onDisk), good, tail)
		}

		// Reopening must be idempotent.
		w2, rep2, err := Open(Options{Dir: dir, FS: noSyncFS{osFS{}}})
		if err != nil {
			t.Fatalf("reopen failed: %v (tail %x)", err, tail)
		}
		if err := w2.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}
		if rep2.Repaired {
			t.Fatalf("reopen reported a repair (tail %x)", tail)
		}
		if !sameEntries(rep2.Entries, want.entries) || rep2.HardState != want.hs || rep2.Snapshot != want.snap {
			t.Fatalf("reopen returned different state (tail %x)", tail)
		}
		again, rerr := os.ReadFile(path) //nolint:gosec // path under t.TempDir()
		if rerr != nil || !bytes.Equal(again, onDisk) {
			t.Fatalf("reopen changed the segment (tail %x)", tail)
		}
	})
}
