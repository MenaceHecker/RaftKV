package statemachine

import (
	"runtime"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

func FuzzDecodeCommand(f *testing.F) {
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
		again := cmd.Encode()
		if string(again) != string(data) {
			t.Fatalf("command round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

func FuzzRestore(f *testing.F) {
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
		before := Command{ClientID: 9, Seq: 1, Op: OpPut, Key: "keep", Value: []byte("me")}
		if err := target.Apply(raft.Entry{Index: 1, Term: 1, Data: before.Encode()}); err != nil {
			t.Fatalf("seeding: %v", err)
		}

		if err := target.Restore(data); err != nil {
			got, ok := target.Get("keep")
			if !ok || string(got) != "me" {
				t.Fatalf("a rejected snapshot damaged the existing state: got %q, present=%v",
					got, ok)
			}
			return
		}

		again, err := target.Snapshot()
		if err != nil {
			t.Fatalf("re-snapshotting a restored store: %v", err)
		}
		if string(again) != string(data) {
			t.Fatalf("snapshot round-tripped to different bytes:\n in: %x\nout: %x", data, again)
		}
	})
}

func TestDecodeCommandRejectsTrailingBytes(t *testing.T) {
	good := Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "k", Value: []byte("v")}.Encode()
	if _, err := DecodeCommand(good); err != nil {
		t.Fatalf("a well-formed command was rejected: %v", err)
	}
	if _, err := DecodeCommand(append(good, 0x00)); err == nil {
		t.Error("a command with a trailing byte was accepted")
	}
}

func TestRestoreRejectsUnsortedKeys(t *testing.T) {
	buf := appendUint64(nil, 0)
	buf = appendUint64(buf, 2)
	buf = appendBytes(buf, []byte("b"))
	buf = appendBytes(buf, []byte("1"))
	buf = appendBytes(buf, []byte("a"))
	buf = appendBytes(buf, []byte("2"))
	buf = (&sessions{entries: map[uint64]*session{}}).encode(buf)

	if err := New().Restore(buf); err == nil {
		t.Error("a snapshot with descending keys was accepted")
	}
}

func TestRestoreRejectsRepeatedKeys(t *testing.T) {
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
	buf := appendUint64(nil, 0)
	buf = appendUint64(buf, 0)
	buf = appendUint64(buf, 1<<20)
	if err := New().Restore(buf); err == nil {
		t.Error("a snapshot declaring a million sessions in eight bytes was accepted")
	}
}
