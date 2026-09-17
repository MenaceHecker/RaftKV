package statemachine

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// fill puts n values of the given size into a store.
func fill(t testing.TB, kv *KV, n, valueSize int) {
	t.Helper()
	value := make([]byte, valueSize)
	for i := range n {
		cmd := Command{ClientID: uint64(i + 1), Seq: 1, Op: OpPut,
			Key: fmt.Sprintf("key-%08d", i), Value: value}
		if err := kv.Apply(raft.Entry{Index: raft.Index(i + 1), Term: 1, Data: cmd.Encode()}); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
}

func TestSnapshotBufferIsSizedExactly(t *testing.T) {
	// The buffer is allocated once, from a computed size, and never grown.
	// Comparing capacity to length is the cheapest way to say that: if the
	// computation were low the buffer would have been reallocated and its
	// capacity would exceed its length, and if it were high the excess is
	// memory reserved at exactly the wrong moment.
	for _, tc := range []struct {
		name      string
		keys      int
		valueSize int
	}{
		{"empty", 0, 0},
		{"tiny values", 64, 8},
		{"large values", 512, 4 << 10},
		{"mixed with sessions", 200, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := New()
			fill(t, kv, tc.keys, tc.valueSize)

			snap, err := kv.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if cap(snap) != len(snap) {
				t.Errorf("snapshot is %d bytes in a buffer of %d; the size computation is %s",
					len(snap), cap(snap),
					map[bool]string{true: "too small, so the buffer was regrown"}[cap(snap) > len(snap)])
			}
		})
	}
}

func TestSnapshotDoesNotAllocateAMultipleOfItsOutput(t *testing.T) {
	// The guard against the regrowth this replaced. A store of sixteen
	// thousand four kilobyte values used to allocate 353 MB to produce 64 MB,
	// because the buffer was sized from a guess of thirty-two bytes per pair
	// and doubled seven times.
	kv := New()
	fill(t, kv, 16<<10, 4<<10)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc

	// Producing n bytes should cost about n. Anything approaching a multiple
	// of the output means the buffer is being grown rather than sized.
	if limit := uint64(len(snap)) * 3 / 2; allocated > limit {
		t.Errorf("producing a %d byte snapshot allocated %d bytes, over the %d byte budget; "+
			"the buffer is being regrown", len(snap), allocated, limit)
	}
}

func TestSnapshotContentIsUnchangedBySizing(t *testing.T) {
	// Sizing the buffer must not change a single byte. Snapshots are compared
	// between replicas to check convergence, so the encoding is part of the
	// contract rather than an implementation detail.
	kv := New()
	fill(t, kv, 32, 64)

	first, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(first); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	second, err := restored.Snapshot()
	if err != nil {
		t.Fatalf("re-Snapshot: %v", err)
	}
	if string(first) != string(second) {
		t.Error("a snapshot did not survive a round trip through Restore unchanged")
	}
}
