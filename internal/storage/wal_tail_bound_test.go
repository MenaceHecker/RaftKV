package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// TestScanWorkIsBoundedOnAdversarialTail: a framing failure followed by bytes
// every one of which reads as a known record type (1) and a plausible length
// (0x01010101, about 16 MiB, under the record cap). A scan that checksums each
// candidate body does about 16 MiB of work per offset, millions of offsets.
func TestScanWorkIsBoundedOnAdversarialTail(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 3)
	data := append(readSegment(t, path), bytes.Repeat([]byte{0x01}, 32<<20)...)
	writeSegment(t, path, data)

	type result struct {
		w   *WAL
		err error
	}
	done := make(chan result, 1)
	go func() {
		w, _, err := Open(Options{Dir: dir, FS: noSyncFS{osFS{}}})
		done <- result{w, err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			_ = r.w.Close()
			t.Fatal("opened a log whose tail could not be shown free of valid records")
		}
		if !errors.Is(r.err, ErrCorruptWAL) {
			t.Fatalf("open error = %v, want ErrCorruptWAL (refuse when the scan budget runs out)", r.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("startup is still scanning the tail after 30s; the forward scan is not bounded")
	}
	if after := readSegment(t, path); !bytes.Equal(after, data) {
		t.Fatal("a refused open changed the segment")
	}
}

// BenchmarkReplayBadTail shows how startup cost scales with the size of a
// garbage tail in which every offset sees a plausible 4 KiB length.
func BenchmarkReplayBadTail(b *testing.B) {
	for _, size := range []int{1 << 20, 2 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			dir := b.TempDir()
			w, _, err := Open(Options{Dir: dir})
			if err != nil {
				b.Fatal(err)
			}
			if err := w.AppendEntries(entries(1, 1, 3)); err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
			if err != nil || len(paths) != 1 {
				b.Fatalf("segments: %v %v", paths, err)
			}
			data, err := os.ReadFile(paths[0]) //nolint:gosec // path under b.TempDir()
			if err != nil {
				b.Fatal(err)
			}
			junk := bytes.Repeat([]byte{0x00, 0x10, 0x00, 0x00}, size/4)
			if err := os.WriteFile(paths[0], append(data, junk...), fileMode); err != nil { //nolint:gosec // path under b.TempDir()
				b.Fatal(err)
			}
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				w, _, err := Open(Options{Dir: dir, FS: noSyncFS{osFS{}}})
				if err == nil {
					_ = w.Close()
				}
			}
		})
	}
}

func TestFrameEmbeddedInAnotherRecordIsRefused(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir, Options{})
	inner := appendRecord(nil, recordEntry, encodeEntry(nil, raft.Entry{Term: 1, Index: 99, Type: raft.EntryNormal, Data: []byte("inner")}))
	outer := raft.Entry{Term: 1, Index: 3, Type: raft.EntryNormal, Data: append([]byte("pad-"), inner...)}
	if err := w.AppendEntries(append(entries(1, 1, 2), outer)); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	path := segmentPaths(t, dir)[0]
	starts := recordStarts(t, readSegment(t, path))
	last := starts[len(starts)-1]

	// Damage the outer record's CRC. A valid-looking frame sits inside its
	// payload; the scan cannot tell it from a real record and must refuse
	// rather than risk dropping data.
	mutateSegment(t, path, func(d []byte) { d[last+lenSize] ^= 0xff })

	assertRefused(t, dir, path, readSegment(t, path))
}

func TestSeveralDamagedRecordsBeforeAValidOneAreRefused(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 4)
	starts := recordStarts(t, readSegment(t, path))

	mutateSegment(t, path, func(d []byte) {
		d[starts[1]+headerSize+typeSize+2] ^= 0xff
		d[starts[2]+headerSize+typeSize+2] ^= 0xff
	})

	assertRefused(t, dir, path, readSegment(t, path))
}

func TestRepairCountedEvenWhenStartupFailsAfterwards(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, walSubdir)

	// The log records a snapshot that does not exist, so OpenDiskStorage fails
	// after the WAL has been opened and repaired. OnRepair means "a repair was
	// performed", not "startup succeeded".
	w, _, err := Open(Options{Dir: walDir})
	if err != nil {
		t.Fatalf("opening WAL: %v", err)
	}
	appendEach(t, w, entries(1, 1, 3))
	if err := w.SaveSnapshotMeta(SnapshotMeta{Index: 2, Term: 1}); err != nil {
		t.Fatalf("SaveSnapshotMeta: %v", err)
	}
	appendEach(t, w, entries(1, 4, 1))
	closeWAL(t, w)

	path := segmentPaths(t, walDir)[0]
	starts := recordStarts(t, readSegment(t, path))
	last := starts[len(starts)-1]
	mutateSegment(t, path, func(d []byte) { d[last+headerSize+typeSize+2] ^= 0xff })

	var repairs []RepairInfo
	_, _, err = OpenDiskStorage(DiskConfig{Dir: dir, OnRepair: func(i RepairInfo) { repairs = append(repairs, i) }})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("open error = %v, want ErrCorruptWAL from the snapshot check", err)
	}
	if len(repairs) != 1 {
		t.Fatalf("OnRepair called %d times, want 1: the repair happened before the failure", len(repairs))
	}
}
