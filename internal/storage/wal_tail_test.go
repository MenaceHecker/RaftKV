package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/raft"
)

// tailLog writes entries 1..n as one record each, closes the WAL and returns
// the path of the (only) segment.
// growBy is added to a record length to push it past the end of the file while
// staying under maxRecordSize, so readRecord reports ErrTornRecord.
const growBy uint32 = 1 << 20

func closeWAL(t *testing.T, w interface{ Close() error }) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Errorf("closing: %v", err)
	}
}

func tailLog(t *testing.T, dir string, n int) string {
	t.Helper()
	w, _ := openWAL(t, dir, Options{})
	appendEach(t, w, entries(1, 1, n))
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	paths := segmentPaths(t, dir)
	if len(paths) != 1 {
		t.Fatalf("expected one segment, got %d", len(paths))
	}
	return paths[0]
}

// recordStarts returns the offset of every record in data, which must be a
// clean segment.
func recordStarts(t *testing.T, data []byte) []int {
	t.Helper()
	var starts []int
	for off := 0; off < len(data); {
		_, _, n, err := readRecord(data[off:])
		if err != nil {
			t.Fatalf("segment is not clean at offset %d: %v", off, err)
		}
		starts = append(starts, off)
		off += n
	}
	return starts
}

func readSegment(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // test path under t.TempDir()
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return data
}

func writeSegment(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, fileMode); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// mutateSegment rewrites the segment with fn applied and fails if the bytes
// did not change, so a case can never silently test an undamaged file.
func mutateSegment(t *testing.T, path string, fn func(data []byte)) {
	t.Helper()
	before := readSegment(t, path)
	after := bytes.Clone(before)
	fn(after)
	if bytes.Equal(before, after) {
		t.Fatal("the mutation did not change the segment")
	}
	writeSegment(t, path, after)
}

func assertRefused(t *testing.T, dir, path string, before []byte) {
	t.Helper()
	w, rep, err := Open(Options{Dir: dir})
	if err == nil {
		closeWAL(t, w)
		t.Fatalf("opened a damaged WAL (repaired=%v, %d entries), want ErrCorruptWAL",
			rep.Repaired, len(rep.Entries))
	}
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("open error = %v, want ErrCorruptWAL", err)
	}
	if rep.Repaired {
		t.Fatal("a refused open reported Repaired")
	}
	if after := readSegment(t, path); !bytes.Equal(before, after) {
		t.Fatalf("a refused open changed the segment: %d bytes before, %d after",
			len(before), len(after))
	}
}

func TestBadCRCAtTailIsRepaired(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 4)
	starts := recordStarts(t, readSegment(t, path))
	last := starts[len(starts)-1]

	mutateSegment(t, path, func(d []byte) { d[last+headerSize+typeSize+2] ^= 0xff })

	_, rep := openWAL(t, dir, Options{})
	if !rep.Repaired {
		t.Fatal("a bad CRC at the tail was not reported as repaired")
	}
	assertEntries(t, rep.Entries, entries(1, 1, 3))
	if got := int64(len(readSegment(t, path))); got != int64(last) {
		t.Fatalf("segment is %d bytes after repair, want %d", got, last)
	}
}

func TestBadTailIsRepairedAtEveryByte(t *testing.T) {
	probe := t.TempDir()
	probePath := tailLog(t, probe, 4)
	clean := readSegment(t, probePath)
	starts := recordStarts(t, clean)
	last := starts[len(starts)-1]

	for off := last; off < len(clean); off++ {
		t.Run(fmt.Sprintf("byte-%d", off-last), func(t *testing.T) {
			dir := t.TempDir()
			path := tailLog(t, dir, 4)
			mutateSegment(t, path, func(d []byte) { d[off] ^= 0xff })

			w, rep, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("open after flipping byte %d of the last record: %v", off-last, err)
			}
			defer closeWAL(t, w)
			if !rep.Repaired {
				t.Fatalf("flipping byte %d of the last record was not reported as repaired", off-last)
			}
			assertEntries(t, rep.Entries, entries(1, 1, 3))
		})
	}
}

func TestOversizeLengthAtTailIsRepaired(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 4)
	starts := recordStarts(t, readSegment(t, path))
	last := starts[len(starts)-1]

	mutateSegment(t, path, func(d []byte) {
		binary.LittleEndian.PutUint32(d[last:], 0xFFFFFFFF)
	})

	_, rep := openWAL(t, dir, Options{})
	if !rep.Repaired {
		t.Fatal("an oversize length at the tail was not reported as repaired")
	}
	assertEntries(t, rep.Entries, entries(1, 1, 3))
}

func TestZeroedTailIsRepaired(t *testing.T) {
	for _, n := range []int{1, 8, 64, 4096} {
		t.Run(fmt.Sprintf("zero-%d", n), func(t *testing.T) {
			dir := t.TempDir()
			w, _ := openWAL(t, dir, Options{})
			big := raft.Entry{Term: 1, Index: 3, Type: raft.EntryNormal, Data: bytes.Repeat([]byte{0xAB}, 9000)}
			if err := w.AppendEntries(append(entries(1, 1, 2), big)); err != nil {
				t.Fatalf("appending: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("closing: %v", err)
			}
			path := segmentPaths(t, dir)[0]

			mutateSegment(t, path, func(d []byte) {
				clear(d[len(d)-n:])
			})

			_, rep := openWAL(t, dir, Options{})
			if !rep.Repaired {
				t.Fatalf("a tail with its last %d bytes zeroed was not reported as repaired", n)
			}
			assertEntries(t, rep.Entries, entries(1, 1, 2))
		})
	}
}

func TestDamagedLengthBeforeValidRecordIsRefused(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 3)
	starts := recordStarts(t, readSegment(t, path))

	// Raise record 2's length so it runs past the end of the file: readRecord
	// reports a torn record, but record 3 behind it is intact and acked.
	mutateSegment(t, path, func(d []byte) {
		n := binary.LittleEndian.Uint32(d[starts[1]:])
		binary.LittleEndian.PutUint32(d[starts[1]:], n+growBy)
	})
	before := readSegment(t, path)
	if _, _, _, err := readRecord(before[starts[1]:]); !errors.Is(err, ErrTornRecord) {
		t.Fatalf("test setup: readRecord on the damaged record gave %v, want ErrTornRecord", err)
	}

	assertRefused(t, dir, path, before)
}

func TestBadRecordFollowedByValidRecordIsRefused(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 3)
	starts := recordStarts(t, readSegment(t, path))

	mutateSegment(t, path, func(d []byte) { d[starts[1]+headerSize+typeSize+2] ^= 0xff })

	assertRefused(t, dir, path, readSegment(t, path))
}

func TestBadCRCInOlderSegmentIsRefused(t *testing.T) {
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{SegmentSize: 256})
	appendEach(t, w, entries(1, 1, 20))
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	paths := segmentPaths(t, dir)
	if len(paths) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(paths))
	}

	mutateSegment(t, paths[0], func(d []byte) { d[len(d)-1] ^= 0xff })

	_, _, err := Open(Options{Dir: dir, SegmentSize: 256})
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("open with a bad CRC in an older segment gave %v, want ErrCorruptWAL", err)
	}
}

func TestCRCValidUndecodableRecordIsRefused(t *testing.T) {
	cases := map[string][]byte{
		"unknown-type":   appendRecord(nil, recordType(99), []byte("whatever")),
		"short-entry":    appendRecord(nil, recordEntry, []byte{1, 2, 3}),
		"short-hs":       appendRecord(nil, recordHardState, []byte{1}),
		"short-snapmeta": appendRecord(nil, recordSnapshotMeta, nil),
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := tailLog(t, dir, 3)
			writeSegment(t, path, append(readSegment(t, path), rec...))
			assertRefused(t, dir, path, readSegment(t, path))
		})
	}
}

func TestOrdinaryTornTailStaysRepaired(t *testing.T) {
	probe := t.TempDir()
	clean := readSegment(t, tailLog(t, probe, 4))
	starts := recordStarts(t, clean)
	last := starts[len(starts)-1]

	for keep := 1; keep < len(clean)-last; keep++ {
		t.Run(fmt.Sprintf("keep-%d", keep), func(t *testing.T) {
			dir := t.TempDir()
			path := tailLog(t, dir, 4)
			if err := os.Truncate(path, int64(last+keep)); err != nil {
				t.Fatalf("truncating: %v", err)
			}

			w, rep, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("open after a tear: %v", err)
			}
			if !rep.Repaired {
				t.Fatal("a torn tail was not reported as repaired")
			}
			assertEntries(t, rep.Entries, entries(1, 1, 3))
			if err := w.Close(); err != nil {
				t.Fatalf("closing: %v", err)
			}

			_, rep2 := openWAL(t, dir, Options{})
			if rep2.Repaired {
				t.Fatal("reopening a repaired WAL reported a repair again")
			}
		})
	}
}

func TestRepairedTailStaysRepaired(t *testing.T) {
	dir := t.TempDir()
	path := tailLog(t, dir, 4)
	starts := recordStarts(t, readSegment(t, path))
	last := starts[len(starts)-1]

	mutateSegment(t, path, func(d []byte) { d[last+headerSize+typeSize+2] ^= 0xff })

	w, rep := openWAL(t, dir, Options{})
	if !rep.Repaired {
		t.Fatal("a bad CRC at the tail was not reported as repaired")
	}
	next := entries(2, 4, 2)
	if err := w.AppendEntries(next); err != nil {
		t.Fatalf("appending after repair: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	want := int64(last)
	for _, e := range next {
		want += int64(len(appendRecord(nil, recordEntry, encodeEntry(nil, e))))
	}
	if got := int64(len(readSegment(t, path))); got != want {
		t.Fatalf("segment is %d bytes, want %d (last good offset plus the new appends)", got, want)
	}

	_, rep2 := openWAL(t, dir, Options{})
	if rep2.Repaired {
		t.Fatal("reopening reported a repair again")
	}
	assertEntries(t, rep2.Entries, append(entries(1, 1, 3), next...))
}

func TestRepairIsReported(t *testing.T) {
	var calls []RepairInfo
	record := func(i RepairInfo) { calls = append(calls, i) }

	dir := t.TempDir()
	path := tailLog(t, dir, 4)
	clean := readSegment(t, path)
	starts := recordStarts(t, clean)
	last := starts[len(starts)-1]

	// A clean open does not call OnRepair.
	w, _, err := Open(Options{Dir: dir, OnRepair: record})
	if err != nil {
		t.Fatalf("clean open: %v", err)
	}
	closeWAL(t, w)
	if len(calls) != 0 {
		t.Fatalf("OnRepair called %d times for a clean open", len(calls))
	}

	mutateSegment(t, path, func(d []byte) { d[last+headerSize+typeSize+2] ^= 0xff })
	w, rep, err := Open(Options{Dir: dir, OnRepair: record})
	if err != nil {
		t.Fatalf("repairing open: %v", err)
	}
	closeWAL(t, w)
	if !rep.Repaired {
		t.Fatal("expected a repair")
	}
	if len(calls) != 1 {
		t.Fatalf("OnRepair called %d times, want 1", len(calls))
	}
	got := calls[0]
	if got.Segment != filepath.Base(path) {
		t.Errorf("Segment = %q, want %q", got.Segment, filepath.Base(path))
	}
	if got.Offset != int64(last) {
		t.Errorf("Offset = %d, want %d", got.Offset, last)
	}
	if want := int64(len(clean) - last); got.Bytes != want {
		t.Errorf("Bytes = %d, want %d", got.Bytes, want)
	}
	if !errors.Is(got.Cause, ErrCorruptRecord) {
		t.Errorf("Cause = %v, want ErrCorruptRecord", got.Cause)
	}

	// Reopening the repaired log is clean: no further call.
	w, _, err = Open(Options{Dir: dir, OnRepair: record})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	closeWAL(t, w)
	if len(calls) != 1 {
		t.Fatalf("OnRepair called again on a clean reopen (%d calls)", len(calls))
	}

	// A refused open does not call it either.
	dir2 := t.TempDir()
	path2 := tailLog(t, dir2, 3)
	starts2 := recordStarts(t, readSegment(t, path2))
	mutateSegment(t, path2, func(d []byte) { d[starts2[1]+headerSize+typeSize+2] ^= 0xff })
	if _, _, err := Open(Options{Dir: dir2, OnRepair: record}); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("mid-log damage gave %v, want ErrCorruptWAL", err)
	}
	if len(calls) != 1 {
		t.Fatalf("OnRepair called for a refused open (%d calls)", len(calls))
	}
}

func TestDiskStorageRepairsTailAndRefusesMidLog(t *testing.T) {
	t.Run("tail", func(t *testing.T) {
		dir := t.TempDir()
		s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
		if err != nil {
			t.Fatalf("opening: %v", err)
		}
		for i := raft.Index(1); i <= 5; i++ {
			if err := s.Append(logEntries(1, i, 1)); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}

		walDir := filepath.Join(dir, walSubdir)
		path := segmentPaths(t, walDir)[0]
		starts := recordStarts(t, readSegment(t, path))
		last := starts[len(starts)-1]
		mutateSegment(t, path, func(d []byte) { d[last+headerSize+typeSize+2] ^= 0xff })

		var repairs []RepairInfo
		s2, _, err := OpenDiskStorage(DiskConfig{Dir: dir, OnRepair: func(i RepairInfo) { repairs = append(repairs, i) }})
		if err != nil {
			t.Fatalf("opening after a bad tail record: %v", err)
		}
		defer closeWAL(t, s2)
		if got := s2.LastIndex(); got != 4 {
			t.Fatalf("LastIndex = %d, want 4", got)
		}
		if len(repairs) != 1 {
			t.Fatalf("OnRepair called %d times, want 1", len(repairs))
		}
	})

	t.Run("mid-log", func(t *testing.T) {
		dir := t.TempDir()
		s, _, err := OpenDiskStorage(DiskConfig{Dir: dir})
		if err != nil {
			t.Fatalf("opening: %v", err)
		}
		for i := raft.Index(1); i <= 5; i++ {
			if err := s.Append(logEntries(1, i, 1)); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("closing: %v", err)
		}

		walDir := filepath.Join(dir, walSubdir)
		path := segmentPaths(t, walDir)[0]
		starts := recordStarts(t, readSegment(t, path))
		mutateSegment(t, path, func(d []byte) {
			n := binary.LittleEndian.Uint32(d[starts[2]:])
			binary.LittleEndian.PutUint32(d[starts[2]:], n+growBy)
		})
		before := readSegment(t, path)

		_, _, err = OpenDiskStorage(DiskConfig{Dir: dir})
		if !errors.Is(err, ErrCorruptWAL) {
			t.Fatalf("open with a damaged length before valid records gave %v, want ErrCorruptWAL", err)
		}
		if after := readSegment(t, path); !bytes.Equal(before, after) {
			t.Fatal("a refused open changed the segment")
		}
	})
}
