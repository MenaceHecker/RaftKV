package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The golden pins the order of filesystem calls but not their arguments, so a
// changed flag, permission, rename target or truncate size would slip through.
// The expected values below are the ones the pre-refactor os calls used.
func TestStorageCallArgumentsAreUnchanged(t *testing.T) {
	dir := t.TempDir()
	fsys := newWrapFS(nil)

	runScriptedStorage(t, dir, func(dir string) (*DiskStorage, Snapshot, error) {
		return OpenDiskStorage(DiskConfig{Dir: dir, Sync: SyncAlways, SegmentSize: 64, FS: fsys})
	})

	const (
		openActive = os.O_WRONLY | os.O_APPEND
		rollNew    = os.O_CREATE | os.O_EXCL | os.O_WRONLY | os.O_APPEND
		saveTemp   = os.O_CREATE | os.O_EXCL | os.O_WRONLY
	)
	seen := map[string]int{}
	var segmentOpens []string
	for _, d := range fsys.details() {
		d = strings.ReplaceAll(d, dir, "$DIR")
		switch {
		case strings.HasPrefix(d, "MkdirAll "):
			if !strings.HasSuffix(d, fmt.Sprintf(" perm=%#o", dirMode)) {
				t.Errorf("MkdirAll with the wrong permission: %s", d)
			}
		case strings.HasPrefix(d, "OpenFile "):
			name := strings.Fields(d)[1]
			args := fmt.Sprintf("flag=%#x perm=%#o", saveTemp, fileMode)
			if strings.HasSuffix(name, snapshotSuffix+tempSuffix) {
				if !strings.HasSuffix(d, args) {
					t.Errorf("snapshot temp opened with the wrong flags or permission: %s", d)
				}
				seen["snapshot"]++
			} else {
				segmentOpens = append(segmentOpens, d)
			}
		case strings.HasPrefix(d, "Rename "):
			f := strings.Fields(d)
			from, to := f[1], strings.TrimPrefix(f[2], "to=")
			if to+tempSuffix != from || !strings.HasSuffix(to, snapshotSuffix) {
				t.Errorf("Rename must publish a temporary snapshot to its final name: %s", d)
			}
			seen["rename"]++
		}
	}

	// The scenario creates every segment while writing, then reopens, so the
	// last segment open is the reopen of the active segment and the rest create.
	if len(segmentOpens) < 3 {
		t.Fatalf("scenario opened %d segments, want at least 3 (creates plus a reopen): %v", len(segmentOpens), segmentOpens)
	}
	for i, d := range segmentOpens {
		want := rollNew
		if i == len(segmentOpens)-1 {
			want = openActive
		}
		if !strings.HasSuffix(d, fmt.Sprintf("flag=%#x perm=%#o", want, fileMode)) {
			t.Errorf("segment open %d has the wrong flags or permission: %s (want flag=%#x perm=%#o)", i, d, want, fileMode)
		}
	}
	for _, want := range []string{"snapshot", "rename"} {
		if seen[want] == 0 {
			t.Errorf("the scenario never exercised %s, so its arguments are not pinned; saw %v", want, seen)
		}
	}
}

func TestTornTailTruncatesToTheLastGoodRecord(t *testing.T) {
	dir := t.TempDir()

	w, _ := openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 1, 3)); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	seg := onlySegment(t, dir)
	info, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	good := info.Size()

	w, _ = openWAL(t, dir, Options{})
	if err := w.AppendEntries(entries(1, 4, 1)); err != nil {
		t.Fatalf("appending doomed entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	truncateTail(t, dir, 5)

	fsys := newWrapFS(nil)
	w, rep, err := Open(Options{Dir: dir, FS: fsys})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w.Close() }()
	if !rep.Repaired {
		t.Fatal("a torn tail was not reported as repaired")
	}

	want := fmt.Sprintf("Truncate %s size=%d", seg, good)
	for _, d := range fsys.details() {
		if d == want {
			return
		}
	}
	t.Fatalf("no %q among %v", want, fsys.details())
}

func onlySegment(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("segments in %s = %v (err %v), want exactly one", dir, matches, err)
	}
	return matches[0]
}
