package faultfs_test

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/faultfs"
	"github.com/MenaceHecker/raftkv/internal/storage/fstest"
)

const appendFlag = os.O_CREATE | os.O_WRONLY | os.O_APPEND

func newFS(seed int64) *faultfs.FS {
	return newFSWith(seed, faultfs.Options{})
}

func newFSWith(seed int64, opts faultfs.Options) *faultfs.FS {
	// A seeded generator is the point: the same seed must give the same crash.
	return faultfs.New(rand.New(rand.NewSource(seed)), opts) //nolint:gosec // deterministic simulation, not security
}

// syncChain makes dir and every ancestor's entry durable.
func syncChain(t *testing.T, f *faultfs.FS, dir string) {
	t.Helper()
	if err := f.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	for p := dir; p != "/"; p = path.Dir(p) {
		if err := f.SyncDir(path.Dir(p)); err != nil {
			t.Fatalf("SyncDir(%s): %v", path.Dir(p), err)
		}
	}
}

// put appends data to name, creating it, and optionally syncs the file (not its directory).
func put(t *testing.T, f *faultfs.FS, name, data string, sync bool) {
	t.Helper()
	h, err := f.OpenFile(name, appendFlag, 0o644)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", name, err)
	}
	if _, err := h.Write([]byte(data)); err != nil {
		t.Fatalf("Write(%s): %v", name, err)
	}
	if sync {
		if err := h.Sync(); err != nil {
			t.Fatalf("Sync(%s): %v", name, err)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close(%s): %v", name, err)
	}
}

func syncDir(t *testing.T, f *faultfs.FS, dir string) {
	t.Helper()
	if err := f.SyncDir(dir); err != nil {
		t.Fatalf("SyncDir(%s): %v", dir, err)
	}
}

// durableFile creates name with data fully durable (data and entry).
func durableFile(t *testing.T, f *faultfs.FS, name, data string) {
	t.Helper()
	syncChain(t, f, path.Dir(name))
	put(t, f, name, data, true)
	syncDir(t, f, path.Dir(name))
}

func get(t *testing.T, f *faultfs.FS, name string) (string, bool) {
	t.Helper()
	data, err := f.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", name, err)
	}
	return string(data), true
}

func wantFile(t *testing.T, f *faultfs.FS, name, want string) {
	t.Helper()
	got, ok := get(t, f, name)
	if !ok {
		t.Fatalf("%s does not exist, want %q", name, want)
	}
	if got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}

func wantMissing(t *testing.T, f *faultfs.FS, name string) {
	t.Helper()
	if got, ok := get(t, f, name); ok {
		t.Fatalf("%s exists with %q, want it gone", name, got)
	}
}

func TestFaultFSModel(t *testing.T) {
	t.Run("unsynced bytes are lost", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		put(t, f, "/d/a", "def", false)
		wantFile(t, f, "/d/a", "abcdef")
		if got := f.Unsynced("/d/a"); got != 3 {
			t.Fatalf("Unsynced = %d, want 3", got)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abc")
	})

	t.Run("unsynced create is lost", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		put(t, f, "/d/a", "abc", false)
		syncDir(t, f, "/d")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "")

		put(t, f, "/d/b", "abc", false)
		f.Crash(faultfs.CrashOptions{})
		wantMissing(t, f, "/d/b")
	})

	t.Run("synced file with unsynced entry is lost", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		put(t, f, "/d/a", "abc", true)
		f.Crash(faultfs.CrashOptions{})
		wantMissing(t, f, "/d/a")
	})

	t.Run("synced file in a directory whose entry was never synced is lost", func(t *testing.T) {
		f := newFS(1)
		if err := f.MkdirAll("/x/y/z", 0o755); err != nil {
			t.Fatal(err)
		}
		put(t, f, "/x/y/z/a", "abc", true)
		syncDir(t, f, "/x/y/z") // the file's own directory, but not z in y, y in x or x in /
		f.Crash(faultfs.CrashOptions{})
		wantMissing(t, f, "/x/y/z/a")
		if _, err := f.Stat("/x"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Stat(/x) = %v, want not-exist: new ancestors must vanish too", err)
		}
	})

	t.Run("every new ancestor must be synced", func(t *testing.T) {
		for _, skip := range []string{"/", "/x", "/x/y"} {
			f := newFS(1)
			if err := f.MkdirAll("/x/y/z", 0o755); err != nil {
				t.Fatal(err)
			}
			put(t, f, "/x/y/z/a", "abc", true)
			for _, d := range []string{"/x/y/z", "/x/y", "/x", "/"} {
				if d != skip {
					syncDir(t, f, d)
				}
			}
			f.Crash(faultfs.CrashOptions{})
			// Skipping the sync of d leaves d's children undiscoverable, so the
			// file survives only if the chain above it was complete.
			_, ok := get(t, f, "/x/y/z/a")
			if ok {
				t.Errorf("file survived although SyncDir(%s) never ran", skip)
			}
		}

		f := newFS(1)
		if err := f.MkdirAll("/x/y/z", 0o755); err != nil {
			t.Fatal(err)
		}
		put(t, f, "/x/y/z/a", "abc", true)
		for _, d := range []string{"/x/y/z", "/x/y", "/x", "/"} {
			syncDir(t, f, d)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/x/y/z/a", "abc")
	})

	t.Run("unsynced remove reverts", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		if err := f.Remove("/d/a"); err != nil {
			t.Fatal(err)
		}
		wantMissing(t, f, "/d/a")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abc")

		if err := f.Remove("/d/a"); err != nil {
			t.Fatal(err)
		}
		syncDir(t, f, "/d")
		f.Crash(faultfs.CrashOptions{})
		wantMissing(t, f, "/d/a")
	})

	t.Run("unsynced rename reverts and synced rename survives", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		if err := f.Rename("/d/a", "/d/b"); err != nil {
			t.Fatal(err)
		}
		wantFile(t, f, "/d/b", "abc")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abc")
		wantMissing(t, f, "/d/b")

		if err := f.Rename("/d/a", "/d/b"); err != nil {
			t.Fatal(err)
		}
		syncDir(t, f, "/d")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/b", "abc")
		wantMissing(t, f, "/d/a")
	})

	t.Run("a file removed or renamed without SyncDir still loses its unsynced bytes", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/gone", "abc")
		durableFile(t, f, "/d/moved", "xyz")
		put(t, f, "/d/gone", "UNSYNCED", false)
		put(t, f, "/d/moved", "UNSYNCED", false)
		if err := f.Remove("/d/gone"); err != nil {
			t.Fatal(err)
		}
		if err := f.Rename("/d/moved", "/d/elsewhere"); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/gone", "abc")
		wantFile(t, f, "/d/moved", "xyz")
		wantMissing(t, f, "/d/elsewhere")
	})

	t.Run("rename over a file is atomic and reverts as a unit", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "new")
		durableFile(t, f, "/d/b", "old")
		if err := f.Rename("/d/a", "/d/b"); err != nil {
			t.Fatal(err)
		}
		wantFile(t, f, "/d/b", "new")
		wantMissing(t, f, "/d/a")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "new")
		wantFile(t, f, "/d/b", "old")
	})

	t.Run("cross-directory rename is refused", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "x")
		syncChain(t, f, "/e")
		if err := f.Rename("/d/a", "/e/a"); !errors.Is(err, faultfs.ErrCrossDirRename) {
			t.Fatalf("Rename across directories = %v, want ErrCrossDirRename", err)
		}
		wantFile(t, f, "/d/a", "x")
	})

	t.Run("a durable entry with unsynced data comes back with its durable bytes", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		put(t, f, "/d/a", "abc", false)
		syncDir(t, f, "/d")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "")
	})

	t.Run("TornTail keeps a prefix of the unsynced bytes", func(t *testing.T) {
		lengths := map[int]bool{}
		for seed := int64(1); seed <= 40; seed++ {
			f := newFS(seed)
			durableFile(t, f, "/d/a", "ab")
			put(t, f, "/d/a", "cdefghij", false)
			f.Crash(faultfs.CrashOptions{Tail: faultfs.TornTail})
			got, _ := get(t, f, "/d/a")
			const live = "abcdefghij"
			if !strings.HasPrefix(live, got) || len(got) < 2 {
				t.Fatalf("seed %d: %q is not a prefix of the live bytes holding the durable ones", seed, got)
			}
			lengths[len(got)] = true
		}
		if len(lengths) < 4 {
			t.Fatalf("40 seeds produced only %d distinct torn lengths: %v", len(lengths), lengths)
		}
	})

	t.Run("TornTailAt keeps exactly the requested bytes", func(t *testing.T) {
		for keep, want := range map[int]string{0: "ab", 3: "abcde", 8: "abcdefghij", 99: "abcdefghij", -4: "ab"} {
			f := newFS(1)
			durableFile(t, f, "/d/a", "ab")
			put(t, f, "/d/a", "cdefghij", false)
			f.Crash(faultfs.CrashOptions{TornTailAt: map[string]int{"/d/a": keep}})
			wantFile(t, f, "/d/a", want)
		}
		f := newFS(1)
		durableFile(t, f, "/d/a", "ab")
		put(t, f, "/d/a", "cdef", false)
		f.Crash(faultfs.CrashOptions{TornTailAt: map[string]int{"a": 1}})
		wantFile(t, f, "/d/a", "abc")
	})

	t.Run("GarbageTail keeps the length and changes the unsynced bytes", func(t *testing.T) {
		changed, nonZero := false, false
		for seed := int64(1); seed <= 10; seed++ {
			f := newFS(seed)
			durableFile(t, f, "/d/a", "ab")
			put(t, f, "/d/a", "cdefghij", false)
			f.Crash(faultfs.CrashOptions{Tail: faultfs.GarbageTail})
			got, _ := get(t, f, "/d/a")
			if len(got) != 10 || !strings.HasPrefix(got, "ab") {
				t.Fatalf("seed %d: %q, want 10 bytes starting with the durable ab", seed, got)
			}
			changed = changed || got != "abcdefghij"
			nonZero = nonZero || strings.Trim(got[2:], "\x00") != ""
		}
		if !changed || !nonZero {
			t.Fatalf("garbage must differ from the original bytes (%v) and not be all zeros (%v)", changed, nonZero)
		}
	})

	t.Run("ZeroTail keeps the length and zeroes the unsynced bytes", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "ab")
		put(t, f, "/d/a", "cdef", false)
		f.Crash(faultfs.CrashOptions{Tail: faultfs.ZeroTail})
		wantFile(t, f, "/d/a", "ab\x00\x00\x00\x00")
	})

	t.Run("truncate without Sync reverts or persists, forced by option", func(t *testing.T) {
		for _, tc := range []struct {
			mode faultfs.TruncateMode
			want string
		}{
			{faultfs.TruncateLost, "abcdef"},
			{faultfs.TruncatePersisted, "ab"},
		} {
			f := newFS(1)
			durableFile(t, f, "/d/a", "abcdef")
			if err := f.Truncate("/d/a", 2); err != nil {
				t.Fatal(err)
			}
			wantFile(t, f, "/d/a", "ab")
			f.Crash(faultfs.CrashOptions{Truncate: tc.mode})
			wantFile(t, f, "/d/a", tc.want)
		}

		seen := map[string]bool{}
		for seed := int64(1); seed <= 30; seed++ {
			f := newFS(seed)
			durableFile(t, f, "/d/a", "abcdef")
			if err := f.Truncate("/d/a", 2); err != nil {
				t.Fatal(err)
			}
			f.Crash(faultfs.CrashOptions{})
			got, _ := get(t, f, "/d/a")
			if got != "abcdef" && got != "ab" {
				t.Fatalf("seed %d: %q is neither the durable nor the live bytes", seed, got)
			}
			seen[got] = true
		}
		if len(seen) != 2 {
			t.Fatalf("30 seeds only ever chose %v", seen)
		}
	})

	t.Run("truncate then Sync then Crash persists", func(t *testing.T) {
		for _, mode := range []faultfs.TruncateMode{faultfs.TruncateBySeed, faultfs.TruncateLost, faultfs.TruncatePersisted} {
			f := newFS(1)
			durableFile(t, f, "/d/a", "abcdef")
			if err := f.Truncate("/d/a", 2); err != nil {
				t.Fatal(err)
			}
			h, err := f.OpenFile("/d/a", os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Sync(); err != nil {
				t.Fatal(err)
			}
			_ = h.Close()
			f.Crash(faultfs.CrashOptions{Truncate: mode})
			wantFile(t, f, "/d/a", "ab")
		}
	})

	t.Run("a live file shorter than the durable one never panics", func(t *testing.T) {
		for _, tail := range []faultfs.TailMode{faultfs.DropTail, faultfs.TornTail, faultfs.GarbageTail, faultfs.ZeroTail} {
			for seed := int64(1); seed <= 10; seed++ {
				f := newFS(seed)
				durableFile(t, f, "/d/a", "abcdef")
				if err := f.Truncate("/d/a", 1); err != nil {
					t.Fatal(err)
				}
				f.Crash(faultfs.CrashOptions{Tail: tail})
				got, _ := get(t, f, "/d/a")
				if got != "abcdef" && got != "a" {
					t.Fatalf("tail %d seed %d: %q is neither d nor l", tail, seed, got)
				}
			}
		}
	})

	t.Run("a rewritten file comes back as the durable or the live bytes", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abcdef")
		h, err := f.OpenFile("/d/a", os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{Truncate: faultfs.TruncateLost, Tail: faultfs.TornTail})
		wantFile(t, f, "/d/a", "abcdef")

		// Forced the other way, the rewrite persists whole, and no tail mode cuts it.
		g := newFS(1)
		durableFile(t, g, "/d/a", "abcdef")
		h, err = g.OpenFile("/d/a", os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		g.Crash(faultfs.CrashOptions{Truncate: faultfs.TruncatePersisted, Tail: faultfs.TornTail})
		wantFile(t, g, "/d/a", "XYcdef")
	})

	t.Run("after Crash old handles are stale and locks are released", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		release, err := f.Lock("/d")
		if err != nil {
			t.Fatal(err)
		}
		h, err := f.OpenFile("/d/a", appendFlag, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})

		if _, err := h.Write([]byte("x")); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("Write on a stale handle = %v, want ErrStaleHandle", err)
		}
		if err := h.Sync(); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("Sync on a stale handle = %v, want ErrStaleHandle", err)
		}
		if _, err := h.Stat(); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("Stat on a stale handle = %v, want ErrStaleHandle", err)
		}
		if err := h.Close(); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("Close on a stale handle = %v, want ErrStaleHandle", err)
		}

		second, err := f.Lock("/d")
		if err != nil {
			t.Fatalf("Lock after Crash: %v; a dead process's lock must be gone", err)
		}
		if err := release(); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("stale release = %v, want ErrStaleHandle", err)
		}
		if _, err := f.Lock("/d"); err == nil {
			t.Error("a stale release freed the new holder's lock")
		}
		if err := second(); err != nil {
			t.Errorf("release: %v", err)
		}
	})

	t.Run("a stale handle never touches the new state", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		h, err := f.OpenFile("/d/a", appendFlag, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		_, _ = h.Write([]byte("zzz"))
		_ = h.Sync()
		wantFile(t, f, "/d/a", "abc")
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abc")
	})

	t.Run("ProcessCrash keeps live data and invalidates handles and locks", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		release, err := f.Lock("/d")
		if err != nil {
			t.Fatal(err)
		}
		h, err := f.OpenFile("/d/a", appendFlag, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Write([]byte("def")); err != nil {
			t.Fatal(err)
		}
		put(t, f, "/d/new", "n", false)

		f.ProcessCrash()

		wantFile(t, f, "/d/a", "abcdef")
		wantFile(t, f, "/d/new", "n")
		if _, err := h.Write([]byte("x")); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("Write after ProcessCrash = %v, want ErrStaleHandle", err)
		}
		if _, err := f.Lock("/d"); err != nil {
			t.Errorf("Lock after ProcessCrash: %v", err)
		}
		if err := release(); !errors.Is(err, faultfs.ErrStaleHandle) {
			t.Errorf("stale release = %v, want ErrStaleHandle", err)
		}

		// The data is still only live, so a later power loss takes it.
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abc")
		wantMissing(t, f, "/d/new")
	})

	t.Run("one-shot Nth fails once and then recovers", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, PathGlob: "*.wal", Nth: 2, Err: faultfs.ErrSync})
		h, err := f.OpenFile("/d/a.wal", appendFlag, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		other, err := f.OpenFile("/d/b.dat", appendFlag, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 4; i++ {
			err := h.Sync()
			if (i == 2) != (err != nil) {
				t.Fatalf("Sync #%d = %v; only #2 should fail", i, err)
			}
			if i == 2 && !errors.Is(err, faultfs.ErrSync) {
				t.Fatalf("Sync #2 = %v, want ErrSync", err)
			}
			if err := other.Sync(); err != nil {
				t.Fatalf("a non-matching path failed: %v", err)
			}
		}
	})

	t.Run("Sticky fails every matching call from the nth on", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 2, Sticky: true, Err: faultfs.ErrSync})
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		for i := 1; i <= 4; i++ {
			err := h.Sync()
			if (i >= 2) != (err != nil) {
				t.Fatalf("Sync #%d = %v; #2 onwards should fail", i, err)
			}
		}
	})

	t.Run("Prob is drawn from the seed", func(t *testing.T) {
		run := func(seed int64) []bool {
			f := newFS(seed)
			syncChain(t, f, "/d")
			f.Inject(faultfs.Rule{Op: faultfs.OpSync, Prob: 0.5, Err: faultfs.ErrSync})
			h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
			var out []bool
			for range 40 {
				out = append(out, h.Sync() != nil)
			}
			return out
		}
		a, b := run(7), run(7)
		if !equalBools(a, b) {
			t.Fatal("the same seed gave different failures")
		}
		failed := 0
		for _, x := range a {
			if x {
				failed++
			}
		}
		if failed == 0 || failed == len(a) {
			t.Fatalf("Prob 0.5 failed %d of %d calls", failed, len(a))
		}
	})

	t.Run("ShortWrite stores k bytes and returns a non-nil error", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		f.Inject(faultfs.Rule{Op: faultfs.OpWrite, Nth: 1, Err: faultfs.ShortWrite(3)})
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		n, err := h.Write([]byte("abcdefgh"))
		if err == nil || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Write = %d, %v; want a non-nil error matching io.ErrShortWrite", n, err)
		}
		if n != 3 {
			t.Fatalf("Write returned n = %d, want 3", n)
		}
		wantFile(t, f, "/d/a", "abc")
		if _, err := h.Write([]byte("xy")); err != nil {
			t.Fatalf("the next Write failed: %v", err)
		}
		wantFile(t, f, "/d/a", "abcxy")
	})

	t.Run("ENOSPC stores nothing", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		f.Inject(faultfs.Rule{Op: faultfs.OpWrite, Nth: 1, Err: faultfs.ErrNoSpace})
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		n, err := h.Write([]byte("abc"))
		if n != 0 || !errors.Is(err, faultfs.ErrNoSpace) {
			t.Fatalf("Write = %d, %v; want 0 and ErrNoSpace", n, err)
		}
		wantFile(t, f, "/d/a", "")

		f.Inject(faultfs.Rule{Op: faultfs.OpOpenFile, PathGlob: "*.new", Nth: 1, Err: faultfs.ErrNoSpace})
		if _, err := f.OpenFile("/d/x.new", appendFlag, 0o644); !errors.Is(err, faultfs.ErrNoSpace) {
			t.Fatalf("OpenFile = %v, want ErrNoSpace", err)
		}
		wantMissing(t, f, "/d/x.new")
	})

	t.Run("a failed Sync leaves the bytes unsynced and the seed decides at Crash", func(t *testing.T) {
		seen := map[string]bool{}
		for seed := int64(1); seed <= 30; seed++ {
			f := newFS(seed)
			durableFile(t, f, "/d/a", "ab")
			put(t, f, "/d/a", "cd", false)
			f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
			h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
			if err := h.Sync(); err == nil {
				t.Fatal("injected Sync succeeded")
			}
			if got := f.Unsynced("/d/a"); got != 2 {
				t.Fatalf("Unsynced = %d after a failed Sync, want 2", got)
			}
			f.Crash(faultfs.CrashOptions{})
			got, _ := get(t, f, "/d/a")
			if got != "ab" && got != "abcd" {
				t.Fatalf("seed %d: %q, want ab or abcd", seed, got)
			}
			seen[got] = true
		}
		if len(seen) != 2 {
			t.Fatalf("30 seeds only ever chose %v", seen)
		}
	})

	t.Run("Fsyncgate off: a later successful Sync makes the bytes durable", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "ab")
		put(t, f, "/d/a", "cd", false)
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		if err := h.Sync(); err == nil {
			t.Fatal("injected Sync succeeded")
		}
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abcd")
	})

	t.Run("Fsyncgate on: failed bytes stay non-durable after a later Sync succeeds", func(t *testing.T) {
		f := newFSWith(1, faultfs.Options{Fsyncgate: true})
		durableFile(t, f, "/d/a", "ab")
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		_, _ = h.Write([]byte("cd"))
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
		if err := h.Sync(); err == nil {
			t.Fatal("injected Sync succeeded")
		}
		_, _ = h.Write([]byte("ef"))
		if err := h.Sync(); err != nil {
			t.Fatalf("the later Sync must report success: %v", err)
		}
		if got := f.Unsynced("/d/a"); got != 0 {
			t.Fatalf("Unsynced = %d after a successful Sync", got)
		}
		wantFile(t, f, "/d/a", "abcdef") // the page cache still serves the bytes
		f.Crash(faultfs.CrashOptions{})
		// The length persisted; the bytes whose write failed did not.
		wantFile(t, f, "/d/a", "ab\x00\x00ef")
	})

	t.Run("Fsyncgate: rewriting the failed range makes it durable again", func(t *testing.T) {
		f := newFSWith(1, faultfs.Options{Fsyncgate: true})
		durableFile(t, f, "/d/a", "ab")
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		_, _ = h.Write([]byte("cd"))
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
		_ = h.Sync()
		if err := f.Truncate("/d/a", 2); err != nil {
			t.Fatal(err)
		}
		_, _ = h.Write([]byte("XY"))
		if err := h.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "abXY")
	})

	t.Run("Fsyncgate: overwriting the failed range in place makes it durable again", func(t *testing.T) {
		f := newFSWith(1, faultfs.Options{Fsyncgate: true})
		durableFile(t, f, "/d/a", "ab")
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		_, _ = h.Write([]byte("cd"))
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
		_ = h.Sync()
		w, err := f.OpenFile("/d/a", os.O_WRONLY, 0o644) // writes from offset 0
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("WXYZ")); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", "WXYZ")
	})

	t.Run("Fsyncgate: truncating away the failed bytes drops the poison", func(t *testing.T) {
		f := newFSWith(1, faultfs.Options{Fsyncgate: true})
		durableFile(t, f, "/d/a", "abcdef")
		w, err := f.OpenFile("/d/a", os.O_WRONLY, 0o644) // rewrite from offset 0
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("XY")); err != nil {
			t.Fatal(err)
		}
		f.Inject(faultfs.Rule{Op: faultfs.OpSync, Nth: 1, Err: faultfs.ErrSync})
		if err := w.Sync(); err == nil {
			t.Fatal("injected Sync succeeded")
		}
		for _, size := range []int64{0, 6} { // cut everything away, then grow back as zeros
			if err := f.Truncate("/d/a", size); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		f.Crash(faultfs.CrashOptions{})
		wantFile(t, f, "/d/a", strings.Repeat("\x00", 6))
	})

	t.Run("the same seed gives the same result", func(t *testing.T) {
		run := func(seed int64) string {
			f := newFS(seed)
			var b strings.Builder
			for _, n := range []string{"/d/a", "/d/b", "/d/c"} {
				durableFile(t, f, n, "xx")
				put(t, f, n, strings.Repeat("y", 20), false)
			}
			f.Crash(faultfs.CrashOptions{Tail: faultfs.TornTail})
			for _, n := range []string{"/d/a", "/d/b", "/d/c"} {
				got, _ := get(t, f, n)
				b.WriteString(strconv.Itoa(len(got)) + ",")
			}
			return b.String()
		}
		distinct := map[string]bool{}
		for seed := int64(1); seed <= 10; seed++ {
			if a, b := run(seed), run(seed); a != b {
				t.Fatalf("seed %d: %s then %s", seed, a, b)
			}
			distinct[run(seed)] = true
		}
		if len(distinct) < 2 {
			t.Fatal("ten seeds gave the same torn lengths")
		}
	})

	t.Run("a crash outcome does not depend on creation order", func(t *testing.T) {
		build := func(order []string) string {
			f := newFS(5)
			syncChain(t, f, "/d")
			for _, n := range order {
				put(t, f, "/d/"+n, "base", true)
			}
			syncDir(t, f, "/d")
			for _, n := range order {
				put(t, f, "/d/"+n, strings.Repeat("z", 30), false)
			}
			f.Crash(faultfs.CrashOptions{Tail: faultfs.TornTail})
			var b strings.Builder
			for _, n := range []string{"a", "b", "c", "d"} {
				got, _ := get(t, f, "/d/"+n)
				b.WriteString(n + strconv.Itoa(len(got)) + ",")
			}
			return b.String()
		}
		if x, y := build([]string{"a", "b", "c", "d"}), build([]string{"d", "c", "b", "a"}); x != y {
			t.Fatalf("creation order changed the outcome: %s vs %s", x, y)
		}
	})

	t.Run("SyncAll makes everything durable", func(t *testing.T) {
		f := newFS(1)
		if err := f.MkdirAll("/p/q", 0o755); err != nil {
			t.Fatal(err)
		}
		put(t, f, "/p/q/a", "abc", false)
		f.SyncAll()
		f.Crash(faultfs.CrashOptions{Tail: faultfs.GarbageTail})
		wantFile(t, f, "/p/q/a", "abc")
	})
}

func TestCrashAtAndAfter(t *testing.T) {
	t.Run("CrashAt fails op n before it takes effect and every later op", func(t *testing.T) {
		f := newFS(1)
		syncChain(t, f, "/d")
		base := f.Ops()
		f.CrashAt(base + 2)
		h, err := f.OpenFile("/d/a", appendFlag, 0o644) // op base+1
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Write([]byte("abc")); !errors.Is(err, faultfs.ErrCrashed) { // op base+2
			t.Fatalf("Write = %v, want ErrCrashed", err)
		}
		if !f.Crashed() {
			t.Fatal("Crashed() = false after the crash point")
		}
		if _, err := f.Stat("/d"); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("Stat after the crash point = %v, want ErrCrashed", err)
		}
		f.Crash(faultfs.CrashOptions{})
		if f.Crashed() {
			t.Fatal("Crash did not clear the crashed state")
		}
		if _, err := f.Stat("/d"); err != nil {
			t.Fatalf("Stat after Crash: %v", err)
		}
	})

	t.Run("CrashAt leaves the failed op without effect", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		f.CrashAt(f.Ops() + 1)
		if err := f.Remove("/d/a"); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("Remove = %v, want ErrCrashed", err)
		}
		f.ProcessCrash()
		wantFile(t, f, "/d/a", "abc")
	})

	t.Run("CrashAfter applies op n and still reports ErrCrashed", func(t *testing.T) {
		f := newFS(1)
		durableFile(t, f, "/d/a", "abc")
		f.CrashAfter(f.Ops() + 1)
		if err := f.Remove("/d/a"); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("Remove = %v, want ErrCrashed", err)
		}
		if err := f.SyncDir("/d"); !errors.Is(err, faultfs.ErrCrashed) {
			t.Fatalf("a later op = %v, want ErrCrashed", err)
		}
		f.ProcessCrash()
		wantMissing(t, f, "/d/a") // live state kept the remove
	})

	t.Run("every FS and File method counts", func(t *testing.T) {
		f := newFS(1)
		before := f.Ops()
		_ = f.MkdirAll("/d", 0o755)
		h, _ := f.OpenFile("/d/a", appendFlag, 0o644)
		_, _ = h.Write([]byte("x"))
		_, _ = h.Stat()
		_ = h.Sync()
		_ = h.Close()
		_, _ = f.ReadFile("/d/a")
		_, _ = f.ReadDir("/d")
		_, _ = f.Stat("/d/a")
		_ = f.Truncate("/d/a", 0)
		_ = f.Rename("/d/a", "/d/b")
		_ = f.Remove("/d/b")
		_ = f.SyncDir("/d")
		release, _ := f.Lock("/d")
		_ = release()
		if got := f.Ops() - before; got != 15 {
			t.Fatalf("15 calls counted %d operations", got)
		}
	})
}

func TestFaultFSMatchesTheRealFilesystemContract(t *testing.T) {
	factory := func(t *testing.T) (storage.FS, string) {
		f := newFS(1)
		if err := f.MkdirAll("/data", 0o755); err != nil {
			t.Fatal(err)
		}
		return f, "/data"
	}
	fstest.RunContract(t, factory)
}

func TestFaultFSMatchesTheRealLockContract(t *testing.T) {
	factory := func(t *testing.T) (storage.FS, string) {
		f := newFS(1)
		if err := f.MkdirAll("/data", 0o755); err != nil {
			t.Fatal(err)
		}
		return f, "/data"
	}
	fstest.RunLockContract(t, factory)
}

// TestFaultFSIsDeterministic is the AST check: the model reads no clock, no
// global random generator, no operating system state and starts no goroutines.
func TestFaultFSIsDeterministic(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "faultfs.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing faultfs.go: %v", err)
	}

	forbidden := map[string]bool{
		"os": true, "runtime": true, "crypto/rand": true, "math/rand/v2": true,
		"net": true, "sync/atomic": true,
	}
	local := map[string]string{} // local name to import path
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if forbidden[p] {
			t.Errorf("faultfs.go imports %q", p)
		}
		name := path.Base(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		local[name] = p
	}
	if len(local) == 0 {
		t.Fatal("found no imports, so this check would pass vacuously")
	}

	// time is allowed for the time.Time type in fs.FileInfo, and math/rand for
	// the *rand.Rand the caller seeds; any other use reads a clock or the
	// global generator.
	allowed := map[string]map[string]bool{
		"time": {"Time": true},
		"rand": {"Rand": true},
	}
	checked := 0
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			t.Errorf("faultfs.go:%d starts a goroutine", fset.Position(x.Pos()).Line)
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, isPkg := local[id.Name]; !isPkg {
				return true
			}
			if set, restricted := allowed[id.Name]; restricted {
				checked++
				if !set[x.Sel.Name] {
					t.Errorf("faultfs.go:%d uses %s.%s", fset.Position(x.Pos()).Line, id.Name, x.Sel.Name)
				}
			}
		}
		return true
	})
	if checked == 0 {
		t.Fatal("found no uses of time or rand, so this check would pass vacuously")
	}
}

func equalBools(a, b []bool) bool {
	return bytes.Equal(boolBytes(a), boolBytes(b))
}

func boolBytes(a []bool) []byte {
	out := make([]byte, len(a))
	for i, x := range a {
		if x {
			out[i] = 1
		}
	}
	return out
}
