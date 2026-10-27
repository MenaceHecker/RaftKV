package storage

import (
	"errors"
	"os"
	"testing"
)

var errPathStat = errors.New("stat by path is not allowed here")

// pathStatFS fails Stat by pathname, so only a Stat on the open handle works.
type pathStatFS struct{ *wrapFS }

func (pathStatFS) Stat(string) (os.FileInfo, error) { return nil, errPathStat }

// openActive sized the active segment with f.Stat() on the descriptor it had
// just opened. A Stat by path can see a different file if the name was
// renamed or replaced after the open, so the handle is what must be asked.
func TestOpenActiveSizesTheOpenHandleNotThePath(t *testing.T) {
	dir := t.TempDir()

	w, _, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("creating the segment: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}

	w, _, err = Open(Options{Dir: dir, FS: pathStatFS{newWrapFS(nil)}})
	if err != nil {
		t.Fatalf("reopen failed; the active segment was sized by path: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
}
