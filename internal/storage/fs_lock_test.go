//go:build unix

package storage

import (
	"strings"
	"testing"
)

// The lock rows of the FS contract. They carry the unix build tag because
// lock_other.go keeps the no-op lock, which cannot refuse a second holder.
func TestOSFSLockContract(t *testing.T) {
	t.Run("Lock on a held directory fails with already in use", func(t *testing.T) {
		dir := t.TempDir()
		first, err := osFS{}.Lock(dir)
		if err != nil {
			t.Fatalf("first Lock: %v", err)
		}
		defer func() { _ = first.release() }()

		second, err := osFS{}.Lock(dir)
		if err == nil {
			_ = second.release()
			t.Fatal("a second Lock on a held directory succeeded")
		}
		if !strings.Contains(err.Error(), "already in use") {
			t.Fatalf("error = %q, want it to say the directory is already in use", err)
		}
	})

	t.Run("release then re-lock succeeds", func(t *testing.T) {
		dir := t.TempDir()
		first, err := osFS{}.Lock(dir)
		if err != nil {
			t.Fatalf("first Lock: %v", err)
		}
		if err := first.release(); err != nil {
			t.Fatalf("release: %v", err)
		}

		second, err := osFS{}.Lock(dir)
		if err != nil {
			t.Fatalf("Lock after release: %v", err)
		}
		if err := second.release(); err != nil {
			t.Fatalf("second release: %v", err)
		}
	})
}
