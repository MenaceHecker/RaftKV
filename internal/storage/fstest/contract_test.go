package fstest_test

import (
	"testing"

	"github.com/MenaceHecker/raftkv/internal/storage"
	"github.com/MenaceHecker/raftkv/internal/storage/fstest"
)

func osFactory(t *testing.T) (storage.FS, string) {
	return storage.NewOSFS(), t.TempDir()
}

func TestOSFSContract(t *testing.T) {
	fstest.RunContract(t, osFactory)
}
