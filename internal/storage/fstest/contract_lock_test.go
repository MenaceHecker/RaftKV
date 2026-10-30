//go:build unix

package fstest_test

import (
	"testing"

	"github.com/MenaceHecker/raftkv/internal/storage/fstest"
)

func TestOSFSLockContract(t *testing.T) {
	fstest.RunLockContract(t, osFactory)
}
