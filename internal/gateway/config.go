package gateway

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"
)

// Limits from RUNBOOK 10.6 and the D5 card.
const (
	MaxKeyBytes   = 64
	MaxValueBytes = 1024
	// MaxBodyBytes bounds what is read from a request. It is larger than
	// MaxValueBytes because the value arrives inside a JSON document; the
	// value limit itself is enforced after parsing.
	MaxBodyBytes     = 2048
	DefaultMaxKeys   = 10000
	DefaultPoolSize  = 8
	DefaultRate      = 20 // requests per second per client address
	DefaultBurst     = 20
	DefaultTimeout   = 3 * time.Second
	DefaultReset     = 6 * time.Hour
	DefaultPollEvery = time.Second
	// ReservedPrefix belongs to the verification run (D5b); the public API
	// refuses it so the two key spaces stay disjoint.
	ReservedPrefix = "__verify/"
)

// Config holds everything a Gateway needs. Cluster, Clock, Rand, StateDir,
// DashboardOrigin and AdminToken are required; zero numeric fields take the
// defaults above.
type Config struct {
	Cluster Cluster
	Clock   Clock
	Rand    Rand
	Logger  *slog.Logger

	StateDir        string
	DashboardOrigin string
	AdminToken      string

	KeyspaceReset  time.Duration
	RequestTimeout time.Duration
	PollTimeout    time.Duration
	PoolSize       int
	MaxKeys        int
	RatePerSecond  float64
	Burst          int
	MaxLimiterIPs  int
}

// LoadTokenFile reads a secret from a file. It fails closed: a missing file,
// an empty file, or a file readable by group or other is an error. The value
// is never included in an error message.
func LoadTokenFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("token file path is empty")
	}
	st, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("token file: %w", pathError(err))
	}
	if !st.Mode().IsRegular() {
		return "", errors.New("token file is not a regular file")
	}
	if st.Mode().Perm()&0o077 != 0 {
		return "", errors.New("token file must not be readable by group or other (mode 0600)")
	}
	b, err := os.ReadFile(path) //nolint:gosec // the operator supplies this path on the command line
	if err != nil {
		return "", fmt.Errorf("token file: %w", pathError(err))
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", errors.New("token file is empty")
	}
	return tok, nil
}

func pathError(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

// ParseOrigin checks the dashboard origin: scheme://host[:port], no path, and
// never a wildcard.
func ParseOrigin(s string) (string, error) {
	if s == "" {
		return "", errors.New("dashboard origin is required")
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil ||
		strings.Contains(s, "*") {
		return "", errors.New("dashboard origin must look like https://host[:port], with no wildcard and no path")
	}
	return s, nil
}
