package gateway

import (
	"context"
	"net/http"
)

// Gateway is the public HTTP front of the cluster.
//
// This is the stub (D5 commit 1): every handler answers 501 and every
// operation does nothing. The real behaviour arrives in the commits that
// follow.
type Gateway struct{}

// New builds a Gateway from cfg.
func New(_ Config) (*Gateway, error) { return &Gateway{}, nil }

// Handler is the HTTP surface.
func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not implemented", http.StatusNotImplemented)
	})
}

// Close releases the state files.
func (g *Gateway) Close() error { return nil }

// PollOnce refreshes the status cache from every node.
func (g *Gateway) PollOnce(_ context.Context) {}

// ResetIfDue deletes every registered public key when the reset interval has
// elapsed and no verification run holds the lock. It reports whether it ran.
func (g *Gateway) ResetIfDue(_ context.Context) (bool, error) { return false, nil }

// WithVerifyLock locks public writes while fn runs and releases the lock when
// fn returns, errors or panics.
func (g *Gateway) WithVerifyLock(_ func() error) error { return nil }

// LimiterSize is the number of client addresses the rate limiter remembers.
func (g *Gateway) LimiterSize() int { return 0 }

// Keys lists the registered public keys.
func (g *Gateway) Keys() []string { return nil }
