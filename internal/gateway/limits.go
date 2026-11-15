package gateway

import (
	"net/netip"
	"sync"
	"time"
	"unicode/utf8"
)

// limiter is a token bucket per client address, held in memory only. It is
// never logged and never written to disk.
//
// A bucket that has refilled to its burst carries no information, so idle
// entries are dropped on a sweep. If the table is still full when a new
// address arrives, an arbitrary entry is evicted (map order, so an attacker
// cannot choose the victim): memory stays bounded whatever the traffic.
type limiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	max       int
	entries   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(rate float64, burst, capacity int) *limiter {
	return &limiter{rate: rate, burst: float64(burst), max: capacity, entries: map[string]*bucket{}}
}

func (l *limiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// allow spends a token for key. If there is none it returns how long until
// there will be.
func (l *limiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if now.Sub(l.lastSweep) >= time.Second {
		l.lastSweep = now
		for k, b := range l.entries {
			if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
				delete(l.entries, k)
			}
		}
	}
	b, ok := l.entries[key]
	if !ok {
		if len(l.entries) >= l.max {
			for k := range l.entries {
				delete(l.entries, k)
				break
			}
		}
		b = &bucket{tokens: l.burst, last: now}
		l.entries[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	return false, wait
}

// clientKey identifies the caller by the connection's address only. The
// gateway faces the Internet directly (RUNBOOK 10.1), so X-Forwarded-For is
// attacker-controlled and is never read. IPv6 callers are grouped by /64,
// because one subscriber usually owns a whole /64.
func clientKey(remoteAddr string) string {
	ap, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	a := ap.Addr().Unmap()
	if a.Is6() {
		return netip.PrefixFrom(a, 64).Masked().String()
	}
	return a.String()
}

// keyProblem validates a public key. It returns an HTTP status and message,
// or 0 when the key is fine.
func keyProblem(key string) (int, string) {
	if len(key) >= len(ReservedPrefix) && key[:len(ReservedPrefix)] == ReservedPrefix {
		return 403, "the __verify/ prefix is reserved"
	}
	switch {
	case key == "":
		return 400, "key is empty"
	case len(key) > MaxKeyBytes:
		return 400, "key is longer than 64 bytes"
	case !utf8.ValidString(key):
		return 400, "key is not valid UTF-8"
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return 400, "key contains a control character"
		}
		if r == '/' {
			return 400, "key must not contain '/'"
		}
	}
	return 0, ""
}
