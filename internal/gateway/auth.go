package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// authenticated reports whether the request carries the admin Bearer token.
//
// Both sides are hashed first, so the comparison is over two fixed-size
// digests: neither the token's content nor its length shows in the timing, and
// ConstantTimeCompare does the byte comparison itself.
func (g *Gateway) authenticated(r *http.Request) bool {
	presented := bearerToken(r)
	digest := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(digest[:], g.adminDigest[:]) == 1
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const scheme = "bearer "
	if len(h) > len(scheme) && strings.EqualFold(h[:len(scheme)], scheme) {
		return strings.TrimSpace(h[len(scheme):])
	}
	return ""
}

// requireAdmin guards a route. A missing token, a malformed header and a wrong
// token all produce the same response, so a caller learns nothing from it.
func (g *Gateway) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !g.authenticated(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}
