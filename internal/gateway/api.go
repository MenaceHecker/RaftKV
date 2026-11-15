package gateway

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func getOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, "GET")
			return
		}
		h(w, r)
	}
}

func (g *Gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", getOnly(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	mux.HandleFunc("/v1/cluster", getOnly(g.handleCluster))
	mux.HandleFunc("/v1/kv/{$}", g.handleEmptyKey)
	mux.HandleFunc("/v1/kv/{key}", g.handleKV)
	mux.HandleFunc("/v1/kv/{key}/cas", g.handleCAS)
	mux.HandleFunc("/v1/admin/reset", g.requireAdmin(g.handleAdminReset))
	mux.HandleFunc("/v1/events", g.handleEvents)
	mux.HandleFunc("/v1/chaos/kill-leader", g.chaosHandler("kill"))
	mux.HandleFunc("/v1/chaos/kill/{id}", g.chaosHandler("kill"))
	mux.HandleFunc("/v1/chaos/pause/{id}", g.chaosHandler("pause"))
	mux.HandleFunc("/v1/chaos/partition", g.chaosHandler("partition"))
	mux.HandleFunc("/v1/chaos/delay", g.chaosHandler("delay"))
	mux.HandleFunc("/v1/chaos/heal", g.chaosHandler("heal"))
	mux.HandleFunc("/v1/verify/run", g.handleVerifyRun)
	mux.HandleFunc("/v1/verify/last", g.handleVerifyLast)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	return g.logRequests(g.cors(g.limit(mux)))
}

// limit applies the per-address rate limit. /healthz is exempt: it touches
// nothing, and a load balancer or the dashboard's liveness check must not be
// starved by it.
func (g *Gateway) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		ok, wait := g.lim.allow(clientKey(r.RemoteAddr), g.clk.Now())
		if !ok {
			secs := int((wait + time.Second - 1) / time.Second)
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// cors allows the dashboard origin and nobody else. "*" is never sent.
func (g *Gateway) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowed := origin != "" && origin == g.cfg.DashboardOrigin
		w.Header().Add("Vary", "Origin")
		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			if !allowed {
				writeError(w, http.StatusForbidden, "origin not allowed")
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the real writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// logRequests writes one structured line per request: method, route template,
// status, latency and a request id. Never the path (it holds keys), the
// address, a header or a body.
func (g *Gateway) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := g.clk.Now()
		id := strconv.FormatUint(g.rnd.Uint64(), 16)
		w.Header().Set("X-Request-Id", id)
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		route := r.Pattern // set by the mux; empty if nothing matched
		if route == "" {
			route = "unmatched"
		}
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		g.log.Info("request",
			"method", r.Method,
			"route", route,
			"status", rec.status,
			"latency_ms", g.clk.Now().Sub(start).Milliseconds(),
			"request_id", id)
	})
}
