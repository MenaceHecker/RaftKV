package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MenaceHecker/raftkv/internal/gateway"
	"github.com/MenaceHecker/raftkv/internal/gateway/fakecluster"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

const (
	tlsTestDomain = "api.example.test"
	tlsTestOrigin = "https://dash.example.test"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// baseArgs is a complete, valid command line without --listen and without any --tls-* flag.
func baseArgs(t *testing.T) (args []string, state string) {
	t.Helper()
	dir := t.TempDir()
	admin := writeSecret(t, dir, "admin", "admin-token-aaaa\n")
	node := writeSecret(t, dir, "node", "node-token-bbbb\n")
	state = filepath.Join(dir, "state")
	return []string{
		"--nodes=" + testNodes, "--admin-token-file=" + admin, "--node-token-file=" + node,
		"--state-dir=" + state, "--dashboard-origin=" + tlsTestOrigin,
	}, state
}

// eventually polls f until it returns true or the deadline passes.
func eventually(t *testing.T, d time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out after %v waiting for %s", d, what)
		case <-tick.C:
		}
	}
}

func TestTLSFlagsRequireDomain(t *testing.T) {
	base, state := baseArgs(t)
	cacheDir := filepath.Join(t.TempDir(), "c")

	// Every --tls-* flag without --tls-domain is refused, and names both flags.
	for name, extra := range map[string][]string{
		"http listen":      {"--tls-http-listen=:8081"},
		"cache dir":        {"--tls-cache-dir=" + cacheDir},
		"staging":          {"--tls-staging"},
		"email":            {"--tls-email=ops@example.test"},
		"empty domain":     {"--tls-domain=", "--tls-staging"},
		"blank domain":     {"--tls-domain=   ", "--tls-email=ops@example.test"},
		"http listen only": {"--tls-http-listen=:80"},
	} {
		_, err := loadSettings(append(append([]string{}, base...), extra...))
		if err == nil {
			t.Errorf("%s: accepted without --tls-domain", name)
			continue
		}
		if !strings.Contains(err.Error(), "--tls-domain") {
			t.Errorf("%s: error %q does not say --tls-domain is required", name, err)
		}
	}

	// A bad domain is refused: empty, wildcard, IP, port, scheme, path, single label, junk.
	for name, d := range map[string]string{
		"wildcard":       "*.example.test",
		"bare wildcard":  "*",
		"ipv4":           "203.0.113.5",
		"ipv6":           "2001:db8::1",
		"with port":      "api.example.test:443",
		"with scheme":    "https://api.example.test",
		"with path":      "api.example.test/x",
		"single label":   "localhost",
		"trailing dot":   "api.example.test.",
		"leading dot":    ".example.test",
		"space":          "api example.test",
		"underscore":     "api_x.example.test",
		"leading hyphen": "-a.example.test",
		"two domains":    "a.example.test,b.example.test",
	} {
		_, err := loadSettings(append(append([]string{}, base...), "--tls-domain="+d))
		if err == nil {
			t.Errorf("domain %s (%q): accepted", name, d)
			continue
		}
		if !strings.Contains(err.Error(), "--tls-domain") {
			t.Errorf("domain %s: error %q does not name --tls-domain", name, err)
		}
	}

	// A malformed email, and the HTTP redirect listener on the HTTPS address, are refused.
	if _, err := loadSettings(append(append([]string{}, base...), "--tls-domain="+tlsTestDomain, "--tls-email=not-an-email")); err == nil || !strings.Contains(err.Error(), "--tls-email") {
		t.Errorf("bad email: %v", err)
	}
	if _, err := loadSettings(append(append([]string{}, base...), "--tls-domain="+tlsTestDomain, "--listen=127.0.0.1:8443", "--tls-http-listen=127.0.0.1:8443")); err == nil || !strings.Contains(err.Error(), "--tls-http-listen") {
		t.Errorf("same address twice: %v", err)
	}

	// No --tls-* flag at all: plain HTTP exactly as before.
	s, err := loadSettings(base)
	if err != nil {
		t.Fatalf("plain command line refused: %v", err)
	}
	if s.tls.enabled() || s.listen != ":8080" {
		t.Fatalf("plain mode changed: tls enabled %v, listen %q, want false and :8080", s.tls.enabled(), s.listen)
	}
	if _, err := os.Stat(filepath.Join(state, "autocert")); !os.IsNotExist(err) {
		t.Fatalf("plain mode created an autocert directory (stat err %v)", err)
	}

	// A domain alone enables TLS with the documented defaults; the domain is lower-cased.
	s, err = loadSettings(append(append([]string{}, base...), "--tls-domain=API.Example.Test"))
	if err != nil {
		t.Fatalf("a valid domain was refused: %v", err)
	}
	if !s.tls.enabled() || s.tls.domain != tlsTestDomain || s.tls.httpListen != ":80" || s.listen != ":8080" {
		t.Fatalf("tls settings = %+v listen %q, want domain %s, redirect on :80", s.tls, s.listen, tlsTestDomain)
	}
}

func TestHostPolicyOnlyConfiguredDomain(t *testing.T) {
	m := newTLSManager(tlsSettings{domain: tlsTestDomain, cacheDir: t.TempDir()})
	if m.HostPolicy == nil {
		t.Fatal("the manager has no HostPolicy: it would request certificates for any name")
	}
	ctx := context.Background()
	for _, ok := range []string{tlsTestDomain, "API.Example.Test", "Api.example.test"} {
		if err := m.HostPolicy(ctx, ok); err != nil {
			t.Errorf("host %q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{
		"", "evil.test", "example.test", "xapi.example.test", "sub.api.example.test",
		"api.example.test.evil.test", "api.example.test.", "203.0.113.5", "[::1]", "api.example.test:443", "*.example.test",
	} {
		if err := m.HostPolicy(ctx, bad); err == nil {
			t.Errorf("host %q accepted", bad)
		}
	}
	// The same policy sits in front of certificate issuance: no certificate, and no network, for another SNI.
	for _, sni := range []string{"evil.test", "203.0.113.5", "api.example.test.evil.test"} {
		if c, err := m.GetCertificate(&tls.ClientHelloInfo{ServerName: sni}); err == nil || c != nil {
			t.Errorf("GetCertificate(%q) = %v, %v, want an error", sni, c, err)
		}
	}
}

func TestTLSConfigALPNAndMinVersion(t *testing.T) {
	cfg := tlsConfig(newTLSManager(tlsSettings{domain: tlsTestDomain, cacheDir: t.TempDir()}))
	have := map[string]bool{}
	for _, p := range cfg.NextProtos {
		have[p] = true
	}
	for _, want := range []string{"h2", "http/1.1", acme.ALPNProto} {
		if !have[want] {
			t.Errorf("NextProtos %v lacks %q", cfg.NextProtos, want)
		}
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x, want TLS 1.2 (%#x)", cfg.MinVersion, tls.VersionTLS12)
	}
	if cfg.GetCertificate == nil {
		t.Error("no GetCertificate: nothing would answer a handshake")
	}
}

func TestRedirectUsesConfiguredDomain(t *testing.T) {
	h := redirectHandler(tlsTestDomain)
	cases := []struct{ method, target, host, want string }{
		{"GET", "/", "evil.test", "https://api.example.test/"},
		{"GET", "/v1/kv/a?x=1&y=%2F", "evil.test", "https://api.example.test/v1/kv/a?x=1&y=%2F"},
		{"POST", "/v1/kv/a/cas", "evil.test:80", "https://api.example.test/v1/kv/a/cas"},
		{"PUT", "/v1/kv/k?z=%20", "203.0.113.5", "https://api.example.test/v1/kv/k?z=%20"},
		{"DELETE", "/v1/kv/k", tlsTestDomain, "https://api.example.test/v1/kv/k"},
		{"HEAD", "/healthz", "", "https://api.example.test/healthz"},
		{"GET", "//evil.test/x", "evil.test", "https://api.example.test//evil.test/x"},
		{"GET", "/a/../b?c=d", "evil.test", "https://api.example.test/a/../b?c=d"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.target, nil)
		req.Host = c.host
		req.Header.Set("X-Forwarded-Host", "evil.test")
		req.Header.Set("Forwarded", "host=evil.test")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusPermanentRedirect {
			t.Errorf("%s %s Host %q: status %d, want 308", c.method, c.target, c.host, rec.Code)
		}
		if got := rec.Header().Get("Location"); got != c.want {
			t.Errorf("%s %s Host %q: Location %q, want %q", c.method, c.target, c.host, got, c.want)
		}
	}

	// OPTIONS * has no path to keep.
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.RequestURI = "*"
	req.Host = "evil.test"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Location"); rec.Code != http.StatusPermanentRedirect || got != "https://api.example.test/" {
		t.Errorf("OPTIONS *: %d %q, want 308 https://api.example.test/", rec.Code, got)
	}

	// Over a real connection with a hostile Host header.
	srv := httptest.NewServer(h)
	defer srv.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r2, _ := http.NewRequest("GET", srv.URL+"/v1/cluster?a=b", nil)
	r2.Host = "evil.test"
	resp, err := client.Do(r2)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if loc := resp.Header.Get("Location"); resp.StatusCode != 308 || loc != "https://api.example.test/v1/cluster?a=b" {
		t.Fatalf("real request: %d %q", resp.StatusCode, loc)
	}
}

func TestHSTSOnlyInTLSMode(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	// TLS mode adds it.
	rec := httptest.NewRecorder()
	hsts(inner).ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	v := rec.Header().Get("Strict-Transport-Security")
	if !strings.HasPrefix(v, "max-age=") || strings.Contains(v, "max-age=0") {
		t.Fatalf("Strict-Transport-Security = %q, want a positive max-age", v)
	}
	if strings.Contains(v, "includeSubDomains") || strings.Contains(v, "preload") {
		t.Fatalf("Strict-Transport-Security = %q: it must not cover sibling hosts or preload", v)
	}

	// The plain HTTP path (HSTS over HTTP would be ignored and wrong) and the redirect never carry it.
	rec = httptest.NewRecorder()
	newHTTPServer(":0", inner).Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if v := rec.Header().Get("Strict-Transport-Security"); v != "" {
		t.Fatalf("plain server sent Strict-Transport-Security %q", v)
	}
	rec = httptest.NewRecorder()
	redirectHandler(tlsTestDomain).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if v := rec.Header().Get("Strict-Transport-Security"); v != "" {
		t.Fatalf("redirect sent Strict-Transport-Security %q", v)
	}
}

func TestPlainHTTPModeUnchanged(t *testing.T) {
	args, _ := baseArgs(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, append(args, "--listen="+addr), quietLogger) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v on shutdown, want nil", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("run did not return within 15s of shutdown")
		}
	}()

	var resp *http.Response
	eventually(t, 15*time.Second, "the plain HTTP gateway to answer /healthz", func() bool {
		r, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}
		resp = r
		return true
	})
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"ok"`) {
		t.Fatalf("/healthz = %d %q", resp.StatusCode, body)
	}
	if resp.TLS != nil || resp.Header.Get("Strict-Transport-Security") != "" {
		t.Fatalf("plain mode answered with TLS %v / HSTS %q", resp.TLS != nil, resp.Header.Get("Strict-Transport-Security"))
	}
	// It does not speak TLS.
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, Timeout: 5 * time.Second}
	if r, err := c.Get("https://" + addr + "/healthz"); err == nil {
		_ = r.Body.Close()
		t.Fatal("plain mode answered an HTTPS request")
	}
}

func TestCacheDirModeAndStagingSeparation(t *testing.T) {
	mode := func(p string) os.FileMode {
		t.Helper()
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() {
			t.Fatalf("%s is not a directory", p)
		}
		return fi.Mode().Perm()
	}

	// Default: <state-dir>/autocert, created 0700.
	base, state := baseArgs(t)
	s, err := loadSettings(append(append([]string{}, base...), "--tls-domain="+tlsTestDomain))
	if err != nil {
		t.Fatal(err)
	}
	prod := filepath.Join(state, "autocert")
	if s.tls.cacheDir != prod {
		t.Fatalf("default cache dir %q, want %q", s.tls.cacheDir, prod)
	}
	if m := mode(prod); m != 0o700 {
		t.Fatalf("cache dir mode %o, want 700", m)
	}

	// Staging: a different directory, below the production one, also 0700; production is not touched.
	base2, state2 := baseArgs(t)
	st, err := loadSettings(append(append([]string{}, base2...), "--tls-domain="+tlsTestDomain, "--tls-staging"))
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(state2, "autocert", "staging")
	if st.tls.cacheDir != want || st.tls.cacheDir == prod {
		t.Fatalf("staging cache dir %q, want %q", st.tls.cacheDir, want)
	}
	if m := mode(want); m != 0o700 {
		t.Fatalf("staging cache dir mode %o, want 700", m)
	}
	if m := mode(filepath.Join(state2, "autocert")); m != 0o700 {
		t.Fatalf("autocert dir mode %o, want 700", m)
	}
	// The manager caches in exactly that directory.
	if c, ok := newTLSManager(st.tls).Cache.(autocert.DirCache); !ok || string(c) != want {
		t.Fatalf("manager cache = %v, want DirCache(%q)", newTLSManager(st.tls).Cache, want)
	}
	if c, ok := newTLSManager(s.tls).Cache.(autocert.DirCache); !ok || string(c) != prod {
		t.Fatalf("manager cache = %v, want DirCache(%q)", newTLSManager(s.tls).Cache, prod)
	}

	// An explicit directory: 0700 is accepted and left alone; anything group/other-accessible is refused.
	for _, c := range []struct {
		perm os.FileMode
		ok   bool
	}{{0o700, true}, {0o500, true}, {0o750, false}, {0o705, false}, {0o755, false}, {0o777, false}, {0o710, false}} {
		dir := filepath.Join(t.TempDir(), "cache")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, c.perm); err != nil {
			t.Fatal(err)
		}
		b, _ := baseArgs(t)
		_, err := loadSettings(append(b, "--tls-domain="+tlsTestDomain, "--tls-cache-dir="+dir))
		if c.ok && err != nil {
			t.Errorf("cache dir mode %o refused: %v", c.perm, err)
		}
		if !c.ok && (err == nil || !strings.Contains(err.Error(), "--tls-cache-dir")) {
			t.Errorf("cache dir mode %o: error %v, want a refusal naming --tls-cache-dir", c.perm, err)
		}
		_ = os.Chmod(dir, 0o700)
	}

	// A regular file is not a cache directory.
	f := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(f, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	b, _ := baseArgs(t)
	if _, err := loadSettings(append(b, "--tls-domain="+tlsTestDomain, "--tls-cache-dir="+f)); err == nil {
		t.Error("a regular file was accepted as the cache directory")
	}

	// Staging below an existing, too-open production cache is refused too.
	open := filepath.Join(t.TempDir(), "cache")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ = baseArgs(t)
	if _, err := loadSettings(append(b, "--tls-domain="+tlsTestDomain, "--tls-staging", "--tls-cache-dir="+open)); err == nil {
		t.Error("staging below a 0755 cache directory was accepted")
	}
	_ = os.Chmod(open, 0o700)
}

func TestStagingDirectorySelection(t *testing.T) {
	if got := acmeDirectoryURL(true); got != "https://acme-staging-v02.api.letsencrypt.org/directory" {
		t.Errorf("staging directory = %q", got)
	}
	if got := acmeDirectoryURL(false); got != acme.LetsEncryptURL {
		t.Errorf("production directory = %q, want %q", got, acme.LetsEncryptURL)
	}
	for _, staging := range []bool{false, true} {
		m := newTLSManager(tlsSettings{domain: tlsTestDomain, cacheDir: t.TempDir(), staging: staging, email: "ops@example.test"})
		if m.Client == nil || m.Client.DirectoryURL != acmeDirectoryURL(staging) {
			t.Errorf("staging=%v: client %+v, want directory %q", staging, m.Client, acmeDirectoryURL(staging))
		}
		if m.Prompt == nil || !m.Prompt("tos") {
			t.Errorf("staging=%v: the manager does not accept the terms of service", staging)
		}
		if m.Email != "ops@example.test" {
			t.Errorf("staging=%v: email %q", staging, m.Email)
		}
	}
}

// dialTLS is tls.Dial with a deadline on connect and handshake.
func dialTLS(addr string, cfg *tls.Config) (*tls.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, cfg)
}

// testPKI makes a CA and a leaf for host, both ECDSA.
func testPKI(t *testing.T, host string) (*x509.CertPool, tls.Certificate) {
	t.Helper()
	key := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	now := time.Now()
	caKey := key()
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	leafKey := key()
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return pool, tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

// tlsRig starts the two TLS-mode servers on loopback with a fake certificate source (no ACME).
type tlsRig struct {
	secureAddr, redirectAddr string
	pool                     *x509.CertPool
	stop                     func() error
}

func startTLSRig(t *testing.T, h http.Handler) *tlsRig {
	t.Helper()
	pool, cert := testPKI(t, tlsTestDomain)
	ts := tlsSettings{domain: tlsTestDomain, httpListen: "127.0.0.1:0", cacheDir: t.TempDir()}
	cfg := tlsConfig(newTLSManager(ts))
	// The fake certificate source with its own host policy: only the configured name gets a certificate.
	cfg.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName != tlsTestDomain {
			return nil, errors.New("host not allowed")
		}
		return &cert, nil
	}
	secure, redirect := newTLSServers(ts, "127.0.0.1:0", h, cfg)
	ls, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lr, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveTLS(ctx, quietLogger, secure, redirect, ls, lr) }()
	r := &tlsRig{secureAddr: ls.Addr().String(), redirectAddr: lr.Addr().String(), pool: pool}
	r.stop = func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			return errors.New("serveTLS did not return within 15s of cancel")
		}
	}
	t.Cleanup(func() { _ = ls.Close(); _ = lr.Close() })
	return r
}

func (r *tlsRig) client() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2: true,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, r.secureAddr)
			},
			TLSClientConfig: &tls.Config{RootCAs: r.pool, ServerName: tlsTestDomain},
		},
	}
}

func TestTLSServerServesGatewayHandler(t *testing.T) {
	g, err := gateway.New(gateway.Config{
		Cluster: fakecluster.New(), Clock: systemClock{}, Rand: systemRand{}, Logger: quietLogger,
		StateDir: t.TempDir(), DashboardOrigin: tlsTestOrigin, AdminToken: "admin-token-aaaa", KeyspaceReset: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	rig := startTLSRig(t, g.Handler())
	c := rig.client()

	get := func(origin string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", "https://"+tlsTestDomain+"/healthz", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		var resp *http.Response
		eventually(t, 10*time.Second, "the TLS server to answer /healthz", func() bool {
			r, err := c.Do(req)
			if err != nil {
				return false
			}
			resp = r
			return true
		})
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp
	}

	resp := get(tlsTestOrigin)
	if resp.StatusCode != 200 {
		t.Fatalf("/healthz = %d", resp.StatusCode)
	}
	if resp.ProtoMajor != 2 {
		t.Errorf("negotiated HTTP/%d, want h2", resp.ProtoMajor)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Errorf("TLS state %+v, want TLS 1.2 or newer", resp.TLS)
	}
	if v := resp.Header.Get("Strict-Transport-Security"); !strings.HasPrefix(v, "max-age=") {
		t.Errorf("Strict-Transport-Security = %q", v)
	}
	// CORS is exactly the dashboard-origin rule: the dashboard is allowed, anyone else is not, never "*".
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != tlsTestOrigin {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, tlsTestOrigin)
	}
	if got := get("https://evil.test").Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin for another origin = %q, want none", got)
	}

	// A handshake for another name gets no certificate, and TLS below 1.2 is refused.
	conn, err := dialTLS(rig.secureAddr, &tls.Config{ServerName: "evil.test", InsecureSkipVerify: true})
	if err == nil {
		_ = conn.Close()
		t.Error("a handshake for evil.test succeeded")
	}
	conn, err = dialTLS(rig.secureAddr, &tls.Config{ServerName: tlsTestDomain, RootCAs: rig.pool, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11})
	if err == nil {
		_ = conn.Close()
		t.Error("a TLS 1.1 handshake succeeded")
	}

	// Port 80 side, through the same rig: redirect to the configured name, never the Host header.
	rc := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequest("GET", "http://"+rig.redirectAddr+"/v1/cluster?x=1", nil)
	req.Host = "evil.test"
	rr, err := rc.Do(req)
	if err != nil {
		t.Fatalf("redirect listener: %v", err)
	}
	_ = rr.Body.Close()
	if loc := rr.Header.Get("Location"); rr.StatusCode != 308 || loc != "https://"+tlsTestDomain+"/v1/cluster?x=1" {
		t.Errorf("redirect listener: %d %q", rr.StatusCode, loc)
	}
	if err := rig.stop(); err != nil {
		t.Errorf("serveTLS = %v", err)
	}
}

func TestShutdownStopsBothServers(t *testing.T) {
	rig := startTLSRig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	eventually(t, 10*time.Second, "both listeners to answer", func() bool {
		c, err := dialTLS(rig.secureAddr, &tls.Config{ServerName: tlsTestDomain, RootCAs: rig.pool})
		if err != nil {
			return false
		}
		_ = c.Close()
		r, err := (&http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Get("http://" + rig.redirectAddr + "/")
		if err != nil {
			return false
		}
		_ = r.Body.Close()
		return true
	})
	if err := rig.stop(); err != nil {
		t.Fatalf("serveTLS after cancel = %v, want nil", err)
	}
	for name, addr := range map[string]string{"https": rig.secureAddr, "redirect": rig.redirectAddr} {
		if c, err := net.DialTimeout("tcp", addr, 2*time.Second); err == nil {
			_ = c.Close()
			t.Errorf("the %s listener still accepts connections after shutdown", name)
		}
	}
}
