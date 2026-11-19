package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

// TLS for the public gateway (RUNBOOK 10.3 D6): Let's Encrypt certificates through
// autocert for ONE configured domain, TLS-ALPN-01 on the HTTPS port (so port 80 is only a
// redirect), a 308 redirect to that configured domain, and HSTS. Without --tls-domain
// nothing here runs and the gateway serves plain HTTP exactly as before.

// stagingDirectoryURL is the Let's Encrypt staging ACME directory: certificates from it are
// not trusted by browsers, so a trial run cannot burn the production rate limits.
const stagingDirectoryURL = "https://acme-staging-v02.api.letsencrypt.org/directory"

// hstsValue pins HTTPS for the configured host for a year. No includeSubDomains (the domain's
// siblings are not ours) and no preload (a one-way door).
const hstsValue = "max-age=31536000"

// tlsSettings is the validated --tls-* command line. The zero value is plain HTTP.
type tlsSettings struct {
	domain     string // lower-case, one fully qualified name; "" means no TLS
	httpListen string // the redirect listener
	cacheDir   string // where autocert keeps keys and certificates; the staging one when staging
	staging    bool
	email      string
}

func (t tlsSettings) enabled() bool { return t.domain != "" }

func defineTLSFlags(fs *flag.FlagSet, t *tlsSettings) {
	fs.StringVar(&t.domain, "tls-domain", "", "serve HTTPS for this one domain with Let's Encrypt certificates (empty: plain HTTP, for local use)")
	fs.StringVar(&t.httpListen, "tls-http-listen", ":80", "address of the plain HTTP listener that only redirects to https://<--tls-domain> (needs --tls-domain)")
	fs.StringVar(&t.cacheDir, "tls-cache-dir", "", "autocert key and certificate cache, created mode 0700 (default <state-dir>/autocert; needs --tls-domain)")
	fs.BoolVar(&t.staging, "tls-staging", false, "use the Let's Encrypt staging CA and a separate cache subdirectory (needs --tls-domain)")
	fs.StringVar(&t.email, "tls-email", "", "contact address for the ACME account (optional; needs --tls-domain)")
}

// validateTLS fails closed. A --tls-* flag without --tls-domain is an error, never a silent
// fall back to plain HTTP, and the domain must be exactly one plain host name: no wildcard,
// no IP address, no port. On success t.cacheDir is the directory to use, created and checked.
func validateTLS(fs *flag.FlagSet, t *tlsSettings, stateDir, listen string) error {
	var given []string
	fs.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "tls-") && f.Name != "tls-domain" {
			given = append(given, "--"+f.Name)
		}
	})
	t.domain = strings.TrimSpace(t.domain)
	if t.domain == "" {
		if len(given) > 0 {
			return fmt.Errorf("%s need --tls-domain: refusing to fall back to plain HTTP", strings.Join(given, ", "))
		}
		return nil
	}
	d, err := parseDomain(t.domain)
	if err != nil {
		return fmt.Errorf("--tls-domain: %w", err)
	}
	t.domain = d
	if t.email != "" {
		if a, err := mail.ParseAddress(t.email); err != nil || a.Address != t.email {
			return fmt.Errorf("--tls-email: %q is not a plain e-mail address", t.email)
		}
	}
	if t.httpListen == "" {
		return errors.New("--tls-http-listen must not be empty")
	}
	if t.httpListen == listen {
		return fmt.Errorf("--tls-http-listen and --listen are both %q", listen)
	}
	root := t.cacheDir
	if root == "" {
		root = filepath.Join(stateDir, "autocert")
	}
	dirs := []string{root}
	if t.staging {
		// A staging certificate must never be served later, so
		// staging keys and certificates live apart from the production ones.
		dirs = append(dirs, filepath.Join(root, "staging"))
	}
	for _, dir := range dirs {
		if err := ensurePrivateDir(dir); err != nil {
			return fmt.Errorf("--tls-cache-dir: %w", err)
		}
	}
	t.cacheDir = dirs[len(dirs)-1]
	return nil
}

// parseDomain accepts one fully qualified, ASCII (punycode for IDN) host name and returns it
// lower-cased.
func parseDomain(s string) (string, error) {
	d := strings.ToLower(s)
	if len(d) > 253 {
		return "", errors.New("too long")
	}
	labels := strings.Split(d, ".")
	if len(labels) < 2 {
		return "", fmt.Errorf("%q is not a fully qualified domain name", s)
	}
	for _, l := range labels {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return "", fmt.Errorf("%q is not a valid domain name (empty, over-long or hyphen-edged label)", s)
		}
		for _, c := range l {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return "", fmt.Errorf("%q is not a plain domain name (no wildcard, scheme, port, path or IP address)", s)
			}
		}
	}
	if tld := labels[len(labels)-1]; strings.Trim(tld, "0123456789") == "" {
		return "", fmt.Errorf("%q looks like an IP address, not a domain name", s)
	}
	return d, nil
}

// ensurePrivateDir creates dir mode 0700 if it is missing and refuses one that is not a
// directory or that group or others can touch: it holds the TLS private keys.
func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s has mode %o: it holds private keys and must not be accessible to group or others (chmod 700)", dir, fi.Mode().Perm())
	}
	return nil
}

func acmeDirectoryURL(staging bool) string {
	if staging {
		return stagingDirectoryURL
	}
	return acme.LetsEncryptURL
}

// hostPolicy allows the one configured domain, in any letter case. autocert.HostWhitelist
// does the matching (exact, no suffix or wildcard rules); host names are case-insensitive, so
// it is given the lower-cased name.
func hostPolicy(domain string) autocert.HostPolicy {
	wl := autocert.HostWhitelist(domain)
	return func(ctx context.Context, host string) error { return wl(ctx, strings.ToLower(host)) }
}

// newTLSManager builds the autocert manager. Its HostPolicy lets it obtain a certificate for
// the one configured domain only; any other SNI name is refused before an ACME order is made.
func newTLSManager(t tlsSettings) *autocert.Manager {
	return &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(t.cacheDir),
		HostPolicy: hostPolicy(t.domain),
		Email:      t.email,
		Client:     &acme.Client{DirectoryURL: acmeDirectoryURL(t.staging)},
	}
}

// tlsConfig is the manager's config (h2, http/1.1 and acme-tls/1 for TLS-ALPN-01) with a
// TLS 1.2 floor.
func tlsConfig(m *autocert.Manager) *tls.Config {
	cfg := m.TLSConfig()
	cfg.MinVersion = tls.VersionTLS12
	return cfg
}

// redirectHandler sends every request to https://<domain> with 308, which keeps the method
// and body. The target is built from the configured domain, never from the Host header
// (autocert's own HTTPHandler uses the Host header, which a client controls).
func redirectHandler(domain string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		if !strings.HasPrefix(uri, "/") { // "OPTIONS *" has no path
			uri = "/"
		}
		// The host part is the configured domain; only the path and query come from the request.
		w.Header().Set("Location", "https://"+domain+uri)
		w.WriteHeader(http.StatusPermanentRedirect)
	})
}

// hsts adds Strict-Transport-Security. It wraps the HTTPS handler only: over plain HTTP the
// header is ignored by browsers.
func hsts(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Strict-Transport-Security", hstsValue)
		h.ServeHTTP(w, r)
	})
}

// newTLSServers returns the HTTPS server (gateway handler plus HSTS) and the redirect server,
// both with the gateway's usual timeouts.
func newTLSServers(t tlsSettings, httpsAddr string, h http.Handler, cfg *tls.Config) (secure, redirect *http.Server) {
	secure = newHTTPServer(httpsAddr, hsts(h))
	secure.TLSConfig = cfg
	redirect = newHTTPServer(t.httpListen, redirectHandler(t.domain))
	return secure, redirect
}

// serveTLS serves both servers on the given listeners until ctx ends or one fails, then shuts
// both down (5 s grace) and returns once both have stopped.
func serveTLS(ctx context.Context, logger *slog.Logger, secure, redirect *http.Server, lnSecure, lnRedirect net.Listener) error {
	errc := make(chan error, 2)
	// Certificates come from TLSConfig.GetCertificate: no cert or key file names.
	go func() { errc <- secure.ServeTLS(lnSecure, "", "") }()
	go func() { errc <- redirect.Serve(lnRedirect) }()

	var errs []error
	got := 0
	select {
	case err := <-errc:
		got++
		if !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs = append(errs, secure.Shutdown(shutdown), redirect.Shutdown(shutdown))
	for ; got < 2; got++ {
		if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, err)
		}
	}
	logger.Info("tls servers stopped")
	return errors.Join(errs...)
}

// serveTLSMode listens on --listen (HTTPS) and --tls-http-listen (redirect) and serves until ctx ends.
func serveTLSMode(ctx context.Context, logger *slog.Logger, s *settings, h http.Handler) error {
	lnSecure, err := net.Listen("tcp", s.listen)
	if err != nil {
		return err
	}
	lnRedirect, err := net.Listen("tcp", s.tls.httpListen)
	if err != nil {
		_ = lnSecure.Close()
		return err
	}
	secure, redirect := newTLSServers(s.tls, s.listen, h, tlsConfig(newTLSManager(s.tls)))
	logger.Info("listening with TLS", "https", s.listen, "redirect", s.tls.httpListen,
		"domain", s.tls.domain, "staging", s.tls.staging)
	return serveTLS(ctx, logger, secure, redirect, lnSecure, lnRedirect)
}
