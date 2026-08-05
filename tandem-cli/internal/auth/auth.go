// Package auth resolves how tandem-cli connects to and authenticates against the Admin
// API - base URL, optional bearer/API-key credentials, extra headers, and TLS settings
// (LLD-cli.md §4). Credentials are never required: Tandem ships the endpoints, not the
// authentication (HLD-admin-api.md §3), so this package sends whatever it was given and
// leaves enforcement to the server.
package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/alirux/tandem/tandem-cli/internal/exitcode"
)

const (
	envBaseURL = "TANDEM_ADMIN_URL"
	envToken   = "TANDEM_ADMIN_TOKEN"
	envAPIKey  = "TANDEM_ADMIN_API_KEY"
	envCACert  = "TANDEM_ADMIN_CA_CERT"
)

// DefaultTimeout is applied when Options.Timeout is left at its zero value; passing a
// negative-or-zero Timeout explicitly (via --timeout 0) disables the timeout instead -
// see Resolve.
const DefaultTimeout = 60 * time.Second

// Options is the raw --base-url/--token/... flag values, before env-var fallback and
// validation. Every field mirrors one row of LLD-cli.md §4's table. TimeoutSet
// distinguishes "the flag was not passed" (apply DefaultTimeout) from "--timeout 0"
// (disable it outright) - a bare zero-value Duration cannot express that difference.
type Options struct {
	BaseURL    string
	Token      string
	APIKey     string
	Headers    []string
	CACert     string
	Insecure   bool
	Timeout    time.Duration
	TimeoutSet bool
}

// Resolved is what a command needs to actually talk to the Admin API.
type Resolved struct {
	BaseURL       string
	HTTPClient    *http.Client
	RequestEditor func(ctx context.Context, req *http.Request) error
}

func envOrFlag(flagVal, envVar string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envVar)
}

// Resolve validates and assembles Options into a Resolved connection. warn receives one
// line of text for each condition worth surfacing on stderr (currently just --insecure);
// pass nil to suppress it, e.g. from a test.
func Resolve(o Options, warn func(string)) (*Resolved, *exitcode.Error) {
	baseURL := envOrFlag(o.BaseURL, envBaseURL)
	if baseURL == "" {
		return nil, exitcode.New(exitcode.UsageError,
			"missing base URL: pass --base-url or set %s", envBaseURL)
	}

	token := envOrFlag(o.Token, envToken)
	apiKey := envOrFlag(o.APIKey, envAPIKey)

	headers, herr := parseHeaders(o.Headers)
	if herr != nil {
		return nil, exitcode.New(exitcode.UsageError, "invalid --header: %v", herr)
	}

	tlsConfig := &tls.Config{}
	caCert := envOrFlag(o.CACert, envCACert)
	if caCert != "" {
		pemBytes, err := os.ReadFile(caCert)
		if err != nil {
			return nil, exitcode.New(exitcode.UsageError, "reading --ca-cert %s: %v", caCert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, exitcode.New(exitcode.UsageError, "--ca-cert %s: no certificates found", caCert)
		}
		tlsConfig.RootCAs = pool
	}
	if o.Insecure {
		tlsConfig.InsecureSkipVerify = true // #nosec G402 - explicit opt-in, warned below
		if warn != nil {
			warn("--insecure: TLS certificate verification is disabled")
		}
	}

	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
	switch {
	case o.TimeoutSet && o.Timeout <= 0:
		// --timeout 0: no client-side deadline.
	case o.TimeoutSet:
		httpClient.Timeout = o.Timeout
	default:
		httpClient.Timeout = DefaultTimeout
	}

	editor := func(_ context.Context, req *http.Request) error {
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}
		// Applied last so an explicit --header can override a scheme set above.
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		return nil
	}

	return &Resolved{BaseURL: baseURL, HTTPClient: httpClient, RequestEditor: editor}, nil
}

// parseHeaders turns "Name: value" strings (one per --header flag occurrence) into a
// name→value map. A later duplicate name overwrites an earlier one, matching a repeated
// flag's usual last-wins behaviour.
func parseHeaders(raw []string) (map[string]string, error) {
	headers := make(map[string]string, len(raw))
	for _, h := range raw {
		name, value, ok := strings.Cut(h, ":")
		if !ok {
			return nil, fmt.Errorf("expected 'Name: value', got %q", h)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("empty header name in %q", h)
		}
		headers[name] = strings.TrimSpace(value)
	}
	return headers, nil
}
