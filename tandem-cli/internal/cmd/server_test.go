package cmd

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// route is one canned response: a fixture file under testdata/, served at the given
// status with the RFC 9457 content type for 4xx/5xx.
type route struct {
	status int
	file   string
}

// newFixtureServer stands up a real httptest.NewServer keyed by Go 1.22+ ServeMux
// "METHOD /path" patterns, each returning a canned fixture (LLD-cli.md §10) - the Go
// equivalent of InMemoryOutbox/TandemTestContainer instead of mocking the generated
// client's interface.
func newFixtureServer(t *testing.T, routes map[string]route) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for pattern, rt := range routes {
		rt := rt
		mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
			body, err := os.ReadFile(filepath.Join("testdata", rt.file))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			contentType := "application/json"
			if rt.status >= 400 {
				contentType = "application/problem+json"
			}
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(rt.status)
			_, _ = w.Write(body)
		})
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}
