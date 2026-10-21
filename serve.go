package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/api/idtoken"
)

// Fidelity Phase C1 — the HTTP service surface.
//
// Two endpoints provide pure Go-AST structural transformations over HTTP:
//   POST /ingest    Go source bytes  -> structural chunks
//   POST /manifest  structural chunks -> gofmt-canonical Go source bytes
//
// Higher-level orchestration — drafts, edits, validation tiers, and graph
// queries — coordinates against Spanner tables and invokes this service over
// HTTP for lossless source↔structure conversions.
//
// Deployment: Cloud Run or standalone HTTP server. Callers authenticate with
// OIDC ID tokens (audience = this service), which the service verifies. With
// no OIDC_AUDIENCE configured, auth is bypassed — dev mode only.

// ---------- request / response types ----------

type ingestRequest struct {
	Path    string `json:"path"`     // used in parse error messages; ignored otherwise
	Source  string `json:"source"`   // the Go source bytes
	PkgPath string `json:"pkg_path"` // file's import path (for declared-symbol FQNs); empty -> no FQNs
}

type ingestChunk struct {
	Kind        string   `json:"kind"` // package | import | decl | comment | raw
	Text        string   `json:"text"`
	FQNs        []string `json:"fqns,omitempty"`
	BlankBefore bool     `json:"blank_before"`
}

type ingestResponse struct {
	Chunks []ingestChunk `json:"chunks"`
}

type manifestRequest struct {
	Chunks []ingestChunk `json:"chunks"`
}

type manifestResponse struct {
	Source string `json:"source"` // gofmt-canonical Go
}

type errorResponse struct {
	Error string `json:"error"`
}

// ---------- handlers ----------

func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	path := req.Path
	if path == "" {
		path = "ingest.go"
	}
	chunks := decomposeGoFile(path, []byte(req.Source), req.PkgPath)
	resp := ingestResponse{Chunks: make([]ingestChunk, len(chunks))}
	for i, c := range chunks {
		resp.Chunks[i] = ingestChunk{Kind: c.kind, Text: c.text, FQNs: c.fqns, BlankBefore: c.blankBefore}
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var req manifestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	chunks := make([]chunkText, len(req.Chunks))
	for i, c := range req.Chunks {
		chunks[i] = chunkText{kind: c.Kind, text: c.Text, blankBefore: c.BlankBefore}
	}
	rendered, err := renderGoFile(chunks)
	if err != nil {
		writeError(w, http.StatusBadRequest, "render: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, manifestResponse{Source: string(rendered)})
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// ---------- OIDC verify (caller SA -> this service) ----------

// oidcMiddleware verifies a bearer OIDC ID token whose audience matches the
// configured audience. /healthz and /readyz bypass auth (load-balancer probes),
// using google.golang.org/api/idtoken.
var validateOIDCToken = idtoken.Validate

func oidcMiddleware(audience string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		token := strings.TrimPrefix(auth, "Bearer ")
		if _, err := validateOIDCToken(r.Context(), token, audience); err != nil {
			writeError(w, http.StatusUnauthorized, "invalid OIDC token: "+err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------- CLI entry ----------

var serveHTTP = func(server *http.Server) error { return server.ListenAndServe() }

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", envOr("PORT_ADDR", ":8080"), "bind address (Cloud Run sets $PORT)")
	audience := fs.String("oidc-audience", os.Getenv("OIDC_AUDIENCE"), "required OIDC token audience; empty disables auth (dev only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if p := os.Getenv("PORT"); p != "" && *addr == ":8080" {
		*addr = ":" + p
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", handleIngest)
	mux.HandleFunc("/manifest", handleManifest)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", handleHealthz)

	var handler http.Handler = mux
	if *audience != "" {
		handler = oidcMiddleware(*audience, mux)
		log.Printf("code-graph serve %s (OIDC audience=%q)", *addr, *audience)
	} else {
		log.Printf("code-graph serve %s — WARNING: OIDC disabled (no --oidc-audience); dev mode", *addr)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return serveHTTP(srv)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// _ keeps fmt imported even if no current use — handler error formats may need it.
var _ = fmt.Sprintf
