package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/api/idtoken"
)

func TestContextFileLifecycleAndCommand(t *testing.T) {
	home := t.TempDir()
	// os.UserHomeDir() reads USERPROFILE on Windows and HOME elsewhere, so a test
	// that sets only HOME redirects nothing on windows-amd64 -- a declared coverage
	// target for this repo. Both are set to THE SAME directory: each OS reads only
	// the one it uses, so this is correct on every platform.
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	wantPath := filepath.Join(home, ".code-graph-context.json")
	if got := contextFilePath(); got != wantPath {
		t.Fatalf("context path = %q, want %q", got, wantPath)
	}
	if got, ok := loadContext(); ok || got != (cgContext{}) {
		t.Fatalf("missing context = %+v, %v", got, ok)
	}
	want := cgContext{Repo: "repo", SnapshotID: "123456789012345"}
	if err := saveContext(want); err != nil {
		t.Fatalf("save context: %v", err)
	}
	if got, ok := loadContext(); !ok || got != want {
		t.Fatalf("loaded context = %+v, %v", got, ok)
	}
	out := captureStdout(t, func() {
		if err := cmdContext(context.Background(), nil); err != nil {
			t.Fatalf("context command: %v", err)
		}
	})
	if !strings.Contains(out, "repo=repo") || !strings.Contains(out, want.SnapshotID) {
		t.Fatalf("context output = %q", out)
	}

	if err := os.WriteFile(wantPath, []byte(`{"repo":"repo"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadContext(); ok {
		t.Fatal("context without snapshot was accepted")
	}
	if err := os.WriteFile(wantPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := loadContext(); ok {
		t.Fatal("malformed context was accepted")
	}
	if err := os.Remove(wantPath); err != nil {
		t.Fatal(err)
	}
	out = captureStdout(t, func() {
		if err := cmdContext(context.Background(), []string{"ignored"}); err != nil {
			t.Fatalf("empty context command: %v", err)
		}
	})
	if !strings.Contains(out, "no current snapshot pinned") {
		t.Fatalf("empty context output = %q", out)
	}
	if repoSuffix("") != "" || repoSuffix("repo") != " for repo repo" {
		t.Fatalf("repo suffixes are incorrect")
	}
}

func TestHTTPIngestAndManifestHandlers(t *testing.T) {
	request := func(handler http.HandlerFunc, method, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler(recorder, httptest.NewRequest(method, "http://service.test/", strings.NewReader(body)))
		return recorder
	}
	if got := request(handleIngest, http.MethodGet, ""); got.Code != http.StatusMethodNotAllowed {
		t.Fatalf("ingest GET status = %d", got.Code)
	}
	if got := request(handleIngest, http.MethodPost, "{"); got.Code != http.StatusBadRequest {
		t.Fatalf("ingest invalid JSON status = %d", got.Code)
	}
	valid := request(handleIngest, http.MethodPost, `{"source":"package p\nfunc F() {}\n","pkg_path":"example.test/p"}`)
	if valid.Code != http.StatusOK || valid.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("ingest response = %d %v", valid.Code, valid.Header())
	}
	var ingested ingestResponse
	if err := json.Unmarshal(valid.Body.Bytes(), &ingested); err != nil {
		t.Fatal(err)
	}
	if len(ingested.Chunks) != 2 || !reflect.DeepEqual(ingested.Chunks[1].FQNs, []string{"example.test/p.F"}) {
		t.Fatalf("ingested chunks = %+v", ingested.Chunks)
	}

	if got := request(handleManifest, http.MethodGet, ""); got.Code != http.StatusMethodNotAllowed {
		t.Fatalf("manifest GET status = %d", got.Code)
	}
	if got := request(handleManifest, http.MethodPost, "{"); got.Code != http.StatusBadRequest {
		t.Fatalf("manifest invalid JSON status = %d", got.Code)
	}
	if got := request(handleManifest, http.MethodPost, `{"chunks":[{"kind":"decl","text":"func {"}]}`); got.Code != http.StatusBadRequest || !strings.Contains(got.Body.String(), "render") {
		t.Fatalf("manifest invalid render = %d %q", got.Code, got.Body.String())
	}
	manifestBody, err := json.Marshal(manifestRequest{Chunks: ingested.Chunks})
	if err != nil {
		t.Fatal(err)
	}
	rendered := request(handleManifest, http.MethodPost, string(manifestBody))
	if rendered.Code != http.StatusOK || !strings.Contains(rendered.Body.String(), "package p") {
		t.Fatalf("manifest response = %d %q", rendered.Code, rendered.Body.String())
	}

	health := httptest.NewRecorder()
	handleHealthz(health, httptest.NewRequest(http.MethodGet, "http://service.test/healthz", nil))
	if health.Code != http.StatusOK || health.Body.String() != "ok\n" {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}
}

func TestOIDCMiddlewareGuardsAndPasses(t *testing.T) {
	previous := validateOIDCToken
	t.Cleanup(func() { validateOIDCToken = previous })
	var validatedToken, validatedAudience string
	validateOIDCToken = func(_ context.Context, token, audience string) (*idtoken.Payload, error) {
		validatedToken, validatedAudience = token, audience
		if token == "valid" {
			return &idtoken.Payload{Audience: audience}, nil
		}
		return nil, errors.New("rejected token")
	}
	called := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := oidcMiddleware("audience", next)

	probe := httptest.NewRecorder()
	handler.ServeHTTP(probe, httptest.NewRequest(http.MethodGet, "http://service.test/readyz", nil))
	if probe.Code != http.StatusNoContent || called != 1 {
		t.Fatalf("probe response/calls = %d/%d", probe.Code, called)
	}
	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "http://service.test/ingest", nil))
	if missing.Code != http.StatusUnauthorized || called != 1 {
		t.Fatalf("missing bearer response/calls = %d/%d", missing.Code, called)
	}
	invalidRequest := httptest.NewRequest(http.MethodPost, "http://service.test/ingest", nil)
	invalidRequest.Header.Set("Authorization", "Bearer invalid")
	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, invalidRequest)
	if invalid.Code != http.StatusUnauthorized || !strings.Contains(invalid.Body.String(), "rejected token") {
		t.Fatalf("invalid response = %d %q", invalid.Code, invalid.Body.String())
	}
	validRequest := httptest.NewRequest(http.MethodPost, "http://service.test/ingest", nil)
	validRequest.Header.Set("Authorization", "Bearer valid")
	valid := httptest.NewRecorder()
	handler.ServeHTTP(valid, validRequest)
	if valid.Code != http.StatusNoContent || called != 2 || validatedToken != "valid" || validatedAudience != "audience" {
		t.Fatalf("valid response/calls/token/audience = %d/%d/%q/%q", valid.Code, called, validatedToken, validatedAudience)
	}
}

func TestFidelityClassificationAndComparison(t *testing.T) {
	if isBuildOutput(nil) || isBuildOutput([]byte("abc")) || isBuildOutput([]byte("text")) {
		t.Fatal("source bytes classified as build output")
	}
	for _, magic := range [][]byte{
		{0x7f, 'E', 'L', 'F'}, {'M', 'Z', 0, 0},
		{0xfe, 0xed, 0xfa, 0xce}, {0xcf, 0xfa, 0xed, 0xfe}, {0xca, 0xfe, 0xba, 0xbe},
	} {
		if !isBuildOutput(magic) {
			t.Fatalf("magic %x was not classified as build output", magic)
		}
	}
	if !(VerifyResult{}).Clean() || (VerifyResult{Differing: []string{"x"}}).Clean() ||
		(VerifyResult{OnlyInSnap: []string{"x"}}).Clean() || (VerifyResult{OnlyOnDisk: []string{"x"}}).Clean() {
		t.Fatal("verify cleanliness classification is incorrect")
	}

	nonGo := snapshotFileRow{path: "README.md", bytes: []byte("same")}
	if !verifyMatch(nonGo, []byte("same"), false, nil) || verifyMatch(nonGo, []byte("different"), false, nil) {
		t.Fatal("raw fidelity comparison is incorrect")
	}
	goFile := snapshotFileRow{path: "main.go", isGo: true}
	chunks := map[string][]chunkText{"main.go": {{kind: "package", text: "package p\n"}, {kind: "decl", text: "func F(){}\n", blankBefore: true}}}
	if !verifyMatch(goFile, []byte("package p\n\nfunc F() { }\n"), true, chunks) {
		t.Fatal("gofmt-canonical structural comparison failed")
	}
	if verifyMatch(goFile, []byte("package p\n"), true, nil) {
		t.Fatal("missing chunks matched")
	}
	if verifyMatch(goFile, []byte("package p\n"), true, map[string][]chunkText{"main.go": {{kind: "decl", text: "func {"}}}) {
		t.Fatal("invalid structural chunks matched")
	}
	if verifyMatch(goFile, []byte("package {"), true, chunks) {
		t.Fatal("invalid disk Go source matched")
	}
	rawFile := snapshotFileRow{path: "broken.go", isGo: true}
	rawChunks := map[string][]chunkText{"broken.go": {{kind: "raw", text: "package {"}}}
	if !verifyMatch(rawFile, []byte("package {"), true, rawChunks) || verifyMatch(rawFile, []byte("other"), true, rawChunks) {
		t.Fatal("raw structural comparison is incorrect")
	}
}

func TestQueryClassificationAndAnnotationHelpers(t *testing.T) {
	if !isTestFile("pkg/x_test.go") || isTestFile("pkg/x.go") {
		t.Fatal("test file classification is incorrect")
	}
	for _, fqn := range []string{"pkg.TestThing", "BenchmarkThing", "pkg.FuzzInput", "pkg.ExampleThing"} {
		if !isTestFuncName(fqn) || !isEntrypoint(fqn) {
			t.Fatalf("test entry %q was not recognized", fqn)
		}
	}
	if isTestFuncName("pkg.helper") || !isEntrypoint("pkg.main") || !isEntrypoint("init") || isEntrypoint("pkg.Helper") {
		t.Fatal("entrypoint classification is incorrect")
	}
	for _, kind := range []string{"func", "method", "type", "var", "const"} {
		if !isContentKind(kind) {
			t.Fatalf("content kind %q rejected", kind)
		}
	}
	if isContentKind("file") {
		t.Fatal("file classified as content kind")
	}
	a := parseVoltAnnotation("pkg.F", "sentinel.raise name=depth ignored spec_section=VOLT-4")
	if a.FQN != "pkg.F" || a.Type != "sentinel.raise" || a.Name != "depth" || a.SpecSection != "VOLT-4" || a.Raw == "" {
		t.Fatalf("annotation = %+v", a)
	}
	if empty := parseVoltAnnotation("pkg.F", "  "); empty.Type != "" || empty.FQN != "pkg.F" {
		t.Fatalf("empty annotation = %+v", empty)
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("CODE_GRAPH_TEST_VALUE", "")
	if got := envOr("CODE_GRAPH_TEST_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("empty env = %q", got)
	}
	t.Setenv("CODE_GRAPH_TEST_VALUE", "configured")
	if got := envOr("CODE_GRAPH_TEST_VALUE", "fallback"); got != "configured" {
		t.Fatalf("configured env = %q", got)
	}
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	defer func() { os.Stdout = original }()
	run()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
