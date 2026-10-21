package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

type testTokenSource struct {
	ctx   context.Context
	token string
	calls *atomic.Int32
}

func (s testTokenSource) Token() (*oauth2.Token, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if s.calls != nil {
		s.calls.Add(1)
	}
	return &oauth2.Token{
		AccessToken: s.token,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}, nil
}

func TestLoadGoogleCredentialSelectorClosesEnvironmentUnion(t *testing.T) {
	valid := map[string]string{
		"CODE_GRAPH_CREDENTIALS_SOURCE":   "application_default",
		"CODE_GRAPH_CREDENTIALS_PROFILE":  "impersonated_service_account",
		"CODE_GRAPH_CREDENTIALS_IDENTITY": "cambium-operator@example-project.iam.gserviceaccount.com",
	}
	getenv := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}

	if _, err := loadGoogleCredentialSelector(getenv(valid)); err != nil {
		t.Fatalf("valid selector: %v", err)
	}
	for _, tt := range []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{name: "missing_source", mutate: func(v map[string]string) { delete(v, "CODE_GRAPH_CREDENTIALS_SOURCE") }, wantErr: "credentials_source"},
		{name: "missing_profile", mutate: func(v map[string]string) { delete(v, "CODE_GRAPH_CREDENTIALS_PROFILE") }, wantErr: "credentials_profile"},
		{name: "missing_identity", mutate: func(v map[string]string) { delete(v, "CODE_GRAPH_CREDENTIALS_IDENTITY") }, wantErr: "credentials_identity"},
		{name: "unknown_source", mutate: func(v map[string]string) { v["CODE_GRAPH_CREDENTIALS_SOURCE"] = "ambient" }, wantErr: "unsupported credentials_source"},
		{name: "unknown_profile", mutate: func(v map[string]string) { v["CODE_GRAPH_CREDENTIALS_PROFILE"] = "human_adc" }, wantErr: "unsupported credentials_profile"},
		{name: "human_identity", mutate: func(v map[string]string) { v["CODE_GRAPH_CREDENTIALS_IDENTITY"] = "operator@example.com" }, wantErr: "service-account email"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := make(map[string]string, len(valid))
			for key, value := range valid {
				values[key] = value
			}
			tt.mutate(values)
			_, err := loadGoogleCredentialSelector(getenv(values))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadGoogleCredentialSelectorWiresDeviceServiceAccountBaseIdentity(t *testing.T) {
	values := map[string]string{
		"CODE_GRAPH_CREDENTIALS_SOURCE":        "application_default",
		"CODE_GRAPH_CREDENTIALS_PROFILE":       "device_service_account",
		"CODE_GRAPH_CREDENTIALS_IDENTITY":      "cambium-operator@example-project.iam.gserviceaccount.com",
		"CODE_GRAPH_CREDENTIALS_BASE_IDENTITY": "workstation-caller@example-project.iam.gserviceaccount.com",
	}
	getenv := func(key string) string { return values[key] }

	selector, err := loadGoogleCredentialSelector(getenv)
	if err != nil {
		t.Fatalf("valid device_service_account selector: %v", err)
	}
	if selector.CredentialsBaseIdentity != values["CODE_GRAPH_CREDENTIALS_BASE_IDENTITY"] {
		t.Fatalf("CredentialsBaseIdentity = %q, want %q", selector.CredentialsBaseIdentity, values["CODE_GRAPH_CREDENTIALS_BASE_IDENTITY"])
	}

	delete(values, "CODE_GRAPH_CREDENTIALS_BASE_IDENTITY")
	if _, err := loadGoogleCredentialSelector(getenv); err == nil || !strings.Contains(err.Error(), "credentials_base_identity") {
		t.Fatalf("missing base identity error = %v, want credentials_base_identity refusal", err)
	}
}

func TestInitializeGoogleCredentialsRefusesSelectorReplacement(t *testing.T) {
	processGoogleCredentials.Lock()
	previous := processGoogleCredentials.runtime
	processGoogleCredentials.runtime = &googleCredentialRuntime{}
	processGoogleCredentials.Unlock()
	t.Cleanup(func() {
		processGoogleCredentials.Lock()
		processGoogleCredentials.runtime = previous
		processGoogleCredentials.Unlock()
	})
	if err := initializeGoogleCredentials(context.Background(), func(string) string { return "" }); err == nil || !strings.Contains(err.Error(), "already initialized") {
		t.Fatalf("replacement error = %v", err)
	}
}

func TestInitializeGoogleCredentialsRejectsInvalidConstruction(t *testing.T) {
	processGoogleCredentials.Lock()
	previous := processGoogleCredentials.runtime
	processGoogleCredentials.runtime = nil
	processGoogleCredentials.Unlock()
	t.Cleanup(func() {
		processGoogleCredentials.Lock()
		processGoogleCredentials.runtime = previous
		processGoogleCredentials.Unlock()
	})

	if err := initializeGoogleCredentials(nil, func(string) string { return "" }); err == nil || !strings.Contains(err.Error(), "construction context is required") {
		t.Fatalf("nil-context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := initializeGoogleCredentials(canceled, func(string) string { return "" }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context error = %v", err)
	}
	if err := initializeGoogleCredentials(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "environment reader is required") {
		t.Fatalf("nil-environment-reader error = %v", err)
	}
}

func TestGoogleClientConstructorsRejectUninitializedRuntime(t *testing.T) {
	processGoogleCredentials.Lock()
	previous := processGoogleCredentials.runtime
	processGoogleCredentials.runtime = nil
	processGoogleCredentials.Unlock()
	t.Cleanup(func() {
		processGoogleCredentials.Lock()
		processGoogleCredentials.runtime = previous
		processGoogleCredentials.Unlock()
	})

	if runtime, err := selectedGoogleCredentials(); runtime != nil || err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("selected runtime = %#v, error = %v", runtime, err)
	}
	if client, err := newSpannerClient(context.Background()); client != nil || err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("Spanner client = %#v, error = %v", client, err)
	}
	if client, err := newDatabaseAdminClient(context.Background()); client != nil || err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("database admin client = %#v, error = %v", client, err)
	}
	req, err := http.NewRequest(http.MethodPost, "https://us-central1-aiplatform.googleapis.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachVertexBearer(req); err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("attach error = %v", err)
	}
}

func TestVertexBearerAuthUsesRequestContextAndCaches(t *testing.T) {
	var factoryCalls atomic.Int32
	var tokenCalls atomic.Int32
	factory := func(ctx context.Context) (oauth2.TokenSource, error) {
		factoryCalls.Add(1)
		return testTokenSource{ctx: ctx, token: "request-bound-token", calls: &tokenCalls}, nil
	}
	auth, err := newVertexBearerAuth(context.Background(), factory)
	if err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://us-central1-aiplatform.googleapis.com/v1/projects/test-project/locations/us-central1/publishers/google/models/test-model:predict", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.attach(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer request-bound-token" {
		t.Fatalf("Authorization = %q", got)
	}

	second := req.Clone(context.Background())
	second.Header = make(http.Header)
	if err := auth.attach(second); err != nil {
		t.Fatal(err)
	}
	if got := second.Header.Get("Authorization"); got != "Bearer request-bound-token" {
		t.Fatalf("second Authorization = %q", got)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("factory calls = %d, want construction validation + one refresh", got)
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want one cached refresh", got)
	}
}

func TestVertexBearerAuthPropagatesCanceledRefreshContext(t *testing.T) {
	refreshStarted := make(chan struct{})
	factory := func(ctx context.Context) (oauth2.TokenSource, error) {
		select {
		case <-refreshStarted:
		default:
			close(refreshStarted)
		}
		return tokenSourceFunc(func() (*oauth2.Token, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}), nil
	}
	auth, err := newVertexBearerAuth(context.Background(), factory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://us-central1-aiplatform.googleapis.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := auth.attach(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("attach error = %v, want context canceled", err)
	}
}

func TestVertexBearerAuthLiveWaiterRetriesAfterCanceledLeader(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	var factoryCalls atomic.Int32
	leaderRefreshStarted := make(chan struct{})
	factory := func(ctx context.Context) (oauth2.TokenSource, error) {
		switch factoryCalls.Add(1) {
		case 1:
			return testTokenSource{ctx: ctx, token: "construction-token"}, nil
		case 2:
			return tokenSourceFunc(func() (*oauth2.Token, error) {
				close(leaderRefreshStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}), nil
		default:
			return testTokenSource{ctx: ctx, token: "live-waiter-token"}, nil
		}
	}
	auth, err := newVertexBearerAuth(context.Background(), factory)
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderReq, err := http.NewRequestWithContext(leaderCtx, http.MethodPost, "https://us-central1-aiplatform.googleapis.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	leaderResult := make(chan error, 1)
	go func() { leaderResult <- auth.attach(leaderReq) }()
	<-leaderRefreshStarted

	waiterReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://us-central1-aiplatform.googleapis.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	waiterScheduled := make(chan struct{})
	waiterResult := make(chan error, 1)
	go func() {
		close(waiterScheduled)
		waiterResult <- auth.attach(waiterReq)
	}()
	<-waiterScheduled
	runtime.Gosched()
	time.Sleep(5 * time.Millisecond)
	cancelLeader()

	select {
	case err := <-leaderResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader refresh did not stop after cancellation")
	}
	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatalf("live waiter error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live waiter did not retry after leader cancellation")
	}
	if got := waiterReq.Header.Get("Authorization"); got != "Bearer live-waiter-token" {
		t.Fatalf("live waiter Authorization = %q", got)
	}
	if got := factoryCalls.Load(); got != 3 {
		t.Fatalf("factory calls = %d, want construction + canceled leader + live waiter retry", got)
	}
}

func TestVertexHTTPClientRefusesRedirects(t *testing.T) {
	client := newVertexHTTPClient()
	if client == nil || client.CheckRedirect == nil {
		t.Fatal("Vertex HTTP client must install a redirect policy")
	}
	req, err := http.NewRequest(http.MethodPost, "https://attacker.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CheckRedirect(req, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("redirect error = %v, want http.ErrUseLastResponse", err)
	}
}

func TestCallEmbedAttachesSelectedBearer(t *testing.T) {
	factory := func(ctx context.Context) (oauth2.TokenSource, error) {
		return testTokenSource{ctx: ctx, token: "call-embed-token"}, nil
	}
	auth, err := newVertexBearerAuth(context.Background(), factory)
	if err != nil {
		t.Fatal(err)
	}

	processGoogleCredentials.Lock()
	previousRuntime := processGoogleCredentials.runtime
	processGoogleCredentials.runtime = &googleCredentialRuntime{vertexAuth: auth}
	processGoogleCredentials.Unlock()
	t.Cleanup(func() {
		processGoogleCredentials.Lock()
		processGoogleCredentials.runtime = previousRuntime
		processGoogleCredentials.Unlock()
	})

	previousFactory := vertexHTTPClientFactory
	var captured *http.Request
	vertexHTTPClientFactory = func() *http.Client {
		return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			captured = req.Clone(req.Context())
			captured.Header = req.Header.Clone()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"predictions":[{"embeddings":{"values":[0.25,0.5]}}]}`)),
				Request:    req,
			}, nil
		})}
	}
	t.Cleanup(func() { vertexHTTPClientFactory = previousFactory })

	vectors, err := callEmbed(context.Background(), []string{"fixture"}, "RETRIEVAL_QUERY")
	if err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 1 || len(vectors[0]) != 2 {
		t.Fatalf("vectors = %#v", vectors)
	}
	if captured == nil {
		t.Fatal("Vertex request was not captured")
	}
	if got := captured.Header.Get("Authorization"); got != "Bearer call-embed-token" {
		t.Fatalf("Authorization = %q", got)
	}
}

func TestExplicitGoogleClientConstructorsCarrySelectedTokenSource(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "credentials.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := map[string]int{
		"spanner.NewClient":               3,
		"database.NewDatabaseAdminClient": 2,
	}
	seen := make(map[string]int, len(wantArgs))
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		name := owner.Name + "." + selector.Sel.Name
		expectedArgs, required := wantArgs[name]
		if !required {
			return true
		}
		seen[name]++
		if len(call.Args) != expectedArgs || !isSelectedTokenSourceOption(call.Args[len(call.Args)-1]) {
			t.Errorf("%s must end with option.WithTokenSource(source)", name)
		}
		return true
	})
	for name := range wantArgs {
		if seen[name] != 1 {
			t.Errorf("%s calls = %d, want exactly 1", name, seen[name])
		}
	}
}

func isSelectedTokenSourceOption(expression ast.Expr) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "WithTokenSource" {
		return false
	}
	owner, ok := selector.X.(*ast.Ident)
	if !ok || owner.Name != "option" {
		return false
	}
	source, ok := call.Args[0].(*ast.Ident)
	return ok && source.Name == "source"
}

func TestGoogleClientConstructionIsCentralized(t *testing.T) {
	forbidden := []string{
		"spanner.NewClient(",
		"database.NewDatabaseAdminClient(",
		"google.DefaultTokenSource(",
	}
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") || path == "credentials.go" || strings.HasPrefix(path, "internal/gcpauth/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, literal := range forbidden {
			if strings.Contains(string(data), literal) {
				t.Errorf("%s contains direct Google credential/client constructor %q", path, literal)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type tokenSourceFunc func() (*oauth2.Token, error)

func (f tokenSourceFunc) Token() (*oauth2.Token, error) { return f() }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestLoadGoogleCredentialSelectorAcceptsExplicitCarrierFile(t *testing.T) {
	getenv := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	base := map[string]string{
		"CODE_GRAPH_CREDENTIALS_PROFILE":  "impersonated_service_account",
		"CODE_GRAPH_CREDENTIALS_IDENTITY": "cambium-operator@example-project.iam.gserviceaccount.com",
	}
	withFile := map[string]string{
		"CODE_GRAPH_CREDENTIALS_FILE":     "/fixture/code-graph-adc.json",
		"CODE_GRAPH_CREDENTIALS_PROFILE":  base["CODE_GRAPH_CREDENTIALS_PROFILE"],
		"CODE_GRAPH_CREDENTIALS_IDENTITY": base["CODE_GRAPH_CREDENTIALS_IDENTITY"],
	}
	selector, err := loadGoogleCredentialSelector(getenv(withFile))
	if err != nil {
		t.Fatalf("explicit carrier selector: %v", err)
	}
	if selector.CredentialsFile != withFile["CODE_GRAPH_CREDENTIALS_FILE"] {
		t.Fatalf("credentials file = %q, want %q", selector.CredentialsFile, withFile["CODE_GRAPH_CREDENTIALS_FILE"])
	}
	if selector.CredentialsSource != "" {
		t.Fatalf("credentials source = %q, want empty", selector.CredentialsSource)
	}

	for _, tt := range []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{
			name: "file_and_source_are_mutually_exclusive",
			values: map[string]string{
				"CODE_GRAPH_CREDENTIALS_FILE":     "/fixture/code-graph-adc.json",
				"CODE_GRAPH_CREDENTIALS_SOURCE":   "application_default",
				"CODE_GRAPH_CREDENTIALS_PROFILE":  base["CODE_GRAPH_CREDENTIALS_PROFILE"],
				"CODE_GRAPH_CREDENTIALS_IDENTITY": base["CODE_GRAPH_CREDENTIALS_IDENTITY"],
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "file_carrier_is_forbidden_for_attached_service_account",
			values: map[string]string{
				"CODE_GRAPH_CREDENTIALS_FILE":     "/fixture/code-graph-adc.json",
				"CODE_GRAPH_CREDENTIALS_PROFILE":  "attached_service_account",
				"CODE_GRAPH_CREDENTIALS_IDENTITY": base["CODE_GRAPH_CREDENTIALS_IDENTITY"],
			},
			wantErr: "attached_service_account forbids credentials_file",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := loadGoogleCredentialSelector(getenv(tt.values)); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
