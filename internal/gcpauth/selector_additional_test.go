package gcpauth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestDefaultADCPathUsesPlatformCarrier(t *testing.T) {
	environmentKey := "HOME"
	directoryParts := []string{".config", "gcloud", "application_default_credentials.json"}
	if runtime.GOOS == "windows" {
		environmentKey = "APPDATA"
		directoryParts = []string{"gcloud", "application_default_credentials.json"}
	}

	t.Setenv(environmentKey, " \t")
	if _, err := defaultADCPath(); err == nil || !strings.Contains(err.Error(), environmentKey+" is required") {
		t.Fatalf("blank %s error = %v", environmentKey, err)
	}

	root := t.TempDir()
	t.Setenv(environmentKey, " "+root+" ")
	want := filepath.Join(append([]string{root}, directoryParts...)...)
	got, err := defaultADCPath()
	if err != nil {
		t.Fatalf("defaultADCPath: %v", err)
	}
	if got != want {
		t.Fatalf("defaultADCPath = %q, want %q", got, want)
	}
}

func TestTokenSourceRejectsNilAndCanceledContextsBeforeCredentialResolution(t *testing.T) {
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	if _, err := selector.TokenSource(nil); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := selector.TokenSourceForRequest(ctx); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("canceled context error = %v", err)
	}
}

func TestRequestTokenSourceFactoryDefersContextAndPreservesBoundScopes(t *testing.T) {
	client := &fakeMetadataClient{onGCE: true, email: testIdentity, raw: `{"access_token":"metadata-token","token_type":"Bearer","expires_in":3600}`}
	originalMetadata := newMetadataClient
	newMetadataClient = func() metadataClient { return client }
	t.Cleanup(func() { newMetadataClient = originalMetadata })
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileAttachedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	factory := selector.RequestTokenSourceFactory("scope-one", "scope-two")
	ctx, cancel := context.WithCancel(context.Background())
	deferred, err := factory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := deferred.Token(); !errors.Is(err, context.Canceled) {
		t.Fatalf("request-bound source error = %v, want context cancellation", err)
	}
	// A fresh request context must produce a source with the factory's exact scopes.
	source, err := factory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(client.path, "scopes=scope-one%2Cscope-two") {
		t.Fatalf("metadata path = %q, want both bound scopes", client.path)
	}
}

func TestAttachedTokenSourceFailsClosedOnMetadataFailures(t *testing.T) {
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	selector := Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileAttachedServiceAccount, CredentialsIdentity: testIdentity}
	originalMetadata, originalCompute := newMetadataClient, computeTokenSource
	t.Cleanup(func() { newMetadataClient, computeTokenSource = originalMetadata, originalCompute })

	newMetadataClient = func() metadataClient { return nil }
	if _, err := selector.TokenSourceForRequest(context.Background()); err == nil || !strings.Contains(err.Error(), "requires Google-hosted metadata") {
		t.Fatalf("nil metadata client error = %v", err)
	}
	newMetadataClient = func() metadataClient { return &fakeMetadataClient{onGCE: false} }
	if _, err := selector.TokenSourceForRequest(context.Background()); err == nil || !strings.Contains(err.Error(), "requires Google-hosted metadata") {
		t.Fatalf("not-hosted error = %v", err)
	}
	newMetadataClient = func() metadataClient {
		return &failingMetadataClient{onGCE: true, emailErr: errors.New("metadata unavailable")}
	}
	if _, err := selector.TokenSourceForRequest(context.Background()); err == nil || !strings.Contains(err.Error(), "resolve attached metadata identity") {
		t.Fatalf("metadata identity error = %v", err)
	}
	newMetadataClient = func() metadataClient { return &fakeMetadataClient{onGCE: true, email: testIdentity} }
	computeTokenSource = func(string, ...string) oauth2.TokenSource { return nil }
	if _, err := selector.TokenSource(context.Background()); err == nil || !strings.Contains(err.Error(), "token source is unavailable") {
		t.Fatalf("nil compute token source error = %v", err)
	}
}

func TestAttachedTokenSourceRejectsFileCredentialOverrides(t *testing.T) {
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}
	t.Setenv("GODEBUG", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileAttachedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, filepath.Join(t.TempDir(), "attacker.json"))
			if _, err := selector.TokenSourceForRequest(context.Background()); err == nil || !strings.Contains(err.Error(), key+" must be unset") {
				t.Fatalf("%s override error = %v", key, err)
			}
		})
	}
}

func TestRequestMetadataTokenSourceRejectsMalformedAndIncompleteTokens(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "invalid JSON", raw: `{`, want: "invalid token JSON"},
		{name: "missing expiry", raw: `{"access_token":"token","expires_in":0}`, want: "incomplete token"},
		{name: "blank access token", raw: `{"access_token":" ","expires_in":1}`, want: "incomplete token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := requestMetadataTokenSource{ctx: context.Background(), client: &fakeMetadataClient{raw: tt.raw}, account: testIdentity}
			if _, err := source.Token(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("token error = %v, want %q", err, tt.want)
			}
		})
	}
	metadataFailure := &failingMetadataClient{getErr: errors.New("metadata token endpoint failed")}
	source := requestMetadataTokenSource{ctx: context.Background(), client: metadataFailure, account: testIdentity}
	if _, err := source.Token(); !errors.Is(err, metadataFailure.getErr) {
		t.Fatalf("metadata error = %v", err)
	}
}

func TestReadOwnerOnlyRegularFileRejectsInvalidAndOversizedCarriers(t *testing.T) {
	if _, err := readOwnerOnlyRegularFile(" "); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("blank path error = %v", err)
	}
	if _, err := readOwnerOnlyRegularFile("relative.json"); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative path error = %v", err)
	}
	directory := t.TempDir()
	if _, err := readOwnerOnlyRegularFile(directory); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory carrier error = %v", err)
	}
	path := filepath.Join(directory, "oversized.json")
	data := make([]byte, maxCredentialConfigurationBytes+1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	if _, err := readOwnerOnlyRegularFile(path); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized carrier error = %v", err)
	}
}

func TestCredentialCarrierBoundaryFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	if _, err := readOwnerOnlyRegularFile(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing carrier error = %v, want os.ErrNotExist", err)
	}

	if err := validateImpersonationURL("https://iamcredentials.googleapis.com/%zz", testIdentity); err == nil || !strings.Contains(err.Error(), "service_account_impersonation_url") {
		t.Fatalf("malformed impersonation URL error = %v", err)
	}

	for _, identity := range []string{"missing-at-sign", "account@"} {
		if got := serviceAccountProject(identity); got != "" {
			t.Fatalf("serviceAccountProject(%q) = %q, want empty", identity, got)
		}
	}

	if err := validateImpersonatedConfiguration([]byte("{"), testIdentity); err == nil || !strings.Contains(err.Error(), "credential JSON") {
		t.Fatalf("malformed credential JSON error = %v", err)
	}
}

func TestValidationRejectsURLAndNestedCredentialConfusion(t *testing.T) {
	valid := validImpersonatedCarrier(testIdentity)
	for _, tt := range []struct {
		name string
		data string
		want string
	}{
		{name: "query authority drift", data: strings.Replace(valid, ":generateAccessToken\"", ":generateAccessToken?alt=json\"", 1), want: "canonical IAMCredentials authority"},
		{name: "wrong scheme", data: strings.Replace(valid, "https://iamcredentials.googleapis.com", "http://iamcredentials.googleapis.com", 1), want: "canonical IAMCredentials authority"},
		{name: "nested private key", data: strings.Replace(valid, `"client_id": "fixture-client"`, `"client_id": "fixture-client", "nested": {"private_key_id":"forbidden"}`, 1), want: "private_key_id"},
		{name: "missing refresh token", data: strings.Replace(valid, `"refresh_token": "fixture-refresh"`, `"refresh_token": " "`, 1), want: "refresh_token is required"},
		{name: "wrong source type", data: strings.Replace(valid, `"type": "authorized_user"`, `"type": "service_account"`, 1), want: "source_credentials.type"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateImpersonatedConfiguration([]byte(tt.data), testIdentity); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validation error = %v, want %q", err, tt.want)
			}
		})
	}
}

type failingMetadataClient struct {
	onGCE    bool
	emailErr error
	getErr   error
}

func (f *failingMetadataClient) OnGCEWithContext(context.Context) bool { return f.onGCE }
func (f *failingMetadataClient) EmailWithContext(context.Context, string) (string, error) {
	return "", f.emailErr
}
func (f *failingMetadataClient) GetWithContext(context.Context, string) (string, error) {
	return "", f.getErr
}
