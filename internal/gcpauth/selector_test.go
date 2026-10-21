package gcpauth

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

const testIdentity = "code-graph-test@fixture-project.iam.gserviceaccount.com"

func validImpersonatedCarrier(identity string) string {
	return fmt.Sprintf(`{
  "type": "impersonated_service_account",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
  "delegates": [],
  "source_credentials": {
    "type": "authorized_user",
    "client_id": "fixture-client",
    "client_secret": "fixture-secret",
    "refresh_token": "fixture-refresh",
    "token_uri": "https://oauth2.googleapis.com/token",
    "universe_domain": "googleapis.com"
  },
  "scopes": ["%s"],
  "quota_project_id": "fixture-project"
}`, identity, CloudPlatformScope)
}

func TestSelectorValidateClosesProfileUnion(t *testing.T) {
	for _, profile := range []string{CredentialsProfileImpersonatedServiceAccount, CredentialsProfileAttachedServiceAccount} {
		selector := Selector{
			CredentialsSource:   CredentialsSourceApplicationDefault,
			CredentialsProfile:  profile,
			CredentialsIdentity: testIdentity,
		}
		if err := selector.Validate("fixture"); err != nil {
			t.Fatalf("profile %s: %v", profile, err)
		}
	}
	for _, tt := range []struct {
		name     string
		selector Selector
		want     string
	}{
		{name: "missing source", selector: Selector{CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: testIdentity}, want: "credentials_source"},
		{name: "unknown source", selector: Selector{CredentialsSource: "ambient", CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: testIdentity}, want: "unsupported credentials_source"},
		{name: "missing profile", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsIdentity: testIdentity}, want: "credentials_profile"},
		{name: "unknown profile", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: "human_adc", CredentialsIdentity: testIdentity}, want: "unsupported credentials_profile"},
		{name: "human identity", selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileImpersonatedServiceAccount, CredentialsIdentity: "person@example.com"}, want: "service-account email"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.selector.Validate("fixture"); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestTokenSourceRejectsAmbientTransportOverridesAcrossProfiles(t *testing.T) {
	wantOverrides := []string{
		"EXPERIMENTAL_GOOGLE_API_USE_S2A",
		"GCE_METADATA_HOST",
		"GRPC_BINARY_LOG_FILTER",
		"GRPC_ENFORCE_ALPN_ENABLED",
		"HTTP_PROXY",
		"HTTPS_PROXY",
		"GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB",
		"GOOGLE_API_GO_EXPERIMENTAL_ENABLE_NEW_AUTH_LIB",
		"GOOGLE_API_CERTIFICATE_CONFIG",
		"GOOGLE_API_USE_CLIENT_CERTIFICATE",
		"GOOGLE_API_USE_MTLS",
		"GOOGLE_API_USE_MTLS_ENDPOINT",
		"GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED",
		"GOOGLE_CLOUD_DISABLE_DIRECT_PATH",
		"GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS",
		"GOOGLE_CLOUD_QUOTA_PROJECT",
		"GOOGLE_CLOUD_UNIVERSE_DOMAIN",
		"GOOGLE_SPANNER_DISABLE_DIRECT_ACCESS_BOUND_TOKEN",
		"GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS",
		"GOOGLE_SPANNER_ENABLE_GCP_FALLBACK",
		"GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API",
		"NO_PROXY",
		"SSL_CERT_DIR",
		"SSL_CERT_FILE",
		"SPANNER_EMULATOR_HOST",
		"SPANNER_MONITORING_HOST",
		"http_proxy",
		"https_proxy",
		"no_proxy",
	}
	if len(ambientGoogleTransportOverrideKeys) != len(wantOverrides) {
		t.Fatalf("ambient transport override count = %d, want %d", len(ambientGoogleTransportOverrideKeys), len(wantOverrides))
	}
	for index, want := range wantOverrides {
		if got := ambientGoogleTransportOverrideKeys[index]; got != want {
			t.Fatalf("ambient transport override[%d] = %q, want %q", index, got, want)
		}
	}
	for _, path := range []string{"../../README.md", "../../RUNBOOK.md"} {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, key := range wantOverrides {
			if !strings.Contains(string(document), key) {
				t.Fatalf("%s does not document required unset key %s", path, key)
			}
		}
	}
	for _, profile := range []string{CredentialsProfileImpersonatedServiceAccount, CredentialsProfileAttachedServiceAccount} {
		selector := Selector{
			CredentialsSource:   CredentialsSourceApplicationDefault,
			CredentialsProfile:  profile,
			CredentialsIdentity: testIdentity,
		}
		for _, key := range wantOverrides {
			t.Run(profile+"/"+key, func(t *testing.T) {
				t.Setenv(key, "attacker.invalid")
				_, err := selector.TokenSource(context.Background(), CloudPlatformScope)
				// Case-insensitive: wantOverrides deliberately lists both an
				// UPPER_CASE and lower_case spelling of the proxy keys
				// (different tools honor different conventions), but Windows
				// environment variables are themselves case-insensitive, so
				// setting "http_proxy" there is indistinguishable from
				// setting "HTTP_PROXY" and sealAmbientGoogleTransport can
				// legitimately report whichever spelling it checks first in
				// its own key list. The reported key and the one this test
				// set are the same variable on that platform even when their
				// case differs.
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(key)+" must be unset") {
					t.Fatalf("ambient transport override error = %v", err)
				}
			})
		}
		t.Run(profile+"/whitespace-certificate-config", func(t *testing.T) {
			t.Setenv("GOOGLE_API_CERTIFICATE_CONFIG", " ")
			if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "GOOGLE_API_CERTIFICATE_CONFIG must be unset") {
				t.Fatalf("whitespace certificate-config error = %v", err)
			}
		})
	}
}

func TestSealAmbientGoogleTransportDisablesDefaultCertificateDiscovery(t *testing.T) {
	for _, key := range ambientGoogleTransportOverrideKeys {
		t.Setenv(key, "")
	}

	if err := sealAmbientGoogleTransport(); err != nil {
		t.Fatalf("seal ambient Google transport: %v", err)
	}
	for key, want := range map[string]string{
		"GOOGLE_API_USE_CLIENT_CERTIFICATE": "false",
		"GOOGLE_API_USE_MTLS_ENDPOINT":      "never",
	} {
		if got := os.Getenv(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if got := os.Getenv("GOOGLE_API_CERTIFICATE_CONFIG"); got != "" {
		t.Fatalf("GOOGLE_API_CERTIFICATE_CONFIG = %q, want unset", got)
	}
}

func TestValidateSecuritySensitiveGODEBUGRejectsCredentialAndPQCOverrides(t *testing.T) {
	wantFragments := []string{"http2debug=", "tlsmlkem=0", "tlssecpmlkem=0"}
	if len(forbiddenGODEBUGFragments) != len(wantFragments) {
		t.Fatalf("forbidden GODEBUG fragment count = %d, want %d", len(forbiddenGODEBUGFragments), len(wantFragments))
	}
	for index, want := range wantFragments {
		if got := forbiddenGODEBUGFragments[index]; got != want {
			t.Fatalf("forbidden GODEBUG fragment[%d] = %q, want %q", index, got, want)
		}
	}
	for _, path := range []string{"../../README.md", "../../RUNBOOK.md"} {
		document, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, fragment := range wantFragments {
			if !strings.Contains(string(document), fragment) {
				t.Fatalf("%s does not document forbidden GODEBUG fragment %s", path, fragment)
			}
		}
	}
	for _, tt := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "http2 bearer logging level one", value: "http2debug=1", want: "http2debug="},
		{name: "http2 bearer logging level two", value: "http2debug=2", want: "http2debug="},
		{name: "http2 substring behavior", value: "netdns=go,http2debug=20", want: "http2debug="},
		{name: "disable x25519 ml-kem", value: "tlsmlkem=0", want: "tlsmlkem=0"},
		{name: "disable secp ml-kem", value: "tlssecpmlkem=0", want: "tlssecpmlkem=0"},
		{name: "dangerous setting before benign duplicate", value: "tlsmlkem=0,tlsmlkem=1", want: "tlsmlkem=0"},
		{name: "dangerous setting after benign duplicate", value: "tlssecpmlkem=1,tlssecpmlkem=0", want: "tlssecpmlkem=0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSecuritySensitiveGODEBUG(tt.value)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestValidateSecuritySensitiveGODEBUGAllowsUnrelatedAndPQCEnablingSettings(t *testing.T) {
	for _, value := range []string{
		"",
		"netdns=go",
		"http2client=0",
		"tlsmlkem=1",
		"tlssecpmlkem=1",
		"netdns=go,tlsmlkem=1,tlssecpmlkem=1",
	} {
		t.Run(value, func(t *testing.T) {
			if err := validateSecuritySensitiveGODEBUG(value); err != nil {
				t.Fatalf("validate GODEBUG %q: %v", value, err)
			}
		})
	}
}

func TestSealAmbientGoogleTransportRejectsDangerousGODEBUG(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  string
	}{
		{value: "http2debug=2", want: "http2debug="},
		{value: "tlsmlkem=0", want: "tlsmlkem=0"},
		{value: "tlssecpmlkem=0", want: "tlssecpmlkem=0"},
	} {
		t.Run(tt.value, func(t *testing.T) {
			for _, key := range ambientGoogleTransportOverrideKeys {
				t.Setenv(key, "")
			}
			t.Setenv("GODEBUG", tt.value)
			if err := sealAmbientGoogleTransport(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want forbidden GODEBUG fragment %q", err, tt.want)
			}
		})
	}
}

func TestImpersonatedCarrierValidationRejectsAuthorityDrift(t *testing.T) {
	valid := validImpersonatedCarrier(testIdentity)
	if err := validateImpersonatedConfiguration([]byte(valid), testIdentity); err != nil {
		t.Fatalf("valid carrier: %v", err)
	}
	withoutQuotaProject := strings.Replace(valid, ",\n  \"quota_project_id\": \"fixture-project\"", "", 1)
	if err := validateImpersonatedConfiguration([]byte(withoutQuotaProject), testIdentity); err != nil {
		t.Fatalf("carrier without optional quota_project_id: %v", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(string) string
		want   string
	}{
		{name: "wrong target", mutate: func(raw string) string {
			return strings.Replace(raw, testIdentity, "other-test@fixture-project.iam.gserviceaccount.com", 1)
		}, want: "does not match credentials_identity"},
		{name: "delegate", mutate: func(raw string) string {
			return strings.Replace(raw, `"delegates": []`, `"delegates": ["delegate@fixture-project.iam.gserviceaccount.com"]`, 1)
		}, want: "delegates must be an empty array"},
		{name: "wrong scope", mutate: func(raw string) string {
			return strings.Replace(raw, CloudPlatformScope, "https://www.googleapis.com/auth/devstorage.read_only", 1)
		}, want: "scopes must contain only"},
		{name: "wrong quota project", mutate: func(raw string) string {
			return strings.Replace(raw, `"quota_project_id": "fixture-project"`, `"quota_project_id": "other-project"`, 1)
		}, want: "must match"},
		{name: "private key", mutate: func(raw string) string {
			return strings.Replace(raw, `"type": "authorized_user"`, `"type": "authorized_user", "private_key": "forbidden"`, 1)
		}, want: "private_key"},
		{name: "non-string account", mutate: func(raw string) string {
			return strings.Replace(raw, `"type": "authorized_user"`, `"type": "authorized_user", "account": {"unexpected": true}`, 1)
		}, want: "account must be a string"},
		{name: "unknown field", mutate: func(raw string) string {
			return strings.Replace(raw, `"delegates": []`, `"delegates": [], "fallback": true`, 1)
		}, want: "not allowed"},
		{name: "duplicate key", mutate: func(raw string) string {
			return strings.Replace(raw, `"delegates": []`, `"delegates": [], "delegates": []`, 1)
		}, want: "duplicate JSON key"},
		{name: "trailing data", mutate: func(raw string) string { return raw + `{}` }, want: "trailing data"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateImpersonatedConfiguration([]byte(tt.mutate(valid)), testIdentity)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestImpersonatedTokenSourceUsesOnlyWellKnownOwnerOnlyCarrier(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "")
	t.Setenv("CLOUDSDK_CONFIG", "")
	directory := t.TempDir()
	path := filepath.Join(directory, "application_default_credentials.json")
	if err := os.WriteFile(path, []byte(validImpersonatedCarrier(testIdentity)), 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	originalPath := defaultADCPath
	defaultADCPath = func() (string, error) { return path, nil }
	t.Cleanup(func() { defaultADCPath = originalPath })

	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	if source, err := selector.TokenSource(context.Background(), CloudPlatformScope); err != nil || source == nil {
		t.Fatalf("token source = %v, error = %v", source, err)
	}

	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "attacker-project")
	if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "GOOGLE_CLOUD_QUOTA_PROJECT must be unset") {
		t.Fatalf("quota-project override error = %v", err)
	}
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "")

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(directory, "alternate.json"))
	if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS must be unset") {
		t.Fatalf("override error = %v", err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "owner-only") {
			t.Fatalf("world-readable carrier error = %v", err)
		}
	}
}

type fakeMetadataClient struct {
	onGCE bool
	email string
	raw   string
	path  string
}

func (f *fakeMetadataClient) OnGCEWithContext(context.Context) bool { return f.onGCE }
func (f *fakeMetadataClient) EmailWithContext(context.Context, string) (string, error) {
	return f.email, nil
}
func (f *fakeMetadataClient) GetWithContext(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.path = path
	return f.raw, nil
}

func TestAttachedTokenSourceBindsExactMetadataIdentity(t *testing.T) {
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "GCE_METADATA_HOST", "GOOGLE_CLOUD_QUOTA_PROJECT"} {
		t.Setenv(key, "")
	}
	client := &fakeMetadataClient{
		onGCE: true,
		email: testIdentity,
		raw:   `{"access_token":"metadata-token","token_type":"Bearer","expires_in":3600}`,
	}
	originalMetadata := newMetadataClient
	newMetadataClient = func() metadataClient { return client }
	t.Cleanup(func() { newMetadataClient = originalMetadata })

	selector := Selector{
		CredentialsSource:   CredentialsSourceApplicationDefault,
		CredentialsProfile:  CredentialsProfileAttachedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	source, err := selector.TokenSourceForRequest(context.Background(), CloudPlatformScope)
	if err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("GOOGLE_API_USE_CLIENT_CERTIFICATE"); got != sealedGoogleClientCertificate {
		t.Fatalf("GOOGLE_API_USE_CLIENT_CERTIFICATE = %q, want %q", got, sealedGoogleClientCertificate)
	}
	if got := os.Getenv("GOOGLE_API_USE_MTLS_ENDPOINT"); got != sealedGoogleMTLSEndpoint {
		t.Fatalf("GOOGLE_API_USE_MTLS_ENDPOINT = %q, want %q", got, sealedGoogleMTLSEndpoint)
	}
	token, err := source.Token()
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "metadata-token" {
		t.Fatalf("access token = %q", token.AccessToken)
	}
	if !strings.Contains(client.path, "service-accounts/"+testIdentity+"/token") {
		t.Fatalf("metadata token path = %q", client.path)
	}

	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "attacker-project")
	if _, err := selector.TokenSourceForRequest(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "GOOGLE_CLOUD_QUOTA_PROJECT must be unset") {
		t.Fatalf("quota-project override error = %v", err)
	}
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", "")

	client.email = "other-test@fixture-project.iam.gserviceaccount.com"
	if _, err := selector.TokenSourceForRequest(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "does not match credentials_identity") {
		t.Fatalf("wrong metadata identity error = %v", err)
	}
	t.Setenv("GCE_METADATA_HOST", "attacker.invalid")
	if _, err := selector.TokenSourceForRequest(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "GCE_METADATA_HOST must be unset") {
		t.Fatalf("metadata authority override error = %v", err)
	}
}

func TestRequestMetadataTokenSourceHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source := requestMetadataTokenSource{
		ctx:     ctx,
		client:  &fakeMetadataClient{raw: `{"access_token":"late","token_type":"Bearer","expires_in":3600}`},
		account: testIdentity,
		scopes:  []string{CloudPlatformScope},
	}
	if _, err := source.Token(); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("canceled request token error = %v", err)
	}
}

var _ oauth2.TokenSource = requestMetadataTokenSource{}

func TestSelectorValidateClosesCredentialsFileArm(t *testing.T) {
	valid := Selector{
		CredentialsFile:     "/fixture/impersonated.json",
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	if err := valid.Validate("fixture"); err != nil {
		t.Fatalf("explicit credentials_file carrier: %v", err)
	}
	for _, tt := range []struct {
		name     string
		selector Selector
		want     string
	}{
		{
			name: "both carriers",
			selector: Selector{
				CredentialsFile:     "/fixture/impersonated.json",
				CredentialsSource:   CredentialsSourceApplicationDefault,
				CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
				CredentialsIdentity: testIdentity,
			},
			want: "mutually exclusive",
		},
		{
			name: "neither carrier",
			selector: Selector{
				CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
				CredentialsIdentity: testIdentity,
			},
			want: "credentials_source",
		},
		{
			name: "attached profile cannot use an explicit carrier",
			selector: Selector{
				CredentialsFile:     "/fixture/impersonated.json",
				CredentialsProfile:  CredentialsProfileAttachedServiceAccount,
				CredentialsIdentity: testIdentity,
			},
			want: "attached_service_account forbids credentials_file",
		},
		{
			name: "explicit carrier still binds a service-account identity",
			selector: Selector{
				CredentialsFile:     "/fixture/impersonated.json",
				CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
				CredentialsIdentity: "person@example.com",
			},
			want: "service-account email",
		},
		{
			name: "explicit carrier still requires a profile",
			selector: Selector{
				CredentialsFile:     "/fixture/impersonated.json",
				CredentialsIdentity: testIdentity,
			},
			want: "credentials_profile",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.selector.Validate("fixture"); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestImpersonatedTokenSourceAcceptsOwnerOnlyExplicitCarrier(t *testing.T) {
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "CLOUDSDK_CONFIG"} {
		t.Setenv(key, "")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "code-graph-adc.json")
	if err := os.WriteFile(path, []byte(validImpersonatedCarrier(testIdentity)), 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	// The explicit carrier must never fall through to the well-known gcloud ADC
	// location; a consulted defaultADCPath fails the test rather than silently
	// resolving the machine's shared ADC slot.
	originalPath := defaultADCPath
	defaultADCPath = func() (string, error) {
		t.Error("explicit credentials_file arm consulted the well-known ADC location")
		return "", fmt.Errorf("well-known ADC location must not be consulted")
	}
	t.Cleanup(func() { defaultADCPath = originalPath })

	selector := Selector{
		CredentialsFile:     path,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	if source, err := selector.TokenSource(context.Background(), CloudPlatformScope); err != nil || source == nil {
		t.Fatalf("token source = %v, error = %v", source, err)
	}

	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"} {
		t.Run("rejects "+key, func(t *testing.T) {
			t.Setenv(key, filepath.Join(directory, "alternate.json"))
			if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), key+" must be unset") {
				t.Fatalf("override error = %v", err)
			}
		})
	}

	t.Run("relative carrier", func(t *testing.T) {
		relative := Selector{
			CredentialsFile:     "relative/impersonated.json",
			CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
			CredentialsIdentity: testIdentity,
		}
		if _, err := relative.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "absolute") {
			t.Fatalf("relative carrier error = %v", err)
		}
	})

	// Requirement: the explicit carrier receives byte-identical validation to the
	// well-known carrier.
	t.Run("carrier bytes are validated identically", func(t *testing.T) {
		driftPath := filepath.Join(directory, "drifted-adc.json")
		drifted := strings.Replace(validImpersonatedCarrier(testIdentity), testIdentity, "other-test@fixture-project.iam.gserviceaccount.com", 1)
		if err := os.WriteFile(driftPath, []byte(drifted), 0o600); err != nil {
			t.Fatal(err)
		}
		ensureCredentialFixtureOwnedByCurrentUser(t, driftPath)
		drift := Selector{
			CredentialsFile:     driftPath,
			CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
			CredentialsIdentity: testIdentity,
		}
		if _, err := drift.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "does not match credentials_identity") {
			t.Fatalf("drifted explicit carrier error = %v", err)
		}
	})

	if runtime.GOOS != "windows" {
		t.Run("owner-only mode", func(t *testing.T) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
			if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "owner-only") {
				t.Fatalf("world-readable explicit carrier error = %v", err)
			}
		})
	}
}

func TestExplicitCarrierStillSealsAmbientTransportAndGODEBUG(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "code-graph-adc.json")
	if err := os.WriteFile(path, []byte(validImpersonatedCarrier(testIdentity)), 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	selector := Selector{
		CredentialsFile:     path,
		CredentialsProfile:  CredentialsProfileImpersonatedServiceAccount,
		CredentialsIdentity: testIdentity,
	}
	t.Run("GODEBUG", func(t *testing.T) {
		for _, key := range ambientGoogleTransportOverrideKeys {
			t.Setenv(key, "")
		}
		t.Setenv("GODEBUG", "tlsmlkem=0")
		if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "tlsmlkem=0") {
			t.Fatalf("explicit carrier GODEBUG error = %v", err)
		}
	})
	for _, key := range []string{"HTTPS_PROXY", "SPANNER_EMULATOR_HOST", "GCE_METADATA_HOST"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "attacker.invalid")
			if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), key+" must be unset") {
				t.Fatalf("explicit carrier ambient override error = %v", err)
			}
		})
	}
}

func validDeviceServiceAccountCarrier(baseIdentity string) string {
	return fmt.Sprintf(`{
  "type": "service_account",
  "client_email": %q,
  "private_key_id": "fixture-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nfixture\n-----END PRIVATE KEY-----\n",
  "token_uri": "https://oauth2.googleapis.com/token"
}`, baseIdentity)
}

func TestSelectorValidateClosesDeviceServiceAccountArm(t *testing.T) {
	baseIdentity := "workstation-fixture-host@fixture-project.iam.gserviceaccount.com"
	valid := Selector{
		CredentialsSource:       CredentialsSourceApplicationDefault,
		CredentialsProfile:      CredentialsProfileDeviceServiceAccount,
		CredentialsIdentity:     testIdentity,
		CredentialsBaseIdentity: baseIdentity,
	}
	if err := valid.Validate("fixture"); err != nil {
		t.Fatalf("valid device_service_account selector: %v", err)
	}
	for _, tt := range []struct {
		name     string
		selector Selector
		want     string
	}{
		{
			name: "missing base identity",
			selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault,
				CredentialsProfile: CredentialsProfileDeviceServiceAccount, CredentialsIdentity: testIdentity},
			want: "credentials_base_identity",
		},
		{
			name: "human base identity",
			selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault,
				CredentialsProfile: CredentialsProfileDeviceServiceAccount, CredentialsIdentity: testIdentity,
				CredentialsBaseIdentity: "person@example.com"},
			want: "service-account email",
		},
		{
			name: "base equals target",
			selector: Selector{CredentialsSource: CredentialsSourceApplicationDefault,
				CredentialsProfile: CredentialsProfileDeviceServiceAccount, CredentialsIdentity: testIdentity,
				CredentialsBaseIdentity: testIdentity},
			want: "must differ",
		},
		{
			name: "explicit file forbidden",
			selector: Selector{CredentialsFile: "/tmp/adc.json",
				CredentialsProfile: CredentialsProfileDeviceServiceAccount, CredentialsIdentity: testIdentity,
				CredentialsBaseIdentity: baseIdentity},
			want: "forbids credentials_file",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.selector.Validate("fixture"); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestDeviceServiceAccountConfigurationClosesMismatchedOrKeylessCarrier(t *testing.T) {
	baseIdentity := "workstation-fixture-host@fixture-project.iam.gserviceaccount.com"
	valid := validDeviceServiceAccountCarrier(baseIdentity)
	if err := validateDeviceServiceAccountConfiguration([]byte(valid), baseIdentity); err != nil {
		t.Fatalf("validateDeviceServiceAccountConfiguration(valid): %v", err)
	}
	for _, body := range []string{
		`not-json`,
		strings.Replace(valid, `"type": "service_account"`, `"type": "authorized_user"`, 1),
		strings.Replace(valid, fmt.Sprintf("%q", baseIdentity), `"someone-else@fixture-project.iam.gserviceaccount.com"`, 1),
		strings.Replace(valid, `"private_key_id": "fixture-key-id"`, `"private_key_id": ""`, 1),
		strings.Replace(valid, `"token_uri": "https://oauth2.googleapis.com/token"`, `"token_uri": ""`, 1),
	} {
		if err := validateDeviceServiceAccountConfiguration([]byte(body), baseIdentity); err == nil {
			t.Fatalf("validateDeviceServiceAccountConfiguration accepted %s", body)
		}
	}
}

func TestDeviceServiceAccountTokenSourceUsesOnlyWellKnownOwnerOnlyCarrier(t *testing.T) {
	baseIdentity := "workstation-fixture-host@fixture-project.iam.gserviceaccount.com"
	directory := t.TempDir()
	path := filepath.Join(directory, "adc.json")
	if err := os.WriteFile(path, []byte(validDeviceServiceAccountCarrier(baseIdentity)), 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	originalPath := legacyCredentialsPath
	legacyCredentialsPath = func(string) (string, error) { return path, nil }
	t.Cleanup(func() { legacyCredentialsPath = originalPath })

	selector := Selector{
		CredentialsSource:       CredentialsSourceApplicationDefault,
		CredentialsProfile:      CredentialsProfileDeviceServiceAccount,
		CredentialsIdentity:     testIdentity,
		CredentialsBaseIdentity: baseIdentity,
	}
	// Unlike the impersonated and attached arms, GOOGLE_APPLICATION_CREDENTIALS
	// must NOT block this profile — it never performs ambient ADC discovery,
	// and a host's backup agent setting it for an unrelated purpose must not
	// take an otherwise-correctly-configured device identity offline.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(directory, "unrelated-backup-agent-carrier.json"))
	if source, err := selector.TokenSource(context.Background(), CloudPlatformScope); err != nil || source == nil {
		t.Fatalf("token source = %v, error = %v", source, err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "owner-only") {
			t.Fatalf("world-readable device carrier error = %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	}
}

func TestDeviceServiceAccountTokenSourceRejectsWrongAccountCarrier(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adc.json")
	// The file at the resolved path claims to belong to a different account
	// than the one named in config — must fail closed rather than silently
	// impersonating using the wrong device's credential.
	if err := os.WriteFile(path, []byte(validDeviceServiceAccountCarrier("someone-else@fixture-project.iam.gserviceaccount.com")), 0o600); err != nil {
		t.Fatal(err)
	}
	ensureCredentialFixtureOwnedByCurrentUser(t, path)
	originalPath := legacyCredentialsPath
	legacyCredentialsPath = func(string) (string, error) { return path, nil }
	t.Cleanup(func() { legacyCredentialsPath = originalPath })

	selector := Selector{
		CredentialsSource:       CredentialsSourceApplicationDefault,
		CredentialsProfile:      CredentialsProfileDeviceServiceAccount,
		CredentialsIdentity:     testIdentity,
		CredentialsBaseIdentity: "workstation-fixture-host@fixture-project.iam.gserviceaccount.com",
	}
	if _, err := selector.TokenSource(context.Background(), CloudPlatformScope); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("TokenSource() error = %v, want client_email mismatch refusal", err)
	}
}
