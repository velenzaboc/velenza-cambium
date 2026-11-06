package gcpauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/auth"
)

func clearWorkloadEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "CLOUDSDK_AUTH_ACCESS_TOKEN", "CLOUDSDK_AUTH_ACCESS_TOKEN_FILE", "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT", "CLOUDSDK_CONFIG", "GOOGLE_GHA_CREDS_PATH", "GOOGLE_API_CERTIFICATE_CONFIG", "GCE_METADATA_HOST", "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
		"EXPERIMENTAL_GOOGLE_API_USE_S2A", "GRPC_BINARY_LOG_FILTER", "GRPC_ENFORCE_ALPN_ENABLED", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB", "GOOGLE_API_GO_EXPERIMENTAL_ENABLE_NEW_AUTH_LIB", "GOOGLE_API_USE_MTLS", "GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED", "GOOGLE_CLOUD_DISABLE_DIRECT_PATH", "GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS", "GOOGLE_CLOUD_QUOTA_PROJECT", "GOOGLE_CLOUD_UNIVERSE_DOMAIN", "GOOGLE_SPANNER_DISABLE_DIRECT_ACCESS_BOUND_TOKEN", "GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API", "SPANNER_MONITORING_HOST", "SSL_CERT_DIR", "SSL_CERT_FILE", "GOOGLE_SDK_GO_LOGGING_LEVEL", "GOOGLE_API_USE_CLIENT_CERTIFICATE", "GOOGLE_API_USE_MTLS_ENDPOINT", "GODEBUG",
	} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

type workloadFixture struct {
	selector                                   Selector
	carrierPath, configPath, certPath, keyPath string
	carrier                                    map[string]any
	config                                     map[string]any
}

func newWorkloadFixture(t *testing.T) workloadFixture {
	t.Helper()
	clearWorkloadEnvironment(t)
	dir := t.TempDir()
	f := workloadFixture{carrierPath: filepath.Join(dir, "verification host.json"), configPath: filepath.Join(dir, "certificate_config.json"), certPath: filepath.Join(dir, "client.pem"), keyPath: filepath.Join(dir, "client.key")}
	f.selector = Selector{CredentialsSource: CredentialsSourceApplicationDefault, CredentialsProfile: CredentialsProfileWorkloadIdentityFederation, CredentialsIdentity: "graph-operator@verification-project.iam.gserviceaccount.com", CredentialsAudience: "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/verification-device/providers/device-certificate"}
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1001), Subject: pkix.Name{Organization: []string{"Velenza"}, CommonName: "verification device federation CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	parsedCA, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(1002), Subject: pkix.Name{Organization: []string{"Velenza"}, CommonName: "verification host"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, leaf, parsedCA, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkloadBytes(t, f.certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeWorkloadBytes(t, f.keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))
	f.config = map[string]any{"cert_configs": map[string]any{"workload": map[string]any{"cert_path": f.certPath, "key_path": f.keyPath}}}
	f.carrier = map[string]any{"type": "external_account", "audience": f.selector.CredentialsAudience, "subject_token_type": "urn:ietf:params:oauth:token-type:mtls", "token_url": "https://sts.mtls.googleapis.com/v1/token", "service_account_impersonation_url": "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/" + f.selector.CredentialsIdentity + ":generateAccessToken", "universe_domain": "googleapis.com", "credential_source": map[string]any{"certificate": map[string]any{"certificate_config_location": f.configPath}}}
	writeWorkloadJSON(t, f.configPath, f.config)
	writeWorkloadJSON(t, f.carrierPath, f.carrier)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", f.carrierPath)
	return f
}
func writeWorkloadBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func writeWorkloadJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkloadBytes(t, path, data)
}

func TestWorkloadProfileConstructsTypedX509AndScopedOperatorSources(t *testing.T) {
	f := newWorkloadFixture(t)
	for _, target := range []string{"code-graph-operator@verification-project.iam.gserviceaccount.com", "history-reader@verification-project.iam.gserviceaccount.com"} {
		s := f.selector
		s.CredentialsIdentity = target
		f.carrier["service_account_impersonation_url"] = "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/" + target + ":generateAccessToken"
		writeWorkloadJSON(t, f.carrierPath, f.carrier)
		if err := s.Validate("google_credentials"); err != nil {
			t.Fatal(err)
		}
		raw, err := s.ValidateWorkloadIdentityConfiguration()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), s.CredentialsIdentity) {
			t.Fatal("validated source lost service identity")
		}
		source, err := s.TokenSource(context.Background())
		if err != nil {
			t.Fatalf("real modern SDK source construction: %v", err)
		}
		if _, ok := source.(*workloadOAuthTokenSource); !ok {
			t.Fatalf("unexpected effective source: %T", source)
		}
		source, err = s.TokenSourceForRequest(context.Background())
		if err != nil || source == nil {
			t.Fatalf("request-scoped source: %v", err)
		}
	}
}

func TestWorkloadSelectorRejectsAmbiguousTopology(t *testing.T) {
	f := newWorkloadFixture(t)
	for _, tc := range []struct {
		name string
		edit func(*Selector)
	}{
		{"wrong profile", func(s *Selector) { s.CredentialsProfile = CredentialsProfileAttachedServiceAccount }},
		{"missing audience", func(s *Selector) { s.CredentialsAudience = "" }},
		{"wrong audience host", func(s *Selector) {
			s.CredentialsAudience = strings.ReplaceAll(s.CredentialsAudience, "iam.googleapis.com", "attacker.invalid")
		}},
		{"host base specified", func(s *Selector) {
			s.CredentialsBaseIdentity = "host-controller@verification-project.iam.gserviceaccount.com"
		}},
		{"target used as base", func(s *Selector) { s.CredentialsBaseIdentity = s.CredentialsIdentity }},
		{"human source", func(s *Selector) { s.CredentialsBaseIdentity = "operator@example.com" }},
		{"human target", func(s *Selector) { s.CredentialsIdentity = "operator@example.com" }},
		{"explicit second file", func(s *Selector) { s.CredentialsFile = f.carrierPath }},
		{"ambient source", func(s *Selector) { s.CredentialsSource = "ambient" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := f.selector
			tc.edit(&s)
			if _, err := s.ValidateWorkloadIdentityConfiguration(); err == nil {
				t.Fatal("invalid selector accepted")
			}
		})
	}
	if _, err := f.selector.TokenSource(context.Background(), "https://www.googleapis.com/auth/drive"); err == nil {
		t.Fatal("broader/different scope accepted")
	}
}

func TestWorkloadCarrierRejectsDifferentIdentityAndCredentialMechanisms(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"different type", func(c map[string]any) { c["type"] = "authorized_user" }},
		{"different audience", func(c map[string]any) {
			c["audience"] = "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/other-device/providers/device-certificate"
		}},
		{"wrong subject type", func(c map[string]any) { c["subject_token_type"] = "urn:ietf:params:oauth:token-type:jwt" }},
		{"non mtls sts", func(c map[string]any) { c["token_url"] = "https://sts.googleapis.com/v1/token" }},
		{"redirected token", func(c map[string]any) { c["token_url"] = "https://attacker.invalid/token" }},
		{"borrowed source", func(c map[string]any) {
			c["service_account_impersonation_url"] = "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/device-other@verification-project.iam.gserviceaccount.com:generateAccessToken"
		}},
		{"non mtls source", func(c map[string]any) {
			c["service_account_impersonation_url"] = strings.Replace(c["service_account_impersonation_url"].(string), "iamcredentials.mtls.", "iamcredentials.", 1)
		}},
		{"different universe", func(c map[string]any) { c["universe_domain"] = "example.com" }},
		{"quota project", func(c map[string]any) { c["quota_project_id"] = "other-project" }},
		{"token source file", func(c map[string]any) { c["credential_source"] = map[string]any{"file": "/tmp/token"} }},
		{"executable fallback", func(c map[string]any) {
			c["credential_source"] = map[string]any{"executable": map[string]any{"command": "/bin/true"}}
		}},
		{"default certificate discovery", func(c map[string]any) {
			c["credential_source"].(map[string]any)["certificate"].(map[string]any)["use_default_certificate_config"] = true
		}},
		{"embedded private key", func(c map[string]any) { c["private_key"] = "must never be displayed" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkloadFixture(t)
			tc.edit(f.carrier)
			writeWorkloadJSON(t, f.carrierPath, f.carrier)
			if _, err := f.selector.TokenSource(context.Background()); err == nil {
				t.Fatal("changed credential boundary accepted")
			} else if strings.Contains(err.Error(), "must never be displayed") {
				t.Fatal("credential material leaked")
			}
		})
	}
}

func TestWorkloadFilesRequireOwnerOnlyRealFilesAndMatchingKey(t *testing.T) {
	for _, tc := range []string{"carrier permission", "certificate config permission", "certificate permission", "key permission", "carrier symlink", "config symlink", "key symlink", "relative config", "relative certificate", "unknown certificate field", "malformed certificate", "mismatched key", "duplicate JSON", "trailing JSON"} {
		t.Run(tc, func(t *testing.T) {
			f := newWorkloadFixture(t)
			switch tc {
			case "carrier permission":
				if err := os.Chmod(f.carrierPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "certificate config permission":
				if err := os.Chmod(f.configPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "certificate permission":
				if err := os.Chmod(f.certPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "key permission":
				if err := os.Chmod(f.keyPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "carrier symlink", "config symlink", "key symlink":
				p := f.carrierPath
				if tc == "config symlink" {
					p = f.configPath
				}
				if tc == "key symlink" {
					p = f.keyPath
				}
				if err := os.Rename(p, p+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(p+".real", p); err != nil {
					t.Fatal(err)
				}
			case "relative config":
				f.carrier["credential_source"].(map[string]any)["certificate"].(map[string]any)["certificate_config_location"] = "certificate_config.json"
				writeWorkloadJSON(t, f.carrierPath, f.carrier)
			case "relative certificate":
				f.config["cert_configs"].(map[string]any)["workload"].(map[string]any)["cert_path"] = "client.pem"
				writeWorkloadJSON(t, f.configPath, f.config)
			case "unknown certificate field":
				f.config["executable"] = "/bin/true"
				writeWorkloadJSON(t, f.configPath, f.config)
			case "malformed certificate":
				writeWorkloadBytes(t, f.certPath, []byte("not a certificate"))
			case "mismatched key":
				key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				der, err := x509.MarshalPKCS8PrivateKey(key)
				if err != nil {
					t.Fatal(err)
				}
				writeWorkloadBytes(t, f.keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
			case "duplicate JSON":
				writeWorkloadBytes(t, f.carrierPath, []byte(`{"type":"external_account","type":"external_account"}`))
			case "trailing JSON":
				raw, err := os.ReadFile(f.carrierPath)
				if err != nil {
					t.Fatal(err)
				}
				writeWorkloadBytes(t, f.carrierPath, append(raw, []byte(" {}")...))
			}
			if _, err := f.selector.ValidateWorkloadIdentityConfiguration(); err == nil {
				t.Fatal("unsafe local carrier accepted")
			}
		})
	}
}

func TestWorkloadAmbientOverridesRefuseBeforePrivateMaterial(t *testing.T) {
	f := newWorkloadFixture(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/missing/own-workload-carrier")
	for _, key := range []string{"CLOUDSDK_AUTH_ACCESS_TOKEN", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT", "GOOGLE_API_CERTIFICATE_CONFIG", "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES", "HTTPS_PROXY", "SSL_CERT_FILE", "GOOGLE_SDK_GO_LOGGING_LEVEL", "GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "GCE_METADATA_HOST"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "forbidden")
			if _, err := f.selector.ValidateWorkloadIdentityConfiguration(); err == nil || !strings.Contains(err.Error(), "ambient") {
				t.Fatalf("override not refused first: %v", err)
			}
		})
	}
	for _, value := range []string{"tlsmlkem=0", "tlssecpmlkem=0", "http2debug=2"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GODEBUG", value)
			if _, err := f.selector.ValidateWorkloadIdentityConfiguration(); err == nil || !strings.Contains(err.Error(), "transport") {
				t.Fatalf("insecure transport accepted: %v", err)
			}
		})
	}
}

type workloadRecordingProvider struct {
	context context.Context
	fail    bool
}

func (p *workloadRecordingProvider) Token(ctx context.Context) (*auth.Token, error) {
	p.context = ctx
	if p.fail {
		return nil, context.Canceled
	}
	return &auth.Token{Value: "ephemeral-unit-token", Type: "Bearer", Expiry: time.Now().Add(time.Minute)}, nil
}
func TestWorkloadOAuthAdapterPreservesRequestCancellationAndHidesProviderErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &workloadRecordingProvider{}
	s := &workloadOAuthTokenSource{ctx: ctx, provider: p}
	token, err := s.Token()
	if err != nil || token.AccessToken != "ephemeral-unit-token" || p.context.Err() != context.Canceled {
		t.Fatalf("context/token adaptation failed: %v", err)
	}
	p.fail = true
	if _, err := s.Token(); err == nil || strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("provider error was not sanitized: %v", err)
	}
}
