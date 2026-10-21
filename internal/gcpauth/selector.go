// Package gcpauth resolves one process-level, identity-bound Google credential
// selector. The local arm validates the exact well-known impersonated ADC
// carrier. The device arm impersonates a scoped operator identity using the
// workstation's own already-registered service account as the caller. The
// hosted arm proves the attached metadata identity.
package gcpauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

const (
	CredentialsSourceApplicationDefault                = "application_default"
	CredentialsProfileImpersonatedServiceAccount       = "impersonated_service_account"
	CredentialsProfileAttachedServiceAccount           = "attached_service_account"
	CredentialsProfileDeviceServiceAccount             = "device_service_account"
	CloudPlatformScope                                 = "https://www.googleapis.com/auth/cloud-platform"
	maxCredentialConfigurationBytes              int64 = 1 << 20
)

var serviceAccountEmailPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$`)

var forbiddenGODEBUGFragments = [...]string{
	"http2debug=",
	"tlsmlkem=0",
	"tlssecpmlkem=0",
}

var ambientGoogleTransportOverrideKeys = [...]string{
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

const (
	sealedGoogleClientCertificate = "false"
	sealedGoogleMTLSEndpoint      = "never"
)

type metadataClient interface {
	OnGCEWithContext(context.Context) bool
	EmailWithContext(context.Context, string) (string, error)
	GetWithContext(context.Context, string) (string, error)
}

var (
	newMetadataClient  = func() metadataClient { return metadata.NewClient(nil) }
	computeTokenSource = func(account string, scopes ...string) oauth2.TokenSource {
		return google.ComputeTokenSource(account, scopes...)
	}
	defaultADCPath = func() (string, error) {
		const filename = "application_default_credentials.json"
		if runtime.GOOS == "windows" {
			root := strings.TrimSpace(os.Getenv("APPDATA"))
			if root == "" {
				return "", fmt.Errorf("APPDATA is required to resolve the well-known ADC carrier")
			}
			return filepath.Join(root, "gcloud", filename), nil
		}
		root := strings.TrimSpace(os.Getenv("HOME"))
		if root == "" {
			return "", fmt.Errorf("HOME is required to resolve the well-known ADC carrier")
		}
		return filepath.Join(root, ".config", "gcloud", filename), nil
	}
	// legacyCredentialsPath resolves gcloud's own well-known storage location
	// for one already-authenticated account's credential — the same file
	// gcloud itself reads for `gcloud auth print-access-token
	// --account=<account>`. This is never a config-supplied path: the only
	// input is the account name, so a config cannot point
	// device_service_account at an arbitrary file on disk.
	legacyCredentialsPath = func(account string) (string, error) {
		if runtime.GOOS == "windows" {
			root := strings.TrimSpace(os.Getenv("APPDATA"))
			if root == "" {
				return "", fmt.Errorf("APPDATA is required to resolve the device identity carrier")
			}
			return filepath.Join(root, "gcloud", "legacy_credentials", account, "adc.json"), nil
		}
		root := strings.TrimSpace(os.Getenv("HOME"))
		if root == "" {
			return "", fmt.Errorf("HOME is required to resolve the device identity carrier")
		}
		return filepath.Join(root, ".config", "gcloud", "legacy_credentials", account, "adc.json"), nil
	}
)

// Selector is the one credential selector shared by every Google client in a
// process. Exactly one carrier is selected: CredentialsFile names an explicit
// impersonated-ADC document, and CredentialsSource selects the well-known
// gcloud ADC location. The explicit carrier exists so a machine whose single
// well-known ADC slot belongs to another identity can still bind this process
// to its own operator; it is not a weaker arm. Both carriers hold the same
// typed impersonation document and receive identical byte-level validation in
// TokenSource. The hosted attached_service_account profile accepts neither
// file carrier.
type Selector struct {
	CredentialsFile     string
	CredentialsSource   string
	CredentialsProfile  string
	CredentialsIdentity string
	// CredentialsBaseIdentity is consumed only by device_service_account: it
	// names the workstation's own already-registered service account, whose
	// credential the profile reads exclusively from gcloud's well-known
	// per-account legacy_credentials location (never a config-supplied path)
	// and uses solely as the caller that impersonates CredentialsIdentity.
	CredentialsBaseIdentity string
}

// Validate closes the selector before any Google client is constructed.
func (s Selector) Validate(label string) error {
	if label == "" {
		label = "google_credentials"
	}
	if s.CredentialsFile != "" && s.CredentialsSource != "" {
		return fmt.Errorf("%s: credentials_file and credentials_source are mutually exclusive", label)
	}
	if s.CredentialsFile == "" && s.CredentialsSource == "" {
		return fmt.Errorf("%s: credentials_source=%s is required", label, CredentialsSourceApplicationDefault)
	}
	if s.CredentialsSource != "" && s.CredentialsSource != CredentialsSourceApplicationDefault {
		return fmt.Errorf("%s: unsupported credentials_source %q", label, s.CredentialsSource)
	}
	if !serviceAccountEmailPattern.MatchString(s.CredentialsIdentity) {
		return fmt.Errorf("%s: credentials_identity %q is not a canonical user-managed service-account email", label, s.CredentialsIdentity)
	}
	switch s.CredentialsProfile {
	case CredentialsProfileImpersonatedServiceAccount:
		return nil
	case CredentialsProfileAttachedServiceAccount:
		// The hosted arm proves its identity from attached metadata. An explicit
		// on-disk carrier would reintroduce a file credential the hosted
		// deployment must never read, so it is refused with its own reason
		// rather than folded into the missing-source case.
		if s.CredentialsFile != "" {
			return fmt.Errorf("%s: attached_service_account forbids credentials_file; the hosted arm proves its identity from attached metadata", label)
		}
		if s.CredentialsSource != CredentialsSourceApplicationDefault {
			return fmt.Errorf("%s: attached_service_account requires credentials_source=%s", label, CredentialsSourceApplicationDefault)
		}
		return nil
	case CredentialsProfileDeviceServiceAccount:
		if s.CredentialsFile != "" {
			return fmt.Errorf("%s: device_service_account forbids credentials_file; the base credential is resolved from credentials_base_identity", label)
		}
		if s.CredentialsSource != CredentialsSourceApplicationDefault {
			return fmt.Errorf("%s: device_service_account requires credentials_source=%s", label, CredentialsSourceApplicationDefault)
		}
		if !serviceAccountEmailPattern.MatchString(s.CredentialsBaseIdentity) {
			return fmt.Errorf("%s: credentials_base_identity %q is not a canonical user-managed service-account email", label, s.CredentialsBaseIdentity)
		}
		if s.CredentialsBaseIdentity == s.CredentialsIdentity {
			return fmt.Errorf("%s: credentials_base_identity must differ from credentials_identity", label)
		}
		return nil
	case "":
		return fmt.Errorf("%s: credentials_profile is required", label)
	default:
		return fmt.Errorf("%s: unsupported credentials_profile %q", label, s.CredentialsProfile)
	}
}

// TokenSource returns a lifetime token source suitable for long-lived Google
// clients. Request-scoped context cancellation cannot poison later refreshes.
func (s Selector) TokenSource(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return s.tokenSource(ctx, true, scopes...)
}

// TokenSourceForRequest binds refresh I/O to ctx.
func (s Selector) TokenSourceForRequest(ctx context.Context, scopes ...string) (oauth2.TokenSource, error) {
	return s.tokenSource(ctx, false, scopes...)
}

// RequestTokenSourceFactory binds the selector and scopes without binding a
// refresh context. The caller supplies one context per refresh.
func (s Selector) RequestTokenSourceFactory(scopes ...string) func(context.Context) (oauth2.TokenSource, error) {
	bound := append([]string(nil), scopes...)
	return func(ctx context.Context) (oauth2.TokenSource, error) {
		return s.TokenSourceForRequest(ctx, bound...)
	}
}

func (s Selector) tokenSource(ctx context.Context, detachRefresh bool, scopes ...string) (oauth2.TokenSource, error) {
	if ctx == nil {
		return nil, fmt.Errorf("google credentials context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("google credentials context: %w", err)
	}
	if err := s.Validate("google_credentials"); err != nil {
		return nil, err
	}
	if err := sealAmbientGoogleTransport(); err != nil {
		return nil, err
	}
	if len(scopes) == 0 {
		scopes = []string{CloudPlatformScope}
	}
	switch s.CredentialsProfile {
	case CredentialsProfileImpersonatedServiceAccount:
		return s.impersonatedTokenSource(ctx, scopes, detachRefresh)
	case CredentialsProfileAttachedServiceAccount:
		return s.attachedTokenSource(ctx, scopes, detachRefresh)
	case CredentialsProfileDeviceServiceAccount:
		return s.deviceServiceAccountTokenSource(ctx, scopes, detachRefresh)
	default:
		return nil, fmt.Errorf("google_credentials: unsupported credentials_profile %q", s.CredentialsProfile)
	}
}

func sealAmbientGoogleTransport() error {
	if err := validateSecuritySensitiveGODEBUG(os.Getenv("GODEBUG")); err != nil {
		return err
	}
	for _, key := range ambientGoogleTransportOverrideKeys {
		value := os.Getenv(key)
		switch key {
		case "GOOGLE_API_USE_CLIENT_CERTIFICATE":
			if value != "" && value != sealedGoogleClientCertificate {
				return fmt.Errorf("google_credentials: %s must be unset or equal %q; ambient Google transport overrides are forbidden", key, sealedGoogleClientCertificate)
			}
		case "GOOGLE_API_USE_MTLS_ENDPOINT":
			if value != "" && value != sealedGoogleMTLSEndpoint {
				return fmt.Errorf("google_credentials: %s must be unset or equal %q; ambient Google transport overrides are forbidden", key, sealedGoogleMTLSEndpoint)
			}
		default:
			if value != "" {
				return fmt.Errorf("google_credentials: %s must be unset; ambient Google transport overrides are forbidden", key)
			}
		}
	}
	for key, value := range map[string]string{
		"GOOGLE_API_USE_CLIENT_CERTIFICATE": sealedGoogleClientCertificate,
		"GOOGLE_API_USE_MTLS_ENDPOINT":      sealedGoogleMTLSEndpoint,
	} {
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("google_credentials: seal %s: %w", key, err)
		}
	}
	return nil
}

func validateSecuritySensitiveGODEBUG(value string) error {
	for _, fragment := range forbiddenGODEBUGFragments {
		if strings.Contains(value, fragment) {
			return fmt.Errorf("google_credentials: GODEBUG contains forbidden setting %q", fragment)
		}
	}
	return nil
}

func (s Selector) impersonatedTokenSource(ctx context.Context, scopes []string, detachRefresh bool) (oauth2.TokenSource, error) {
	// Both carriers reject every ambient credential-path override. Rejecting
	// CLOUDSDK_CONFIG on the explicit arm too is deliberately stricter than the
	// arm strictly requires: an operator who set it meant to redirect gcloud
	// credentials, and this process refuses to run beside that intent.
	carrier := "the validated well-known gcloud ADC carrier"
	if s.CredentialsFile != "" {
		carrier = "the validated explicit credentials_file carrier"
	}
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE", "CLOUDSDK_CONFIG"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return nil, fmt.Errorf("google_credentials: %s must be unset; impersonated credentials use only %s", key, carrier)
		}
	}
	path := s.CredentialsFile
	if path == "" {
		resolved, err := defaultADCPath()
		if err != nil {
			return nil, fmt.Errorf("google_credentials: resolve well-known impersonated ADC carrier: %w", err)
		}
		path = resolved
	}
	data, err := readOwnerOnlyRegularFile(path)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: read impersonated ADC carrier: %w", err)
	}
	if err := validateImpersonatedConfiguration(data, s.CredentialsIdentity); err != nil {
		return nil, fmt.Errorf("google_credentials: validate impersonated ADC carrier: %w", err)
	}
	credentialContext := ctx
	if detachRefresh {
		credentialContext = context.Background()
		if client, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && client != nil {
			credentialContext = context.WithValue(credentialContext, oauth2.HTTPClient, client)
		}
	}
	credentials, err := google.CredentialsFromJSONWithType(credentialContext, data, google.ImpersonatedServiceAccount, scopes...)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: parse validated impersonated ADC carrier: %w", err)
	}
	if credentials == nil || credentials.TokenSource == nil {
		return nil, fmt.Errorf("google_credentials: validated impersonated ADC carrier returned no token source")
	}
	return credentials.TokenSource, nil
}

func (s Selector) attachedTokenSource(ctx context.Context, scopes []string, detachRefresh bool) (oauth2.TokenSource, error) {
	for _, key := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return nil, fmt.Errorf("google_credentials: %s must be unset for attached_service_account", key)
		}
	}
	client := newMetadataClient()
	if client == nil || !client.OnGCEWithContext(ctx) {
		return nil, fmt.Errorf("google_credentials: attached_service_account requires Google-hosted metadata")
	}
	email, err := client.EmailWithContext(ctx, "default")
	if err != nil {
		return nil, fmt.Errorf("google_credentials: resolve attached metadata identity: %w", err)
	}
	email = strings.TrimSpace(email)
	if email != s.CredentialsIdentity {
		return nil, fmt.Errorf("google_credentials: attached metadata identity %q does not match credentials_identity %q", email, s.CredentialsIdentity)
	}
	if !detachRefresh {
		return requestMetadataTokenSource{ctx: ctx, client: client, account: s.CredentialsIdentity, scopes: append([]string(nil), scopes...)}, nil
	}
	source := computeTokenSource(s.CredentialsIdentity, scopes...)
	if source == nil {
		return nil, fmt.Errorf("google_credentials: metadata token source is unavailable")
	}
	return source, nil
}

// deviceServiceAccountTokenSource impersonates CredentialsIdentity using the
// workstation's own already-registered device service account
// (CredentialsBaseIdentity, e.g. workstation-<host>@...) as the caller. This is the
// non-human counterpart to impersonatedTokenSource: same target-identity
// scoping and same impersonation mechanism, but the caller is a machine
// identity gcloud already manages on this host rather than a human OAuth
// session, so no interactive login is ever required to keep it minting.
//
// The base identity's own credential is read from exactly one place — the
// well-known, gcloud-managed legacy_credentials/<account>/adc.json location
// keyed by CredentialsBaseIdentity — never from a config-supplied path. That
// is what keeps this profile from becoming a second on-ramp for the scattered
// raw-key-file references the keyless migration removed: the only file this
// code will ever open is the one gcloud itself wrote for the exact account
// named in config, and it is used for nothing except minting a token for the
// scoped, independently-revocable operator identity. The device identity's
// own (broad, workstation-wide) permissions are never exercised directly by
// this process; every subsequent API call runs as CredentialsIdentity.
//
// Deliberately does not check GOOGLE_APPLICATION_CREDENTIALS or
// CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE: unlike the other two profiles, this
// one never performs ambient ADC discovery or reads gcloud's active-config
// location, so those variables have no effect on which bytes it reads.
// Refusing on their mere presence would only recreate the exact footgun this
// profile exists to remove — a host's backup agent setting
// GOOGLE_APPLICATION_CREDENTIALS at user scope for an unrelated purpose must
// not be able to take an otherwise-correctly-configured device identity
// offline.
func (s Selector) deviceServiceAccountTokenSource(ctx context.Context, scopes []string, detachRefresh bool) (oauth2.TokenSource, error) {
	basePath, err := legacyCredentialsPath(s.CredentialsBaseIdentity)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: resolve device base identity carrier: %w", err)
	}
	data, err := readOwnerOnlyRegularFile(basePath)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: read device base identity carrier: %w", err)
	}
	if err := validateDeviceServiceAccountConfiguration(data, s.CredentialsBaseIdentity); err != nil {
		return nil, fmt.Errorf("google_credentials: validate device base identity carrier: %w", err)
	}
	credentialContext := ctx
	var testClient *http.Client
	if client, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && client != nil {
		testClient = client
	}
	if detachRefresh {
		credentialContext = context.Background()
		if testClient != nil {
			credentialContext = context.WithValue(credentialContext, oauth2.HTTPClient, testClient)
		}
	}
	opts := []option.ClientOption{option.WithCredentialsJSON(data)}
	if testClient != nil {
		// impersonate.CredentialsTokenSource builds its own transport via
		// option.ClientOption rather than honoring the oauth2.HTTPClient
		// context value google.CredentialsFromJSONWithType reads, so the
		// fixture transport the rest of this package's tests inject through
		// context must be forwarded explicitly here too.
		opts = append(opts, option.WithHTTPClient(testClient))
	}
	source, err := impersonate.CredentialsTokenSource(credentialContext, impersonate.CredentialsConfig{
		TargetPrincipal: s.CredentialsIdentity,
		Scopes:          scopes,
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("google_credentials: build device-identity impersonation source: %w", err)
	}
	return source, nil
}

// validateDeviceServiceAccountConfiguration confirms the file at the resolved
// legacy_credentials path is actually a service-account key for the exact
// account named in config, rather than trusting the path alone. Unlike
// validateImpersonatedConfiguration, this intentionally does not reject
// private_key fields — a service-account key is the one legitimate carrier
// shape this function exists to accept, precisely because it is scoped to a
// single well-known per-account gcloud-managed location rather than an
// arbitrary config-supplied path.
func validateDeviceServiceAccountConfiguration(data []byte, expectedIdentity string) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if credentialType, _ := raw["type"].(string); credentialType != "service_account" {
		return fmt.Errorf("top-level type must be %q", "service_account")
	}
	clientEmail, _ := raw["client_email"].(string)
	if clientEmail != expectedIdentity {
		return fmt.Errorf("client_email %q does not match credentials_base_identity %q", clientEmail, expectedIdentity)
	}
	for _, field := range []string{"private_key", "private_key_id", "token_uri"} {
		value, ok := raw[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	return nil
}

type requestMetadataTokenSource struct {
	ctx     context.Context
	client  metadataClient
	account string
	scopes  []string
}

func (s requestMetadataTokenSource) Token() (*oauth2.Token, error) {
	tokenURI := "instance/service-accounts/" + url.PathEscape(s.account) + "/token"
	if len(s.scopes) > 0 {
		values := url.Values{}
		values.Set("scopes", strings.Join(s.scopes, ","))
		tokenURI += "?" + values.Encode()
	}
	raw, err := s.client.GetWithContext(s.ctx, tokenURI)
	if err != nil {
		return nil, err
	}
	var response oauth2.Token
	if err := json.NewDecoder(strings.NewReader(raw)).Decode(&response); err != nil {
		return nil, fmt.Errorf("google_credentials: invalid token JSON from metadata: %w", err)
	}
	if response.ExpiresIn == 0 || strings.TrimSpace(response.AccessToken) == "" {
		return nil, fmt.Errorf("google_credentials: incomplete token received from metadata")
	}
	token := &oauth2.Token{
		AccessToken: response.AccessToken,
		TokenType:   response.TokenType,
		Expiry:      time.Now().Add(time.Duration(response.ExpiresIn) * time.Second),
	}
	return token.WithExtra(map[string]any{
		"oauth2.google.tokenSource":    "compute-metadata",
		"oauth2.google.serviceAccount": s.account,
	}), nil
}

func readOwnerOnlyRegularFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" || !filepath.IsAbs(path) {
		return nil, fmt.Errorf("credential path must be absolute")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("credential path must not be a symlink")
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("credential path must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return nil, fmt.Errorf("credential file identity changed while opening")
	}
	if err := validateCredentialFileAccess(file, openedInfo); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxCredentialConfigurationBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxCredentialConfigurationBytes {
		return nil, fmt.Errorf("credential configuration exceeds %d bytes", maxCredentialConfigurationBytes)
	}
	return data, nil
}

func validateImpersonatedConfiguration(data []byte, identity string) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := validateObjectKeys(raw, "top-level", []string{"type", "service_account_impersonation_url", "delegates", "source_credentials", "scopes", "quota_project_id"}); err != nil {
		return err
	}
	if err := rejectForbiddenCredentialFields(raw); err != nil {
		return err
	}
	if credentialType, _ := raw["type"].(string); credentialType != CredentialsProfileImpersonatedServiceAccount {
		return fmt.Errorf("top-level type must be %q", CredentialsProfileImpersonatedServiceAccount)
	}
	targetURL, _ := raw["service_account_impersonation_url"].(string)
	if err := validateImpersonationURL(targetURL, identity); err != nil {
		return err
	}
	if configuredQuotaProject, exists := raw["quota_project_id"]; exists {
		value, ok := configuredQuotaProject.(string)
		if !ok || value != serviceAccountProject(identity) {
			return fmt.Errorf("quota_project_id %q must match the impersonated service account project %q when present", value, serviceAccountProject(identity))
		}
	}
	source, ok := raw["source_credentials"].(map[string]any)
	if !ok {
		return fmt.Errorf("source_credentials object is required")
	}
	if err := validateObjectKeys(source, "source_credentials", []string{"type", "client_id", "client_secret", "refresh_token", "token_uri", "rapt_token", "universe_domain", "account"}); err != nil {
		return err
	}
	if sourceType, _ := source["type"].(string); sourceType != "authorized_user" {
		return fmt.Errorf("source_credentials.type must be %q", "authorized_user")
	}
	for _, field := range []string{"client_id", "client_secret", "refresh_token"} {
		value, ok := source[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("source_credentials.%s is required", field)
		}
	}
	if tokenURI, exists := source["token_uri"]; exists {
		value, ok := tokenURI.(string)
		if !ok || value != "https://oauth2.googleapis.com/token" {
			return fmt.Errorf("source_credentials.token_uri must equal %q when present", "https://oauth2.googleapis.com/token")
		}
	}
	if universeDomain, exists := source["universe_domain"]; exists {
		value, ok := universeDomain.(string)
		if !ok || value != "googleapis.com" {
			return fmt.Errorf("source_credentials.universe_domain must equal %q when present", "googleapis.com")
		}
	}
	for _, field := range []string{"rapt_token", "account"} {
		if value, exists := source[field]; exists {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("source_credentials.%s must be a string when present", field)
			}
		}
	}
	delegates, ok := raw["delegates"].([]any)
	if !ok || len(delegates) != 0 {
		return fmt.Errorf("delegates must be an empty array")
	}
	if configuredScopes, exists := raw["scopes"]; exists {
		scopes, ok := configuredScopes.([]any)
		if !ok || len(scopes) != 1 || scopes[0] != CloudPlatformScope {
			return fmt.Errorf("scopes must contain only %q when present", CloudPlatformScope)
		}
	}
	return nil
}

func validateObjectKeys(object map[string]any, label string, allowedKeys []string) error {
	allowed := make(map[string]struct{}, len(allowedKeys))
	for _, key := range allowedKeys {
		allowed[key] = struct{}{}
	}
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s field %q is not allowed", label, key)
		}
	}
	return nil
}

func validateImpersonationURL(rawURL, identity string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("service_account_impersonation_url: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "iamcredentials.googleapis.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("service_account_impersonation_url must use the canonical IAMCredentials authority")
	}
	wantPath := "/v1/projects/-/serviceAccounts/" + identity + ":generateAccessToken"
	if parsed.EscapedPath() != wantPath {
		return fmt.Errorf("service_account_impersonation_url target %q does not match credentials_identity %q", parsed.EscapedPath(), identity)
	}
	return nil
}

func serviceAccountProject(identity string) string {
	localAndProject := strings.TrimSuffix(identity, ".iam.gserviceaccount.com")
	separator := strings.LastIndexByte(localAndProject, '@')
	if separator < 0 || separator == len(localAndProject)-1 {
		return ""
	}
	return localAndProject[separator+1:]
}

func rejectForbiddenCredentialFields(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "private_key" || key == "private_key_id" {
				return fmt.Errorf("credential field %q is forbidden", key)
			}
			if err := rejectForbiddenCredentialFields(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectForbiddenCredentialFields(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("credential JSON contains trailing data")
		}
		return fmt.Errorf("credential JSON trailing data: %w", err)
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode credential JSON: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode credential JSON key: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("credential JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("credential JSON object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("credential JSON array is not closed")
		}
	default:
		return fmt.Errorf("unexpected credential JSON delimiter %q", delimiter)
	}
	return nil
}
