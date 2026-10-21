package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/velenzaboc/velenza-cambium/internal/gcpauth"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

const vertexTokenRefreshTimeout = 30 * time.Second

type googleCredentialRuntime struct {
	selector   gcpauth.Selector
	vertexAuth *vertexBearerAuth
}

var processGoogleCredentials struct {
	sync.RWMutex
	runtime *googleCredentialRuntime
}

func loadGoogleCredentialSelector(getenv func(string) string) (gcpauth.Selector, error) {
	if getenv == nil {
		return gcpauth.Selector{}, fmt.Errorf("code_graph_google_credentials: environment reader is required")
	}
	firstEnv := func(primary, legacy string) string {
		if v := strings.TrimSpace(getenv(primary)); v != "" {
			return v
		}
		return strings.TrimSpace(getenv(legacy))
	}
	selector := gcpauth.Selector{
		CredentialsFile:         firstEnv("VELENZA_CAMBIUM_CREDENTIALS_FILE", "CODE_GRAPH_CREDENTIALS_FILE"),
		CredentialsSource:       firstEnv("VELENZA_CAMBIUM_CREDENTIALS_SOURCE", "CODE_GRAPH_CREDENTIALS_SOURCE"),
		CredentialsProfile:      firstEnv("VELENZA_CAMBIUM_CREDENTIALS_PROFILE", "CODE_GRAPH_CREDENTIALS_PROFILE"),
		CredentialsIdentity:     firstEnv("VELENZA_CAMBIUM_CREDENTIALS_IDENTITY", "CODE_GRAPH_CREDENTIALS_IDENTITY"),
		CredentialsBaseIdentity: firstEnv("VELENZA_CAMBIUM_CREDENTIALS_BASE_IDENTITY", "CODE_GRAPH_CREDENTIALS_BASE_IDENTITY"),
	}
	if err := selector.Validate("code_graph_google_credentials"); err != nil {
		return gcpauth.Selector{}, err
	}
	return selector, nil
}

func initializeGoogleCredentials(ctx context.Context, getenv func(string) string) error {
	if ctx == nil {
		return fmt.Errorf("code_graph_google_credentials: construction context is required")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("code_graph_google_credentials: construction context: %w", err)
	}
	processGoogleCredentials.Lock()
	defer processGoogleCredentials.Unlock()
	if processGoogleCredentials.runtime != nil {
		return fmt.Errorf("code_graph_google_credentials: process selector is already initialized")
	}
	selector, err := loadGoogleCredentialSelector(getenv)
	if err != nil {
		return err
	}
	if source, err := selector.TokenSource(ctx, gcpauth.CloudPlatformScope); err != nil {
		return fmt.Errorf("code_graph_google_credentials: validate Google client token source: %w", err)
	} else if source == nil {
		return fmt.Errorf("code_graph_google_credentials: Google client token source is nil")
	}
	vertexAuth, err := newVertexBearerAuth(ctx, selector.RequestTokenSourceFactory(gcpauth.CloudPlatformScope))
	if err != nil {
		return fmt.Errorf("code_graph_google_credentials: initialize Vertex authentication: %w", err)
	}
	processGoogleCredentials.runtime = &googleCredentialRuntime{selector: selector, vertexAuth: vertexAuth}
	return nil
}

func selectedGoogleCredentials() (*googleCredentialRuntime, error) {
	processGoogleCredentials.RLock()
	runtime := processGoogleCredentials.runtime
	processGoogleCredentials.RUnlock()
	if runtime == nil {
		return nil, fmt.Errorf("code_graph_google_credentials: process selector is not initialized")
	}
	return runtime, nil
}

func newSpannerClient(ctx context.Context) (*spanner.Client, error) {
	runtime, err := selectedGoogleCredentials()
	if err != nil {
		return nil, err
	}
	source, err := runtime.selector.TokenSource(ctx, gcpauth.CloudPlatformScope)
	if err != nil {
		return nil, fmt.Errorf("code_graph_google_credentials: construct Spanner token source: %w", err)
	}
	return spanner.NewClient(ctx, resolveSpannerDBPath(), option.WithTokenSource(source))
}

func newDatabaseAdminClient(ctx context.Context) (*database.DatabaseAdminClient, error) {
	runtime, err := selectedGoogleCredentials()
	if err != nil {
		return nil, err
	}
	source, err := runtime.selector.TokenSource(ctx, gcpauth.CloudPlatformScope)
	if err != nil {
		return nil, fmt.Errorf("code_graph_google_credentials: construct Spanner Admin token source: %w", err)
	}
	return database.NewDatabaseAdminClient(ctx, option.WithTokenSource(source))
}

func attachVertexBearer(req *http.Request) error {
	runtime, err := selectedGoogleCredentials()
	if err != nil {
		return err
	}
	return runtime.vertexAuth.attach(req)
}

type vertexBearerAuth struct {
	mu      sync.Mutex
	factory func(context.Context) (oauth2.TokenSource, error)
	cached  *oauth2.Token
	refresh *vertexTokenRefresh
	timeout time.Duration
}

type vertexTokenRefresh struct {
	done  chan struct{}
	token *oauth2.Token
	err   error
}

func newVertexBearerAuth(ctx context.Context, factory func(context.Context) (oauth2.TokenSource, error)) (*vertexBearerAuth, error) {
	if ctx == nil {
		return nil, fmt.Errorf("vertex authentication construction context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("vertex authentication construction context: %w", err)
	}
	if factory == nil {
		return nil, fmt.Errorf("vertex authentication token-source factory is required")
	}
	source, err := factory(ctx)
	if err != nil {
		return nil, fmt.Errorf("validate Vertex token-source factory: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("Vertex token-source factory returned nil")
	}
	return &vertexBearerAuth{factory: factory, timeout: vertexTokenRefreshTimeout}, nil
}

func (a *vertexBearerAuth) attach(req *http.Request) error {
	if a == nil || req == nil {
		return fmt.Errorf("Vertex bearer request and authentication are required")
	}
	timeout := a.timeout
	if timeout <= 0 {
		timeout = vertexTokenRefreshTimeout
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	token, err := a.token(ctx)
	if err != nil {
		return fmt.Errorf("get Vertex access token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	return nil
}

func (a *vertexBearerAuth) token(ctx context.Context) (*oauth2.Token, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.mu.Lock()
		if a.cached != nil && a.cached.Valid() {
			token := a.cached
			a.mu.Unlock()
			return token, nil
		}
		if inFlight := a.refresh; inFlight != nil {
			a.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-inFlight.done:
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if errors.Is(inFlight.err, context.Canceled) || errors.Is(inFlight.err, context.DeadlineExceeded) {
					continue
				}
				return inFlight.token, inFlight.err
			}
		}
		inFlight := &vertexTokenRefresh{done: make(chan struct{})}
		a.refresh = inFlight
		factory := a.factory
		a.mu.Unlock()

		var token *oauth2.Token
		source, err := factory(ctx)
		if err != nil {
			err = fmt.Errorf("construct request-bound token source: %w", err)
		} else if source == nil {
			err = fmt.Errorf("request-bound token source is nil")
		} else {
			token, err = source.Token()
			if err == nil && (token == nil || !token.Valid()) {
				err = fmt.Errorf("request-bound token source returned an invalid token")
			}
		}

		a.mu.Lock()
		if err == nil {
			a.cached = token
		}
		inFlight.token = token
		inFlight.err = err
		a.refresh = nil
		close(inFlight.done)
		a.mu.Unlock()
		return token, err
	}
}
