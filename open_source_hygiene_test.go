package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestOpenSourceHygieneEnvOverridesAndNoHardcodedProjectConstants(t *testing.T) {
	t.Setenv("VELENZA_CAMBIUM_GCP_PROJECT", "custom-oss-project")
	t.Setenv("VELENZA_CAMBIUM_SPANNER_INSTANCE", "custom-oss-instance")
	t.Setenv("VELENZA_CAMBIUM_SPANNER_DATABASE", "custom-oss-db")

	if got := resolveDefaultGCPProject(); got != "custom-oss-project" {
		t.Fatalf("expected resolveDefaultGCPProject() = %q, got %q", "custom-oss-project", got)
	}
	if got := resolveDefaultSpannerInstance(); got != "custom-oss-instance" {
		t.Fatalf("expected resolveDefaultSpannerInstance() = %q, got %q", "custom-oss-instance", got)
	}
	if got := resolveDefaultSpannerDatabase(); got != "custom-oss-db" {
		t.Fatalf("expected resolveDefaultSpannerDatabase() = %q, got %q", "custom-oss-db", got)
	}

	// Verify CODE_GRAPH_* fallback env vars also work when VELENZA_CAMBIUM_* are unset
	t.Setenv("VELENZA_CAMBIUM_GCP_PROJECT", "")
	t.Setenv("CODE_GRAPH_GCP_PROJECT", "legacy-env-project")
	if got := resolveDefaultGCPProject(); got != "legacy-env-project" {
		t.Fatalf("expected resolveDefaultGCPProject() fallback to CODE_GRAPH_GCP_PROJECT = %q, got %q", "legacy-env-project", got)
	}

	// Verify admin.go and serve.go do not contain hardcoded internal Spanner instance or staging identifiers
	forbiddenInstance := strings.Join([]string{"jeev", "es-rg-spanner-prod-", "4d0e", "4c43"}, "")
	forbiddenStaging := strings.Join([]string{"merid", "ian-staging"}, "")
	for _, rel := range []string{"admin.go", "serve.go"} {
		raw, err := os.ReadFile(rel)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		text := string(raw)
		if strings.Contains(text, forbiddenInstance) {
			t.Fatalf("expected %s to not hardcode internal Spanner instance", rel)
		}
		if strings.Contains(text, forbiddenStaging) {
			t.Fatalf("expected %s to not reference internal staging identifier", rel)
		}
	}
}

func TestWalkRepoExtractsSpecAndVoltAnnotationsAndHazardEdges(t *testing.T) {
	dir := writeHazardFixtureModule(t)

	symbols, edges, err := walkRepo(dir, "hazardfixture", "snap-1", func(string, int, int, string) {})
	if err != nil {
		t.Fatalf("walkRepo failed: %v", err)
	}

	var hasSpecAnnotation, hasVoltAnnotation, hasHazardEdge, hasSinkSymbol bool
	for _, e := range edges {
		if e.Kind == "MAP_ESCAPE_UNDER_LOCK" || e.Kind == "CALLS_UNDER_LOCK" || e.Kind == "IO_SINK" {
			hasHazardEdge = true
		}
	}
	for _, s := range symbols {
		for _, ann := range s.Annotations {
			if ann == "SPEC-HAZARD-FIXTURE-001" {
				hasSpecAnnotation = true
			}
			if ann == "VOLT-HAZARD-FIXTURE-001" {
				hasVoltAnnotation = true
			}
		}
		if s.Kind == "sink" {
			hasSinkSymbol = true
			if s.ContentHash != "" {
				t.Fatalf("expected synthetic sink symbol %q to have empty ContentHash so Vertex embedding skips it, got %q", s.FQN, s.ContentHash)
			}
		}
	}

	if !hasSpecAnnotation {
		t.Fatalf("expected walkRepo to extract //spec:SPEC-HAZARD-FIXTURE-001 into symbol.Annotations")
	}
	if !hasVoltAnnotation {
		t.Fatalf("expected walkRepo to preserve //volt:VOLT-HAZARD-FIXTURE-001 in symbol.Annotations")
	}
	if !hasHazardEdge {
		t.Fatalf("expected walkRepo to integrate collectHazardEdges and emit SSA/VTA hazard edges")
	}
	if !hasSinkSymbol {
		t.Fatalf("expected walkRepo to emit synthetic Kind=sink symbols for external/hazard sinks")
	}
}

func TestSchemaSupports64CharBonsaiCommitSHAAndMCPHazardsToolRegistered(t *testing.T) {
	schemaBytes, err := os.ReadFile(filepath.Join(".", "schema.sql"))
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	schemaText := string(schemaBytes)
	if strings.Contains(schemaText, "commit_sha      STRING(40)") || strings.Contains(schemaText, "commit_sha       STRING(40)") || strings.Contains(schemaText, "parent_sha      STRING(40)") {
		t.Fatalf("expected schema.sql to widen commit_sha and parent_sha from STRING(40) to STRING(64) for Velenza Inarch BLAKE3 Bonsai changesets")
	}
	if !strings.Contains(schemaText, "commit_sha      STRING(64)") && !strings.Contains(schemaText, "commit_sha       STRING(64)") {
		t.Fatalf("expected schema.sql to declare commit_sha as STRING(64)")
	}

	oldNewClient := mcpNewSpannerClient
	oldLatestSnapshot := mcpLatestSnapshot
	oldRunServer := mcpRunServer
	t.Cleanup(func() {
		mcpNewSpannerClient = oldNewClient
		mcpLatestSnapshot = oldLatestSnapshot
		mcpRunServer = oldRunServer
	})

	var server *mcp.Server
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) {
		return nil, nil
	}
	mcpLatestSnapshot = func(context.Context, *spanner.Client, string) (cgContext, error) {
		return cgContext{Repo: "code-graph", SnapshotID: "snap-1"}, nil
	}
	mcpRunServer = func(_ context.Context, got *mcp.Server) error {
		server = got
		return nil
	}

	if err := cmdMCP(context.Background(), nil); err != nil {
		t.Fatalf("cmdMCP returned error: %v", err)
	}
	if server == nil {
		t.Fatal("cmdMCP did not build an MCP server")
	}

	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	defer serverSession.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer clientSession.Close()

	toolsRes, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("clientSession.ListTools: %v", err)
	}
	var foundHazardsTool bool
	for _, tool := range toolsRes.Tools {
		if tool != nil && tool.Name == "code_graph_hazards" {
			foundHazardsTool = true
			break
		}
	}
	if !foundHazardsTool {
		t.Fatalf("expected cmdMCP server to register code_graph_hazards tool")
	}
}

// Requirement: REQ-CAMBIUM-NO-HARDCODED-IDS
func TestOpenSourceHygiene_NoHardcodedProjectIdsInSourceOrDocs(t *testing.T) {
	join := func(parts ...string) string {
		return strings.Join(parts, "")
	}
	forbiddenLiterals := []string{
		join("jeev", "es"),
		join("486", "102"),
		join("4d0e", "4c43"),
		join("jeev", "es-", "486", "102"),
		join("jeev", "es-rg-spanner-prod-", "4d0e", "4c43"),
		join("jeev", "es-rg-spanner"),
		join("no", "13"),
		join("no", "13core"),
		join("no", "13-infra"),
		join("ini", "tech"),
		join("ba", "os"),
		join("ba", "os-kernel"),
		join("merid", "ian"),
		join("merid", "ian-staging"),
		join("merid", "ian-prod"),
		join("ragg", "edy"),
		join("ragg", "edy-graphy"),
		join("kop", "ia"),
		join("kop", "ia-mac", "mini"),
		join("kop", "ia-mac", "book"),
		join("kop", "ia-"),
		join("velen", "za-core"),
		join("ker", "nel-staging"),
		join("cor", "pus-graph"),
		join("fle", "et-plan"),
		join("arc-", "runner"),
		join("arc", "server"),
		join("arc", "server2"),
		join("arc", "runner"),
		join("mac", "mini"),
		join("mac", "book"),
		join("bran", "don"),
		join("open", "scion"),
		join("/Users/bran", "don"),
		join("/Users/bran", "donmeyer"),
		join("/home/", "lab"),
		join("D:\\Meri", "dian"),
		join("d:/meri", "dian"),
		join("100.", "86."),
		join("100.", "104."),
		join("100.", "110."),
		join("100.", "101."),
		join("100.", "66."),
		join("velenzaboc/", "code-graph"),
		join("velenzaboc/", "spec-graph"),
		join("velenzaboc/", "task-graph"),
		join("doc", "render"),
		join("SystemTable", "Protection"),
		join("Circuit", "Repository"),
		join("SpannerMutation", "Service"),
		join("cmd/ker", "nel"),
	}

	seenFiles := make(map[string]struct{})
	if tracked, err := listTrackedFiles("."); err == nil {
		for _, rel := range tracked {
			seenFiles[filepath.ToSlash(rel)] = struct{}{}
		}
	}
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel := filepath.ToSlash(path)
		if entry.IsDir() {
			if rel == ".git" || strings.HasPrefix(rel, ".git/") || rel == ".inarch" || strings.HasPrefix(rel, ".inarch/") || rel == "bin" || strings.HasPrefix(rel, "bin/") {
				return filepath.SkipDir
			}
			return nil
		}
		if rel == "." || rel == "velenza-cambium" || rel == "code-graph" {
			return nil
		}
		seenFiles[rel] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository files: %v", err)
	}
	if len(seenFiles) == 0 {
		t.Fatal("expected non-empty repository file set for open-source hygiene scan")
	}

	for file := range seenFiles {
		info, statErr := os.Stat(file)
		if statErr != nil || info.IsDir() {
			continue
		}
		raw, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatalf("read %s: %v", file, readErr)
		}
		text := string(raw)
		lowerText := strings.ToLower(text)
		for _, forbidden := range forbiddenLiterals {
			if strings.Contains(text, forbidden) || strings.Contains(lowerText, strings.ToLower(forbidden)) {
				t.Errorf("%s contains forbidden hardcoded internal identifier %q", file, forbidden)
			}
		}
	}

	licenseBytes, err := os.ReadFile("LICENSE")
	if err != nil {
		t.Fatalf("expected Apache-2.0 LICENSE file at repository root: %v", err)
	}
	licenseText := string(licenseBytes)
	if !strings.Contains(licenseText, "Apache License") || !strings.Contains(licenseText, "Version 2.0") {
		t.Fatalf("LICENSE does not contain Apache License Version 2.0 header")
	}

	noticeBytes, err := os.ReadFile("NOTICE")
	if err != nil {
		t.Fatalf("expected NOTICE file at repository root: %v", err)
	}
	if !strings.Contains(string(noticeBytes), "Velenza Cambium (velenza-cambium)") {
		t.Fatalf("NOTICE does not contain Velenza Cambium attribution header")
	}

	contributingBytes, err := os.ReadFile("CONTRIBUTING.md")
	if err != nil {
		t.Fatalf("expected CONTRIBUTING.md file at repository root: %v", err)
	}
	if !strings.Contains(string(contributingBytes), "Open-Source Hygiene Policy") {
		t.Fatalf("CONTRIBUTING.md does not contain Open-Source Hygiene Policy header")
	}

	ciBytes, err := os.ReadFile(filepath.Join(".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read .github/workflows/ci.yml: %v", err)
	}
	ciText := string(ciBytes)
	for _, requiredCI := range []string{"actions/checkout@v4", "actions/setup-go@v5", "go vet ./...", "go build ./...", "go test -race -count=1 ./..."} {
		if !strings.Contains(ciText, requiredCI) {
			t.Errorf(".github/workflows/ci.yml missing self-contained CI step %q", requiredCI)
		}
	}

	readmeBytes, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	readmeText := string(readmeBytes)
	for _, required := range []string{"velenza-cambium", "arbor-info", "hazards", "Velenza Arbor Stack"} {
		if !strings.Contains(readmeText, required) {
			t.Errorf("README.md missing required Arbor Stack documentation token %q", required)
		}
	}
}

// Requirement: REQ-CAMBIUM-NO-HARDCODED-IDS
func TestOpenSourceHygiene_EnvVarResolutionSupportsCambiumAndLegacyAliases(t *testing.T) {
	envKeys := []string{
		"VELENZA_CAMBIUM_GCP_PROJECT",
		"VELENZA_CAMBIUM_PROJECT",
		"CODE_GRAPH_GCP_PROJECT",
		"ARBOR_GCP_PROJECT",
		"GOOGLE_CLOUD_PROJECT",
		"VELENZA_CAMBIUM_SPANNER_INSTANCE",
		"VELENZA_CAMBIUM_INSTANCE",
		"CODE_GRAPH_SPANNER_INSTANCE",
		"ARBOR_SPANNER_INSTANCE",
		"VELENZA_CAMBIUM_SPANNER_DATABASE",
		"VELENZA_CAMBIUM_DATABASE",
		"CODE_GRAPH_SPANNER_DATABASE",
	}
	saved := make(map[string]string, len(envKeys))
	for _, k := range envKeys {
		saved[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	defer func() {
		for k, v := range saved {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}()

	if got := resolveDefaultGCPProject(); got != "" {
		t.Fatalf("expected empty default GCP project when unset, got %q", got)
	}
	if got := resolveDefaultSpannerInstance(); got != "" {
		t.Fatalf("expected empty default Spanner instance when unset, got %q", got)
	}
	if got := resolveDefaultSpannerDatabase(); got != "code-graph" {
		t.Fatalf("expected default database name 'code-graph', got %q", got)
	}

	// Arbor stack-wide fallback aliases ARBOR_GCP_PROJECT / ARBOR_SPANNER_INSTANCE
	_ = os.Setenv("ARBOR_GCP_PROJECT", "arbor-proj")
	_ = os.Setenv("ARBOR_SPANNER_INSTANCE", "arbor-inst")
	if got := resolveDefaultGCPProject(); got != "arbor-proj" {
		t.Fatalf("expected ARBOR_GCP_PROJECT 'arbor-proj', got %q", got)
	}
	if got := resolveDefaultSpannerInstance(); got != "arbor-inst" {
		t.Fatalf("expected ARBOR_SPANNER_INSTANCE 'arbor-inst', got %q", got)
	}

	// Short aliases VELENZA_CAMBIUM_PROJECT / _INSTANCE / _DATABASE
	_ = os.Setenv("VELENZA_CAMBIUM_PROJECT", "short-proj")
	_ = os.Setenv("VELENZA_CAMBIUM_INSTANCE", "short-inst")
	_ = os.Setenv("VELENZA_CAMBIUM_DATABASE", "short-db")
	if got := resolveDefaultGCPProject(); got != "short-proj" {
		t.Fatalf("expected VELENZA_CAMBIUM_PROJECT 'short-proj', got %q", got)
	}
	if got := resolveDefaultSpannerInstance(); got != "short-inst" {
		t.Fatalf("expected VELENZA_CAMBIUM_INSTANCE 'short-inst', got %q", got)
	}
	if got := resolveDefaultSpannerDatabase(); got != "short-db" {
		t.Fatalf("expected VELENZA_CAMBIUM_DATABASE 'short-db', got %q", got)
	}

	// Explicit VELENZA_CAMBIUM_GCP_PROJECT / _SPANNER_INSTANCE / _SPANNER_DATABASE take precedence
	_ = os.Setenv("VELENZA_CAMBIUM_GCP_PROJECT", "explicit-proj")
	_ = os.Setenv("VELENZA_CAMBIUM_SPANNER_INSTANCE", "explicit-inst")
	_ = os.Setenv("VELENZA_CAMBIUM_SPANNER_DATABASE", "explicit-db")
	if got := resolveDefaultGCPProject(); got != "explicit-proj" {
		t.Fatalf("expected VELENZA_CAMBIUM_GCP_PROJECT 'explicit-proj', got %q", got)
	}
	if got := resolveDefaultSpannerInstance(); got != "explicit-inst" {
		t.Fatalf("expected VELENZA_CAMBIUM_SPANNER_INSTANCE 'explicit-inst', got %q", got)
	}
	if got := resolveDefaultSpannerDatabase(); got != "explicit-db" {
		t.Fatalf("expected VELENZA_CAMBIUM_SPANNER_DATABASE 'explicit-db', got %q", got)
	}
}

// Requirement: REQ-CAMBIUM-ARBOR-INFO
func TestCambiumArborInfo_DescriptorAndCLISubcommand(t *testing.T) {
	desc := BuildCambiumArborDescriptor()
	if desc.Layer != 1 {
		t.Fatalf("Layer = %d, want 1", desc.Layer)
	}
	if desc.Name != "velenza-cambium" {
		t.Fatalf("Name = %q, want 'velenza-cambium'", desc.Name)
	}
	if desc.Alias != "code-graph" {
		t.Fatalf("Alias = %q, want 'code-graph'", desc.Alias)
	}
	if desc.CanonicalBinary != "velenza-cambium" {
		t.Fatalf("CanonicalBinary = %q, want 'velenza-cambium'", desc.CanonicalBinary)
	}
	if desc.LegacyAlias != "code-graph" {
		t.Fatalf("LegacyAlias = %q, want 'code-graph'", desc.LegacyAlias)
	}
	wantSubcommands := []string{"inr cambium", "inr code"}
	if !reflect.DeepEqual(desc.UmbrellaSubcommands, wantSubcommands) {
		t.Fatalf("UmbrellaSubcommands = %#v, want %#v", desc.UmbrellaSubcommands, wantSubcommands)
	}
	if !desc.InarchNativeVCS {
		t.Fatalf("InarchNativeVCS = %v, want true", desc.InarchNativeVCS)
	}
	wantEnvVars := []string{
		"VELENZA_CAMBIUM_GCP_PROJECT",
		"VELENZA_CAMBIUM_PROJECT",
		"VELENZA_CAMBIUM_SPANNER_INSTANCE",
		"VELENZA_CAMBIUM_INSTANCE",
		"VELENZA_CAMBIUM_SPANNER_DATABASE",
		"VELENZA_CAMBIUM_DATABASE",
		"CODE_GRAPH_GCP_PROJECT",
		"CODE_GRAPH_SPANNER_INSTANCE",
		"CODE_GRAPH_SPANNER_DATABASE",
		"ARBOR_GCP_PROJECT",
		"ARBOR_SPANNER_INSTANCE",
	}
	if !reflect.DeepEqual(desc.ConfigEnvVars, wantEnvVars) {
		t.Fatalf("ConfigEnvVars = %#v, want %#v", desc.ConfigEnvVars, wantEnvVars)
	}
	wantVCS := []string{"inarch", "git"}
	if !reflect.DeepEqual(desc.VCSBackends, wantVCS) {
		t.Fatalf("VCSBackends = %#v, want %#v", desc.VCSBackends, wantVCS)
	}
	if desc.HazardCategoryCount != 12 {
		t.Fatalf("HazardCategoryCount = %d, want 12", desc.HazardCategoryCount)
	}

	var out bytes.Buffer
	code, err := runArborInfoCLI(context.Background(), nil, &out)
	if err != nil || code != 0 {
		t.Fatalf("runArborInfoCLI failed: code=%d err=%v", code, err)
	}
	var decoded CambiumArborDescriptor
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal arbor-info output: %v", err)
	}
	if !reflect.DeepEqual(decoded, desc) {
		t.Fatalf("decoded descriptor = %#v, want %#v", decoded, desc)
	}

	badCode, badErr := runArborInfoCLI(context.Background(), []string{"--bogus"}, &out)
	if badErr == nil || badCode != 2 {
		t.Fatalf("expected exit code 2 on unknown flag, got code=%d err=%v", badCode, badErr)
	}
}

// Requirement: REQ-CAMBIUM-INARCH-NATIVE-VCS
func TestInarchNativeVCSIngest_ResolvesMetadataAndFilesWithoutGit(t *testing.T) {
	tmpDir := t.TempDir()
	inarchDir := filepath.Join(tmpDir, ".inarch")
	refsHeadsDir := filepath.Join(inarchDir, "refs", "heads")
	if err := os.MkdirAll(refsHeadsDir, 0o755); err != nil {
		t.Fatalf("mkdir .inarch/refs/heads: %v", err)
	}

	configToml := "repo_id = \"velenzaboc/arbor-native-service\"\n"
	if err := os.WriteFile(filepath.Join(inarchDir, "config.toml"), []byte(configToml), 0o644); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inarchDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatalf("write HEAD: %v", err)
	}
	wantChangesetID := "a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0"
	if err := os.WriteFile(filepath.Join(refsHeadsDir, "main"), []byte(wantChangesetID+"\n"), 0o644); err != nil {
		t.Fatalf("write refs/heads/main: %v", err)
	}

	pkgDir := filepath.Join(tmpDir, "internal", "svc")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatalf("mkdir internal/svc: %v", err)
	}
	srcContent := "package svc\n\nfunc Serve() string {\n\treturn \"ok\"\n}\n"
	if err := os.WriteFile(filepath.Join(pkgDir, "serve.go"), []byte(srcContent), 0o644); err != nil {
		t.Fatalf("write serve.go: %v", err)
	}
	testContent := "package svc\n\nimport \"testing\"\n\nfunc TestServe(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(pkgDir, "serve_test.go"), []byte(testContent), 0o644); err != nil {
		t.Fatalf("write serve_test.go: %v", err)
	}

	repoName, err := deriveRepoName(tmpDir)
	if err != nil {
		t.Fatalf("deriveRepoName on pure .inarch workspace failed: %v", err)
	}
	if repoName != "arbor-native-service" {
		t.Fatalf("deriveRepoName = %q, want 'arbor-native-service'", repoName)
	}

	headSHA, err := gitHead(tmpDir)
	if err != nil {
		t.Fatalf("gitHead on pure .inarch workspace failed: %v", err)
	}
	if headSHA != wantChangesetID {
		t.Fatalf("gitHead = %q, want %q", headSHA, wantChangesetID)
	}

	branch := gitBranch(tmpDir, headSHA)
	if branch != "main" {
		t.Fatalf("gitBranch = %q, want 'main'", branch)
	}

	commitTime, err := gitCommitTime(tmpDir, headSHA)
	if err != nil || commitTime.IsZero() {
		t.Fatalf("gitCommitTime on pure .inarch workspace failed: time=%v err=%v", commitTime, err)
	}

	parents, err := gitParents(tmpDir, headSHA)
	if err != nil {
		t.Fatalf("gitParents on pure .inarch workspace failed: %v", err)
	}
	if len(parents) != 0 {
		t.Fatalf("gitParents = %#v, want empty slice", parents)
	}

	tracked, err := listTrackedFiles(tmpDir)
	if err != nil {
		t.Fatalf("listTrackedFiles on pure .inarch workspace failed: %v", err)
	}
	wantTracked := []string{"internal/svc/serve.go", "internal/svc/serve_test.go"}
	if !reflect.DeepEqual(tracked, wantTracked) {
		t.Fatalf("listTrackedFiles = %#v, want %#v", tracked, wantTracked)
	}
}
