package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestCmdMCPRegistersProtocolToolsAndReturnsSnapshotErrors(t *testing.T) {
	oldClient, oldLatest, oldRun := mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer
	t.Cleanup(func() { mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer = oldClient, oldLatest, oldRun })
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, nil }
	mcpLatestSnapshot = func(context.Context, *spanner.Client, string) (cgContext, error) {
		return cgContext{}, errors.New("no snapshot")
	}
	var server *mcp.Server
	mcpRunServer = func(_ context.Context, got *mcp.Server) error { server = got; return nil }
	if err := cmdMCP(context.Background(), nil); err != nil || server == nil {
		t.Fatalf("cmdMCP = %v, server=%v", err, server)
	}
	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"code_graph_search", "code_graph_neighbors", "code_graph_get", "code_graph_symbols", "code_graph_annotations", "code_graph_coverage", "code_graph_findings"} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name})
		if err != nil || result == nil || !result.IsError {
			t.Fatalf("%s: result=%#v err=%v", name, result, err)
		}
		if len(result.Content) != 1 {
			t.Fatalf("%s content=%#v", name, result.Content)
		}
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "not-a-tool"}); err == nil {
		t.Fatal("unknown tool must be rejected by JSON-RPC dispatch")
	}
}

func TestCmdMCPDispatchesEveryToolWithStructuredOutput(t *testing.T) {
	oldClient, oldLatest, oldRun := mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer
	oldSearch, oldNeighbors, oldGet, oldIngest := mcpDoSearch, mcpDoNeighbors, mcpDoGet, mcpDoIngest
	oldCommits, oldSymbols, oldAnnotations, oldCoverage := mcpDoListCommits, mcpDoListSymbols, mcpDoAnnotations, mcpDoCoverage
	oldDiff, oldLineage, oldDrift, oldFindings, oldDependents := mcpDoDiff, mcpDoLineage, mcpDoDrift, mcpDoFindings, mcpDoDependents
	t.Cleanup(func() {
		mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer = oldClient, oldLatest, oldRun
		mcpDoSearch, mcpDoNeighbors, mcpDoGet, mcpDoIngest = oldSearch, oldNeighbors, oldGet, oldIngest
		mcpDoListCommits, mcpDoListSymbols, mcpDoAnnotations, mcpDoCoverage = oldCommits, oldSymbols, oldAnnotations, oldCoverage
		mcpDoDiff, mcpDoLineage, mcpDoDrift, mcpDoFindings, mcpDoDependents = oldDiff, oldLineage, oldDrift, oldFindings, oldDependents
	})
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, nil }
	mcpLatestSnapshot = func(context.Context, *spanner.Client, string) (cgContext, error) {
		return cgContext{Repo: "repo", SnapshotID: "snap"}, nil
	}
	mcpDoSearch = func(context.Context, *spanner.Client, string, string, string, int) ([]SearchResult, error) {
		return []SearchResult{}, nil
	}
	mcpDoNeighbors = func(context.Context, *spanner.Client, string, int, string) ([]NeighborResult, error) {
		return []NeighborResult{}, nil
	}
	mcpDoGet = func(context.Context, *spanner.Client, string, string) (*SymbolDetail, error) {
		return &SymbolDetail{}, nil
	}
	mcpDoIngest = func(context.Context, *spanner.Client, string, ingestOpts, ProgressFunc) (*IngestStats, error) {
		return &IngestStats{}, nil
	}
	mcpDoListCommits = func(context.Context, *spanner.Client) ([]CommitInfo, error) { return []CommitInfo{}, nil }
	mcpDoListSymbols = func(context.Context, *spanner.Client, string, string, string, string, bool, int) ([]SymbolRow, error) {
		return []SymbolRow{}, nil
	}
	mcpDoAnnotations = func(context.Context, *spanner.Client, string, string, string, string) ([]AnnotationRow, error) {
		return []AnnotationRow{}, nil
	}
	mcpDoCoverage = func(context.Context, *spanner.Client, string, string, bool) (*CoverageReport, error) {
		return &CoverageReport{}, nil
	}
	mcpDoDiff = func(context.Context, *spanner.Client, string, string, string) (*DiffResult, error) {
		return &DiffResult{}, nil
	}
	mcpDoLineage = func(context.Context, *spanner.Client, string) (*LineageResult, error) { return &LineageResult{}, nil }
	mcpDoDrift = func(context.Context, *spanner.Client, string, string, string) (*DriftResult, error) {
		return &DriftResult{}, nil
	}
	mcpDoFindings = func(context.Context, *spanner.Client, string, string, int) (*FindingsResult, error) {
		return &FindingsResult{}, nil
	}
	mcpDoDependents = func(context.Context, *spanner.Client, string) (*DependentsResult, error) {
		return &DependentsResult{}, nil
	}

	var server *mcp.Server
	mcpRunServer = func(_ context.Context, got *mcp.Server) error { server = got; return nil }
	if err := cmdMCP(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	if _, err := server.Connect(context.Background(), st, nil); err != nil {
		t.Fatal(err)
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]any{
		"code_graph_use": map[string]any{}, "code_graph_search": map[string]any{"query": "q", "limit": 99}, "code_graph_neighbors": map[string]any{"fqn": "f", "depth": 0},
		"code_graph_get": map[string]any{"fqn": "f"}, "code_graph_ingest": map[string]any{"repo_path": "/repo"}, "code_graph_list_commits": map[string]any{},
		"code_graph_symbols": map[string]any{}, "code_graph_annotations": map[string]any{}, "code_graph_coverage": map[string]any{}, "code_graph_diff": map[string]any{"repo": "r", "before_commit": "a", "after_commit": "b"},
		"code_graph_lineage": map[string]any{"fqn": "f"}, "code_graph_drift": map[string]any{"repo": "r", "before_commit": "a", "after_commit": "b"}, "code_graph_findings": map[string]any{}, "code_graph_dependents": map[string]any{"fqn": "f"},
		"code_graph_hypothesize": map[string]any{"base_snapshot": "s", "scratch_repo_path": "/repo"},
	}
	for name, arguments := range calls {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: arguments})
		if err != nil || result == nil || result.IsError || len(result.Content) != 1 {
			t.Fatalf("%s: %#v %v", name, result, err)
		}
		text, ok := result.Content[0].(*mcp.TextContent)
		if !ok || !json.Valid([]byte(text.Text)) {
			t.Fatalf("%s output=%#v", name, result.Content)
		}
	}
}

func TestCmdMCPOperationFailuresAreProtocolErrors(t *testing.T) {
	oldClient, oldLatest, oldRun, oldSnapshotRepo := mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer, mcpSnapshotRepo
	oldSearch, oldNeighbors, oldGet, oldIngest := mcpDoSearch, mcpDoNeighbors, mcpDoGet, mcpDoIngest
	oldCommits, oldSymbols, oldAnnotations, oldCoverage := mcpDoListCommits, mcpDoListSymbols, mcpDoAnnotations, mcpDoCoverage
	oldDiff, oldLineage, oldDrift, oldFindings, oldDependents := mcpDoDiff, mcpDoLineage, mcpDoDrift, mcpDoFindings, mcpDoDependents
	t.Cleanup(func() {
		mcpNewSpannerClient, mcpLatestSnapshot, mcpRunServer, mcpSnapshotRepo = oldClient, oldLatest, oldRun, oldSnapshotRepo
		mcpDoSearch, mcpDoNeighbors, mcpDoGet, mcpDoIngest = oldSearch, oldNeighbors, oldGet, oldIngest
		mcpDoListCommits, mcpDoListSymbols, mcpDoAnnotations, mcpDoCoverage = oldCommits, oldSymbols, oldAnnotations, oldCoverage
		mcpDoDiff, mcpDoLineage, mcpDoDrift, mcpDoFindings, mcpDoDependents = oldDiff, oldLineage, oldDrift, oldFindings, oldDependents
	})
	backendErr := errors.New("backend unavailable")
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, nil }
	mcpLatestSnapshot = func(context.Context, *spanner.Client, string) (cgContext, error) {
		return cgContext{Repo: "repo", SnapshotID: "snap"}, nil
	}
	mcpSnapshotRepo = func(context.Context, *spanner.Client, string) (string, error) { return "", backendErr }
	mcpDoSearch = func(context.Context, *spanner.Client, string, string, string, int) ([]SearchResult, error) {
		return nil, backendErr
	}
	mcpDoNeighbors = func(context.Context, *spanner.Client, string, int, string) ([]NeighborResult, error) {
		return nil, backendErr
	}
	mcpDoGet = func(context.Context, *spanner.Client, string, string) (*SymbolDetail, error) { return nil, backendErr }
	mcpDoIngest = func(context.Context, *spanner.Client, string, ingestOpts, ProgressFunc) (*IngestStats, error) {
		return nil, backendErr
	}
	mcpDoListCommits = func(context.Context, *spanner.Client) ([]CommitInfo, error) { return nil, backendErr }
	mcpDoListSymbols = func(context.Context, *spanner.Client, string, string, string, string, bool, int) ([]SymbolRow, error) {
		return nil, backendErr
	}
	mcpDoAnnotations = func(context.Context, *spanner.Client, string, string, string, string) ([]AnnotationRow, error) {
		return nil, backendErr
	}
	mcpDoCoverage = func(context.Context, *spanner.Client, string, string, bool) (*CoverageReport, error) {
		return nil, backendErr
	}
	mcpDoDiff = func(context.Context, *spanner.Client, string, string, string) (*DiffResult, error) {
		return nil, backendErr
	}
	mcpDoLineage = func(context.Context, *spanner.Client, string) (*LineageResult, error) { return nil, backendErr }
	mcpDoDrift = func(context.Context, *spanner.Client, string, string, string) (*DriftResult, error) {
		return nil, backendErr
	}
	mcpDoFindings = func(context.Context, *spanner.Client, string, string, int) (*FindingsResult, error) {
		return nil, backendErr
	}
	mcpDoDependents = func(context.Context, *spanner.Client, string) (*DependentsResult, error) { return nil, backendErr }

	var server *mcp.Server
	mcpRunServer = func(_ context.Context, got *mcp.Server) error { server = got; return nil }
	if err := cmdMCP(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	if _, err := server.Connect(context.Background(), st, nil); err != nil {
		t.Fatal(err)
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "failure-test", Version: "1"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]any{
		"code_graph_use": map[string]any{"snapshot_id": "explicit"}, "code_graph_search": map[string]any{"query": "q"}, "code_graph_neighbors": map[string]any{"fqn": "f"},
		"code_graph_get": map[string]any{"fqn": "f"}, "code_graph_ingest": map[string]any{"repo_path": "/repo"}, "code_graph_list_commits": map[string]any{},
		"code_graph_symbols": map[string]any{}, "code_graph_annotations": map[string]any{}, "code_graph_coverage": map[string]any{}, "code_graph_diff": map[string]any{"repo": "r", "before_commit": "a", "after_commit": "b"},
		"code_graph_lineage": map[string]any{"fqn": "f"}, "code_graph_drift": map[string]any{"repo": "r", "before_commit": "a", "after_commit": "b"}, "code_graph_findings": map[string]any{}, "code_graph_dependents": map[string]any{"fqn": "f"},
		"code_graph_hypothesize": map[string]any{"base_snapshot": "s", "scratch_repo_path": "/repo"},
	}
	for name, arguments := range calls {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: arguments})
		if err != nil || result == nil || !result.IsError || len(result.Content) != 1 {
			t.Fatalf("%s: result=%#v err=%v", name, result, err)
		}
		text, ok := result.Content[0].(*mcp.TextContent)
		if !ok || text.Text == "" {
			t.Fatalf("%s error output=%#v", name, result.Content)
		}
	}
}

func TestCmdMCPRejectsClientConstructionFailure(t *testing.T) {
	old := mcpNewSpannerClient
	t.Cleanup(func() { mcpNewSpannerClient = old })
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, errors.New("credentials unavailable") }
	if err := cmdMCP(context.Background(), nil); err == nil || err.Error() != "spanner client: credentials unavailable" {
		t.Fatalf("cmdMCP client error = %v", err)
	}
}

func TestTextResultPreservesStructuredJSON(t *testing.T) {
	result := textResult(map[string]any{"answer": 42, "nested": map[string]string{"state": "ready"}})
	if result == nil || len(result.Content) != 1 {
		t.Fatalf("content = %#v", result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T", result.Content[0])
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil || got["answer"] != float64(42) {
		t.Fatalf("JSON result = %q, %v", text.Text, err)
	}
	if textResult(func() {}) != nil {
		t.Fatal("unmarshallable value must not claim a text result")
	}
}

func TestMCPProgressWithoutTokenIsSilent(t *testing.T) {
	old := mcpNotifyProgress
	t.Cleanup(func() { mcpNotifyProgress = old })
	called := 0
	mcpNotifyProgress = func(context.Context, *mcp.CallToolRequest, *mcp.ProgressNotificationParams) error {
		called++
		return nil
	}
	progress := mcpProgressFunc(context.Background(), &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{}})
	progress("embed", 1, 3, "one")
	progress("finalize", 3, 0, "done")
	if called != 0 {
		t.Fatalf("progress notifications = %d, want 0 without a protocol token", called)
	}
}

func TestMCPProgressSendsProtocolNotification(t *testing.T) {
	old := mcpNotifyProgress
	t.Cleanup(func() { mcpNotifyProgress = old })
	var got *mcp.ProgressNotificationParams
	mcpNotifyProgress = func(_ context.Context, _ *mcp.CallToolRequest, params *mcp.ProgressNotificationParams) error {
		got = params
		return errors.New("transport unavailable")
	}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Meta: mcp.Meta{"progressToken": "token"}}}
	mcpProgressFunc(context.Background(), req)("embed", 2, 4, "chunk")
	if got == nil || got.ProgressToken != "token" || got.Progress != 2 || got.Total != 4 || got.Message != "embed chunk" {
		t.Fatalf("progress = %#v", got)
	}
}

func TestMCPInputJSONContracts(t *testing.T) {
	for _, input := range []any{
		searchInput{Query: "find writes", Limit: 15, SnapshotID: "snapshot"},
		neighborsInput{FQN: "example.org/p.F", Depth: 2},
		getInput{FQN: "example.org/p.F"},
		diffInput{Repo: "repo", Before: "before", After: "after"},
		ingestInput{RepoPath: "/repo"},
		symbolsInput{Kind: "func", Match: "F", Orphans: true, Limit: 1},
		annotationsInput{Type: "sentinel.raise", Spec: "4.2"},
		coverageInput{Untested: true},
		lineageInput{FQN: "example.org/p.F"},
		driftInput{Repo: "repo", Before: "before", After: "after"},
		findingsInput{Limit: 1},
		dependentsInput{FQN: "example.org/p.F"},
		hypothesizeInput{BaseSnapshot: "base", ScratchRepoPath: "/scratch"},
		useInput{Repo: "repo", SnapshotID: "snapshot"},
	} {
		data, err := json.Marshal(input)
		if err != nil || len(data) == 0 {
			t.Fatalf("marshal %T: %v", input, err)
		}
	}
}
