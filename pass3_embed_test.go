package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"golang.org/x/tools/go/ssa"
)

func TestPass3EmbedModelIsGeminiEmbedding2Preview(t *testing.T) {
	if embedModel != "gemini-embedding-2-preview" {
		t.Fatalf("expected embedModel to be gemini-embedding-2-preview, got %q", embedModel)
	}
}

func TestPass3BuildEmbedTextCoversAllSymbolKinds(t *testing.T) {
	kinds := []string{"package", "file", "column", "field", "sink", "func", "method", "type", "const", "var"}
	for _, kind := range kinds {
		sym := Symbol{
			Kind:       kind,
			FQN:        "github.com/velenzaboc/velenza-cambium/internal/pkg." + kind + "Sym",
			FilePath:   "internal/pkg/file.go",
			Signature:  kind + " signature",
			DocComment: "doc for " + kind,
		}
		got := buildEmbedText(sym)
		if strings.TrimSpace(got) == "" {
			t.Fatalf("expected non-empty embed text for symbol kind %q, got empty string", kind)
		}
	}

	// Structural symbol without Signature/DocComment/Body still produces non-empty embed text from Kind/FQN/FilePath
	minimalPkg := Symbol{
		Kind:     "package",
		FQN:      "github.com/velenzaboc/velenza-cambium/internal/pkg",
		FilePath: "internal/pkg",
	}
	if strings.TrimSpace(buildEmbedText(minimalPkg)) == "" {
		t.Fatalf("expected non-empty embed text for minimal package symbol with FQN/FilePath, got empty string")
	}
}

func TestPass3StructuralEdgesAndHazardsQueryCoverage(t *testing.T) {
	if funcCallsClose(nil) {
		t.Fatal("expected funcCallsClose(nil) to return false")
	}
	emptyFn := &ssa.Function{}
	if funcCallsClose(emptyFn) {
		t.Fatal("expected funcCallsClose(emptyFn) to return false")
	}
	if got := clip("short", 10); got != "short" {
		t.Fatalf("expected clip(\"short\", 10) = \"short\", got %q", got)
	}
	if got := clip("hello world", 5); got != "hello" {
		t.Fatalf("expected clip(\"hello world\", 5) = \"hello\", got %q", got)
	}

	ctx := context.Background()
	if _, err := doHazards(ctx, nil, "repo", "snap", HazardQueryOpts{}); err == nil {
		t.Fatal("expected doHazards without spanner client or repo_path to fail")
	}

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)

	hashA := strings.Repeat("a", 64)
	newSeededClient := func() *spanner.Client {
		c := newCodeGraphTestDB(t)
		seedCommandGraph(t, c)
		applyTestMutations(t, c,
			spanner.Insert("SnapshotEdges",
				[]string{"repo", "snapshot_id", "symbol_id", "dst_symbol_id", "kind"},
				[]any{"repo", "snapshot-two", uuidFromFQN("example.test/fixture"), uuidFromFQN("example.test/fixture.A"), "DECLARES"}),
			spanner.Insert("SymbolBlobs",
				[]string{"content_hash", "embedding"},
				[]any{hashA, []float32{0.25, 0.75}}),
		)
		return c
	}

	directClient := newSeededClient()
	if embs, err := fetchEmbeddings(ctx, directClient, nil); err != nil || len(embs) != 0 {
		t.Fatalf("fetchEmbeddings(nil) = %v, %v", embs, err)
	}
	if embs, err := fetchEmbeddings(ctx, directClient, []string{"missing"}); err != nil || len(embs) != 0 {
		t.Fatalf("fetchEmbeddings(missing) = %v, %v", embs, err)
	}
	if _, err := fetchEmbeddings(ctx, directClient, []string{hashA}); err == nil {
		t.Fatal("expected fetchEmbeddings(hashA) against testdb ARRAY<FLOAT64> to return decode error")
	}
	_, _ = doDependents(ctx, directClient, "example.test/fixture.A")

	previousFactory := commandSpannerClient
	commandSpannerClient = func(context.Context) (*spanner.Client, error) {
		return newSeededClient(), nil
	}
	t.Cleanup(func() { commandSpannerClient = previousFactory })

	previousRequestEmbeddings := requestEmbeddings
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range texts {
			vectors[i] = []float32{float32(i + 1), 0.5}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequestEmbeddings })

	if err := cmdUse(ctx, []string{"repo", "snapshot-two"}); err != nil {
		t.Fatalf("cmdUse failed: %v", err)
	}

	var runErr error
	out := captureStdout(t, func() {
		runErr = cmdHazards(ctx, []string{"--snapshot", "snapshot-two", "--limit", "10"})
	})
	if runErr != nil {
		t.Fatalf("cmdHazards against Spanner failed: %v", runErr)
	}
	if !strings.Contains(out, "hazards: repo=repo snapshot=snapshot-two") {
		t.Fatalf("unexpected cmdHazards output: %q", out)
	}

	_ = captureStdout(t, func() {
		_ = cmdDependents(ctx, []string{"example.test/fixture.A"})
	})

	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod": "module example.test/hazfix\n\ngo 1.23\n",
		"haz.go": `package hazfix

import "os"

func leak() {
	f, _ := os.Open("x")
	_ = f
}
`,
	})
	outLocal := captureStdout(t, func() {
		runErr = cmdHazards(ctx, []string{"--repo-path", repo, "--category", "UNCLOSED_RESOURCE", "--file-prefix", "haz.go", "--limit", "5"})
	})
	if runErr != nil {
		t.Fatalf("cmdHazards --repo-path failed: %v", runErr)
	}
	if !strings.Contains(outLocal, "UNCLOSED_RESOURCE") {
		t.Fatalf("expected UNCLOSED_RESOURCE in local cmdHazards output, got %q", outLocal)
	}

	hypOut := captureStdout(t, func() {
		runErr = cmdHypothesize(ctx, []string{"snapshot-two", repo})
	})
	if runErr != nil || !strings.Contains(hypOut, "hypothetical snapshot created") {
		t.Fatalf("cmdHypothesize output=%q err=%v", hypOut, runErr)
	}
	_ = filepath.Join(repo, "haz.go")
}

