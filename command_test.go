package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
)

func TestCLIQueryCommandsRenderPersistedResults(t *testing.T) {
	previousEmbeddingLoader := loadSymbolEmbeddings
	loadSymbolEmbeddings = func(context.Context, *spanner.Client, []string) (map[string][]float32, error) {
		return map[string][]float32{
			strings.Repeat("a", 64): {1, 0},
			strings.Repeat("b", 64): {1, 0.01},
			strings.Repeat("c", 64): {0, 1},
			strings.Repeat("t", 64): {1, 0},
		}, nil
	}
	t.Cleanup(func() { loadSymbolEmbeddings = previousEmbeddingLoader })

	commands := []struct {
		name    string
		args    []string
		run     func(context.Context, []string) error
		wantOut string
	}{
		{"use-explicit", []string{"repo", "snapshot-two"}, cmdUse, "current snapshot pinned"},
		{"use-latest", []string{"repo"}, cmdUse, "snapshot-two"},
		{"neighbors", []string{"--snapshot", "snapshot-two", "example.test/fixture.A"}, cmdNeighbors, "example.test/fixture.C"},
		{"diff", []string{"repo", "snapshot-one", "snapshot-two"}, cmdDiff, "added=4"},
		{"lineage", []string{"example.test/fixture.A"}, cmdLineage, "1 versions"},
		{"drift", []string{"repo", "snapshot-one", "snapshot-two"}, cmdDrift, "added=4"},
		{"findings", []string{"--snapshot", "snapshot-two", "--limit", "5"}, cmdFindings, "MISUSE"},
		{"annotations", []string{"--snapshot", "snapshot-two", "--type", "sentinel.raise"}, cmdAnnotations, "fixture.check"},
		{"symbols", []string{"--snapshot", "snapshot-two", "--kind", "func"}, cmdSymbols, "example.test/fixture.A"},
		{"coverage", []string{"--snapshot", "snapshot-two"}, cmdCoverage, "reachability coverage"},
		{"list-commits", nil, cmdListCommits, "snapshot-two"},
		{"get", []string{"--snapshot", "snapshot-two", "example.test/fixture.A"}, cmdGet, "func A()"},
		{"prune", []string{"repo", "snapshot-one"}, cmdPrune, "pruned: repo=repo snapshot=snapshot-one"},
	}
	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			client := newCodeGraphTestDB(t)
			seedCommandGraph(t, client)
			// os.UserHomeDir() reads USERPROFILE on Windows and HOME elsewhere, so a test
			// that sets only HOME redirects nothing on windows-amd64 -- a declared coverage
			// target for this repo. Both are set to THE SAME directory: each OS reads only
			// the one it uses, so this is correct on every platform.
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			t.Setenv("USERPROFILE", homeDir)
			previousFactory := commandSpannerClient
			commandSpannerClient = func(context.Context) (*spanner.Client, error) { return client, nil }
			t.Cleanup(func() { commandSpannerClient = previousFactory })
			var runErr error
			out := captureStdout(t, func() { runErr = command.run(context.Background(), command.args) })
			if runErr != nil {
				t.Fatalf("command error: %v", runErr)
			}
			if !strings.Contains(out, command.wantOut) {
				t.Fatalf("output %q does not contain %q", out, command.wantOut)
			}
		})
	}
}

func TestCLICommandsRejectInvalidArgumentsAndClientFailure(t *testing.T) {
	invalid := []struct {
		name string
		run  func(context.Context, []string) error
		args []string
	}{
		{"ingest", cmdIngest, nil},
		{"use", cmdUse, nil},
		{"search", cmdSearch, nil},
		{"neighbors", cmdNeighbors, nil},
		{"diff", cmdDiff, nil},
		{"lineage", cmdLineage, nil},
		{"drift", cmdDrift, nil},
		{"dependents", cmdDependents, nil},
		{"hypothesize", cmdHypothesize, nil},
		{"get", cmdGet, nil},
		{"list-commits", cmdListCommits, []string{"extra"}},
		{"prune", cmdPrune, nil},
		{"manifest", cmdManifest, nil},
		{"verify", cmdVerify, nil},
	}
	for _, command := range invalid {
		t.Run(command.name, func(t *testing.T) {
			if err := command.run(context.Background(), command.args); err == nil {
				t.Fatal("invalid arguments were accepted")
			}
		})
	}

	previousFactory := commandSpannerClient
	commandSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, errors.New("spanner unavailable") }
	t.Cleanup(func() { commandSpannerClient = previousFactory })
	for _, command := range []struct {
		name string
		run  func(context.Context, []string) error
		args []string
	}{
		{"ingest", cmdIngest, []string{"."}},
		{"use", cmdUse, []string{"repo"}},
		{"search", cmdSearch, []string{"query"}},
		{"neighbors", cmdNeighbors, []string{"fqn"}},
		{"diff", cmdDiff, []string{"repo", "one", "two"}},
		{"lineage", cmdLineage, []string{"fqn"}},
		{"drift", cmdDrift, []string{"repo", "one", "two"}},
		{"findings", cmdFindings, nil},
		{"dependents", cmdDependents, []string{"fqn"}},
		{"hypothesize", cmdHypothesize, []string{"base", "."}},
		{"annotations", cmdAnnotations, nil},
		{"symbols", cmdSymbols, nil},
		{"coverage", cmdCoverage, nil},
		{"list-commits", cmdListCommits, nil},
		{"prune", cmdPrune, []string{"repo", "snap"}},
		{"get", cmdGet, []string{"fqn"}},
		{"manifest", cmdManifest, []string{"repo", "snap", t.TempDir()}},
		{"verify", cmdVerify, []string{"repo", "snap", t.TempDir()}},
	} {
		t.Run("client-"+command.name, func(t *testing.T) {
			if err := command.run(context.Background(), command.args); err == nil || !strings.Contains(err.Error(), "spanner unavailable") {
				t.Fatalf("client failure = %v", err)
			}
		})
	}
}

func TestCLIProgressAndUsageOutput(t *testing.T) {
	out := captureStdout(t, func() {
		progress := cliProgress()
		progress("walk", 2, 4, "files")
		progress("cache", 0, 0, "ready")
		progress("silent", 0, 0, "")
	})
	if !strings.Contains(out, "walk 2/4 files") || !strings.Contains(out, "cache: ready") || strings.Contains(out, "silent") {
		t.Fatalf("progress output = %q", out)
	}
	usageOutput := captureStderr(t, usage)
	if !strings.Contains(usageOutput, "code-graph ingest") || !strings.Contains(usageOutput, "code-graph mcp") {
		t.Fatalf("usage output = %q", usageOutput)
	}
}

func TestRunMainDispatchAndExitCodes(t *testing.T) {
	previousInitialize := initializeCommandCredentials
	previousClient := commandSpannerClient
	previousMCPClient := mcpNewSpannerClient
	previousServe := serveHTTP
	t.Cleanup(func() {
		initializeCommandCredentials = previousInitialize
		commandSpannerClient = previousClient
		mcpNewSpannerClient = previousMCPClient
		serveHTTP = previousServe
	})

	if code := runMain(context.Background(), []string{"code-graph"}, func(string) string { return "" }); code != 2 {
		t.Fatalf("missing-command exit code = %d, want 2", code)
	}

	initializeCommandCredentials = func(context.Context, func(string) string) error {
		return errors.New("credential initialization failed")
	}
	stderr := captureStderr(t, func() {
		if code := runMain(context.Background(), []string{"code-graph", "context"}, func(string) string { return "" }); code != 1 {
			t.Fatalf("credential-failure exit code = %d, want 1", code)
		}
	})
	if !strings.Contains(stderr, "credential initialization failed") {
		t.Fatalf("credential-failure stderr = %q", stderr)
	}
	hazardFixtureDir := writeHazardFixtureModule(t)
	hazardStdout := captureStdout(t, func() {
		if code := runMain(context.Background(), []string{"velenza-cambium", "hazards", "--analyze", hazardFixtureDir}, func(string) string { return "" }); code != 0 {
			t.Fatalf("hazards --analyze exit code = %d, want 0 without Google credentials", code)
		}
	})
	if !strings.Contains(hazardStdout, "hazards: repo=local snapshot=working-tree") {
		t.Fatalf("hazards --analyze stdout = %q", hazardStdout)
	}

	initializeCommandCredentials = func(context.Context, func(string) string) error { return nil }
	// os.UserHomeDir() reads USERPROFILE on Windows and HOME elsewhere, so a test
	// that sets only HOME redirects nothing on windows-amd64 -- a declared coverage
	// target for this repo. Both are set to THE SAME directory: each OS reads only
	// the one it uses, so this is correct on every platform.
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	if code := runMain(context.Background(), []string{"code-graph", "context"}, func(string) string { return "" }); code != 0 {
		t.Fatalf("context exit code = %d, want 0", code)
	}
	if code := runMain(context.Background(), []string{"code-graph", "unknown"}, func(string) string { return "" }); code != 2 {
		t.Fatalf("unknown-command exit code = %d, want 2", code)
	}

	clientFailure := errors.New("command client unavailable")
	commandSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, clientFailure }
	mcpNewSpannerClient = func(context.Context) (*spanner.Client, error) { return nil, clientFailure }
	serveHTTP = func(*http.Server) error { return errors.New("listener stopped") }

	errorCases := []struct {
		command string
		args    []string
	}{
		{"ingest", nil},
		{"search", nil},
		{"neighbors", nil},
		{"get", nil},
		{"mcp", nil},
		{"diff", nil},
		{"list-commits", []string{"extra"}},
		{"use", nil},
		{"symbols", nil},
		{"annotations", nil},
		{"coverage", nil},
		{"manifest", nil},
		{"verify", nil},
		{"serve", []string{"--addr", ":0"}},
		{"prune", nil},
		{"create-db", []string{"unexpected"}},
		{"migrate", nil},
		{"lineage", nil},
		{"drift", nil},
		{"findings", nil},
		{"dependents", nil},
		{"hypothesize", nil},
		{"replay", nil},
		{"update", nil},
	}
	for _, testCase := range errorCases {
		t.Run(testCase.command, func(t *testing.T) {
			argv := append([]string{"code-graph", testCase.command}, testCase.args...)
			stderr := captureStderr(t, func() {
				if code := runMain(context.Background(), argv, func(string) string { return "" }); code != 1 {
					t.Fatalf("exit code = %d, want 1", code)
				}
			})
			if !strings.Contains(stderr, "error:") {
				t.Fatalf("stderr = %q, want command error", stderr)
			}
		})
	}
}

func TestCLIManifestAndVerifyCommands(t *testing.T) {
	for _, structural := range []bool{false, true} {
		name := "raw"
		if structural {
			name = "structural"
		}
		t.Run("manifest-"+name, func(t *testing.T) {
			client := newCodeGraphTestDB(t)
			seedCommandGraph(t, client)
			installCommandClient(t, client)
			outDir := t.TempDir()
			args := []string{"repo", "snapshot-two", outDir}
			if structural {
				args = append([]string{"--from-structure"}, args...)
			}
			var runErr error
			out := captureStdout(t, func() { runErr = cmdManifest(context.Background(), args) })
			if runErr != nil {
				t.Fatalf("manifest command: %v", runErr)
			}
			contents, err := os.ReadFile(filepath.Join(outDir, "fixture.go"))
			if err != nil || string(contents) != "package fixture\n" || !strings.Contains(out, name) {
				t.Fatalf("manifest output=%q contents=%q err=%v", out, contents, err)
			}
		})

		t.Run("verify-"+name, func(t *testing.T) {
			client := newCodeGraphTestDB(t)
			seedCommandGraph(t, client)
			installCommandClient(t, client)
			repo := newTrackedTestRepo(t, map[string]string{"fixture.go": "package fixture\n"})
			args := []string{"repo", "snapshot-two", repo}
			if structural {
				args = append([]string{"--from-structure"}, args...)
			}
			var runErr error
			out := captureStdout(t, func() { runErr = cmdVerify(context.Background(), args) })
			if runErr != nil || !strings.Contains(out, "VERIFY CLEAN") {
				t.Fatalf("verify output=%q err=%v", out, runErr)
			}
		})
	}

	client := newCodeGraphTestDB(t)
	seedCommandGraph(t, client)
	installCommandClient(t, client)
	repo := newTrackedTestRepo(t, map[string]string{
		"fixture.go": "package changed\n",
		"extra.txt":  "extra\n",
	})
	var verifyErr error
	out := captureStdout(t, func() {
		verifyErr = cmdVerify(context.Background(), []string{"repo", "snapshot-two", repo})
	})
	if verifyErr == nil || !strings.Contains(verifyErr.Error(), "verify failed") || !strings.Contains(out, "content differs") || !strings.Contains(out, "on disk, not in snapshot") {
		t.Fatalf("mismatch output=%q err=%v", out, verifyErr)
	}
}

func TestServeCommandBuildsHardenedServer(t *testing.T) {
	sentinel := errors.New("listener stopped")
	previousServe := serveHTTP
	t.Cleanup(func() { serveHTTP = previousServe })

	t.Setenv("PORT_ADDR", "")
	t.Setenv("PORT", "9191")
	t.Setenv("OIDC_AUDIENCE", "")
	serveHTTP = func(server *http.Server) error {
		if server.Addr != ":9191" || server.ReadHeaderTimeout != 10*time.Second || server.Handler == nil {
			t.Fatalf("development server = %+v", server)
		}
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("development health status = %d", recorder.Code)
		}
		return sentinel
	}
	if err := cmdServe(context.Background(), nil); !errors.Is(err, sentinel) {
		t.Fatalf("development serve error = %v", err)
	}

	serveHTTP = func(server *http.Server) error {
		if server.Addr != ":9292" {
			t.Fatalf("OIDC server address = %q", server.Addr)
		}
		healthRecorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(healthRecorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if healthRecorder.Code != http.StatusOK {
			t.Fatalf("OIDC health probe status = %d", healthRecorder.Code)
		}
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/ingest", nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("OIDC ingest without bearer status = %d", recorder.Code)
		}
		return sentinel
	}
	if err := cmdServe(context.Background(), []string{"--addr", ":9292", "--oidc-audience", "https://service.example"}); !errors.Is(err, sentinel) {
		t.Fatalf("OIDC serve error = %v", err)
	}
}

func installCommandClient(t *testing.T, client *spanner.Client) {
	t.Helper()
	previousFactory := commandSpannerClient
	commandSpannerClient = func(context.Context) (*spanner.Client, error) { return client, nil }
	t.Cleanup(func() { commandSpannerClient = previousFactory })
}

func seedCommandGraph(t *testing.T, client *spanner.Client) {
	t.Helper()
	seedSnapshots(t, client)
	now := time.Now()
	idA := uuidFromFQN("example.test/fixture.A")
	idB := uuidFromFQN("example.test/fixture.B")
	idC := uuidFromFQN("example.test/fixture.C")
	idTest := uuidFromFQN("example.test/fixture.TestA")
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	hashC := strings.Repeat("c", 64)
	hashTest := strings.Repeat("t", 64)
	fileBytes := []byte("package fixture\n")
	fileHash := sha256Hex(string(fileBytes))
	declHash := sha256Hex("package fixture\n")
	applyTestMutations(t, client,
		spanner.Insert("SnapshotMembers", []string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"}, []any{"repo", "snapshot-two", idA, "example.test/fixture.A", hashA, "func", "a.go", int64(1), int64(2)}),
		spanner.Insert("SnapshotMembers", []string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"}, []any{"repo", "snapshot-two", idB, "example.test/fixture.B", hashB, "func", "b.go", int64(3), int64(4)}),
		spanner.Insert("SnapshotMembers", []string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"}, []any{"repo", "snapshot-two", idC, "example.test/fixture.C", hashC, "func", "c.go", int64(5), int64(6)}),
		spanner.Insert("SnapshotMembers", []string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"}, []any{"repo", "snapshot-two", idTest, "example.test/fixture.TestA", hashTest, "func", "a_test.go", int64(7), int64(8)}),
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{idA, hashA, "example.test/fixture.A", "func", "func A()", "A docs", "func A() {}", []string{"sentinel.raise name=fixture.check spec_section=fixture"}, now}),
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{idB, hashB, "example.test/fixture.B", "func", "func B()", "", "func B() {}", []string{}, now}),
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{idC, hashC, "example.test/fixture.C", "func", "func C()", "", "func C() {}", []string{}, now}),
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{idTest, hashTest, "example.test/fixture.TestA", "func", "func TestA()", "", "func TestA() {}", []string{}, now}),
		spanner.Insert("SymbolEdges", []string{"src_symbol_id", "src_content_hash", "dst_symbol_id", "kind"}, []any{idTest, hashTest, idA, "CALLS"}),
		spanner.Insert("SymbolEdges", []string{"src_symbol_id", "src_content_hash", "dst_symbol_id", "kind"}, []any{idA, hashA, idC, "CALLS"}),
		spanner.Insert("FileContents", []string{"content_sha", "chunk_index", "bytes", "created_at"}, []any{fileHash, int64(0), fileBytes, now}),
		spanner.Insert("SnapshotFiles", []string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"}, []any{"repo", "snapshot-two", "fixture.go", fileHash, true, int64(0)}),
		spanner.Insert("DeclSource", []string{"content_sha", "source", "created_at"}, []any{declHash, "package fixture\n", now}),
		spanner.Insert("GoFileChunks", []string{"repo", "snapshot_id", "file_path", "chunk_index", "chunk_kind", "symbol_fqns", "content_sha", "blank_before"}, []any{"repo", "snapshot-two", "fixture.go", int64(0), "package", []string{}, declHash, false}),
	)
}

func captureStderr(t *testing.T, run func()) string {
	t.Helper()
	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = writer
	defer func() { os.Stderr = original }()
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
