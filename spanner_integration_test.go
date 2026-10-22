package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const codeGraphTestSchema = `
CREATE TABLE SymbolBlobs (
  content_hash STRING(64) NOT NULL,
  embedding ARRAY<FLOAT64>,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (content_hash);
CREATE TABLE SymbolVersions (
  symbol_id STRING(36) NOT NULL,
  content_hash STRING(64) NOT NULL,
  fqn STRING(MAX) NOT NULL,
  kind STRING(32) NOT NULL,
  signature STRING(MAX),
  doc_comment STRING(MAX),
  body STRING(MAX),
  volt_annotations ARRAY<STRING(MAX)>,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (symbol_id, content_hash);
CREATE TABLE SymbolEdges (
  src_symbol_id STRING(36) NOT NULL,
  src_content_hash STRING(64) NOT NULL,
  dst_symbol_id STRING(36) NOT NULL,
  kind STRING(32) NOT NULL
) PRIMARY KEY (src_symbol_id, src_content_hash, dst_symbol_id, kind);
CREATE TABLE Snapshots (
  repo STRING(128) NOT NULL,
  snapshot_id STRING(64) NOT NULL,
  storage_snapshot_id STRING(64),
  label STRING(256) NOT NULL,
  commit_sha STRING(40) NOT NULL,
  commit_time TIMESTAMP,
  kind STRING(20) NOT NULL,
  base_snapshot_id STRING(64),
  parent_snapshot_ids ARRAY<STRING(64)>,
  branch STRING(256),
  symbol_count INT64 NOT NULL,
  ingested_at TIMESTAMP NOT NULL
) PRIMARY KEY (repo, snapshot_id);
CREATE TABLE SnapshotMembers (
  repo STRING(128) NOT NULL,
  snapshot_id STRING(64) NOT NULL,
  symbol_id STRING(36) NOT NULL,
  fqn STRING(MAX) NOT NULL,
  content_hash STRING(64),
  kind STRING(32) NOT NULL,
  file_path STRING(MAX) NOT NULL,
  line_start INT64 NOT NULL,
  line_end INT64 NOT NULL
) PRIMARY KEY (repo, snapshot_id, symbol_id);
CREATE TABLE SnapshotEdges (
  repo STRING(128) NOT NULL,
  snapshot_id STRING(64) NOT NULL,
  symbol_id STRING(36) NOT NULL,
  dst_symbol_id STRING(36) NOT NULL,
  kind STRING(32) NOT NULL
) PRIMARY KEY (repo, snapshot_id, symbol_id, dst_symbol_id, kind);
CREATE TABLE FileContents (
  content_sha STRING(64) NOT NULL,
  chunk_index INT64 NOT NULL,
  bytes BYTES(MAX) NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (content_sha, chunk_index);
CREATE TABLE SnapshotFiles (
  repo STRING(128) NOT NULL,
  snapshot_id STRING(64) NOT NULL,
  file_path STRING(1024) NOT NULL,
  content_sha STRING(64) NOT NULL,
  is_go BOOL NOT NULL,
  file_order INT64 NOT NULL
) PRIMARY KEY (repo, snapshot_id, file_path);
CREATE TABLE DeclSource (
  content_sha STRING(64) NOT NULL,
  source STRING(MAX) NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (content_sha);
CREATE TABLE GoFileChunks (
  repo STRING(128) NOT NULL,
  snapshot_id STRING(64) NOT NULL,
  file_path STRING(1024) NOT NULL,
  chunk_index INT64 NOT NULL,
  chunk_kind STRING(16) NOT NULL,
  symbol_fqns ARRAY<STRING(MAX)>,
  content_sha STRING(64) NOT NULL,
  blank_before BOOL
) PRIMARY KEY (repo, snapshot_id, file_path, chunk_index);
CREATE TABLE SymbolAliases (
  repo STRING(128) NOT NULL,
  from_symbol_id STRING(36) NOT NULL,
  to_symbol_id STRING(36) NOT NULL,
  from_fqn STRING(MAX) NOT NULL,
  to_fqn STRING(MAX) NOT NULL,
  embedding_distance FLOAT64 NOT NULL,
  caller_overlap FLOAT64 NOT NULL,
  detected_at TIMESTAMP NOT NULL
) PRIMARY KEY (repo, from_symbol_id, to_symbol_id);
`

func newCodeGraphTestDB(t *testing.T) *spanner.Client {
	t.Helper()
	server, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("start in-memory Spanner: %v", err)
	}
	server.SetLogger(func(string, ...interface{}) {})
	t.Cleanup(server.Close)
	ddl, err := spansql.ParseDDL("test-schema.sql", codeGraphTestSchema)
	if err != nil {
		t.Fatalf("parse test schema: %v", err)
	}
	if err := server.UpdateDDL(ddl); err != nil {
		t.Fatalf("apply test schema: %v", err)
	}
	conn, err := grpc.NewClient(server.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connect to in-memory Spanner: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := spanner.NewClient(context.Background(), "projects/test/instances/test/databases/test", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("create Spanner client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func applyTestMutations(t *testing.T, client *spanner.Client, mutations ...*spanner.Mutation) {
	t.Helper()
	if _, err := client.Apply(context.Background(), mutations); err != nil {
		t.Fatalf("apply test mutations: %v", err)
	}
}

func TestEmbeddingCacheWritesMissingContent(t *testing.T) {
	client := newCodeGraphTestDB(t)
	ctx := context.Background()
	hashA := sha256Hex("func A()")
	hashB := sha256Hex("func B()")
	syms := []Symbol{
		{FQN: "example.test/p.A", Kind: "func", Signature: "func A()", ContentHash: hashA},
		{FQN: "example.test/p.Alias", Kind: "func", Signature: "func A()", ContentHash: hashA},
		{FQN: "example.test/p.B", Kind: "func", Signature: "func B()", ContentHash: hashB},
	}
	previousRequest := requestEmbeddings
	requestEmbeddings = func(_ context.Context, texts []string, taskType string) ([][]float32, error) {
		if taskType != "RETRIEVAL_DOCUMENT" || len(texts) != 2 {
			return nil, errors.New("unexpected embedding request")
		}
		return [][]float32{{1, 0}, {0, 1}}, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequest })
	var progressed int
	err := embedAndWriteBlobsWithProgress(ctx, client, syms, []string{hashA, hashB}, func(stage string, done, total int, _ string) {
		if stage == "embed" && done == total {
			progressed = done
		}
	})
	if err != nil || progressed != 2 {
		t.Fatalf("embedding write = progress %d, %v", progressed, err)
	}
	missing, err := selectMissingBlobs(ctx, client, syms)
	if err != nil || len(missing) != 0 {
		t.Fatalf("embedding cache misses after write = %v, %v", missing, err)
	}
	if err := embedAndWriteBlobs(ctx, client, syms, nil); err != nil {
		t.Fatalf("empty embedding write: %v", err)
	}
	requestEmbeddings = func(context.Context, []string, string) ([][]float32, error) {
		return nil, errors.New("vertex unavailable")
	}
	if err := embedAndWriteBlobs(ctx, client, syms, []string{hashA}); err == nil || !strings.Contains(err.Error(), "vertex unavailable") {
		t.Fatalf("embedding request failure = %v", err)
	}
}

func seedSnapshots(t *testing.T, client *spanner.Client) {
	t.Helper()
	older := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	newer := older.Add(time.Hour)
	applyTestMutations(t, client,
		spanner.Insert("Snapshots",
			[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
			[]any{"repo", "snapshot-one", "first", "1111111111111111111111111111111111111111", older, "ingested", nil, []string{}, "main", int64(0), older}),
		spanner.Insert("Snapshots",
			[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
			[]any{"repo", "snapshot-two", "second", "2222222222222222222222222222222222222222", newer, "ingested", nil, []string{"snapshot-one"}, "main", int64(0), newer}),
	)
}

func TestSnapshotContextAndFidelityQueries(t *testing.T) {
	client := newCodeGraphTestDB(t)
	ctx := context.Background()
	if exists, err := snapshotExists(ctx, client, "repo", "missing"); err != nil || exists {
		t.Fatalf("missing snapshot = %v, %v", exists, err)
	}
	if _, err := latestSnapshot(ctx, client, "repo"); err == nil || !strings.Contains(err.Error(), "for repo repo") {
		t.Fatalf("empty latest snapshot error = %v", err)
	}
	if _, err := snapshotRepo(ctx, client, "missing"); err == nil || !strings.Contains(err.Error(), "not ingested") {
		t.Fatalf("missing snapshot repo error = %v", err)
	}
	seedSnapshots(t, client)
	if exists, err := snapshotExists(ctx, client, "repo", "snapshot-two"); err != nil || !exists {
		t.Fatalf("existing snapshot = %v, %v", exists, err)
	}
	latest, err := latestSnapshot(ctx, client, "repo")
	if err != nil || latest != (cgContext{Repo: "repo", SnapshotID: "snapshot-two"}) {
		t.Fatalf("latest snapshot = %+v, %v", latest, err)
	}
	if repo, err := snapshotRepo(ctx, client, "snapshot-one"); err != nil || repo != "repo" {
		t.Fatalf("snapshot repo = %q, %v", repo, err)
	}

	home := t.TempDir()
	// os.UserHomeDir() reads USERPROFILE on Windows and HOME elsewhere, so a test
	// that sets only HOME redirects nothing on windows-amd64 -- a declared coverage
	// target for this repo. Both are set to THE SAME directory: each OS reads only
	// the one it uses, so this is correct on every platform.
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	repo, snapshot, note, err := resolveQuerySnapshot(ctx, client, "snapshot-one")
	if err != nil || repo != "repo" || snapshot != "snapshot-one" || !strings.Contains(note, "--snapshot") {
		t.Fatalf("explicit resolution = %q/%q/%q, %v", repo, snapshot, note, err)
	}
	if err := saveContext(cgContext{Repo: "repo", SnapshotID: "snapshot-one"}); err != nil {
		t.Fatal(err)
	}
	repo, snapshot, note, err = resolveQuerySnapshot(ctx, client, "")
	if err != nil || snapshot != "snapshot-one" || !strings.Contains(note, "pinned via `use`") {
		t.Fatalf("pinned resolution = %q/%q/%q, %v", repo, snapshot, note, err)
	}
	if err := saveContext(cgContext{Repo: "repo", SnapshotID: "stale"}); err != nil {
		t.Fatal(err)
	}
	repo, snapshot, note, err = resolveQuerySnapshot(ctx, client, "")
	if err != nil || snapshot != "snapshot-two" || !strings.Contains(note, "newest") {
		t.Fatalf("fallback resolution = %q/%q/%q, %v", repo, snapshot, note, err)
	}

	fileSHA := sha256Hex("package p\n")
	pkgSHA := sha256Hex("package p\n")
	declSHA := sha256Hex("func F() {}\n")
	applyTestMutations(t, client,
		spanner.Insert("FileContents", []string{"content_sha", "chunk_index", "bytes", "created_at"}, []any{fileSHA, int64(0), []byte("package "), time.Now()}),
		spanner.Insert("FileContents", []string{"content_sha", "chunk_index", "bytes", "created_at"}, []any{fileSHA, int64(1), []byte("p\n"), time.Now()}),
		spanner.Insert("SnapshotFiles", []string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"}, []any{"repo", "snapshot-two", "main.go", fileSHA, true, int64(0)}),
		spanner.Insert("DeclSource", []string{"content_sha", "source", "created_at"}, []any{pkgSHA, "package p\n", time.Now()}),
		spanner.Insert("DeclSource", []string{"content_sha", "source", "created_at"}, []any{declSHA, "func F() {}\n", time.Now()}),
		spanner.Insert("GoFileChunks", []string{"repo", "snapshot_id", "file_path", "chunk_index", "chunk_kind", "symbol_fqns", "content_sha", "blank_before"}, []any{"repo", "snapshot-two", "main.go", int64(0), "package", []string{}, pkgSHA, false}),
		spanner.Insert("GoFileChunks", []string{"repo", "snapshot_id", "file_path", "chunk_index", "chunk_kind", "symbol_fqns", "content_sha", "blank_before"}, []any{"repo", "snapshot-two", "main.go", int64(1), "decl", []string{"example.test/p.F"}, declSHA, true}),
	)
	present, err := selectPresentShas(ctx, client, map[string]struct{}{fileSHA: {}, "missing": {}})
	if err != nil || !reflect.DeepEqual(present, map[string]struct{}{fileSHA: {}}) {
		t.Fatalf("present file SHAs = %v, %v", present, err)
	}
	decls, err := selectPresentDeclShas(ctx, client, map[string]struct{}{declSHA: {}, "missing": {}})
	if err != nil || !reflect.DeepEqual(decls, map[string]struct{}{declSHA: {}}) {
		t.Fatalf("present decl SHAs = %v, %v", decls, err)
	}
	files, err := loadSnapshotFiles(ctx, client, "repo", "snapshot-two")
	if err != nil || len(files) != 1 || files[0].path != "main.go" || string(files[0].bytes) != "package p\n" {
		t.Fatalf("snapshot files = %+v, %v", files, err)
	}
	chunks, err := loadGoFileChunks(ctx, client, "repo", "snapshot-two")
	if err != nil || len(chunks["main.go"]) != 2 || chunks["main.go"][1].text != "func F() {}\n" || !chunks["main.go"][1].blankBefore {
		t.Fatalf("snapshot chunks = %+v, %v", chunks, err)
	}

	outDir := t.TempDir()
	count, err := doManifest(ctx, client, "repo", "snapshot-two", outDir, false)
	if err != nil || count != 1 {
		t.Fatalf("manifest = %d, %v", count, err)
	}
	manifested, err := os.ReadFile(filepath.Join(outDir, "main.go"))
	if err != nil || string(manifested) != "package p\n" {
		t.Fatalf("manifested bytes = %q, %v", manifested, err)
	}
	structuredDir := t.TempDir()
	count, err = doManifest(ctx, client, "repo", "snapshot-two", structuredDir, true)
	if err != nil || count != 1 {
		t.Fatalf("structural manifest = %d, %v", count, err)
	}
	structured, err := os.ReadFile(filepath.Join(structuredDir, "main.go"))
	if err != nil || string(structured) != "package p\n\nfunc F() {}\n" {
		t.Fatalf("structural manifested bytes = %q, %v", structured, err)
	}
	if _, err := doManifest(ctx, client, "repo", "missing", t.TempDir(), false); err == nil || !strings.Contains(err.Error(), "not ingested") {
		t.Fatalf("missing manifest error = %v", err)
	}
	if _, err := doManifest(ctx, client, "repo", "snapshot-one", t.TempDir(), false); err == nil || !strings.Contains(err.Error(), "no captured files") {
		t.Fatalf("empty manifest error = %v", err)
	}

	rawRepo := newTrackedTestRepo(t, map[string]string{"main.go": "package p\n"})
	verified, err := doVerify(ctx, client, "repo", "snapshot-two", rawRepo, false)
	if err != nil || !verified.Clean() || verified.Files != 1 {
		t.Fatalf("raw verify = %+v, %v", verified, err)
	}
	structuredRepo := newTrackedTestRepo(t, map[string]string{"main.go": "package p\n\nfunc F() { }\n"})
	verified, err = doVerify(ctx, client, "repo", "snapshot-two", structuredRepo, true)
	if err != nil || !verified.Clean() {
		t.Fatalf("structural verify = %+v, %v", verified, err)
	}
	if err := os.WriteFile(filepath.Join(rawRepo, "main.go"), []byte("package changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	verified, err = doVerify(ctx, client, "repo", "snapshot-two", rawRepo, false)
	if err != nil || !reflect.DeepEqual(verified.Differing, []string{"main.go"}) || verified.Clean() {
		t.Fatalf("differing verify = %+v, %v", verified, err)
	}
	if _, err := doVerify(ctx, client, "repo", "snapshot-one", rawRepo, false); err == nil || !strings.Contains(err.Error(), "no captured files") {
		t.Fatalf("empty verify error = %v", err)
	}
}

func TestIngestPersistsAnalysisAndFidelity(t *testing.T) {
	client := newCodeGraphTestDB(t)
	ctx := context.Background()
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod": "module example.test/fixture\n\ngo 1.24.0\n",
		"fixture.go": `// Package fixture exercises the graph extractor.
package fixture

import "fmt"

type Speaker interface { Speak() string }
type Base struct { Name string }
type Thing struct {
	Base
	Count int
}

const Limit = 3
var Default = Thing{Count: Limit}

func (t *Thing) Speak() string { return fmt.Sprint(t.Name) }
func Use(s Speaker) string {
	//volt: sentinel.raise spec_section=fixture name=called
	v := Thing{Count: Limit}
	_ = v.Count
	return s.Speak()
}
`,
		"fixture_test.go": `package fixture

import "testing"

func TestUse(t *testing.T) {
	if got := Use(&Thing{Base: Base{Name: "fixture"}}); got != "fixture" {
		t.Fatalf("Use() = %q", got)
	}
}
`,
		"schema.sql": `CREATE TABLE Widgets (
  widget_id STRING(36) NOT NULL,
  label STRING(MAX)
) PRIMARY KEY (widget_id);
CREATE INDEX WidgetsByLabel ON Widgets(label);
`,
		"conformance_manifest.json": `{"passes":{"analysis":{"ok":true}}}`,
	})
	repoName, err := deriveRepoName(repo)
	if err != nil {
		t.Fatalf("derive repo name: %v", err)
	}
	commit, err := gitHead(repo)
	if err != nil {
		t.Fatalf("git head: %v", err)
	}

	var walkProgress []string
	syms, edges, err := walkRepo(repo, repoName, commit, func(stage string, _, _ int, _ string) {
		walkProgress = append(walkProgress, stage)
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	wantKinds := map[string]bool{"package": false, "file": false, "type": false, "field": false, "const": false, "var": false, "func": false, "method": false}
	for _, symbol := range syms {
		if _, ok := wantKinds[symbol.Kind]; ok {
			wantKinds[symbol.Kind] = true
		}
	}
	for kind, found := range wantKinds {
		if !found {
			t.Errorf("walk did not emit %s symbol: %+v", kind, syms)
		}
	}
	edgeKinds := map[string]bool{}
	for _, edge := range edges {
		edgeKinds[edge.Kind] = true
	}
	for _, kind := range []string{"DECLARES", "IMPORTS", "CALLS", "REFERENCES", "IMPLEMENTS", "EMBEDS"} {
		if !edgeKinds[kind] {
			t.Errorf("walk did not emit %s edge: %+v", kind, edges)
		}
	}
	if len(walkProgress) == 0 {
		t.Fatal("walk emitted no progress")
	}
	walkSymbols, walkEdges := len(syms), len(edges)
	ddlSymbols, ddlEdges, err := scanDDL(repo, repoName, commit)
	if err != nil {
		t.Fatalf("scan fixture DDL: %v", err)
	}
	manifestSymbols, _, err := scanManifest(repo, repoName, commit)
	if err != nil {
		t.Fatalf("scan fixture manifest: %v", err)
	}
	syms = append(syms, ddlSymbols...)
	syms = append(syms, manifestSymbols...)
	edges = append(edges, ddlEdges...)

	// Seed the immutable embedding cache so this integration test exercises the
	// complete ingest/persistence workflow without making an external Vertex call.
	seenHashes := map[string]struct{}{}
	var blobMutations []*spanner.Mutation
	for _, symbol := range syms {
		text := buildEmbedText(symbol)
		if text == "" {
			continue
		}
		hash := sha256Hex(text)
		if _, exists := seenHashes[hash]; exists {
			continue
		}
		seenHashes[hash] = struct{}{}
		blobMutations = append(blobMutations, spanner.Insert("SymbolBlobs",
			[]string{"content_hash", "embedding", "created_at"},
			[]any{hash, []float64{0.25, 0.75}, time.Now()}))
	}
	applyTestMutations(t, client, blobMutations...)

	var stages []string
	stats, err := doIngest(ctx, client, repo, ingestOpts{}, func(stage string, _, _ int, _ string) {
		stages = append(stages, stage)
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if stats.Repo != repoName || stats.SnapshotID != commit || stats.CommitSHA != commit || stats.Kind != "ingested" {
		t.Fatalf("ingest identity = %+v", stats)
	}
	if stats.Symbols != len(syms) || stats.Symbols <= walkSymbols || stats.Edges != len(edges) || stats.Edges <= walkEdges || stats.CacheMisses != 0 || stats.CacheHits != len(seenHashes) {
		t.Fatalf("ingest counts = %+v; expected symbols/edges=%d/%d walk=%d/%d hashes=%d", stats, len(syms), len(edges), walkSymbols, walkEdges, len(seenHashes))
	}
	if len(stages) == 0 || stages[0] != "start" || stages[len(stages)-1] != "done" {
		t.Fatalf("ingest stages = %v", stages)
	}
	if exists, err := snapshotExists(ctx, client, repoName, commit); err != nil || !exists {
		t.Fatalf("persisted snapshot = %v, %v", exists, err)
	}
	files, err := loadSnapshotFiles(ctx, client, repoName, commit)
	if err != nil || len(files) != 5 {
		t.Fatalf("captured files = %d, %v", len(files), err)
	}
	chunks, err := loadGoFileChunks(ctx, client, repoName, commit)
	if err != nil || len(chunks["fixture.go"]) == 0 || len(chunks["fixture_test.go"]) == 0 {
		t.Fatalf("captured Go chunks = %+v, %v", chunks, err)
	}

	rows, err := doListSymbols(ctx, client, repoName, commit, "func", `Use|TestUse`, false, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("list symbols = %+v, %v", rows, err)
	}
	orphans, err := doListSymbols(ctx, client, repoName, commit, "", "", true, 1)
	if err != nil || len(orphans) > 1 {
		t.Fatalf("orphan symbols = %+v, %v", orphans, err)
	}
	if _, err := doListSymbols(ctx, client, repoName, "missing", "", "", false, 0); err == nil {
		t.Fatal("list symbols accepted a missing snapshot")
	}
	if _, err := doListSymbols(ctx, client, repoName, commit, "", "[", false, 0); err == nil {
		t.Fatal("list symbols accepted an invalid regular expression")
	}

	coverage, err := doCoverage(ctx, client, repoName, commit, false)
	if err != nil || coverage.ProductionFuncs == 0 || coverage.Covered == 0 || coverage.TestFuncs == 0 {
		t.Fatalf("reachability coverage = %+v, %v", coverage, err)
	}
	untested, err := doCoverage(ctx, client, repoName, commit, true)
	if err != nil || len(untested.Symbols) >= len(coverage.Symbols) {
		t.Fatalf("untested-only coverage = %+v, %v", untested, err)
	}
	if _, err := doCoverage(ctx, client, repoName, "missing", false); err == nil {
		t.Fatal("coverage accepted a missing snapshot")
	}

	wantFQN := "example.test/fixture.Use"
	symbolID, resolvedRepo, resolvedSnapshot, err := resolveFQN(ctx, client, wantFQN, commit)
	if err != nil || symbolID != uuidFromFQN(wantFQN) || resolvedRepo != repoName || resolvedSnapshot != commit {
		t.Fatalf("pinned FQN resolution = %q/%q/%q, %v", symbolID, resolvedRepo, resolvedSnapshot, err)
	}
	if _, _, _, err := resolveFQN(ctx, client, "example.test/fixture.Missing", commit); err == nil {
		t.Fatal("pinned FQN resolution accepted missing symbol")
	}
	edgeRows, err := loadSnapshotEdges(ctx, client, repoName, commit)
	if err != nil || len(edgeRows) == 0 {
		t.Fatalf("snapshot edges = %+v, %v", edgeRows, err)
	}
	meta, err := snapshotMemberMeta(ctx, client, repoName, commit)
	if err != nil || meta[symbolID].FQN != wantFQN {
		t.Fatalf("snapshot member metadata = %+v, %v", meta[symbolID], err)
	}
	neighbors, err := doNeighbors(ctx, client, wantFQN, 2, commit)
	if err != nil || len(neighbors) == 0 {
		t.Fatalf("neighbors = %+v, %v", neighbors, err)
	}
	defaultDepth, err := doNeighbors(ctx, client, wantFQN, 0, commit)
	if err != nil || len(defaultDepth) == 0 {
		t.Fatalf("default-depth neighbors = %+v, %v", defaultDepth, err)
	}
	clampedDepth, err := doNeighbors(ctx, client, wantFQN, 20, commit)
	if err != nil || len(clampedDepth) < len(defaultDepth) {
		t.Fatalf("clamped-depth neighbors = %+v, %v", clampedDepth, err)
	}
	detail, err := doGet(ctx, client, wantFQN, commit)
	if err != nil || detail.FQN != wantFQN || !strings.Contains(detail.Body, "func Use") {
		t.Fatalf("symbol detail = %+v, %v", detail, err)
	}
	commits, err := doListCommits(ctx, client)
	if err != nil || len(commits) != 1 || commits[0].SnapshotID != commit {
		t.Fatalf("commit list = %+v, %v", commits, err)
	}
	if _, err := doAnnotations(ctx, client, repoName, "missing", "", ""); err == nil {
		t.Fatal("annotations accepted a missing snapshot")
	}
	annotations, err := doAnnotations(ctx, client, repoName, commit, "sentinel.raise", "fixture")
	if err != nil || len(annotations) != 1 || annotations[0].FQN != wantFQN || annotations[0].Name != "called" {
		t.Fatalf("annotations = %+v, %v", annotations, err)
	}
	filteredAnnotations, err := doAnnotations(ctx, client, repoName, commit, "other", "")
	if err != nil || len(filteredAnnotations) != 0 {
		t.Fatalf("filtered annotations = %+v, %v", filteredAnnotations, err)
	}
	dag, err := loadRepoDAG(ctx, client, repoName)
	if err != nil || dag[commit] == nil || dag[commit].commitSHA != commit {
		t.Fatalf("repository DAG = %+v, %v", dag, err)
	}
	aliasChain, err := resolveAliasChain(ctx, client, repoName, symbolID)
	if err != nil || len(aliasChain) != 1 {
		t.Fatalf("alias chain = %+v, %v", aliasChain, err)
	}
	reingested, err := doIngest(ctx, client, repo, ingestOpts{}, nil)
	if err != nil || reingested.SnapshotID != commit || reingested.Symbols != stats.Symbols {
		t.Fatalf("replacement ingest = %+v, %v", reingested, err)
	}

	members, err := contentMembers(ctx, client, repoName, commit)
	if err != nil || len(members) == 0 {
		t.Fatalf("content members = %+v, %v", members, err)
	}
	byFQN := map[string]memberLite{}
	for _, member := range members {
		byFQN[member.fqn] = member
	}
	oldUse := byFQN[wantFQN]
	testUse := byFQN["example.test/fixture.TestUse"]
	if oldUse.contentHash == "" || testUse.contentHash == "" {
		t.Fatalf("missing alias fixtures: use=%+v test=%+v", oldUse, testUse)
	}
	newSnapshot := "next-snapshot"
	newFQN := "example.test/fixture.UseRenamed"
	newID := uuidFromFQN(newFQN)
	newHash := strings.Repeat("a", 64)
	modifiedHash := strings.Repeat("b", 64)
	findingHashA := strings.Repeat("c", 64)
	findingHashB := strings.Repeat("d", 64)
	findingHashC := strings.Repeat("e", 64)
	newCommitTime := time.Now().Add(24 * time.Hour)
	applyTestMutations(t, client,
		spanner.Insert("Snapshots",
			[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
			[]any{repoName, newSnapshot, "next", strings.Repeat("f", 40), newCommitTime, "ingested", nil, []string{commit}, "main", int64(2), newCommitTime}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, newSnapshot, testUse.symbolID, testUse.fqn, testUse.contentHash, "func", "fixture_test.go", int64(5), int64(9)}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, newSnapshot, newID, newFQN, newHash, "func", "fixture.go", int64(20), int64(24)}),
		spanner.Insert("SymbolEdges",
			[]string{"src_symbol_id", "src_content_hash", "dst_symbol_id", "kind"},
			[]any{testUse.symbolID, testUse.contentHash, newID, "CALLS"}),
	)
	prior, err := priorSnapshot(ctx, client, repoName, newSnapshot)
	if err != nil || prior != commit {
		t.Fatalf("prior snapshot = %q, %v", prior, err)
	}
	oldCallers, err := callerSets(ctx, client, repoName, commit, []string{oldUse.symbolID})
	if err != nil || len(oldCallers[oldUse.symbolID]) == 0 {
		t.Fatalf("old caller set = %+v, %v", oldCallers, err)
	}
	if empty, err := callerSets(ctx, client, repoName, commit, nil); err != nil || len(empty) != 0 {
		t.Fatalf("empty caller set = %+v, %v", empty, err)
	}

	previousEmbeddingLoader := loadSymbolEmbeddings
	loadSymbolEmbeddings = func(context.Context, *spanner.Client, []string) (map[string][]float32, error) {
		return map[string][]float32{
			oldUse.contentHash: {1, 0},
			newHash:            {1, 0},
			modifiedHash:       {0, 1},
			findingHashA:       {1, 0},
			findingHashB:       {1, 0.01},
			findingHashC:       {0, 1},
		}, nil
	}
	t.Cleanup(func() { loadSymbolEmbeddings = previousEmbeddingLoader })
	var aliasCount int
	if err := detectAliases(ctx, client, repoName, newSnapshot, func(stage string, done, _ int, _ string) {
		if stage == "aliases" {
			aliasCount = done
		}
	}); err != nil {
		t.Fatalf("detect aliases: %v", err)
	}
	if aliasCount != 1 {
		t.Fatalf("detected alias count = %d", aliasCount)
	}
	aliasChain, err = resolveAliasChain(ctx, client, repoName, oldUse.symbolID)
	if err != nil || len(aliasChain) != 2 {
		t.Fatalf("stitched alias chain = %+v, %v", aliasChain, err)
	}
	lineage, err := doLineage(ctx, client, wantFQN)
	if err != nil || lineage.Repo != repoName || len(lineage.Versions) != 2 {
		t.Fatalf("symbol lineage = %+v, %v", lineage, err)
	}
	if lineage.Versions[0].ContentHash != oldUse.contentHash || lineage.Versions[1].ContentHash != newHash || lineage.Versions[1].SemanticDelta != 0 {
		t.Fatalf("lineage versions = %+v", lineage.Versions)
	}
	diff, err := doDiff(ctx, client, repoName, commit, newSnapshot)
	if err != nil || len(diff.Added) != 1 || len(diff.Removed) == 0 {
		t.Fatalf("snapshot diff = %+v, %v", diff, err)
	}
	drift, err := doDrift(ctx, client, repoName, commit, newSnapshot)
	if err != nil || drift.Added != 1 || drift.Removed == 0 {
		t.Fatalf("snapshot drift = %+v, %v", drift, err)
	}

	modifiedSnapshot := "modified-snapshot"
	modifiedTime := newCommitTime.Add(24 * time.Hour)
	applyTestMutations(t, client,
		spanner.Insert("Snapshots",
			[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
			[]any{repoName, modifiedSnapshot, "modified", strings.Repeat("e", 40), modifiedTime, "ingested", nil, []string{commit}, "feature", int64(1), modifiedTime}),
		spanner.Insert("SymbolVersions",
			[]string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"},
			[]any{oldUse.symbolID, modifiedHash, wantFQN, "func", "func Use(Speaker) string", "", `func Use(Speaker) string { return "rewritten" }`, []string{}, modifiedTime}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, modifiedSnapshot, oldUse.symbolID, wantFQN, modifiedHash, "func", "fixture.go", int64(20), int64(20)}),
	)
	modifiedDiff, err := doDiff(ctx, client, repoName, commit, modifiedSnapshot)
	if err != nil || len(modifiedDiff.Modified) != 1 || modifiedDiff.Modified[0].FQN != wantFQN {
		t.Fatalf("modified snapshot diff = %+v, %v", modifiedDiff, err)
	}
	modifiedDrift, err := doDrift(ctx, client, repoName, commit, modifiedSnapshot)
	if err != nil || len(modifiedDrift.Changed) != 1 || modifiedDrift.Changed[0].FQN != wantFQN || modifiedDrift.Changed[0].Quadrant != "rewrite" {
		t.Fatalf("modified snapshot drift = %+v, %v", modifiedDrift, err)
	}
	if _, err := doDrift(ctx, client, repoName, commit, "missing"); err == nil {
		t.Fatal("drift accepted a missing snapshot")
	}

	findingSnapshot := "findings-snapshot"
	findingTime := modifiedTime.Add(24 * time.Hour)
	findingA := uuidFromFQN("example.test/fixture.A")
	findingB := uuidFromFQN("example.test/fixture.B")
	findingC := uuidFromFQN("example.test/fixture.C")
	applyTestMutations(t, client,
		spanner.Insert("Snapshots",
			[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
			[]any{repoName, findingSnapshot, "findings", strings.Repeat("d", 40), findingTime, "ingested", nil, []string{modifiedSnapshot}, "findings", int64(4), findingTime}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, findingSnapshot, findingA, "example.test/fixture.A", findingHashA, "func", "a.go", int64(1), int64(1)}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, findingSnapshot, findingB, "example.test/fixture.B", findingHashB, "func", "b.go", int64(1), int64(1)}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, findingSnapshot, findingC, "example.test/fixture.C", findingHashC, "func", "c.go", int64(1), int64(1)}),
		spanner.Insert("SnapshotMembers",
			[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
			[]any{repoName, findingSnapshot, uuidFromFQN("example.test/fixture.TestDuplicate"), "example.test/fixture.TestDuplicate", findingHashB, "func", "a_test.go", int64(1), int64(1)}),
		spanner.Insert("SymbolEdges",
			[]string{"src_symbol_id", "src_content_hash", "dst_symbol_id", "kind"},
			[]any{findingA, findingHashA, findingC, "CALLS"}),
	)
	findings, err := doFindings(ctx, client, repoName, findingSnapshot, 10)
	if err != nil || len(findings.Duplication) != 1 || len(findings.Misuse) != 1 {
		t.Fatalf("graph findings = %+v, %v", findings, err)
	}
	duplicationPair := findings.Duplication[0].FQNA + "|" + findings.Duplication[0].FQNB
	if duplicationPair != "example.test/fixture.A|example.test/fixture.B" && duplicationPair != "example.test/fixture.B|example.test/fixture.A" {
		t.Fatalf("duplication finding = %+v", findings.Duplication)
	}
	if findings.Misuse[0].SrcFQN != "example.test/fixture.A" || findings.Misuse[0].DstFQN != "example.test/fixture.C" {
		t.Fatalf("misuse finding = %+v", findings.Misuse)
	}
	if _, err := doFindings(ctx, client, repoName, "missing", 0); err == nil {
		t.Fatal("findings accepted a missing snapshot")
	}
}

func newTrackedTestRepo(t *testing.T, files map[string]string) string {
	t.Helper()
	repo := t.TempDir()
	mustRunGit(t, repo, "init", "-b", "main")
	mustRunGit(t, repo, "config", "user.email", "coverage@example.invalid")
	mustRunGit(t, repo, "config", "user.name", "Coverage Test")
	for path, contents := range files {
		mustWriteTestFile(t, filepath.Join(repo, filepath.FromSlash(path)), contents)
		mustRunGit(t, repo, "add", path)
	}
	mustRunGit(t, repo, "commit", "-m", "fixture")
	return repo
}
