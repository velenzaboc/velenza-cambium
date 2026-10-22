package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func auditNoProgress(string, int, int, string) {}

func auditInsertSnapshot(t *testing.T, client *spanner.Client, repo, snapshot string) {
	t.Helper()
	now := time.Now()
	commit := snapshot
	if len(commit) > 40 {
		commit = commit[:40]
	}
	applyTestMutations(t, client, spanner.Insert("Snapshots",
		[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
		[]any{repo, snapshot, snapshot, commit, now, "ingested", nil, []string{}, "main", int64(0), now}))
}
func auditWorkspaceHead(t *testing.T, repo string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repo, ".inarch", "workspace_state.toml"))
	if err != nil {
		t.Fatalf("read Inarch workspace state: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(key) == "changeset_id" {
			return strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	t.Fatalf("Inarch workspace state has no changeset_id: %s", raw)
	return ""
}

func auditRunInr(t *testing.T, home, binary string, args ...string) string {
	t.Helper()
	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(), "HOME="+home, "USERPROFILE="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("inr %v: %v: %s", args, err, out)
	}
	return string(out)
}

func auditInstallEmbeddings(t *testing.T) {
	t.Helper()
	previous := requestEmbeddings
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, float32(i + 1)}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previous })
}

func TestAuditC1ConsumesRealInarchWorkspaceWithoutGit(t *testing.T) {
	binary := os.Getenv("VELENZA_INR_BIN")
	if binary == "" {
		t.Fatal("set VELENZA_INR_BIN to the verified source-built Inarch producer")
	}
	home := t.TempDir()
	repo := filepath.Join(t.TempDir(), "native")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	auditRunInr(t, home, binary, "init", repo)
	mustWriteTestFile(t, filepath.Join(repo, "go.mod"), "module example.test/native\n\ngo 1.27\n")
	mustWriteTestFile(t, filepath.Join(repo, "native.go"), "package native\n\nfunc Value() int { return 1 }\n")
	auditRunInr(t, home, binary, "commit", "-m", "first", "--author", "audit@example.invalid", repo)
	first := auditWorkspaceHead(t, repo)
	mustWriteTestFile(t, filepath.Join(repo, "native.go"), "package native\n\nfunc Value() int { return 2 }\n")
	auditRunInr(t, home, binary, "commit", "-m", "second", "--author", "audit@example.invalid", repo)
	second := auditWorkspaceHead(t, repo)

	if hasGitDir(repo) {
		t.Fatal("Inarch producer unexpectedly created a Git repository")
	}
	if got, err := gitHead(repo); err != nil || got != second {
		t.Errorf("native head = %q, %v; want %q", got, err, second)
	}
	if got, err := gitParents(repo, second); err != nil || !reflect.DeepEqual(got, []string{first}) {
		t.Errorf("native parents = %v, %v; want [%s]", got, err, first)
	}

	client := newAuditDB(t)
	auditInstallEmbeddings(t)
	stats, err := doIngest(context.Background(), client, repo, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatalf("ingest real native Inarch workspace: %v", err)
	}
	if stats.SnapshotID != second {
		t.Fatalf("native snapshot = %q, want workspace head %q", stats.SnapshotID, second)
	}
	row, err := client.Single().ReadRow(context.Background(), "Snapshots",
		spanner.Key{stats.Repo, second}, []string{"parent_snapshot_ids"})
	if err != nil {
		t.Fatalf("read native snapshot: %v", err)
	}
	var parents []string
	if err := row.Columns(&parents); err != nil {
		t.Fatalf("decode native snapshot parents: %v", err)
	}
	if !reflect.DeepEqual(parents, []string{first}) {
		t.Fatalf("persisted native parents = %v, want [%s]", parents, first)
	}
	members := client.Single().Query(context.Background(), spanner.Statement{
		SQL:    "SELECT COUNT(*) FROM SnapshotMembers WHERE repo=@repo AND snapshot_id=@snap",
		Params: map[string]any{"repo": stats.Repo, "snap": second},
	})
	memberRow, err := members.Next()
	members.Stop()
	if err != nil {
		t.Fatalf("read native snapshot members: %v", err)
	}
	var memberCount int64
	if err := memberRow.Columns(&memberCount); err != nil {
		t.Fatalf("decode native snapshot members: %v", err)
	}
	if memberCount == 0 {
		t.Fatal("native snapshot persisted no members")
	}
	files, err := loadSnapshotFiles(context.Background(), client, stats.Repo, second)
	if err != nil {
		t.Fatalf("load native fidelity files: %v", err)
	}
	if got := auditFile(t, files, "native.go"); !bytes.Equal(got, []byte("package native\n\nfunc Value() int { return 2 }\n")) {
		t.Fatalf("native fidelity content = %q", got)
	}
	chunks, err := loadGoFileChunks(context.Background(), client, stats.Repo, second)
	if err != nil || len(chunks["native.go"]) == 0 {
		t.Fatalf("native Go chunks = %v, %v", chunks, err)
	}
}

func newAuditInterceptedDB(t *testing.T, interceptor grpc.UnaryClientInterceptor) *spanner.Client {
	t.Helper()
	server, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("start in-memory Spanner: %v", err)
	}
	server.SetLogger(func(string, ...interface{}) {})
	t.Cleanup(server.Close)
	auditSchema := strings.Replace(codeGraphTestSchema, "commit_sha STRING(40)", "commit_sha STRING(64)", 1)
	ddl, err := spansql.ParseDDL("audit-schema.sql", auditSchema)
	if err != nil {
		t.Fatalf("parse audit schema: %v", err)
	}
	if err := server.UpdateDDL(ddl); err != nil {
		t.Fatalf("apply audit schema: %v", err)
	}
	conn, err := grpc.NewClient(server.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor))
	if err != nil {
		t.Fatalf("connect to audit Spanner: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := spanner.NewClient(context.Background(), "projects/test/instances/test/databases/audit", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("create audit Spanner client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func newAuditDB(t *testing.T) *spanner.Client {
	t.Helper()
	return newAuditInterceptedDB(t, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ctx, method, req, reply, cc, opts...)
	})
}

func newAuditEmulatorInterceptedDB(t *testing.T, interceptor grpc.UnaryClientInterceptor) *spanner.Client {
	t.Helper()
	host := strings.TrimSpace(os.Getenv("SPANNER_EMULATOR_HOST"))
	if host == "" {
		t.Fatal("C2 integration regression requires SPANNER_EMULATOR_HOST")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dialOptions := []option.ClientOption{
		option.WithEndpoint(host),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, dialOptions...)
	if err != nil {
		t.Fatalf("create emulator instance admin client: %v", err)
	}
	t.Cleanup(func() { _ = instanceAdmin.Close() })

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	projectID := "cambium-audit"
	instanceID := "c2-" + suffix
	databaseID := "c2-" + suffix
	instancePath := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)
	createInstance, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + projectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Fatalf("create emulator instance: %v", err)
	}
	if _, err := createInstance.Wait(ctx); err != nil {
		t.Fatalf("wait for emulator instance: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = instanceAdmin.DeleteInstance(cleanupCtx, &instancepb.DeleteInstanceRequest{Name: instancePath})
	})

	databaseAdmin, err := database.NewDatabaseAdminClient(ctx, dialOptions...)
	if err != nil {
		t.Fatalf("create emulator database admin client: %v", err)
	}
	auditSchema := strings.Replace(codeGraphTestSchema, "commit_sha STRING(40)", "commit_sha STRING(64)", 1)
	createDatabase, err := databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: splitDDL(strings.ReplaceAll(auditSchema, "TIMESTAMP NOT NULL", "TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)")),
	})
	if err != nil {
		_ = databaseAdmin.Close()
		t.Fatalf("create emulator database: %v", err)
	}
	if _, err := createDatabase.Wait(ctx); err != nil {
		_ = databaseAdmin.Close()
		t.Fatalf("wait for emulator database: %v", err)
	}
	if err := databaseAdmin.Close(); err != nil {
		t.Fatalf("close emulator database admin client: %v", err)
	}

	conn, err := grpc.NewClient(host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor))
	if err != nil {
		t.Fatalf("connect to emulator data API: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := spanner.NewClient(context.Background(),
		fmt.Sprintf("%s/databases/%s", instancePath, databaseID), option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("create emulator Spanner client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func auditCommitWritesTable(req any, table string) bool {
	commit, ok := req.(*spannerpb.CommitRequest)
	if !ok {
		return false
	}
	for _, mutation := range commit.GetMutations() {
		if write := mutation.GetInsertOrUpdate(); write != nil && write.GetTable() == table {
			return true
		}
	}
	return false
}

func auditFile(t *testing.T, files []snapshotFileRow, path string) []byte {
	t.Helper()
	for _, file := range files {
		if file.path == path {
			return file.bytes
		}
	}
	return nil
}

func TestAuditC2InterruptedReplacementIsInvisibleAndRetryRepairsWholeBlob(t *testing.T) {
	var enabled atomic.Bool
	var fileCommits atomic.Int32
	var injected atomic.Bool
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if auditCommitWritesTable(req, "FileContents") {
			n := fileCommits.Add(1)
			if enabled.Load() && n >= 2 {
				injected.Store(true)
				return status.Error(codes.PermissionDenied, "audit injected second FileContents commit failure")
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	client := newAuditEmulatorInterceptedDB(t, interceptor)
	previousRequest := requestEmbeddings
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, float32(i + 1)}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequest })

	oldBytes := bytes.Repeat([]byte("o"), fidelityCommitBytes+fileChunkSize)
	newBytes := bytes.Repeat([]byte("n"), fidelityCommitBytes+fileChunkSize)
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod":    "module example.test/atomic\n\ngo 1.27\n",
		"main.go":   "package atomic\n\nfunc Stable() int { return 1 }\n",
		"large.txt": string(oldBytes),
	})
	ctx := context.Background()
	initial, err := doIngest(ctx, client, repo, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	before, err := loadSnapshotFiles(ctx, client, initial.Repo, initial.SnapshotID)
	if err != nil || !bytes.Equal(auditFile(t, before, "large.txt"), oldBytes) {
		t.Fatalf("initial fidelity bytes missing: files=%d err=%v", len(before), err)
	}
	if err := os.WriteFile(filepath.Join(repo, "large.txt"), newBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	// Keep HEAD unchanged: this is a replacement attempt for an already-published
	// snapshot, so a failed recapture must leave the old snapshot usable.
	replacementSnapshot := initial.SnapshotID

	fileCommits.Store(0)
	injected.Store(false)
	enabled.Store(true)
	if _, err := doIngest(ctx, client, repo, ingestOpts{}, auditNoProgress); err == nil {
		t.Error("replacement ingest succeeded despite injected second FileContents commit failure")
	}
	enabled.Store(false)
	if !injected.Load() {
		t.Fatal("fixture did not reach the second FileContents commit")
	}
	afterFailure, err := loadSnapshotFiles(ctx, client, initial.Repo, initial.SnapshotID)
	if err != nil || !bytes.Equal(auditFile(t, afterFailure, "large.txt"), oldBytes) {
		t.Errorf("failed same-snapshot replacement did not preserve old usable content: files=%d err=%v", len(afterFailure), err)
	}

	retried, err := doIngest(ctx, client, repo, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatalf("retry replacement: %v", err)
	}
	if retried.SnapshotID != replacementSnapshot {
		t.Fatalf("retry snapshot = %q, want %q", retried.SnapshotID, replacementSnapshot)
	}
	afterRetry, err := loadSnapshotFiles(ctx, client, retried.Repo, retried.SnapshotID)
	if err != nil {
		t.Fatalf("load retried snapshot: %v", err)
	}
	if got := auditFile(t, afterRetry, "large.txt"); !bytes.Equal(got, newBytes) {
		t.Fatalf("retried large file length/content = %d bytes, want complete %d-byte content", len(got), len(newBytes))
	}
}

func TestAuditC2RejectsIncompleteOrCorruptBlobRows(t *testing.T) {
	cases := []struct {
		name   string
		chunks map[int64][]byte
	}{
		{name: "missing suffix", chunks: map[int64][]byte{0: []byte("prefix")}},
		{name: "noncontiguous indices with valid reconstructed hash", chunks: map[int64][]byte{0: []byte("prefix-middle-"), 2: []byte("suffix")}},
		{name: "hash mismatch", chunks: map[int64][]byte{0: []byte("different")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newCodeGraphTestDB(t)
			repo, snapshot := "repo", strings.Repeat("a", 40)
			auditInsertSnapshot(t, client, repo, snapshot)
			want := []byte("prefix-middle-suffix")
			sha := sha256Hex(string(want))
			muts := []*spanner.Mutation{spanner.Insert("SnapshotFiles",
				[]string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"},
				[]any{repo, snapshot, "data.txt", sha, false, int64(0)})}
			for index, chunk := range tc.chunks {
				muts = append(muts, spanner.Insert("FileContents",
					[]string{"content_sha", "chunk_index", "bytes", "created_at"},
					[]any{sha, index, chunk, time.Now()}))
			}
			applyTestMutations(t, client, muts...)
			if files, err := loadSnapshotFiles(context.Background(), client, repo, snapshot); err == nil {
				t.Fatalf("accepted incomplete/corrupt blob as %d reconstructed file(s)", len(files))
			}
		})
	}
}

func auditDatabaseContainsMarker(t *testing.T, client *spanner.Client, marker []byte) bool {
	t.Helper()
	ctx := context.Background()
	textQueries := []struct {
		name string
		sql  string
	}{
		{name: "SymbolVersions.body", sql: "SELECT body FROM SymbolVersions"},
		{name: "DeclSource.source", sql: "SELECT source FROM DeclSource"},
	}
	for _, query := range textQueries {
		iter := client.Single().Query(ctx, spanner.Statement{SQL: query.sql})
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				t.Fatalf("scan %s: %v", query.name, err)
			}
			var value spanner.NullString
			if err := row.Columns(&value); err != nil {
				iter.Stop()
				t.Fatalf("decode %s: %v", query.name, err)
			}
			if value.Valid && bytes.Contains([]byte(value.StringVal), marker) {
				iter.Stop()
				return true
			}
		}
		iter.Stop()
	}
	iter := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT bytes FROM FileContents"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			t.Fatalf("scan FileContents.bytes: %v", err)
		}
		var value []byte
		if err := row.Columns(&value); err != nil {
			t.Fatalf("decode FileContents.bytes: %v", err)
		}
		if bytes.Contains(value, marker) {
			return true
		}
	}
}

func TestAuditC3TrackedExternalSymlinkIsRejectedOrStoredAsLink(t *testing.T) {
	client := newAuditDB(t)
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod":  "module example.test/symlink\n\ngo 1.27\n",
		"main.go": "package symlink\n",
	})
	externalDir := t.TempDir()
	externalPath := filepath.Join(externalDir, "external.go")
	marker := []byte("AUDIT_EXTERNAL_HOST_SECRET_9e60a48b")
	secret := []byte("package symlink\n\nfunc ExternalSecret() string { return \"AUDIT_EXTERNAL_HOST_SECRET_9e60a48b\" }\n")
	previousRequest := requestEmbeddings
	var sentToEmbeddings atomic.Bool
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			if strings.Contains(text, string(marker)) {
				sentToEmbeddings.Store(true)
			}
			vectors[i] = []float32{1, float32(i + 1)}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequest })
	if err := os.WriteFile(externalPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(repo, "leak.go")
	if err := os.Symlink(externalPath, linkPath); err != nil {
		t.Fatalf("create tracked symlink: %v", err)
	}
	mustRunGit(t, repo, "add", "leak.go")
	mustRunGit(t, repo, "commit", "-m", "track symlink")

	stats, err := doIngest(context.Background(), client, repo, ingestOpts{}, auditNoProgress)
	if sentToEmbeddings.Load() {
		t.Fatal("tracked Go symlink sent external host source bytes to the embedding boundary")
	}
	if auditDatabaseContainsMarker(t, client, marker) {
		t.Fatal("tracked Go symlink persisted external host source bytes")
	}
	if err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "symlink") && !strings.Contains(lower, "outside") && !strings.Contains(lower, "escape") {
			t.Fatalf("external symlink refusal lacks boundary reason: %v", err)
		}
		return
	}
	files, err := loadSnapshotFiles(context.Background(), client, stats.Repo, stats.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	got := auditFile(t, files, "leak.go")
	if !bytes.Equal(got, []byte(externalPath)) {
		t.Fatalf("symlink was neither rejected nor preserved as link text: %q", got)
	}
}

func auditInsertMember(t *testing.T, client *spanner.Client, repo, snapshot, id, fqn string, hash any) {
	t.Helper()
	applyTestMutations(t, client, spanner.Insert("SnapshotMembers",
		[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash", "kind", "file_path", "line_start", "line_end"},
		[]any{repo, snapshot, id, fqn, hash, "func", "fixture.go", int64(1), int64(2)}))
}

func TestAuditC4HistoricalEdgesRemainSnapshotScoped(t *testing.T) {
	client := newCodeGraphTestDB(t)
	ctx := context.Background()
	repo := "repo"
	snapshotA := strings.Repeat("a", 40)
	snapshotB := strings.Repeat("b", 40)
	auditInsertSnapshot(t, client, repo, snapshotA)
	auditInsertSnapshot(t, client, repo, snapshotB)
	sourceID := uuidFromFQN("example.test/p.Call")
	targetA := uuidFromFQN("example.test/p.ImplA")
	targetB := uuidFromFQN("example.test/p.ImplB")
	hash := strings.Repeat("c", 64)
	auditInsertMember(t, client, repo, snapshotA, sourceID, "example.test/p.Call", hash)
	auditInsertMember(t, client, repo, snapshotA, targetA, "example.test/p.ImplA", nil)
	auditInsertMember(t, client, repo, snapshotB, sourceID, "example.test/p.Call", hash)
	auditInsertMember(t, client, repo, snapshotB, targetB, "example.test/p.ImplB", nil)

	if err := writeSymbolEdges(ctx, client, []Edge{{Repo: repo, SnapshotID: snapshotA, SrcID: sourceID, SrcContentHash: hash, DstID: targetA, Kind: "CALLS"}}, auditNoProgress); err != nil {
		t.Fatal(err)
	}
	if err := writeSymbolEdges(ctx, client, []Edge{{Repo: repo, SnapshotID: snapshotB, SrcID: sourceID, SrcContentHash: hash, DstID: targetB, Kind: "CALLS"}}, auditNoProgress); err != nil {
		t.Fatal(err)
	}
	edgesA, err := loadSnapshotEdges(ctx, client, repo, snapshotA)
	if err != nil {
		t.Fatal(err)
	}
	edgesB, err := loadSnapshotEdges(ctx, client, repo, snapshotB)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(edgesA, []EdgeRow{{Src: sourceID, Dst: targetA, Kind: "CALLS"}}) {
		t.Errorf("historical snapshot A edges rewritten: %+v", edgesA)
	}
	if !reflect.DeepEqual(edgesB, []EdgeRow{{Src: sourceID, Dst: targetB, Kind: "CALLS"}}) {
		t.Errorf("snapshot B edges = %+v", edgesB)
	}
}

func TestAuditC4ZeroEdgeReingestClearsStaleVersionEdges(t *testing.T) {
	client := newCodeGraphTestDB(t)
	previousRequest := requestEmbeddings
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequest })
	repoPath := newTrackedTestRepo(t, map[string]string{
		"go.mod":  "module example.test/leaf\n\ngo 1.27\n",
		"leaf.go": "package leaf\n\nfunc Leaf() int { return 1 }\n",
	})
	ctx := context.Background()
	stats, err := doIngest(ctx, client, repoPath, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatal(err)
	}
	symbolID := uuidFromFQN("example.test/leaf.Leaf")
	var contentHash string
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT content_hash FROM SnapshotMembers WHERE repo=@repo AND snapshot_id=@snap AND symbol_id=@sid",
		Params: map[string]any{"repo": stats.Repo, "snap": stats.SnapshotID, "sid": symbolID},
	})
	row, err := iter.Next()
	iter.Stop()
	if err != nil || row.Columns(&contentHash) != nil {
		t.Fatalf("load leaf version hash: %v", err)
	}
	staleID := uuidFromFQN("example.test/leaf.Stale")
	if err := writeSymbolEdges(ctx, client, []Edge{{Repo: stats.Repo, SnapshotID: stats.SnapshotID, SrcID: symbolID, SrcContentHash: contentHash, DstID: staleID, Kind: "CALLS"}}, auditNoProgress); err != nil {
		t.Fatal(err)
	}
	before, err := loadSnapshotEdges(ctx, client, stats.Repo, stats.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, edge := range before {
		if edge.Src == symbolID && edge.Dst == staleID && edge.Kind == "CALLS" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("fixture did not establish a visible stale edge")
	}
	if _, err := doIngest(ctx, client, repoPath, ingestOpts{}, auditNoProgress); err != nil {
		t.Fatal(err)
	}
	after, err := loadSnapshotEdges(ctx, client, stats.Repo, stats.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range after {
		if edge.Src == symbolID && edge.Dst == staleID && edge.Kind == "CALLS" {
			t.Fatalf("zero-edge reingest retained a visible stale edge: %+v", edge)
		}
	}
}

func TestAuditC5PinnedContextKeepsRepositoryIdentity(t *testing.T) {
	client := newCodeGraphTestDB(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	snapshot := strings.Repeat("d", 40)
	fqn := "example.test/shared.Value"
	id := uuidFromFQN(fqn)
	hashA := strings.Repeat("a", 64)
	hashZ := strings.Repeat("z", 64)
	now := time.Now()
	for _, repo := range []string{"a-repo", "z-repo"} {
		auditInsertSnapshot(t, client, repo, snapshot)
	}
	applyTestMutations(t, client,
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{id, hashA, fqn, "func", "func Value() string", "", `func Value() string { return "a" }`, []string{}, now}),
		spanner.Insert("SymbolVersions", []string{"symbol_id", "content_hash", "fqn", "kind", "signature", "doc_comment", "body", "volt_annotations", "created_at"}, []any{id, hashZ, fqn, "func", "func Value() string", "", `func Value() string { return "z" }`, []string{}, now}),
	)
	auditInsertMember(t, client, "a-repo", snapshot, id, fqn, hashA)
	if err := saveContext(cgContext{Repo: "z-repo", SnapshotID: snapshot}); err != nil {
		t.Fatal(err)
	}
	installCommandClient(t, client)
	var getErr error
	_ = captureStdout(t, func() {
		getErr = cmdGet(context.Background(), []string{fqn})
	})
	if getErr == nil {
		t.Fatal("pinned z-repo query returned a symbol that exists only in a-repo")
	}

	auditInsertMember(t, client, "z-repo", snapshot, id, fqn, hashZ)
	getErr = nil
	out := captureStdout(t, func() {
		getErr = cmdGet(context.Background(), []string{fqn})
	})
	if getErr != nil {
		t.Fatalf("pinned get after adding z-repo member: %v", getErr)
	}
	if !strings.Contains(out, "repo:       z-repo") || !strings.Contains(out, `return "z"`) {
		t.Fatalf("pinned z-repo query crossed into another repo:\n%s", out)
	}
}

func TestAuditC6MultipleInitDeclarationsRemainDistinct(t *testing.T) {
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod": "module example.test/multiinit\n\ngo 1.27\n",
		"a.go": `package multiinit

func init() { first() }
func init() { second() }
func first() {}
func second() {}
`,
		"b.go": `package multiinit

func init() { third() }
func third() {}
`,
	})
	syms, _, err := walkRepo(repo, "multiinit", strings.Repeat("e", 40), auditNoProgress)
	if err != nil {
		t.Fatal(err)
	}
	initIDs := map[string]struct{}{}
	for _, symbol := range syms {
		if symbol.Kind == "func" && strings.HasPrefix(strings.TrimSpace(symbol.Signature), "func init(") {
			initIDs[symbol.ID] = struct{}{}
		}
	}
	if len(initIDs) != 3 {
		t.Errorf("three valid init declarations produced %d distinct symbol identities", len(initIDs))
	}
	client := newCodeGraphTestDB(t)
	snapshot := strings.Repeat("e", 40)
	auditInsertSnapshot(t, client, "multiinit", snapshot)
	if err := writeSnapshotMembers(context.Background(), client, syms, auditNoProgress); err != nil {
		t.Fatal(err)
	}
	for id := range initIDs {
		row, err := client.Single().ReadRow(context.Background(), "SnapshotMembers",
			spanner.Key{"multiinit", snapshot, id}, []string{"symbol_id"})
		if err != nil {
			t.Fatalf("read persisted init symbol %s: %v", id, err)
		}
		var persistedID string
		if err := row.Columns(&persistedID); err != nil {
			t.Fatalf("decode persisted init symbol %s: %v", id, err)
		}
		if persistedID != id {
			t.Fatalf("persisted init symbol = %q, want %q", persistedID, id)
		}
	}
}

func TestAuditC7ShiftBoundMustConstrainTheShiftOperandOnAllPaths(t *testing.T) {
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod": "module example.test/shifts\n\ngo 1.27\n",
		"shifts.go": `package shifts

func UnsafeUnrelated(attempt, unrelated int) int {
	if unrelated < 10 {
		unrelated++
	}
	return 1 << attempt
}

func UnsafeNonDominating(attempt int, branch bool) int {
	if branch && attempt < 10 {
		return 1 << attempt
	}
	return 1 << attempt
}

func SafeDominating(attempt int) int {
	if attempt > 30 {
		attempt = 30
	}
	return 1 << attempt
}
`,
	})
	_, _, hazards, err := analyzePackageHazards(repo)
	if err != nil {
		t.Fatal(err)
	}
	unsafe := map[string]bool{}
	for _, hazard := range hazards {
		if hazard.Category == "UNBOUNDED_SHIFT" {
			unsafe[hazard.FunctionFQN] = true
		}
	}
	for _, name := range []string{"UnsafeUnrelated", "UnsafeNonDominating"} {
		found := false
		for fqn := range unsafe {
			if strings.HasSuffix(fqn, "."+name) {
				found = true
			}
		}
		if !found {
			t.Errorf("missing UNBOUNDED_SHIFT for %s", name)
		}
	}
	for fqn := range unsafe {
		if strings.HasSuffix(fqn, ".SafeDominating") {
			t.Errorf("dominating operand bound was not honored: %s", fqn)
		}
	}
}

func TestAuditC8ReplayAndUpdateReturnAggregateFailures(t *testing.T) {
	repo := replayFixtureRepo(t)
	t.Run("replay ingest failure", func(t *testing.T) {
		service := &fakeReplayService{exists: map[string]bool{}, ingestErr: errors.New("audit ingest failure")}
		installReplayService(t, service, nil)
		if err := cmdReplay(context.Background(), []string{"--workers=1", "--min-free-gb=0", repo}); err == nil {
			t.Fatal("replay returned success after every ingest failed")
		}
	})
	t.Run("replay partial ingest failure", func(t *testing.T) {
		service := &fakeReplayService{
			exists:       map[string]bool{},
			ingestErrors: []error{nil, errors.New("audit second ingest failure")},
		}
		installReplayService(t, service, nil)
		if err := cmdReplay(context.Background(), []string{"--workers=1", "--min-free-gb=0", repo}); err == nil {
			t.Fatal("replay returned success after one of two ingests failed")
		}
		if len(service.ingested) != 2 {
			t.Fatalf("partial replay attempted %d ingests, want 2", len(service.ingested))
		}
	})
	t.Run("update ingest failure", func(t *testing.T) {
		service := &fakeReplayService{exists: map[string]bool{}, ingestErr: errors.New("audit update failure")}
		installReplayService(t, service, nil)
		if err := cmdUpdate(context.Background(), []string{repo}); err == nil {
			t.Fatal("update returned success after ingest failure")
		}
	})
	t.Run("update partial ingest failure", func(t *testing.T) {
		mixedRepo := replayFixtureRepo(t)
		mustRunGit(t, mixedRepo, "branch", "older", "HEAD~1")
		service := &fakeReplayService{
			exists:       map[string]bool{},
			ingestErrors: []error{nil, errors.New("audit second update failure")},
		}
		installReplayService(t, service, nil)
		if err := cmdUpdate(context.Background(), []string{mixedRepo}); err == nil {
			t.Fatal("update returned success after one of two branch ingests failed")
		}
		if len(service.ingested) != 2 {
			t.Fatalf("partial update attempted %d ingests, want 2", len(service.ingested))
		}
	})
	t.Run("replay worktree creation failure", func(t *testing.T) {
		service := &fakeReplayService{exists: map[string]bool{}}
		installReplayService(t, service, nil)
		badTemp := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(badTemp, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TMPDIR", badTemp)
		if err := cmdReplay(context.Background(), []string{"--workers=1", "--min-free-gb=0", repo}); err == nil {
			t.Fatal("replay returned success after worker worktree creation failed")
		}
	})
}
