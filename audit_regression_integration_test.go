//go:build integration

package main

import (
	"bytes"
	"context"
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
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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
