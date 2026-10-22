//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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

func auditPassInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, reply, cc, opts...)
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

	client := newAuditEmulatorInterceptedDB(t, auditPassInterceptor)
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
		t.Fatal("Cambium integration regressions require SPANNER_EMULATOR_HOST")
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

	var suffixBytes [12]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatalf("generate emulator database suffix: %v", err)
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	projectID := "cambium-audit"
	instanceID := "audit-" + suffix
	databaseID := "audit-" + suffix
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
		if write := mutation.GetInsert(); write != nil && write.GetTable() == table {
			return true
		}
		if write := mutation.GetUpdate(); write != nil && write.GetTable() == table {
			return true
		}
		if write := mutation.GetReplace(); write != nil && write.GetTable() == table {
			return true
		}
	}
	return false
}

func auditCommitDeletesTable(req any, table string) bool {
	commit, ok := req.(*spannerpb.CommitRequest)
	if !ok {
		return false
	}
	for _, mutation := range commit.GetMutations() {
		if deletion := mutation.GetDelete(); deletion != nil && deletion.GetTable() == table {
			return true
		}
	}
	return false
}

func auditVisibleSnapshotMemberCount(t *testing.T, client *spanner.Client, repo, snapshot string) int64 {
	t.Helper()
	members, err := snapshotMemberMeta(context.Background(), client, repo, snapshot)
	if err != nil {
		t.Fatalf("read visible snapshot members: %v", err)
	}
	return int64(len(members))
}

func auditPhysicalSnapshotID(t *testing.T, client *spanner.Client, repo, snapshot string) string {
	t.Helper()
	iter := client.Single().Query(context.Background(), spanner.Statement{
		SQL: `SELECT COALESCE(storage_snapshot_id, snapshot_id)
		      FROM Snapshots
		      WHERE repo=@repo AND snapshot_id=@snapshot`,
		Params: map[string]any{"repo": repo, "snapshot": snapshot},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		t.Fatalf("resolve physical snapshot for %s@%s: %v", repo, snapshot, err)
	}
	var physical string
	if err := row.Columns(&physical); err != nil {
		t.Fatalf("decode physical snapshot for %s@%s: %v", repo, snapshot, err)
	}
	if _, err := iter.Next(); err == nil {
		t.Fatalf("logical snapshot %s@%s resolved to multiple rows", repo, snapshot)
	}
	return physical
}

type auditGeneration struct {
	snapshotID string
	memberID   string
	memberFQN  string
	targetID   string
	fileBody   string
	chunkBody  string
}

func auditSeedGeneration(t *testing.T, client *spanner.Client, repo string, generation auditGeneration, kind string) {
	t.Helper()
	ctx := context.Background()
	if err := writeSnapshot(ctx, client, repo, generation.snapshotID, generation.snapshotID,
		strings.Repeat("a", 40), time.Now().UTC(), "main", nil, ingestOpts{kind: kind}, 2); err != nil {
		t.Fatalf("seed %s generation row: %v", kind, err)
	}
	auditInsertMember(t, client, repo, generation.snapshotID, generation.memberID, generation.memberFQN, nil)
	auditInsertMember(t, client, repo, generation.snapshotID, generation.targetID, generation.memberFQN+"Target", nil)
	if err := writeSnapshotSymbolEdges(ctx, client, repo, generation.snapshotID, []Edge{{
		Repo:       repo,
		SnapshotID: generation.snapshotID,
		SrcID:      generation.memberID,
		DstID:      generation.targetID,
		Kind:       "CALLS",
	}}, auditNoProgress); err != nil {
		t.Fatalf("seed %s generation edges: %v", kind, err)
	}
	fileSHA := sha256Hex(generation.fileBody)
	chunkSHA := sha256Hex(generation.chunkBody)
	applyTestMutations(t, client,
		spanner.Insert("FileContents",
			[]string{"content_sha", "chunk_index", "bytes", "created_at"},
			[]any{fileSHA, int64(0), []byte(generation.fileBody), spanner.CommitTimestamp}),
		spanner.Insert("SnapshotFiles",
			[]string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"},
			[]any{repo, generation.snapshotID, "generation.go", fileSHA, true, int64(0)}),
		spanner.Insert("DeclSource",
			[]string{"content_sha", "source", "created_at"},
			[]any{chunkSHA, generation.chunkBody, spanner.CommitTimestamp}),
		spanner.Insert("GoFileChunks",
			[]string{"repo", "snapshot_id", "file_path", "chunk_index", "chunk_kind", "symbol_fqns", "content_sha", "blank_before"},
			[]any{repo, generation.snapshotID, "generation.go", int64(0), "decl", []string{generation.memberFQN}, chunkSHA, false}),
	)
}

func auditAssertPublishedGeneration(t *testing.T, client *spanner.Client, repo, logicalSnapshot string, generation auditGeneration) {
	t.Helper()
	ctx := context.Background()
	if got := auditPhysicalSnapshotID(t, client, repo, logicalSnapshot); got != generation.snapshotID {
		t.Fatalf("logical snapshot %s points to %q, want generation %q", logicalSnapshot, got, generation.snapshotID)
	}
	members, err := snapshotMemberMeta(ctx, client, repo, logicalSnapshot)
	if err != nil {
		t.Fatalf("load public generation members: %v", err)
	}
	if len(members) != 2 || members[generation.memberID].FQN != generation.memberFQN || members[generation.targetID].FQN != generation.memberFQN+"Target" {
		t.Fatalf("public members do not belong exclusively to %s: %+v", generation.snapshotID, members)
	}
	edges, err := loadSnapshotEdges(ctx, client, repo, logicalSnapshot)
	if err != nil {
		t.Fatalf("load public generation edges: %v", err)
	}
	if len(edges) != 1 || edges[0].Src != generation.memberID || edges[0].Dst != generation.targetID || edges[0].Kind != "CALLS" {
		t.Fatalf("public edges do not belong exclusively to %s: %+v", generation.snapshotID, edges)
	}
	files, err := loadSnapshotFiles(ctx, client, repo, logicalSnapshot)
	if err != nil {
		t.Fatalf("load public generation files: %v", err)
	}
	if got := auditFile(t, files, "generation.go"); !bytes.Equal(got, []byte(generation.fileBody)) {
		t.Fatalf("public file bytes = %q, want generation %s bytes %q", got, generation.snapshotID, generation.fileBody)
	}
	chunks, err := loadGoFileChunks(ctx, client, repo, logicalSnapshot)
	if err != nil {
		t.Fatalf("load public generation chunks: %v", err)
	}
	if got := chunks["generation.go"]; len(got) != 1 || got[0].text != generation.chunkBody {
		t.Fatalf("public chunks do not belong exclusively to %s: %+v", generation.snapshotID, got)
	}
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

func TestAuditC2RepublicationKeepsLogicalIDAndSwitchesWholeGeneration(t *testing.T) {
	var rejectPublication atomic.Bool
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if rejectPublication.Load() && auditCommitWritesTable(req, "Snapshots") {
			return status.Error(codes.PermissionDenied, "audit injected publication refusal")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	client := newAuditEmulatorInterceptedDB(t, interceptor)
	ctx := context.Background()
	repo := "generation-switch-repo"
	logicalSnapshot := strings.Repeat("b", 40)
	legacy := auditGeneration{
		snapshotID: logicalSnapshot,
		memberID:   "legacy-member",
		memberFQN:  "example.test/generation.Legacy",
		targetID:   "legacy-target",
		fileBody:   "package generation\n\nfunc Legacy() int { return 0 }\n",
		chunkBody:  "func Legacy() int { return 0 }",
	}
	auditSeedGeneration(t, client, repo, legacy, "ingested")
	legacyRow, err := client.Single().ReadRow(ctx, "Snapshots", spanner.Key{repo, logicalSnapshot}, []string{"storage_snapshot_id"})
	if err != nil {
		t.Fatalf("read legacy public snapshot: %v", err)
	}
	var legacyStorage spanner.NullString
	if err := legacyRow.Columns(&legacyStorage); err != nil || legacyStorage.Valid {
		t.Fatalf("legacy public snapshot storage pointer = %+v, %v; want NULL", legacyStorage, err)
	}
	auditAssertPublishedGeneration(t, client, repo, logicalSnapshot, legacy)

	first := auditGeneration{
		snapshotID: "private-generation-one",
		memberID:   "first-member",
		memberFQN:  "example.test/generation.First",
		targetID:   "first-target",
		fileBody:   "package generation\n\nfunc First() int { return 1 }\n",
		chunkBody:  "func First() int { return 1 }",
	}
	auditSeedGeneration(t, client, repo, first, "staging")
	if err := publishStagedSnapshot(ctx, client, repo, first.snapshotID, logicalSnapshot, "ingested"); err != nil {
		t.Fatalf("publish first generation: %v", err)
	}
	auditAssertPublishedGeneration(t, client, repo, logicalSnapshot, first)
	if visible, err := snapshotExists(ctx, client, repo, first.snapshotID); err != nil || visible {
		t.Fatalf("first private generation became independently visible: visible=%v err=%v", visible, err)
	}

	second := auditGeneration{
		snapshotID: "private-generation-two",
		memberID:   "second-member",
		memberFQN:  "example.test/generation.Second",
		targetID:   "second-target",
		fileBody:   "package generation\n\nfunc Second() int { return 2 }\n",
		chunkBody:  "func Second() int { return 2 }",
	}
	auditSeedGeneration(t, client, repo, second, "staging")
	rejectPublication.Store(true)
	if err := publishStagedSnapshot(ctx, client, repo, second.snapshotID, logicalSnapshot, "ingested"); err == nil {
		t.Fatal("second generation published despite injected publication refusal")
	}
	rejectPublication.Store(false)
	auditAssertPublishedGeneration(t, client, repo, logicalSnapshot, first)

	if err := publishStagedSnapshot(ctx, client, repo, second.snapshotID, logicalSnapshot, "ingested"); err != nil {
		t.Fatalf("publish second generation: %v", err)
	}
	auditAssertPublishedGeneration(t, client, repo, logicalSnapshot, second)
	if visible, err := snapshotExists(ctx, client, repo, second.snapshotID); err != nil || visible {
		t.Fatalf("second private generation became independently visible: visible=%v err=%v", visible, err)
	}
	commits, err := doListCommits(ctx, client)
	if err != nil || len(commits) != 1 || commits[0].SnapshotID != logicalSnapshot {
		t.Fatalf("public commit identity changed across generations: commits=%+v err=%v", commits, err)
	}
}

func TestAuditC2LostPublicationAcknowledgementPreservesPublishedGeneration(t *testing.T) {
	var faultEnabled atomic.Bool
	var snapshotCommits atomic.Int32
	var publicationCommitted atomic.Bool
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if !faultEnabled.Load() || !auditCommitWritesTable(req, "Snapshots") {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if snapshotCommits.Add(1) == 1 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if publicationCommitted.CompareAndSwap(false, true) {
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				publicationCommitted.Store(false)
				return err
			}
		}
		return status.Error(codes.Unavailable, "audit dropped committed publication response")
	}
	client := newAuditEmulatorInterceptedDB(t, interceptor)
	auditInstallEmbeddings(t)
	ctx := context.Background()
	repoPath := newTrackedTestRepo(t, map[string]string{
		"go.mod":  "module example.test/lostack\n\ngo 1.27\n",
		"main.go": "package lostack\n\nfunc Value() int { return 1 }\n",
	})
	initial, err := doIngest(ctx, client, repoPath, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte("package lostack\n\nfunc Value() int { return 2 }\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	faultEnabled.Store(true)
	_, err = doIngest(ctx, client, repoPath, ingestOpts{}, auditNoProgress)
	faultEnabled.Store(false)
	if err == nil {
		t.Fatal("fixture did not surface the lost publication acknowledgement")
	}
	if !publicationCommitted.Load() {
		t.Fatal("fixture did not commit publication before dropping its response")
	}
	physical := auditPhysicalSnapshotID(t, client, initial.Repo, initial.SnapshotID)
	if physical == initial.SnapshotID {
		t.Fatalf("published snapshot still owns mutable children directly: physical=%q", physical)
	}
	row, err := client.Single().ReadRow(ctx, "Snapshots", spanner.Key{initial.Repo, physical}, []string{"kind"})
	if err != nil {
		t.Fatalf("cleanup removed the committed storage generation row: %v", err)
	}
	var kind string
	if err := row.Columns(&kind); err != nil || kind != "staging" {
		t.Fatalf("committed storage generation kind=%q err=%v, want private staging", kind, err)
	}
	if visible, err := snapshotExists(ctx, client, initial.Repo, physical); err != nil || visible {
		t.Fatalf("committed storage generation became independently public: visible=%v err=%v", visible, err)
	}
	files, err := loadSnapshotFiles(ctx, client, initial.Repo, initial.SnapshotID)
	if err != nil {
		t.Fatalf("load generation after lost publication acknowledgement: %v", err)
	}
	want := []byte("package lostack\n\nfunc Value() int { return 2 }\n")
	if got := auditFile(t, files, "main.go"); !bytes.Equal(got, want) {
		t.Fatalf("cleanup removed the committed generation after lost acknowledgement: got %q want %q", got, want)
	}
	chunks, err := loadGoFileChunks(ctx, client, initial.Repo, initial.SnapshotID)
	if err != nil || len(chunks["main.go"]) == 0 {
		t.Fatalf("cleanup removed published structural chunks after lost acknowledgement: chunks=%+v err=%v", chunks, err)
	}
}

func TestAuditUnpinnedResolutionIgnoresPrivateStagingSnapshots(t *testing.T) {
	client := newAuditEmulatorInterceptedDB(t, auditPassInterceptor)
	ctx := context.Background()
	repo := "resolution-repo"
	publicSnapshot := strings.Repeat("a", 40)
	stagingSnapshot := "private-staging-snapshot"
	publicFQN := "example.test/resolution.Visible"
	stagingOnlyFQN := "example.test/resolution.Private"
	now := time.Now().UTC()
	if err := writeSnapshot(ctx, client, repo, publicSnapshot, publicSnapshot, publicSnapshot, now, "main", nil, ingestOpts{kind: "ingested"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := writeSnapshot(ctx, client, repo, stagingSnapshot, stagingSnapshot, strings.Repeat("b", 40), now.Add(time.Hour), "main", nil, ingestOpts{kind: "staging"}, 2); err != nil {
		t.Fatal(err)
	}
	auditInsertMember(t, client, repo, publicSnapshot, "public-id", publicFQN, nil)
	auditInsertMember(t, client, repo, stagingSnapshot, "staging-id", publicFQN, nil)
	auditInsertMember(t, client, repo, stagingSnapshot, "staging-only-id", stagingOnlyFQN, nil)

	_, gotRepo, gotSnapshot, err := resolveFQNInRepo(ctx, client, publicFQN, "", "")
	if err != nil {
		t.Fatalf("resolve published symbol: %v", err)
	}
	if gotRepo != repo || gotSnapshot != publicSnapshot {
		t.Errorf("unpinned public resolution selected %s@%s, want %s@%s", gotRepo, gotSnapshot, repo, publicSnapshot)
	}
	if _, _, _, err := resolveFQNInRepo(ctx, client, stagingOnlyFQN, "", ""); err == nil {
		t.Fatal("unpinned resolution exposed a symbol that exists only in a private staging snapshot")
	}
}

func TestAuditConcurrentBlobRepairCannotDeleteNewlyPublishedIdenticalContent(t *testing.T) {
	type writerContextKeyType struct{}
	writerContextKey := writerContextKeyType{}
	mutationReached := make(chan struct{}, 1)
	releaseMutation := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseMutation) }) }
	defer release()
	var raceEnabled atomic.Bool
	var writerPaused atomic.Bool
	var failWriterMutation atomic.Bool
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		markedWriter, _ := ctx.Value(writerContextKey).(bool)
		if raceEnabled.Load() && markedWriter && (auditCommitDeletesTable(req, "FileContents") || auditCommitWritesTable(req, "FileContents")) {
			if writerPaused.CompareAndSwap(false, true) {
				mutationReached <- struct{}{}
				<-releaseMutation
			}
			if failWriterMutation.Load() && auditCommitWritesTable(req, "FileContents") {
				return status.Error(codes.PermissionDenied, "audit injected marked-writer content failure")
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	client := newAuditEmulatorInterceptedDB(t, interceptor)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("shared-content-"), fileChunkSize/15+2)
	sha := sha256Hex(string(payload))
	repo := newTrackedTestRepo(t, map[string]string{"payload.txt": string(payload)})
	auditInsertSnapshot(t, client, "published-repo", "published-snapshot")
	auditInsertSnapshot(t, client, "repair-repo", "repair-snapshot")
	applyTestMutations(t, client, spanner.InsertOrUpdate("FileContents",
		[]string{"content_sha", "chunk_index", "bytes", "created_at"},
		[]any{sha, int64(0), payload[:fileChunkSize], spanner.CommitTimestamp}))

	racerDone := make(chan error, 1)
	raceEnabled.Store(true)
	writerCtx := context.WithValue(ctx, writerContextKey, true)
	go func() {
		racerDone <- captureFiles(writerCtx, client, repo, "repair-repo", "repair-snapshot", auditNoProgress)
	}()
	select {
	case <-mutationReached:
	case err := <-racerDone:
		t.Fatalf("repair writer exited before the content-mutation barrier: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("repair writer did not reach the content-mutation barrier")
	}
	var complete []*spanner.Mutation
	for index, offset := int64(0), 0; offset < len(payload); index, offset = index+1, offset+fileChunkSize {
		end := offset + fileChunkSize
		if end > len(payload) {
			end = len(payload)
		}
		complete = append(complete, spanner.InsertOrUpdate("FileContents",
			[]string{"content_sha", "chunk_index", "bytes", "created_at"},
			[]any{sha, index, payload[offset:end], spanner.CommitTimestamp}))
	}
	complete = append(complete, spanner.InsertOrUpdate("SnapshotFiles",
		[]string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"},
		[]any{"published-repo", "published-snapshot", "payload.txt", sha, false, int64(0)}))
	if _, err := client.Apply(ctx, complete); err != nil {
		t.Fatalf("publish complete identical content while repair is paused: %v", err)
	}
	failWriterMutation.Store(true)
	release()
	var repairErr error
	select {
	case repairErr = <-racerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("repair writer did not return after the content-mutation barrier was released")
	}
	if repairErr == nil {
		t.Fatal("repair writer succeeded despite injected content-mutation failure")
	}
	raceEnabled.Store(false)
	files, err := loadSnapshotFiles(ctx, client, "published-repo", "published-snapshot")
	if err != nil {
		t.Fatalf("failed repair corrupted another writer's published content: %v", err)
	}
	if got := auditFile(t, files, "payload.txt"); !bytes.Equal(got, payload) {
		t.Fatalf("published identical content changed after concurrent repair: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestAuditLargeSnapshotPublicationIsBoundedAndAtomicallyVisible(t *testing.T) {
	const memberCount = 80001
	const mutationLimit = 80000
	repo := "large-publication-repo"
	publicSnapshot := strings.Repeat("c", 40)
	stagingSnapshot := "large-private-staging"
	var client *spanner.Client
	var publishing atomic.Bool
	var maxMutations atomic.Int64
	var observedMu sync.Mutex
	var observedCounts []int64
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		commit, isCommit := req.(*spannerpb.CommitRequest)
		if !publishing.Load() || !isCommit {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		count := int64(len(commit.GetMutations()))
		for {
			previous := maxMutations.Load()
			if count <= previous || maxMutations.CompareAndSwap(previous, count) {
				break
			}
		}
		if count > mutationLimit {
			return status.Errorf(codes.ResourceExhausted, "audit commit has %d mutations, limit %d", count, mutationLimit)
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			visible := auditVisibleSnapshotMemberCount(t, client, repo, publicSnapshot)
			observedMu.Lock()
			observedCounts = append(observedCounts, visible)
			observedMu.Unlock()
		}
		return err
	}
	client = newAuditEmulatorInterceptedDB(t, interceptor)
	ctx := context.Background()
	auditInsertSnapshot(t, client, repo, publicSnapshot)
	auditInsertMember(t, client, repo, publicSnapshot, "old-id", "example.test/large.Old", nil)
	if err := writeSnapshot(ctx, client, repo, stagingSnapshot, stagingSnapshot, strings.Repeat("d", 40), time.Now(), "main", nil, ingestOpts{kind: "staging"}, memberCount); err != nil {
		t.Fatal(err)
	}
	members := make([]Symbol, memberCount)
	for i := range members {
		members[i] = Symbol{
			ID:         fmt.Sprintf("large-%05d", i),
			FQN:        fmt.Sprintf("example.test/large.Symbol%05d", i),
			Kind:       "func",
			FilePath:   "large.go",
			LineStart:  i + 1,
			LineEnd:    i + 1,
			Repo:       repo,
			SnapshotID: stagingSnapshot,
		}
	}
	if err := writeSnapshotMembers(ctx, client, members, auditNoProgress); err != nil {
		t.Fatalf("seed large staged snapshot: %v", err)
	}

	publishing.Store(true)
	err := publishStagedSnapshot(ctx, client, repo, stagingSnapshot, publicSnapshot, "ingested")
	publishing.Store(false)
	if err != nil {
		t.Errorf("bounded large snapshot publication failed: %v", err)
	}
	if got := maxMutations.Load(); got != 1 {
		t.Errorf("publication sent %d mutations, want one bounded pointer mutation", got)
	}
	observedMu.Lock()
	for _, count := range observedCounts {
		if count != 1 && count != memberCount {
			t.Errorf("public snapshot exposed partial graph with %d/%d members", count, memberCount)
		}
	}
	observedMu.Unlock()
	if err == nil {
		if got := auditPhysicalSnapshotID(t, client, repo, publicSnapshot); got != stagingSnapshot {
			t.Errorf("large logical snapshot points to %q, want immutable generation %q", got, stagingSnapshot)
		}
		if got := auditVisibleSnapshotMemberCount(t, client, repo, publicSnapshot); got != memberCount {
			t.Errorf("published member count = %d, want %d", got, memberCount)
		}
	} else if got := auditVisibleSnapshotMemberCount(t, client, repo, publicSnapshot); got != 1 {
		t.Errorf("failed publication changed prior public snapshot to %d members, want 1", got)
	}
}

func TestAuditC4ZeroEdgeReingestClearsStaleVersionEdges(t *testing.T) {
	client := newAuditEmulatorInterceptedDB(t, auditPassInterceptor)
	auditInstallEmbeddings(t)
	repoPath := newTrackedTestRepo(t, map[string]string{
		"go.mod":  "module example.test/leaf\n\ngo 1.27\n",
		"leaf.go": "package leaf\n\nfunc Leaf() int { return 1 }\n",
	})
	ctx := context.Background()
	stats, err := doIngest(ctx, client, repoPath, ingestOpts{}, auditNoProgress)
	if err != nil {
		t.Fatal(err)
	}
	physicalSnapshot := auditPhysicalSnapshotID(t, client, stats.Repo, stats.SnapshotID)
	symbolID := uuidFromFQN("example.test/leaf.Leaf")
	var contentHash string
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL:    "SELECT content_hash FROM SnapshotMembers WHERE repo=@repo AND snapshot_id=@snap AND symbol_id=@sid",
		Params: map[string]any{"repo": stats.Repo, "snap": physicalSnapshot, "sid": symbolID},
	})
	row, err := iter.Next()
	iter.Stop()
	if err != nil || row.Columns(&contentHash) != nil {
		t.Fatalf("load leaf version hash: %v", err)
	}
	staleID := uuidFromFQN("example.test/leaf.Stale")
	if err := writeSymbolEdges(ctx, client, []Edge{{Repo: stats.Repo, SnapshotID: physicalSnapshot, SrcID: symbolID, SrcContentHash: contentHash, DstID: staleID, Kind: "CALLS"}}, auditNoProgress); err != nil {
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

func TestAuditTrackedFileThroughParentSymlinkIsRejectedBeforeEmbedding(t *testing.T) {
	client := newAuditEmulatorInterceptedDB(t, auditPassInterceptor)
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod":        "module example.test/parentlink\n\ngo 1.27\n",
		"dir/source.go": "package dir\n\nfunc Local() string { return \"local\" }\n",
	})
	external := t.TempDir()
	marker := "AUDIT_EXTERNAL_PARENT_SYMLINK_SECRET"
	if err := os.WriteFile(filepath.Join(external, "source.go"), []byte("package dir\n\nfunc External() string { return \""+marker+"\" }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(repo, "dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(repo, "dir")); err != nil {
		t.Fatal(err)
	}
	previousRequest := requestEmbeddings
	var exposed atomic.Bool
	requestEmbeddings = func(_ context.Context, texts []string, _ string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			if strings.Contains(text, marker) {
				exposed.Store(true)
			}
			vectors[i] = []float32{1, float32(i + 1)}
		}
		return vectors, nil
	}
	t.Cleanup(func() { requestEmbeddings = previousRequest })
	_, err := doIngest(context.Background(), client, repo, ingestOpts{}, auditNoProgress)
	if exposed.Load() {
		t.Fatal("tracked file reached through a parent symlink was sent to the embedding boundary")
	}
	if auditDatabaseContainsMarker(t, client, []byte(marker)) {
		t.Fatal("tracked file reached through a parent symlink was persisted from outside the checkout")
	}
	if err == nil {
		t.Fatal("ingest accepted a tracked file reached through a parent symlink outside the checkout")
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "symlink") && !strings.Contains(lower, "outside") && !strings.Contains(lower, "escape") {
		t.Fatalf("parent symlink refusal lacks boundary reason: %v", err)
	}
}
