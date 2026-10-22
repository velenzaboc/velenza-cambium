package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeReplayService struct {
	mu          sync.Mutex
	exists       map[string]bool
	snapshotErr  error
	ingestErr    error
	ingestErrors []error
	ingested     []string
	closed       bool
}

func (s *fakeReplayService) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *fakeReplayService) SnapshotExists(_ context.Context, _, snapshot string) (bool, error) {
	if s.snapshotErr != nil {
		return false, s.snapshotErr
	}
	return s.exists[snapshot], nil
}

func (s *fakeReplayService) Ingest(_ context.Context, repoPath string) (*IngestStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingested = append(s.ingested, repoPath)
	call := len(s.ingested) - 1
	if call < len(s.ingestErrors) && s.ingestErrors[call] != nil {
		return nil, s.ingestErrors[call]
	}
	if s.ingestErr != nil {
		return nil, s.ingestErr
	}
	return &IngestStats{Symbols: 3, Edges: 2, CacheHits: 1, UniqueHashes: 2}, nil
}

func installReplayService(t *testing.T, service replayService, err error) {
	t.Helper()
	previous := newReplayService
	newReplayService = func(context.Context) (replayService, error) { return service, err }
	t.Cleanup(func() { newReplayService = previous })
}

func replayFixtureRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	mustRunGit(t, repo, "init", "-b", "main")
	mustRunGit(t, repo, "config", "user.email", "coverage@example.invalid")
	mustRunGit(t, repo, "config", "user.name", "Coverage Test")
	mustWriteTestFile(t, filepath.Join(repo, "main.go"), "package sample\nconst Version = 1\n")
	mustRunGit(t, repo, "add", "main.go")
	mustRunGit(t, repo, "commit", "-m", "first")
	mustWriteTestFile(t, filepath.Join(repo, "main.go"), "package sample\nconst Version = 2\n")
	mustRunGit(t, repo, "add", "main.go")
	mustRunGit(t, repo, "commit", "-m", "second")
	return repo
}

func TestReplayCommandUsesRealSelectionAndWorktrees(t *testing.T) {
	ctx := context.Background()
	if err := cmdReplay(ctx, nil); err == nil || !strings.Contains(err.Error(), "needs one arg") {
		t.Fatalf("missing argument error = %v", err)
	}
	repo := replayFixtureRepo(t)
	service := &fakeReplayService{exists: map[string]bool{}}
	installReplayService(t, service, nil)

	if err := cmdReplay(ctx, []string{"--shard=not-a-shard", repo}); err == nil || !strings.Contains(err.Error(), "shard must") {
		t.Fatalf("invalid shard error = %v", err)
	}
	if err := cmdReplay(ctx, []string{"--shard=1/2", "--workers=1", repo}); err != nil {
		t.Fatalf("empty shard replay: %v", err)
	}
	if err := cmdReplay(ctx, []string{"--workers=1", "--min-free-gb=999999", repo}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if !service.closed || len(service.ingested) != 3 {
		t.Fatalf("service lifecycle/ingests = closed:%v paths:%v", service.closed, service.ingested)
	}
	for _, worktree := range service.ingested {
		if !strings.Contains(filepath.Base(worktree), "cg-replay-") {
			t.Fatalf("ingest did not receive replay worktree: %q", worktree)
		}
	}
}

func TestReplayCommandRecordsSnapshotAndIngestFailures(t *testing.T) {
	ctx, repo := context.Background(), replayFixtureRepo(t)
	for _, tc := range []struct {
		name    string
		service *fakeReplayService
	}{
		{"snapshot failure", &fakeReplayService{exists: map[string]bool{}, snapshotErr: errors.New("snapshot unavailable")}},
		{"ingest failure", &fakeReplayService{exists: map[string]bool{}, ingestErr: errors.New("ingest unavailable")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installReplayService(t, tc.service, nil)
			if err := cmdReplay(ctx, []string{"--workers=1", repo}); err == nil {
				t.Fatal("replay returned success after worker failure")
			}
			if !tc.service.closed {
				t.Fatal("replay did not close its service")
			}
		})
	}
	installReplayService(t, nil, errors.New("credentials unavailable"))
	if err := cmdReplay(ctx, []string{repo}); err == nil || !strings.Contains(err.Error(), "credentials unavailable") {
		t.Fatalf("service construction error = %v", err)
	}
}

func TestUpdateCommandSelectsOnlyMissingTips(t *testing.T) {
	ctx, repo := context.Background(), replayFixtureRepo(t)
	if err := cmdUpdate(ctx, []string{filepath.Join(repo, "missing")}); err == nil {
		t.Fatal("update accepted a non-repository path")
	}
	tip := strings.TrimSpace(mustRunGit(t, repo, "rev-parse", "HEAD"))
	current := &fakeReplayService{exists: map[string]bool{tip: true}}
	installReplayService(t, current, nil)
	if err := cmdUpdate(ctx, []string{repo}); err != nil {
		t.Fatalf("current update: %v", err)
	}
	if len(current.ingested) != 0 || !current.closed {
		t.Fatalf("current update unexpectedly ingested or leaked service: %+v", current)
	}

	missing := &fakeReplayService{exists: map[string]bool{}}
	installReplayService(t, missing, nil)
	if err := cmdUpdate(ctx, []string{repo}); err != nil {
		t.Fatalf("missing update: %v", err)
	}
	if len(missing.ingested) != 1 || !missing.closed || !strings.Contains(filepath.Base(missing.ingested[0]), "cg-update-") {
		t.Fatalf("missing update did not ingest exactly the detached update worktree: %+v", missing)
	}

	if err := cmdUpdate(ctx, nil); err == nil || !strings.Contains(err.Error(), "needs one arg") {
		t.Fatalf("missing update argument error = %v", err)
	}
	broken := &fakeReplayService{exists: map[string]bool{}, snapshotErr: errors.New("read failed")}
	installReplayService(t, broken, nil)
	if err := cmdUpdate(ctx, []string{repo}); err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("snapshot error = %v", err)
	}
	ingestFailure := &fakeReplayService{exists: map[string]bool{}, ingestErr: errors.New("ingest failed")}
	installReplayService(t, ingestFailure, nil)
	if err := cmdUpdate(ctx, []string{repo}); err == nil {
		t.Fatal("update returned success after ingest failure")
	}
	if len(ingestFailure.ingested) != 1 || !ingestFailure.closed {
		t.Fatalf("failed update did not attempt exactly one cleanup-bounded ingest: %+v", ingestFailure)
	}
	installReplayService(t, nil, errors.New("credentials unavailable"))
	if err := cmdUpdate(ctx, []string{repo}); err == nil || !strings.Contains(err.Error(), "credentials unavailable") {
		t.Fatalf("update service construction error = %v", err)
	}
}

type fakeDatabaseAdmin struct {
	closed      bool
	dropErr     error
	createErr   error
	updateErr   error
	createWait  error
	updateWait  error
	dropRequest *databasepb.DropDatabaseRequest
	createReq   *databasepb.CreateDatabaseRequest
	updateReq   *databasepb.UpdateDatabaseDdlRequest
}

func installDatabaseAdmin(t *testing.T, admin *fakeDatabaseAdmin, err error) {
	t.Helper()
	previousNew, previousClose := newCodeGraphDatabaseAdmin, closeCodeGraphDatabaseAdmin
	previousDrop, previousCreate, previousUpdate := dropCodeGraphDatabase, createCodeGraphDatabase, updateCodeGraphDatabaseDDL
	newCodeGraphDatabaseAdmin = func(context.Context) (*database.DatabaseAdminClient, error) {
		return &database.DatabaseAdminClient{}, err
	}
	closeCodeGraphDatabaseAdmin = func(*database.DatabaseAdminClient) error { admin.closed = true; return nil }
	dropCodeGraphDatabase = func(_ context.Context, _ *database.DatabaseAdminClient, req *databasepb.DropDatabaseRequest) error {
		admin.dropRequest = req
		return admin.dropErr
	}
	createCodeGraphDatabase = func(_ context.Context, _ *database.DatabaseAdminClient, req *databasepb.CreateDatabaseRequest) (func(context.Context) error, error) {
		admin.createReq = req
		if admin.createErr != nil {
			return nil, admin.createErr
		}
		return func(context.Context) error { return admin.createWait }, nil
	}
	updateCodeGraphDatabaseDDL = func(_ context.Context, _ *database.DatabaseAdminClient, req *databasepb.UpdateDatabaseDdlRequest) (func(context.Context) error, error) {
		admin.updateReq = req
		if admin.updateErr != nil {
			return nil, admin.updateErr
		}
		return func(context.Context) error { return admin.updateWait }, nil
	}
	t.Cleanup(func() {
		newCodeGraphDatabaseAdmin, closeCodeGraphDatabaseAdmin = previousNew, previousClose
		dropCodeGraphDatabase, createCodeGraphDatabase, updateCodeGraphDatabaseDDL = previousDrop, previousCreate, previousUpdate
	})
}

func TestCreateDatabaseAdmitsOnlyParsedDDL(t *testing.T) {
	ctx := context.Background()
	if err := cmdCreateDB(ctx, []string{"--wrong"}); err == nil || !strings.Contains(err.Error(), "unknown arg") {
		t.Fatalf("unknown create argument error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(path, []byte("-- comment\nCREATE TABLE Things (id INT64) PRIMARY KEY (id);\n\nALTER TABLE Things ADD COLUMN note STRING(MAX);"), 0o600); err != nil {
		t.Fatal(err)
	}
	admin := &fakeDatabaseAdmin{dropErr: status.Error(codes.NotFound, "absent")}
	installDatabaseAdmin(t, admin, nil)
	if err := cmdCreateDB(ctx, []string{"--drop", path}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if !admin.closed || admin.dropRequest.GetDatabase() != spannerDB {
		t.Fatalf("drop lifecycle/request = %+v", admin)
	}
	if admin.createReq.GetParent() != spannerInstance || admin.createReq.GetCreateStatement() != "CREATE DATABASE `code-graph`" {
		t.Fatalf("create request identity = %+v", admin.createReq)
	}
	if want := []string{"CREATE TABLE Things (id INT64) PRIMARY KEY (id)", "ALTER TABLE Things ADD COLUMN note STRING(MAX)"}; !reflect.DeepEqual(admin.createReq.GetExtraStatements(), want) {
		t.Fatalf("extra statements = %#v, want %#v", admin.createReq.GetExtraStatements(), want)
	}

	for _, tc := range []struct {
		name  string
		admin *fakeDatabaseAdmin
		want  string
	}{
		{"drop failure", &fakeDatabaseAdmin{dropErr: errors.New("drop unavailable")}, "drop:"},
		{"create failure", &fakeDatabaseAdmin{createErr: errors.New("create unavailable")}, "create:"},
		{"create wait failure", &fakeDatabaseAdmin{createWait: errors.New("wait unavailable")}, "create wait:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installDatabaseAdmin(t, tc.admin, nil)
			args := []string{path}
			if tc.name == "drop failure" {
				args = []string{"--drop", path}
			}
			if err := cmdCreateDB(ctx, args); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	installDatabaseAdmin(t, &fakeDatabaseAdmin{}, errors.New("admin unavailable"))
	if err := cmdCreateDB(ctx, []string{path}); err == nil || !strings.Contains(err.Error(), "admin unavailable") {
		t.Fatalf("admin construction error = %v", err)
	}
}

func TestMigrateAdmitsOnlyNonemptyDDL(t *testing.T) {
	ctx := context.Background()
	if err := cmdMigrate(ctx, nil); err == nil || !strings.Contains(err.Error(), "needs one arg") {
		t.Fatalf("missing migration argument error = %v", err)
	}
	empty := filepath.Join(t.TempDir(), "empty.sql")
	if err := os.WriteFile(empty, []byte("-- comment only\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmdMigrate(ctx, []string{empty}); err == nil || !strings.Contains(err.Error(), "no statements") {
		t.Fatalf("empty DDL error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "migration.sql")
	if err := os.WriteFile(path, []byte("-- ignored\nALTER TABLE Things ADD COLUMN score INT64;\nCREATE INDEX ThingsByScore ON Things(score);"), 0o600); err != nil {
		t.Fatal(err)
	}
	admin := &fakeDatabaseAdmin{}
	installDatabaseAdmin(t, admin, nil)
	if err := cmdMigrate(ctx, []string{path}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if !admin.closed || admin.updateReq.GetDatabase() != spannerDB {
		t.Fatalf("migration lifecycle/request = %+v", admin)
	}
	if want := []string{"ALTER TABLE Things ADD COLUMN score INT64", "CREATE INDEX ThingsByScore ON Things(score)"}; !reflect.DeepEqual(admin.updateReq.GetStatements(), want) {
		t.Fatalf("migration statements = %#v, want %#v", admin.updateReq.GetStatements(), want)
	}
	for _, tc := range []struct {
		name  string
		admin *fakeDatabaseAdmin
		want  string
	}{
		{"update failure", &fakeDatabaseAdmin{updateErr: errors.New("update unavailable")}, "update ddl:"},
		{"wait failure", &fakeDatabaseAdmin{updateWait: errors.New("wait unavailable")}, "ddl wait:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installDatabaseAdmin(t, tc.admin, nil)
			if err := cmdMigrate(ctx, []string{path}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSplitDDLRemovesOnlyFullLineComments(t *testing.T) {
	got := splitDDL("  -- one\nCREATE TABLE T (note STRING(MAX));\n-- two\nALTER TABLE T ADD COLUMN score INT64;\n")
	if want := []string{"CREATE TABLE T (note STRING(MAX))", "ALTER TABLE T ADD COLUMN score INT64"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("split DDL = %#v, want %#v", got, want)
	}
}

func TestDefaultAdminSeamsPropagateUnavailableTransport(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	client, err := database.NewDatabaseAdminClient(context.Background(), option.WithEndpoint(endpoint), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("construct isolated client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := dropCodeGraphDatabase(ctx, client, &databasepb.DropDatabaseRequest{Database: "projects/p/instances/i/databases/d"}); err == nil {
		t.Fatal("drop seam accepted unavailable transport")
	}
	if _, err := createCodeGraphDatabase(ctx, client, &databasepb.CreateDatabaseRequest{Parent: "projects/p/instances/i", CreateStatement: "CREATE DATABASE `d`"}); err == nil {
		t.Fatal("create seam accepted unavailable transport")
	}
	if _, err := updateCodeGraphDatabaseDDL(ctx, client, &databasepb.UpdateDatabaseDdlRequest{Database: "projects/p/instances/i/databases/d", Statements: []string{"CREATE TABLE T (id INT64) PRIMARY KEY (id)"}}); err == nil {
		t.Fatal("update seam accepted unavailable transport")
	}
	if err := closeCodeGraphDatabaseAdmin(client); err != nil {
		t.Fatalf("close seam: %v", err)
	}
}
