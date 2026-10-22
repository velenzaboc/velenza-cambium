package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func auditFile(t *testing.T, files []snapshotFileRow, path string) []byte {
	t.Helper()
	for _, file := range files {
		if file.path == path {
			return file.bytes
		}
	}
	return nil
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
	if _, err := doGetInRepo(context.Background(), client, fqn, "z-repo", snapshot); err == nil {
		t.Fatal("pinned z-repo query returned a symbol that exists only in a-repo")
	}

	auditInsertMember(t, client, "z-repo", snapshot, id, fqn, hashZ)
	installCommandClient(t, client)
	var getErr error
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

func TestAuditC6SameLineInitDeclarationsRemainDistinctInSymbolsAndChunks(t *testing.T) {
	const source = "package multiinit\n\nfunc init() {}; func init() {}\n"
	repo := newTrackedTestRepo(t, map[string]string{
		"go.mod": "module example.test/multiinit\n\ngo 1.27\n",
		"same.go": source,
	})
	syms, _, err := walkRepo(repo, "multiinit", strings.Repeat("e", 40), auditNoProgress)
	if err != nil {
		t.Fatal(err)
	}
	symbolIDs := map[string]struct{}{}
	for _, symbol := range syms {
		if symbol.Kind == "func" && strings.HasPrefix(strings.TrimSpace(symbol.Signature), "func init(") {
			symbolIDs[symbol.ID] = struct{}{}
		}
	}
	if len(symbolIDs) != 2 {
		t.Errorf("two same-line init declarations produced %d symbol identities", len(symbolIDs))
	}

	chunks := decomposeGoFile("same.go", []byte(source), "example.test/multiinit")
	chunkFQNs := map[string]struct{}{}
	declarations := 0
	for _, chunk := range chunks {
		if chunk.kind != "decl" || !strings.Contains(chunk.text, "func init()") {
			continue
		}
		declarations++
		for _, fqn := range chunk.fqns {
			chunkFQNs[fqn] = struct{}{}
		}
	}
	if declarations != 2 || len(chunkFQNs) != 2 {
		t.Errorf("same-line init chunks = %d declarations with %d distinct FQNs, want 2 and 2", declarations, len(chunkFQNs))
	}
}

func TestAuditC6CoordinateQualifiedInitRemainsEntrypoint(t *testing.T) {
	initFQNs := []string{
		"example.test/multiinit.init@a.go:2:6",
		"example.test/multiinit.init@same.go:3:17",
	}
	for _, fqn := range initFQNs {
		if !isEntrypoint(fqn) {
			t.Errorf("coordinate-qualified init %q was classified as an ordinary production function", fqn)
		}
	}
	if isEntrypoint("example.test/multiinit.Initialize") {
		t.Fatal("ordinary Initialize function was classified as an entrypoint")
	}

	client := newCodeGraphTestDB(t)
	repo, snapshot := "multiinit", strings.Repeat("6", 40)
	auditInsertSnapshot(t, client, repo, snapshot)
	for _, fqn := range append(initFQNs, "example.test/multiinit.Initialize") {
		auditInsertMember(t, client, repo, snapshot, uuidFromFQN(fqn), fqn, nil)
	}
	rows, err := doListSymbols(context.Background(), client, repo, snapshot, "func", 0)
	if err != nil {
		t.Fatal(err)
	}
	orphans := map[string]bool{}
	for _, row := range rows {
		orphans[row.FQN] = row.Orphan
	}
	for _, fqn := range initFQNs {
		if orphans[fqn] {
			t.Errorf("list-symbols reported coordinate-qualified init %q as an orphan", fqn)
		}
	}
	if !orphans["example.test/multiinit.Initialize"] {
		t.Fatal("list-symbols stopped reporting an ordinary uncalled function as an orphan")
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

func UnsafeNegativeConvertedToUint(n int) uint64 {
	if n < 64 {
		return 1 << uint(n)
	}
	return 0
}

func SafeUnsigned(n uint) uint64 {
	if n < 64 {
		return 1 << n
	}
	return 0
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
	for _, name := range []string{"UnsafeUnrelated", "UnsafeNonDominating", "UnsafeNegativeConvertedToUint"} {
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
		if strings.HasSuffix(fqn, ".SafeUnsigned") {
			t.Errorf("unsigned upper bound was not honored: %s", fqn)
		}
	}
}

func TestAuditC1LegacyInarchLeaseSupportsJSONAndTOMLRepresentations(t *testing.T) {
	head := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name       string
		content    string
		wantBranch string
	}{
		{
			name:       "historical JSON",
			content:    `{"head_changeset_id":"` + head + `","bookmark":"legacy"}`,
			wantBranch: "legacy",
		},
		{
			name:       "historical JSON tolerates unrelated mixed-type metadata",
			content:    `{"head_changeset_id":"` + head + `","bookmark":"mixed","epoch":7,"metadata":{"source":"legacy"}}`,
			wantBranch: "mixed",
		},
		{
			name:       "historical JSON preserves equals sign inside a string",
			content:    `{"head_changeset_id":"` + head + `","bookmark":"topic=x"}`,
			wantBranch: "topic=x",
		},
		{
			name:       "native TOML in legacy filename",
			content:    "head_changeset_id = \"" + head + "\"\nbookmark = \"native\"\n",
			wantBranch: "native",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			inarchDir := filepath.Join(repo, ".inarch")
			if err := os.MkdirAll(inarchDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(inarchDir, "jit_lease.json"), []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			gotHead, err := inarchHead(repo)
			if err != nil || gotHead != head {
				t.Errorf("inarchHead = %q, %v; want %q", gotHead, err, head)
			}
			if got := inarchBranch(repo); got != tc.wantBranch {
				t.Errorf("inarchBranch = %q, want %q", got, tc.wantBranch)
			}
		})
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
