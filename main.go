// code-graph: ingest Go source into a Spanner property graph with Vertex embeddings.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const (
	gcpProject    = ""
	gcpRegion     = "us-central1"
	embedModel    = "gemini-embedding-2-preview"
	embedDims     = 3072
	embedBatch    = 100
	spannerDB     = "projects//instances//databases/code-graph"
	bodyMaxBytes  = 50_000
	embedMaxChars = 6_000
	mutationBatch = 200
)

var commandSpannerClient = newSpannerClient

type Symbol struct {
	ID          string
	Repo        string
	SnapshotID  string
	FQN         string
	Kind        string
	FilePath    string
	LineStart   int
	LineEnd     int
	Signature   string
	DocComment  string
	Body        string
	ContentHash string    // SHA-256 of embed text; "" for symbols with no embedding (packages, files)
	Embedding   []float32 // populated only for cache misses needing a fresh Vertex call
	Annotations []string  // //volt: annotation lines found inside this symbol (text after "//volt:")
}

type Edge struct {
	Repo           string
	SnapshotID     string
	SrcID          string
	SrcContentHash string // "" when the source is a package/file (structural edge)
	DstID          string
	Kind           string
}

var initializeCommandCredentials = initializeGoogleCredentials

func main() {
	if code := runMain(context.Background(), os.Args, os.Getenv); code != 0 {
		os.Exit(code)
	}
}

func runMain(ctx context.Context, argv []string, getenv func(string) string) int {
	if len(argv) < 2 {
		usage()
		return 2
	}
	cmd := argv[1]
	args := argv[2:]
	if cmd == "arbor-info" {
		code, err := runArborInfoCLI(ctx, args, os.Stdout)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		return code
	}
	if !(cmd == "hazards" && hazardsHasLocalAnalyzeFlag(args)) {
		if err := initializeCommandCredentials(ctx, getenv); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
	}
	var err error
	switch cmd {
	case "ingest":
		err = cmdIngest(ctx, args)
	case "search":
		err = cmdSearch(ctx, args)
	case "neighbors":
		err = cmdNeighbors(ctx, args)
	case "get":
		err = cmdGet(ctx, args)
	case "mcp":
		err = cmdMCP(ctx, args)
	case "diff":
		err = cmdDiff(ctx, args)
	case "list-commits":
		err = cmdListCommits(ctx, args)
	case "use":
		err = cmdUse(ctx, args)
	case "context":
		err = cmdContext(ctx, args)
	case "symbols":
		err = cmdSymbols(ctx, args)
	case "annotations":
		err = cmdAnnotations(ctx, args)
	case "coverage":
		err = cmdCoverage(ctx, args)
	case "manifest":
		err = cmdManifest(ctx, args)
	case "verify":
		err = cmdVerify(ctx, args)
	case "serve":
		err = cmdServe(ctx, args)
	case "prune":
		err = cmdPrune(ctx, args)
	case "create-db":
		err = cmdCreateDB(ctx, args)
	case "migrate":
		err = cmdMigrate(ctx, args)
	case "lineage":
		err = cmdLineage(ctx, args)
	case "drift":
		err = cmdDrift(ctx, args)
	case "findings":
		err = cmdFindings(ctx, args)
	case "hazards":
		err = cmdHazards(ctx, args)
	case "dependents":
		err = cmdDependents(ctx, args)
	case "hypothesize":
		err = cmdHypothesize(ctx, args)
	case "replay":
		err = cmdReplay(ctx, args)
	case "update":
		err = cmdUpdate(ctx, args)
	default:
		usage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, `velenza-cambium (code-graph) — Layer 1 of the Velenza Arbor Stack (SSA/VTA/CFG code symbol graph)

usage:
  code-graph ingest    <repo-path>
  code-graph use       <repo> [<snapshot>]   (pin the current snapshot; default = newest)
  code-graph context                          (show the current snapshot)
  code-graph search    [--limit N] [--snapshot S] "<query>"
  code-graph neighbors [--depth N] [--snapshot S] <fqn>
  code-graph get       [--snapshot S] <fqn>
  code-graph symbols   [--kind K] [--match RE] [--orphans] [--limit N] [--snapshot S]
  code-graph annotations [--type T] [--spec S] [--snapshot S]
  code-graph coverage  [--untested] [--snapshot S]   (which production funcs a test reaches)
  code-graph manifest  <repo> <snapshot-id> <out-dir>     (recreate the app's files)
  code-graph verify    <repo> <snapshot-id> <original-dir> (fidelity round-trip diff)
  code-graph serve     [--addr :8080] [--oidc-audience URL]  (HTTP service: /ingest, /manifest)
  code-graph findings  [--limit N] [--snapshot S]
  code-graph hazards   [--repo-path|--analyze|--dir PATH] [--category C] [--file-prefix P] [--limit N] [--snapshot S]
  code-graph diff      <repo> <before-commit> <after-commit>
  code-graph drift     <repo> <before-commit> <after-commit>
  code-graph lineage   <fqn>
  code-graph dependents <fqn>
  code-graph hypothesize <base-snapshot> <scratch-repo-path>
  code-graph replay    [--go-only] [--every N] [--max N] <repo-path>
  code-graph update    <repo-path>
  code-graph list-commits
  code-graph prune     <repo> <snapshot-id>
  code-graph create-db [--drop] [schema.sql]
  code-graph migrate   <ddl-file>     (additive DDL — ADD COLUMN / CREATE TABLE)
  code-graph arbor-info [--json]      (emit Layer 1 Velenza Arbor Stack JSON descriptor)
  code-graph mcp       (stdio MCP server)`)
}

// ---------- ingest ----------

// ProgressFunc is the shared progress channel. CLI passes a printf impl; the
// MCP server passes one that also emits notifications/progress. Total may be
// 0 for indeterminate stages (e.g. "extracting").
type ProgressFunc func(stage string, done, total int, msg string)

// IngestStats is the structured return value from a successful ingest.
type IngestStats struct {
	Repo         string `json:"repo"`
	SnapshotID   string `json:"snapshot_id"`
	CommitSHA    string `json:"commit_sha"`
	Kind         string `json:"kind"`
	Symbols      int    `json:"symbols"`
	Edges        int    `json:"edges"`
	UniqueHashes int    `json:"unique_hashes"`
	CacheHits    int    `json:"cache_hits"`
	CacheMisses  int    `json:"cache_misses"`
}

// ingestOpts selects whether an ingest produces a normal commit snapshot or a
// hypothetical one parented to an existing snapshot (Phase 6).
type ingestOpts struct {
	kind       string // "ingested" (default) or "hypothetical"
	baseSnapID string // base snapshot for kind=="hypothetical"
	branch     string // ref this snapshot was ingested under; "" = auto-detect
}

func cmdIngest(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("ingest needs one arg: <repo-path>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	stats, err := doIngest(ctx, sp, args[0], ingestOpts{}, cliProgress())
	if err != nil {
		return err
	}
	fmt.Printf("done: repo=%s commit=%s symbols=%d edges=%d (cache: %d hits / %d misses)\n",
		stats.Repo, stats.CommitSHA, stats.Symbols, stats.Edges, stats.CacheHits, stats.CacheMisses)
	return nil
}

// cliProgress is the standard stdout progress printer for CLI commands.
func cliProgress() ProgressFunc {
	return func(stage string, done, total int, msg string) {
		if total > 0 {
			fmt.Printf("  %s %d/%d %s\n", stage, done, total, msg)
		} else if msg != "" {
			fmt.Printf("  %s: %s\n", stage, msg)
		}
	}
}

// doIngest is the shared ingest workflow used by the CLI, the MCP tool, and
// hypothesize. progress is called at each stage boundary; pass nil to disable.
func doIngest(ctx context.Context, sp *spanner.Client, repoPathRaw string, opts ingestOpts, progress ProgressFunc) (*IngestStats, error) {
	if progress == nil {
		progress = func(string, int, int, string) {}
	}
	if opts.kind == "" {
		opts.kind = "ingested"
	}
	repoPath, err := filepath.Abs(repoPathRaw)
	if err != nil {
		return nil, err
	}
	if err := validateTrackedFileBoundaries(repoPath); err != nil {
		return nil, fmt.Errorf("tracked file boundary: %w", err)
	}
	// Derive repo name from the SHARED git dir so worktrees of the same repo
	// produce the same name (otherwise diff across worktrees breaks).
	repoName, err := deriveRepoName(repoPath)
	if err != nil {
		return nil, fmt.Errorf("derive repo name: %w", err)
	}
	commitSHA, err := gitHead(repoPath)
	if err != nil {
		return nil, fmt.Errorf("git rev-parse: %w", err)
	}
	commitTime, err := gitCommitTime(repoPath, commitSHA)
	if err != nil {
		return nil, fmt.Errorf("git commit time: %w", err)
	}
	parents, err := gitParents(repoPath, commitSHA)
	if err != nil {
		return nil, fmt.Errorf("git parents: %w", err)
	}
	branch := opts.branch
	if branch == "" {
		branch = gitBranch(repoPath, commitSHA) // best-effort
	}
	// snapshot identity: an ingested snapshot IS its commit; a hypothetical one
	// gets a fresh id so it never collides with the real commit it derives from.
	snapshotID := commitSHA
	label := commitSHA
	if opts.kind == "hypothetical" {
		snapshotID = commitSHA + "-hyp-" + uuid.NewString()[:8]
		label = "hypothetical of " + opts.baseSnapID
		commitTime = time.Now()
		parents = []string{opts.baseSnapID}
	}
	stagingSnapshotID := uuid.NewString()
	progress("start", 0, 0, fmt.Sprintf("repo=%s snapshot=%s kind=%s", repoName, snapshotID, opts.kind))

	progress("walk", 0, 0, "loading packages + AST + CHA")
	syms, edges, err := walkRepo(repoPath, repoName, snapshotID, progress)
	if err != nil {
		return nil, fmt.Errorf("walk: %w", err)
	}
	// Non-Go evidence: DDL (.sql → table/column/index) and the conformance
	// manifest become first-class snapshot symbols alongside the Go symbols.
	if ddlSyms, ddlEdges, e := scanDDL(repoPath, repoName, snapshotID); e == nil {
		syms = append(syms, ddlSyms...)
		edges = append(edges, ddlEdges...)
	} else {
		progress("ddl", 0, 0, "DDL scan skipped: "+e.Error())
	}
	if mfSyms, _, e := scanManifest(repoPath, repoName, snapshotID); e == nil {
		syms = append(syms, mfSyms...)
	} else {
		progress("manifest", 0, 0, "manifest scan skipped: "+e.Error())
	}
	for i := range syms {
		if txt := buildEmbedText(syms[i]); txt != "" {
			syms[i].ContentHash = sha256Hex(txt)
		}
	}
	progress("extract", len(syms), len(syms), fmt.Sprintf("symbols=%d edges=%d", len(syms), len(edges)))

	unique := uniqueHashCount(syms)
	missing, err := selectMissingBlobs(ctx, sp, syms)
	if err != nil {
		return nil, fmt.Errorf("blob cache lookup: %w", err)
	}
	cacheHits := unique - len(missing)
	progress("cache", cacheHits, unique, fmt.Sprintf("%d cache hits, %d to embed", cacheHits, len(missing)))

	if len(missing) > 0 {
		if err := embedAndWriteBlobsWithProgress(ctx, sp, syms, missing, progress); err != nil {
			return nil, fmt.Errorf("embed misses: %w", err)
		}
	}

	// Split edges for progress reporting and write ordering. Both sets are
	// snapshot-scoped because whole-program resolution affects content sources.
	hashByID := make(map[string]string, len(syms))
	kindByID := make(map[string]string, len(syms))
	for _, s := range syms {
		hashByID[s.ID] = s.ContentHash
		kindByID[s.ID] = s.Kind
	}
	var symbolEdges, structuralEdges []Edge
	for _, e := range edges {
		e.SnapshotID = stagingSnapshotID
		srcKind := kindByID[e.SrcID]
		if h := hashByID[e.SrcID]; h != "" && srcKind != "package" && srcKind != "file" {
			e.SrcContentHash = h
			symbolEdges = append(symbolEdges, e)
		} else {
			structuralEdges = append(structuralEdges, e)
		}
	}

	stagedSyms := make([]Symbol, len(syms))
	copy(stagedSyms, syms)
	for i := range stagedSyms {
		stagedSyms[i].SnapshotID = stagingSnapshotID
	}

	// Build a complete private staging snapshot. The existing public snapshot,
	// if any, remains readable until publishStagedSnapshot atomically replaces it.
	publishedKind := opts.kind
	stagingOpts := opts
	stagingOpts.kind = "staging"
	if err := writeSnapshot(ctx, sp, repoName, stagingSnapshotID, label, commitSHA, commitTime, branch, parents, stagingOpts, distinctSymbolCount(syms)); err != nil {
		return nil, fmt.Errorf("write staging snapshot: %w", err)
	}
	stagingLive := true
	publicationAttempted := false
	defer func() {
		if stagingLive && !publicationAttempted {
			// Before publication starts, this generation cannot be visible. Once
			// a publication commit is attempted, any error may be a lost success
			// acknowledgement, so preserve the generation for public readers.
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = deleteSnapshotGenerationData(cleanupCtx, sp, repoName, stagingSnapshotID)
		}
	}()
	if err := writeSymbolVersions(ctx, sp, stagedSyms, progress); err != nil {
		return nil, fmt.Errorf("write symbol versions: %w", err)
	}
	if err := writeSnapshotMembers(ctx, sp, stagedSyms, progress); err != nil {
		return nil, fmt.Errorf("write snapshot members: %w", err)
	}
	if err := writeSnapshotSymbolEdges(ctx, sp, repoName, stagingSnapshotID, symbolEdges, progress); err != nil {
		return nil, fmt.Errorf("write symbol edges: %w", err)
	}
	if err := writeStructuralEdges(ctx, sp, structuralEdges, progress); err != nil {
		return nil, fmt.Errorf("write structural edges: %w", err)
	}
	progress("edges", len(edges), len(edges),
		fmt.Sprintf("%d structural + %d content-sourced (recomputed fresh)", len(structuralEdges), len(symbolEdges)))
	// Fidelity layer: phase A captures raw bytes of every git-tracked file (the
	// byte-exact backstop + non-Go carrier); phase B1 decomposes every .go file
	// into the structural master (GoFileChunks + DeclSource). Both additive —
	// independent of the analysis graph above.
	if err := captureFiles(ctx, sp, repoPath, repoName, stagingSnapshotID, progress); err != nil {
		return nil, fmt.Errorf("capture files: %w", err)
	}
	if err := captureGoChunks(ctx, sp, repoPath, repoName, stagingSnapshotID, progress); err != nil {
		return nil, fmt.Errorf("capture go chunks: %w", err)
	}
	publicationAttempted = true
	if err := publishStagedSnapshot(ctx, sp, repoName, stagingSnapshotID, snapshotID, publishedKind); err != nil {
		return nil, fmt.Errorf("publish snapshot: %w", err)
	}
	stagingLive = false
	// Rename/move stitching only applies to real commit history, not hypotheticals.
	if opts.kind == "ingested" {
		if err := detectAliases(ctx, sp, repoName, snapshotID, progress); err != nil {
			progress("aliases", 0, 0, "alias detection skipped: "+err.Error())
		}
	}
	progress("done", 1, 1, "ingest complete")

	return &IngestStats{
		Repo:         repoName,
		SnapshotID:   snapshotID,
		CommitSHA:    commitSHA,
		Kind:         opts.kind,
		Symbols:      len(syms),
		Edges:        len(edges),
		UniqueHashes: unique,
		CacheHits:    cacheHits,
		CacheMisses:  len(missing),
	}, nil
}

func uniqueHashCount(syms []Symbol) int {
	seen := map[string]struct{}{}
	for _, s := range syms {
		if s.ContentHash != "" {
			seen[s.ContentHash] = struct{}{}
		}
	}
	return len(seen)
}

// distinctSymbolCount counts unique symbol_ids. Snapshot metadata records the
// stored membership count even if another extractor ever emits a duplicate.
func distinctSymbolCount(syms []Symbol) int {
	seen := map[string]struct{}{}
	for _, s := range syms {
		seen[s.ID] = struct{}{}
	}
	return len(seen)
}

func gitHead(repoPath string) (string, error) {
	if !hasGitDir(repoPath) && hasInarchDir(repoPath) {
		return inarchHead(repoPath)
	}
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		if hasInarchDir(repoPath) {
			return inarchHead(repoPath)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// gitCommitTime returns the committer timestamp of a commit as a time.Time.
// This drives lineage ordering — the moment a version came into existence,
// independent of when we happened to ingest it.
func gitCommitTime(repoPath, sha string) (time.Time, error) {
	if !hasGitDir(repoPath) && hasInarchDir(repoPath) {
		return inarchCommitTime(repoPath, sha)
	}
	out, err := exec.Command("git", "-C", repoPath, "show", "-s", "--format=%cI", sha).Output()
	if err != nil {
		if hasInarchDir(repoPath) {
			return inarchCommitTime(repoPath, sha)
		}
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, strings.TrimSpace(string(out)))
}

// gitParents returns the parent commit SHAs of a commit (2+ for a merge,
// 0 for the root commit). These become Snapshots.parent_snapshot_ids — the DAG.
func gitParents(repoPath, sha string) ([]string, error) {
	if !hasGitDir(repoPath) && hasInarchDir(repoPath) {
		return inarchParents(repoPath, sha)
	}
	out, err := exec.Command("git", "-C", repoPath, "rev-list", "--parents", "-n", "1", sha).Output()
	if err != nil {
		if hasInarchDir(repoPath) {
			return inarchParents(repoPath, sha)
		}
		return nil, err
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) <= 1 {
		return nil, nil // root commit
	}
	return fields[1:], nil // fields[0] is the commit itself
}

// gitBranch is a best-effort branch label for a commit. Precision order:
//  1. a branch whose TIP is exactly this commit (`--points-at`) — precise, and
//     the common case when ingesting a branch tip;
//  2. the current branch, if the checkout is not detached;
//  3. any branch that merely contains the commit (`--contains`) — least
//     precise, since many branches can contain one commit.
func gitBranch(repoPath, sha string) string {
	if !hasGitDir(repoPath) && hasInarchDir(repoPath) {
		return inarchBranch(repoPath)
	}
	if b := firstBranch(exec.Command("git", "-C", repoPath,
		"branch", "--all", "--format=%(refname:short)", "--points-at", sha)); b != "" {
		return b
	}
	out, err := exec.Command("git", "-C", repoPath, "rev-parse", "--abbrev-ref", "HEAD").Output()
	if b := strings.TrimSpace(string(out)); err == nil && b != "" && b != "HEAD" {
		return b
	}
	if b := firstBranch(exec.Command("git", "-C", repoPath,
		"branch", "--all", "--format=%(refname:short)", "--contains", sha)); b != "" {
		return b
	}
	if hasInarchDir(repoPath) {
		return inarchBranch(repoPath)
	}
	return ""
}

// firstBranch runs a `git branch` listing command and returns the first real
// branch name — skipping git's detached-HEAD pseudo-entries, stripping the
// remotes/ and origin/ prefixes.
func firstBranch(cmd *exec.Cmd) string {
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		b := strings.TrimSpace(line)
		if b == "" || b == "HEAD" || strings.HasPrefix(b, "(") || strings.Contains(b, "detached") {
			continue
		}
		return strings.TrimPrefix(strings.TrimPrefix(b, "remotes/"), "origin/")
	}
	return ""
}

// deriveRepoName returns a stable repo identifier that's the same for any
// worktree of the same repo. Uses `git rev-parse --git-common-dir` which points
// to the shared .git directory; the parent of that is the canonical repo root.
func deriveRepoName(repoPath string) (string, error) {
	if !hasGitDir(repoPath) && hasInarchDir(repoPath) {
		return inarchDeriveRepoName(repoPath)
	}
	out, err := exec.Command("git", "-C", repoPath, "rev-parse", "--git-common-dir").Output()
	if err != nil {
		if hasInarchDir(repoPath) {
			return inarchDeriveRepoName(repoPath)
		}
		return "", err
	}
	commonDir := strings.TrimSpace(string(out))
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(repoPath, commonDir)
	}
	commonDir, _ = filepath.Abs(commonDir)
	return filepath.Base(filepath.Dir(commonDir)), nil
}

// ---------- AST walk ----------

func walkRepo(repoPath, repoName, snapshotID string, progress ProgressFunc) ([]Symbol, []Edge, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedDeps |
			packages.NeedImports | packages.NeedCompiledGoFiles | packages.NeedModule,
		Dir: repoPath,
		// Tests:true loads _test.go files so test functions and the edges
		// from them into production code become part of the graph — that is
		// what makes "is this symbol exercised by a test" answerable.
		Tests: true,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, nil, err
	}
	if packages.PrintErrors(pkgs) > 0 {
		// non-fatal: continue with what loaded
		fmt.Fprintln(os.Stderr, "(continuing despite package load errors)")
	}

	// With Tests:true, packages.Load returns several variants per testable
	// package: "foo" (production only), "foo [foo.test]" (production + the
	// in-package _test.go files), "foo_test [foo.test]" (the external
	// package foo_test test files), and the synthesized "foo.test" main.
	// Choose exactly one package per PkgPath — the test-augmented variant
	// when it exists — so every physical .go file is walked exactly once and
	// production code is never double-ingested. Sorted for deterministic
	// symbol order across ingests.
	chosenByPath := map[string]*packages.Package{}
	for _, p := range pkgs {
		if p.Types == nil || strings.HasSuffix(p.PkgPath, ".test") {
			continue // skip the synthesized test-main package
		}
		cur, ok := chosenByPath[p.PkgPath]
		isTestVariant := strings.Contains(p.ID, ".test]")
		if !ok || (isTestVariant && !strings.Contains(cur.ID, ".test]")) {
			chosenByPath[p.PkgPath] = p
		}
	}
	chosen := make([]*packages.Package, 0, len(chosenByPath))
	for _, p := range chosenByPath {
		chosen = append(chosen, p)
	}
	sort.Slice(chosen, func(i, j int) bool { return chosen[i].PkgPath < chosen[j].PkgPath })

	var symbols []Symbol
	var edges []Edge
	seenSymIDs := make(map[string]struct{})
	addSym := func(s Symbol) {
		if _, exists := seenSymIDs[s.ID]; exists {
			return
		}
		seenSymIDs[s.ID] = struct{}{}
		symbols = append(symbols, s)
	}
	addEdge := func(srcFQN, dstFQN, kind string) {
		if srcFQN == "" || dstFQN == "" || srcFQN == dstFQN {
			return
		}
		edges = append(edges, Edge{
			Repo: repoName, SnapshotID: snapshotID,
			SrcID: uuidFromFQN(srcFQN), DstID: uuidFromFQN(dstFQN), Kind: kind,
		})
	}

	// First pass: declare package + file + top-level symbols (so IDs exist before edge resolution).
	for _, pkg := range chosen {
		pkgFQN := pkg.PkgPath
		addSym(Symbol{
			ID: uuidFromFQN(pkgFQN), Repo: repoName, SnapshotID: snapshotID,
			FQN: pkgFQN, Kind: "package", FilePath: pkg.PkgPath,
		})
		for i, f := range pkg.Syntax {
			fpath := pkg.CompiledGoFiles[i]
			rel, _ := filepath.Rel(repoPath, fpath)
			fileFQN := pkgFQN + "/" + filepath.Base(fpath)
			addSym(Symbol{
				ID: uuidFromFQN(fileFQN), Repo: repoName, SnapshotID: snapshotID,
				FQN: fileFQN, Kind: "file", FilePath: rel,
			})
			addEdge(pkgFQN, fileFQN, "DECLARES")
			collectDecls(pkg, f, pkg.Fset, repoPath, repoName, snapshotID, pkgFQN, fileFQN, addSym, addEdge)
		}
	}

	// Second pass: call edges + type-references + implements + embeds (needs ID table populated).
	for _, pkg := range chosen {
		if pkg.TypesInfo == nil {
			continue
		}
		for _, f := range pkg.Syntax {
			collectEdges(pkg, f, pkg.Fset, addEdge)
		}
	}

	// Implements: scan named types vs interfaces across loaded module.
	collectImplements(chosen, addEdge)

	// Interface-dispatch CALLS + SSA/VTA/AST hazard edges via the static call graph.
	collectCallAndHazardEdges(chosen, repoPath, repoName, snapshotID, addSym, addEdge, progress)

	// Filter edges whose SRC isn't a declared symbol (e.g. CHA edges from stdlib callers).
	// Dst is allowed to be anything; dangling dst is fine and useful for later cross-repo ingestion.
	declaredIDs := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		declaredIDs[s.ID] = struct{}{}
	}
	kept := edges[:0]
	dropped := 0
	for _, e := range edges {
		if _, ok := declaredIDs[e.SrcID]; ok {
			kept = append(kept, e)
		} else {
			dropped++
		}
	}
	edges = kept
	if dropped > 0 {
		progress("walk", dropped, dropped, fmt.Sprintf("filtered edges with non-declared src (e.g. stdlib callers)"))
	}

	// Deduplicate edges (same triple).
	edges = dedupEdges(edges)

	return symbols, edges, nil
}

func collectDecls(
	pkg *packages.Package, f *ast.File, fset *token.FileSet,
	repoPath, repoName, snapshotID, pkgFQN, fileFQN string,
	addSym func(Symbol), addEdge func(string, string, string),
) {
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			fqn := funcFQNAt(pkgFQN, d, fset)
			sig, body := renderFunc(fset, d)
			rel, _ := filepath.Rel(repoPath, fset.Position(d.Pos()).Filename)
			kind := "func"
			if d.Recv != nil {
				kind = "method"
			}
			addSym(Symbol{
				ID:          uuidFromFQN(fqn),
				Repo:        repoName,
				SnapshotID:  snapshotID,
				FQN:         fqn,
				Kind:        kind,
				FilePath:    rel,
				LineStart:   fset.Position(d.Pos()).Line,
				LineEnd:     fset.Position(d.End()).Line,
				Signature:   sig,
				DocComment:  docText(d.Doc),
				Body:        clip(body, bodyMaxBytes),
				Annotations: extractVoltAnnotations(f, d.Pos(), d.End()),
			})
			addEdge(fileFQN, fqn, "DECLARES")
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					fqn := pkgFQN + "." + s.Name.Name
					rel, _ := filepath.Rel(repoPath, fset.Position(s.Pos()).Filename)
					body := renderNode(fset, s)
					addSym(Symbol{
						ID:         uuidFromFQN(fqn),
						Repo:       repoName,
						SnapshotID: snapshotID,
						FQN:        fqn,
						Kind:       "type",
						FilePath:   rel,
						LineStart:  fset.Position(s.Pos()).Line,
						LineEnd:    fset.Position(s.End()).Line,
						Signature:  "type " + s.Name.Name,
						DocComment: docText(d.Doc),
						Body:       clip(body, bodyMaxBytes),
					})
					addEdge(fileFQN, fqn, "DECLARES")
					// Struct fields → `field` symbols (pkg.Type.field) + the
					// type→field DECLARES edge; embedded fields → EMBEDS edges.
					if st, ok := s.Type.(*ast.StructType); ok {
						for _, fld := range st.Fields.List {
							if len(fld.Names) == 0 { // embedded field
								if obj := pkg.TypesInfo.TypeOf(fld.Type); obj != nil {
									addEdge(fqn, typeFQN(obj), "EMBEDS")
								}
								continue
							}
							ftype := renderNode(fset, fld.Type)
							for _, fn := range fld.Names {
								ffqn := fqn + "." + fn.Name
								addSym(Symbol{
									ID:         uuidFromFQN(ffqn),
									Repo:       repoName,
									SnapshotID: snapshotID,
									FQN:        ffqn,
									Kind:       "field",
									FilePath:   rel,
									LineStart:  fset.Position(fn.Pos()).Line,
									LineEnd:    fset.Position(fn.End()).Line,
									Signature:  "field " + s.Name.Name + "." + fn.Name + " " + ftype,
									DocComment: docText(fld.Doc),
								})
								addEdge(fqn, ffqn, "DECLARES")
							}
						}
					}
				case *ast.ValueSpec:
					kind := "var"
					if d.Tok == token.CONST {
						kind = "const"
					}
					for _, n := range s.Names {
						fqn := pkgFQN + "." + n.Name
						rel, _ := filepath.Rel(repoPath, fset.Position(n.Pos()).Filename)
						addSym(Symbol{
							ID:         uuidFromFQN(fqn),
							Repo:       repoName,
							SnapshotID: snapshotID,
							FQN:        fqn,
							Kind:       kind,
							FilePath:   rel,
							LineStart:  fset.Position(n.Pos()).Line,
							LineEnd:    fset.Position(n.End()).Line,
							Signature:  kind + " " + n.Name,
							DocComment: docText(d.Doc),
						})
						addEdge(fileFQN, fqn, "DECLARES")
					}
				case *ast.ImportSpec:
					// File-granular: file --IMPORTS--> imported package. Strictly
					// more information than package-level (aggregate files to get
					// package imports) — makes "which files import X" a query.
					target := strings.Trim(s.Path.Value, `"`)
					addEdge(fileFQN, target, "IMPORTS")
				}
			}
		}
	}
}

func collectEdges(pkg *packages.Package, f *ast.File, fset *token.FileSet, addEdge func(string, string, string)) {
	pkgFQN := pkg.PkgPath
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		caller := funcFQNAt(pkgFQN, fd, fset)
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if callee := resolveCallee(pkg, x); callee != "" {
					addEdge(caller, callee, "CALLS")
				}
			case *ast.SelectorExpr:
				// Field access x.f. TypesInfo.Selections distinguishes a field
				// selection (FieldVal) from a method selection; only the field
				// case yields a `field` symbol edge — method calls/refs are
				// handled by the CallExpr / Ident paths.
				if seln := pkg.TypesInfo.Selections[x]; seln != nil && seln.Kind() == types.FieldVal {
					if ref := fieldFQN(seln.Recv(), seln.Obj().Name()); ref != "" {
						addEdge(caller, ref, "REFERENCES")
					}
				}
			case *ast.CompositeLit:
				// Keyed struct initialization T{Field: v} — each key is a
				// field reference (a write) on the literal's struct type.
				lt := pkg.TypesInfo.TypeOf(x)
				for _, el := range x.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); ok {
						if ref := fieldFQN(lt, key.Name); ref != "" {
							addEdge(caller, ref, "REFERENCES")
						}
					}
				}
			case *ast.Ident:
				obj := pkg.TypesInfo.Uses[x]
				if obj == nil {
					return true
				}
				if ref := objectFQN(obj); ref != "" {
					addEdge(caller, ref, "REFERENCES")
				}
			}
			return true
		})
	}
}

func resolveCallee(pkg *packages.Package, call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if obj := pkg.TypesInfo.Uses[fn]; obj != nil {
			return objectFQN(obj)
		}
	case *ast.SelectorExpr:
		if obj := pkg.TypesInfo.Uses[fn.Sel]; obj != nil {
			return objectFQN(obj)
		}
	}
	return ""
}

func collectImplements(pkgs []*packages.Package, addEdge func(string, string, string)) {
	var ifaces []*types.Named
	var concretes []*types.Named
	for _, p := range pkgs {
		if p.Types == nil {
			continue
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			obj, ok := scope.Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			named, ok := obj.Type().(*types.Named)
			if !ok {
				continue
			}
			if _, ok := named.Underlying().(*types.Interface); ok {
				ifaces = append(ifaces, named)
			} else {
				concretes = append(concretes, named)
			}
		}
	}
	for _, c := range concretes {
		for _, i := range ifaces {
			iface, _ := i.Underlying().(*types.Interface)
			if iface.NumMethods() == 0 {
				continue
			}
			if types.Implements(c, iface) || types.Implements(types.NewPointer(c), iface) {
				addEdge(namedFQN(c), namedFQN(i), "IMPLEMENTS")
			}
		}
	}
}

func collectCallEdges(pkgs []*packages.Package, addEdge func(string, string, string), progress ProgressFunc) {
	collectCallAndHazardEdges(pkgs, "", "", "", func(Symbol) {}, addEdge, progress)
}

func collectCallAndHazardEdges(
	pkgs []*packages.Package,
	repoPath, repoName, snapshotID string,
	addSym func(Symbol),
	addEdge func(string, string, string),
	progress ProgressFunc,
) {
	progress("vta", 0, 0, "building SSA + VTA call graph (precise interface/function-value dispatch)")
	prog, _ := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	// VTA (Variable Type Analysis) refines a CHA bootstrap. CHA is sound but
	// imprecise: it resolves a call through an interface or a func value to
	// EVERY type-compatible target in the program — so a call to f() where f
	// is func() error becomes an edge to every func() error, polluting the
	// graph (e.g. AwaitFuture appearing to "call" cmd/server.run). VTA tracks
	// where values actually flow and narrows those targets to the ones a
	// variable can really hold, which removes the spurious edges.
	cg := vta.CallGraph(ssautil.AllFunctions(prog), cha.CallGraph(prog))
	if cg == nil {
		return
	}
	added := 0
	for _, node := range cg.Nodes {
		if node == nil || node.Func == nil {
			continue
		}
		callerFQN := ssaFuncFQN(node.Func)
		if callerFQN == "" {
			continue
		}
		for _, e := range node.Out {
			if e.Callee == nil || e.Callee.Func == nil {
				continue
			}
			calleeFQN := ssaFuncFQN(e.Callee.Func)
			if calleeFQN == "" {
				continue
			}
			addEdge(callerFQN, calleeFQN, "CALLS")
			added++
		}
	}
	collectSSAHazardEdges(pkgs, prog, cg, repoPath, repoName, snapshotID, addSym, addEdge)
	progress("vta", added, added, "call edges (interface + function-value dispatch)")
}

func ssaFuncFQN(fn *ssa.Function) string {
	if fn == nil {
		return ""
	}
	// Anonymous/closure functions: walk up to enclosing named function.
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	if fn.Pkg == nil || fn.Pkg.Pkg == nil {
		// Bound method, synthetic, or wrapper. Use Object() if available.
		if obj := fn.Object(); obj != nil {
			return objectFQN(obj)
		}
		return ""
	}
	pkgPath := fn.Pkg.Pkg.Path()
	if recv := fn.Signature.Recv(); recv != nil {
		recvFQN := typeFQN(recv.Type())
		if recvFQN != "" {
			return recvFQN + "." + fn.Name()
		}
	}
	if (fn.Name() == "init" || strings.HasPrefix(fn.Name(), "init#")) && fn.Prog != nil && fn.Prog.Fset != nil && fn.Pos().IsValid() {
		pos := fn.Prog.Fset.Position(fn.Pos())
		return fmt.Sprintf("%s.init@%s:%d:%d", pkgPath, filepath.Base(pos.Filename), pos.Line, pos.Column)
	}
	return pkgPath + "." + fn.Name()
}

func namedFQN(n *types.Named) string {
	obj := n.Obj()
	if obj.Pkg() == nil {
		return obj.Name()
	}
	return obj.Pkg().Path() + "." + obj.Name()
}

func typeFQN(t types.Type) string {
	switch x := t.(type) {
	case *types.Named:
		return namedFQN(x)
	case *types.Pointer:
		return typeFQN(x.Elem())
	}
	return ""
}

// fieldFQN builds the FQN of a struct field — pkg.Type.field — from the
// receiver type the field was selected on and the field name. Returns "" when
// the receiver is not a named struct (anonymous struct, slice/map literal),
// in which case no `field` symbol exists to point at. Promoted fields from an
// embedded struct attribute to the outer type the access used, not the inner
// declaring type — a v1 imprecision (the edge dangles for promoted accesses).
func fieldFQN(recv types.Type, name string) string {
	base := typeFQN(recv)
	if base == "" {
		return ""
	}
	return base + "." + name
}

func objectFQN(o types.Object) string {
	if o.Pkg() == nil {
		return ""
	}
	pkgPath := o.Pkg().Path()
	switch x := o.(type) {
	case *types.Func:
		if sig, ok := x.Type().(*types.Signature); ok && sig.Recv() != nil {
			recv := typeFQN(sig.Recv().Type())
			if recv != "" {
				return recv + "." + x.Name()
			}
		}
		return pkgPath + "." + x.Name()
	case *types.Var:
		// Struct fields are ingested as `field` symbols (pkg.Type.field) and
		// reached through the field-access edge path in collectEdges — not as
		// a flat pkg.name reference. Returning "" here keeps the generic ident
		// walk from emitting a bogus pkg.<fieldname> edge for a field selector
		// or composite-literal key.
		if x.IsField() {
			return ""
		}
		if !isPackageScoped(x) {
			return "" // function-local variable, parameter, or named result
		}
		return pkgPath + "." + x.Name()
	case *types.Const:
		if !isPackageScoped(x) {
			return "" // function-local const
		}
		return pkgPath + "." + x.Name()
	case *types.TypeName:
		if !isPackageScoped(x) {
			return "" // function-local type declaration
		}
		return pkgPath + "." + x.Name()
	}
	return ""
}

// isPackageScoped reports whether o is declared at package scope. Only
// package-scoped objects are graph symbols; a function-local variable,
// parameter, named result, local const or local type is below the symbol
// boundary — emitting an edge to a synthetic pkg.<localname> is a dangling,
// often colliding, false edge. o.Pkg() is assumed non-nil (checked by caller).
func isPackageScoped(o types.Object) bool {
	return o.Parent() == o.Pkg().Scope()
}

func funcFQN(pkgFQN string, d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return pkgFQN + "." + d.Name.Name
	}
	recv := d.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if id, ok := recv.(*ast.Ident); ok {
		return pkgFQN + "." + id.Name + "." + d.Name.Name
	}
	return pkgFQN + "." + d.Name.Name
}

func funcFQNAt(pkgFQN string, d *ast.FuncDecl, fset *token.FileSet) string {
	base := funcFQN(pkgFQN, d)
	if d.Recv != nil || d.Name.Name != "init" || fset == nil {
		return base
	}
	pos := fset.Position(d.Name.Pos())
	file := filepath.Base(pos.Filename)
	return fmt.Sprintf("%s@%s:%d:%d", base, file, pos.Line, pos.Column)
}

func renderFunc(fset *token.FileSet, d *ast.FuncDecl) (sig, body string) {
	var sigBuf bytes.Buffer
	// Render signature only by clearing body.
	clone := *d
	clone.Body = nil
	_ = printer.Fprint(&sigBuf, fset, &clone)
	var bodyBuf bytes.Buffer
	_ = printer.Fprint(&bodyBuf, fset, d)
	return sigBuf.String(), bodyBuf.String()
}

func renderNode(fset *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, n)
	return b.String()
}

// extractVoltAnnotations returns the text (after the "//volt:" prefix) of every
// //volt: annotation comment positioned within [lo, hi] — the kernel's
// sentinel-raise-site convention. Used to make sentinel coverage queryable.
func extractVoltAnnotations(f *ast.File, lo, hi token.Pos) []string {
	var out []string
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if c.Slash < lo || c.Slash > hi {
				continue
			}
			t := strings.TrimSpace(c.Text)
			if strings.HasPrefix(t, "//volt:") {
				out = append(out, strings.TrimSpace(strings.TrimPrefix(t, "//volt:")))
			} else if strings.HasPrefix(t, "//spec:") {
				out = append(out, strings.TrimSpace(strings.TrimPrefix(t, "//spec:")))
			}
		}
	}
	return out
}

func docText(g *ast.CommentGroup) string {
	if g == nil {
		return ""
	}
	return strings.TrimSpace(g.Text())
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func uuidFromFQN(fqn string) string {
	// stable UUIDv5 over a fixed namespace so re-ingest of the same commit is idempotent on IDs.
	ns := uuid.MustParse("c0de9aaf-0000-0000-0000-000000000001")
	return uuid.NewSHA1(ns, []byte(fqn)).String()
}

func dedupEdges(in []Edge) []Edge {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, e := range in {
		k := e.SrcID + "|" + e.DstID + "|" + e.Kind
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, e)
	}
	return out
}

// ---------- embed (content-hash cached) ----------

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// selectMissingBlobs returns the set of content_hashes that need a fresh embedding.
// Queries SymbolBlobs for hashes already present, subtracts from the set we want.
func selectMissingBlobs(ctx context.Context, sp *spanner.Client, syms []Symbol) ([]string, error) {
	want := map[string]struct{}{}
	for _, s := range syms {
		if s.ContentHash != "" {
			want[s.ContentHash] = struct{}{}
		}
	}
	if len(want) == 0 {
		return nil, nil
	}
	hashes := make([]string, 0, len(want))
	for h := range want {
		hashes = append(hashes, h)
	}
	// Lookup in chunks of 1000 to stay under Spanner query-parameter array limits.
	const chunk = 1000
	hit := map[string]struct{}{}
	for start := 0; start < len(hashes); start += chunk {
		end := start + chunk
		if end > len(hashes) {
			end = len(hashes)
		}
		stmt := spanner.Statement{
			SQL:    `SELECT content_hash FROM SymbolBlobs WHERE content_hash IN UNNEST(@h) AND embedding IS NOT NULL`,
			Params: map[string]interface{}{"h": hashes[start:end]},
		}
		iter := sp.Single().Query(ctx, stmt)
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return nil, err
			}
			var h string
			if err := row.Columns(&h); err != nil {
				iter.Stop()
				return nil, err
			}
			hit[h] = struct{}{}
		}
		iter.Stop()
	}
	var miss []string
	for h := range want {
		if _, ok := hit[h]; !ok {
			miss = append(miss, h)
		}
	}
	return miss, nil
}

// embedAndWriteBlobsWithProgress is the progress-aware variant used by
// doIngest. It emits ("embed", done, total, "") at each batch boundary.
func embedAndWriteBlobsWithProgress(ctx context.Context, sp *spanner.Client, syms []Symbol, missing []string, progress ProgressFunc) error {
	return embedAndWriteBlobsImpl(ctx, sp, syms, missing, progress)
}

// embedAndWriteBlobs is the original no-progress callable (kept for any direct callers).
func embedAndWriteBlobs(ctx context.Context, sp *spanner.Client, syms []Symbol, missing []string) error {
	return embedAndWriteBlobsImpl(ctx, sp, syms, missing, func(string, int, int, string) {})
}

func embedAndWriteBlobsImpl(ctx context.Context, sp *spanner.Client, syms []Symbol, missing []string, progress ProgressFunc) error {
	missSet := make(map[string]struct{}, len(missing))
	for _, h := range missing {
		missSet[h] = struct{}{}
	}
	// Pick one representative symbol per missing hash.
	repByHash := make(map[string]string, len(missing))
	for _, s := range syms {
		if _, want := missSet[s.ContentHash]; !want {
			continue
		}
		if _, have := repByHash[s.ContentHash]; have {
			continue
		}
		repByHash[s.ContentHash] = buildEmbedText(s)
	}
	hashes := make([]string, 0, len(repByHash))
	texts := make([]string, 0, len(repByHash))
	for h, t := range repByHash {
		hashes = append(hashes, h)
		texts = append(texts, t)
	}
	for start := 0; start < len(texts); start += embedBatch {
		end := start + embedBatch
		if end > len(texts) {
			end = len(texts)
		}
		vecs, err := requestEmbeddings(ctx, texts[start:end], "RETRIEVAL_DOCUMENT")
		if err != nil {
			return err
		}
		muts := make([]*spanner.Mutation, 0, end-start)
		for i, v := range vecs {
			muts = append(muts, spanner.InsertOrUpdate("SymbolBlobs",
				[]string{"content_hash", "embedding", "created_at"},
				[]interface{}{hashes[start+i], v, spanner.CommitTimestamp},
			))
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		progress("embed", end, len(texts), "")
	}
	return nil
}

func buildEmbedText(s Symbol) string {
	var b strings.Builder
	if s.Signature != "" {
		b.WriteString(s.Signature)
		b.WriteString("\n")
	}
	if s.DocComment != "" {
		b.WriteString(s.DocComment)
		b.WriteString("\n")
	}
	if s.Body != "" {
		b.WriteString(s.Body)
	}
	if b.Len() == 0 {
		for _, part := range []string{s.Kind, s.FQN, s.FilePath} {
			if strings.TrimSpace(part) != "" {
				if b.Len() > 0 {
					b.WriteString(" ")
				}
				b.WriteString(strings.TrimSpace(part))
			}
		}
	}
	return clip(b.String(), embedMaxChars)
}

func newVertexHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 120 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

var vertexHTTPClientFactory = newVertexHTTPClient
var requestEmbeddings = callEmbed

func callEmbed(ctx context.Context, texts []string, taskType string) ([][]float32, error) {
	url := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
		gcpRegion, resolveDefaultGCPProject(), gcpRegion, embedModel)
	type instance struct {
		Content  string `json:"content"`
		TaskType string `json:"taskType,omitempty"`
	}
	type params struct {
		OutputDimensionality int `json:"outputDimensionality"`
	}
	type request struct {
		Instances  []instance `json:"instances"`
		Parameters params     `json:"parameters"`
	}
	insts := make([]instance, len(texts))
	for i, t := range texts {
		insts[i] = instance{Content: t, TaskType: taskType}
	}
	body, _ := json.Marshal(request{Instances: insts, Parameters: params{OutputDimensionality: embedDims}})

	client := vertexHTTPClientFactory()
	if client == nil {
		return nil, fmt.Errorf("Vertex HTTP client is unavailable")
	}

	// Retry on throttling (429) and transient 5xx with exponential backoff +
	// jitter. Per the "fire in parallel, retry the throttled ones" rule —
	// concurrent replay workers SHOULD all embed at once; Vertex will throttle
	// some, and those just back off and retry. 4xx (bad request, auth) fail fast.
	const maxAttempts = 7
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second // 2,4,8,16,32,64
			backoff += time.Duration(rand.IntN(1000)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if err := attachVertexBearer(req); err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err // network error — retry
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			var out struct {
				Predictions []struct {
					Embeddings struct {
						Values []float32 `json:"values"`
					} `json:"embeddings"`
				} `json:"predictions"`
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return nil, err
			}
			vecs := make([][]float32, len(out.Predictions))
			for i, p := range out.Predictions {
				vecs[i] = p.Embeddings.Values
			}
			return vecs, nil
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("vertex embed %d: %s", resp.StatusCode, string(raw))
			continue // throttled / transient — retry
		}
		return nil, fmt.Errorf("vertex embed %d: %s", resp.StatusCode, string(raw)) // 4xx — fail fast
	}
	return nil, fmt.Errorf("vertex embed: exhausted %d retries: %w", maxAttempts, lastErr)
}

// ---------- write ----------

// writeSnapshot upserts the Snapshots parent row. Must run before the
// interleaved SnapshotMembers / SnapshotEdges children.
func writeSnapshot(ctx context.Context, sp *spanner.Client, repo, snapshotID, label, commitSHA string, commitTime time.Time, branch string, parents []string, opts ingestOpts, symbolCount int) error {
	var base interface{} = spanner.NullString{}
	if opts.baseSnapID != "" {
		base = opts.baseSnapID
	}
	var br interface{} = spanner.NullString{}
	if branch != "" {
		br = branch
	}
	mut := spanner.InsertOrUpdate("Snapshots",
		[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time",
			"kind", "base_snapshot_id", "parent_snapshot_ids", "branch",
			"symbol_count", "ingested_at"},
		[]interface{}{repo, snapshotID, label, commitSHA, commitTime,
			opts.kind, base, parents, br,
			int64(symbolCount), spanner.CommitTimestamp},
	)
	_, err := sp.Apply(ctx, []*spanner.Mutation{mut})
	return err
}

func publishStagedSnapshot(ctx context.Context, sp *spanner.Client, repo, stagingSnapshotID, snapshotID, publishedKind string) error {
	row, err := sp.Single().ReadRow(ctx, "Snapshots", spanner.Key{repo, stagingSnapshotID},
		[]string{"label", "commit_sha", "commit_time", "base_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"})
	if err != nil {
		return err
	}
	var label, commitSHA string
	var commitTime spanner.NullTime
	var base, branch spanner.NullString
	var parents []string
	var symbolCount int64
	var ingestedAt time.Time
	if err := row.Columns(&label, &commitSHA, &commitTime, &base, &parents, &branch, &symbolCount, &ingestedAt); err != nil {
		return err
	}
	// The completed staging row is the immutable storage generation. Publishing
	// flips one logical pointer, independent of the number or size of children.
	mut := spanner.InsertOrUpdate("Snapshots",
		[]string{"repo", "snapshot_id", "label", "commit_sha", "commit_time", "kind", "base_snapshot_id", "storage_snapshot_id", "parent_snapshot_ids", "branch", "symbol_count", "ingested_at"},
		[]interface{}{repo, snapshotID, label, commitSHA, commitTime, publishedKind, base, stagingSnapshotID, parents, branch, symbolCount, ingestedAt})
	_, err = sp.Apply(ctx, []*spanner.Mutation{mut})
	return err
}

// writeSymbolVersions upserts the immutable (symbol_id, content_hash) spine.
// Only content-bearing symbols; idempotent — re-ingesting an existing version
// is a no-op overwrite.
func writeSymbolVersions(ctx context.Context, sp *spanner.Client, syms []Symbol, progress ProgressFunc) error {
	var versioned []Symbol
	for _, s := range syms {
		if s.ContentHash != "" {
			versioned = append(versioned, s)
		}
	}
	for start := 0; start < len(versioned); start += mutationBatch {
		end := start + mutationBatch
		if end > len(versioned) {
			end = len(versioned)
		}
		muts := make([]*spanner.Mutation, 0, end-start)
		for _, s := range versioned[start:end] {
			muts = append(muts, spanner.InsertOrUpdate("SymbolVersions",
				[]string{"symbol_id", "content_hash", "fqn", "kind",
					"signature", "doc_comment", "body", "volt_annotations", "created_at"},
				[]interface{}{s.ID, s.ContentHash, s.FQN, s.Kind,
					s.Signature, s.DocComment, s.Body, s.Annotations, spanner.CommitTimestamp},
			))
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		progress("versions", end, len(versioned), "")
	}
	return nil
}

func writeSnapshotMembers(ctx context.Context, sp *spanner.Client, syms []Symbol, progress ProgressFunc) error {
	for start := 0; start < len(syms); start += mutationBatch {
		end := start + mutationBatch
		if end > len(syms) {
			end = len(syms)
		}
		muts := make([]*spanner.Mutation, 0, end-start)
		for _, s := range syms[start:end] {
			var hash interface{}
			if s.ContentHash != "" {
				hash = s.ContentHash
			} else {
				hash = spanner.NullString{}
			}
			muts = append(muts, spanner.InsertOrUpdate("SnapshotMembers",
				[]string{"repo", "snapshot_id", "symbol_id", "fqn", "content_hash",
					"kind", "file_path", "line_start", "line_end"},
				[]interface{}{s.Repo, s.SnapshotID, s.ID, s.FQN, hash,
					s.Kind, s.FilePath, int64(s.LineStart), int64(s.LineEnd)},
			))
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		progress("members", end, len(syms), "")
	}
	return nil
}

// writeStructuralEdges appends DECLARES/IMPORTS rows to the snapshot's complete
// edge set after content-derived rows have established the replacement marker.
func writeStructuralEdges(ctx context.Context, sp *spanner.Client, edges []Edge, progress ProgressFunc) error {
	for start := 0; start < len(edges); start += mutationBatch {
		end := start + mutationBatch
		if end > len(edges) {
			end = len(edges)
		}
		muts := make([]*spanner.Mutation, 0, end-start)
		for _, e := range edges[start:end] {
			muts = append(muts, spanner.InsertOrUpdate("SnapshotEdges",
				[]string{"repo", "snapshot_id", "symbol_id", "dst_symbol_id", "kind"},
				[]interface{}{e.Repo, e.SnapshotID, e.SrcID, e.DstID, e.Kind},
			))
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		progress("structural-edges", end, len(edges), "")
	}
	return nil
}

// writeSymbolEdges preserves the historical helper boundary while storing the
// derived set in the repository snapshot carried by every edge.

const snapshotEdgeSetMarkerKind = "EDGE_SET_COMPLETE"

var snapshotEdgeSetMarkerID = uuidFromFQN("velenza.cambium.snapshot-edge-set")

func writeSymbolEdges(ctx context.Context, sp *spanner.Client, edges []Edge, progress ProgressFunc) error {
	if len(edges) == 0 {
		return nil
	}
	repo, snapshotID := edges[0].Repo, edges[0].SnapshotID
	for _, edge := range edges[1:] {
		if edge.Repo != repo || edge.SnapshotID != snapshotID {
			return fmt.Errorf("symbol edges span multiple snapshot contexts")
		}
	}
	return writeSnapshotSymbolEdges(ctx, sp, repo, snapshotID, edges, progress)
}

func writeSnapshotSymbolEdges(ctx context.Context, sp *spanner.Client, repo, snapshotID string, edges []Edge, progress ProgressFunc) error {
	// A source body is not the full analysis context: interface dispatch and
	// type resolution can change while the source hash stays constant. Persist
	// every derived edge in its exact repository snapshot.
	if _, err := sp.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("SnapshotEdges", spanner.Key{repo, snapshotID}.AsPrefix()),
	}); err != nil {
		return err
	}
	apply := func(muts []*spanner.Mutation) error {
		for start := 0; start < len(muts); start += mutationBatch {
			end := start + mutationBatch
			if end > len(muts) {
				end = len(muts)
			}
			if _, err := sp.Apply(ctx, muts[start:end]); err != nil {
				return err
			}
		}
		return nil
	}
	ins := make([]*spanner.Mutation, 0, len(edges))
	for _, e := range edges {
		ins = append(ins, spanner.InsertOrUpdate("SnapshotEdges",
			[]string{"repo", "snapshot_id", "symbol_id", "dst_symbol_id", "kind"},
			[]interface{}{repo, snapshotID, e.SrcID, e.DstID, e.Kind},
		))
	}
	if err := apply(ins); err != nil {
		return err
	}
	if _, err := sp.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("SnapshotEdges",
		[]string{"repo", "snapshot_id", "symbol_id", "dst_symbol_id", "kind"},
		[]interface{}{repo, snapshotID, snapshotEdgeSetMarkerID, snapshotEdgeSetMarkerID, snapshotEdgeSetMarkerKind},
	)}); err != nil {
		return err
	}
	progress("symbol-edges", len(ins), len(ins), fmt.Sprintf("%d snapshot-scoped content edges", len(ins)))
	return nil
}

// ---------- search ----------

func cmdSearch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	limit := fs.Int("limit", 15, "top-K results")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("search needs one positional arg: \"<query>\"")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	results, err := doSearch(ctx, sp, fs.Arg(0), repo, snap, *limit)
	if err != nil {
		return err
	}
	fmt.Printf("%-8s  %-7s  %s\n", "DIST", "KIND", "FQN  (file:line)")
	for _, r := range results {
		fmt.Printf("%-8.4f  %-7s  %s  (%s:%d)\n", r.Distance, r.Kind, r.FQN, r.FilePath, r.LineStart)
	}
	return nil
}

// ---------- neighbors ----------

func cmdNeighbors(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("neighbors", flag.ExitOnError)
	depth := fs.Int("depth", 1, "hops to traverse (max 3)")
	snapshot := fs.String("snapshot", "", "pin traversal to this snapshot (default: latest containing the symbol)")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("neighbors needs one positional arg: <fqn>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	results, err := doNeighborsInRepo(ctx, sp, fs.Arg(0), *depth, repo, snap)
	if err != nil {
		return err
	}
	fmt.Printf("%-5s  %-4s  %-12s  %s\n", "DEPTH", "DIR", "KIND", "FQN")
	for _, r := range results {
		fmt.Printf("%-5d  %-4s  %-12s  %s\n", r.Depth, r.Direction, r.Kind, r.FQN)
	}
	fmt.Printf("(%d edges)\n", len(results))
	return nil
}

// ---------- diff ----------

func cmdDiff(ctx context.Context, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("diff needs three args: <repo> <before-commit> <after-commit>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	res, err := doDiff(ctx, sp, args[0], args[1], args[2])
	if err != nil {
		return err
	}
	fmt.Printf("repo=%s  before=%s  after=%s\n", res.Repo, res.Before, res.After)
	fmt.Printf("added=%d removed=%d modified=%d\n\n", len(res.Added), len(res.Removed), len(res.Modified))
	print := func(label string, entries []DiffEntry) {
		if len(entries) == 0 {
			return
		}
		fmt.Printf("=== %s (%d) ===\n", label, len(entries))
		for _, e := range entries {
			fp := e.FilePathNew
			if fp == "" {
				fp = e.FilePathOld
			}
			line := e.LineNew
			if line == 0 {
				line = e.LineOld
			}
			fmt.Printf("  %-7s  %s  (%s:%d)\n", e.Kind, e.FQN, fp, line)
		}
		fmt.Println()
	}
	print("ADDED", res.Added)
	print("REMOVED", res.Removed)
	print("MODIFIED", res.Modified)
	return nil
}

// ---------- lineage ----------

func cmdLineage(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("lineage needs one arg: <fqn>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	res, err := doLineage(ctx, sp, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("lineage: %s  (repo=%s, %d versions)\n", res.FQN, res.Repo, len(res.Versions))
	fmt.Printf("%-3s  %-12s  %-20s  %-16s  %-9s  %s\n", "#", "1ST_COMMIT", "1ST_SEEN", "1ST_BRANCH", "SEM_DELTA", "SNAPSHOTS")
	for i, v := range res.Versions {
		sha := v.FirstCommitSHA
		if len(sha) > 12 {
			sha = sha[:12]
		}
		delta := "-"
		if i > 0 {
			delta = fmt.Sprintf("%.4f", v.SemanticDelta)
		}
		branch := v.FirstBranch
		if branch == "" {
			branch = "-"
		}
		fmt.Printf("%-3d  %-12s  %-20s  %-16s  %-9s  %d\n", i+1, sha, v.FirstCommitTime, branch, delta, v.SnapshotCount)
	}
	return nil
}

// ---------- drift ----------

func cmdDrift(ctx context.Context, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("drift needs three args: <repo> <before-commit> <after-commit>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	res, err := doDrift(ctx, sp, args[0], args[1], args[2])
	if err != nil {
		return err
	}
	fmt.Printf("drift: repo=%s  before=%s  after=%s\n", res.Repo, res.Before, res.After)
	fmt.Printf("added=%d removed=%d changed=%d\n\n", res.Added, res.Removed, len(res.Changed))
	fmt.Printf("%-9s  %-9s  %-15s  %s\n", "SEM", "TEXT", "QUADRANT", "FQN")
	for _, e := range res.Changed {
		fmt.Printf("%-9.4f  %-9.4f  %-15s  %s\n", e.SemanticDelta, e.TextualDelta, e.Quadrant, e.FQN)
	}
	return nil
}

// ---------- findings ----------

func cmdFindings(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("findings", flag.ExitOnError)
	limit := fs.Int("limit", 50, "max findings per category")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	res, err := doFindings(ctx, sp, repo, snap, *limit)
	if err != nil {
		return err
	}
	fmt.Printf("findings: repo=%s snapshot=%s\n\n", res.Repo, res.Snapshot)
	fmt.Printf("=== DUPLICATION — semantically near-identical, no edge (%d) ===\n", len(res.Duplication))
	for _, d := range res.Duplication {
		fmt.Printf("  %.4f  %s\n          %s\n", d.Distance, d.FQNA, d.FQNB)
	}
	fmt.Printf("\n=== MISUSE — edge endpoints semantically far apart (%d) ===\n", len(res.Misuse))
	for _, m := range res.Misuse {
		fmt.Printf("  %.4f  [%s]  %s\n          -> %s\n", m.Distance, m.EdgeKind, m.SrcFQN, m.DstFQN)
	}
	return nil
}

// ---------- dependents ----------

func cmdDependents(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("dependents needs one arg: <fqn>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	res, err := doDependents(ctx, sp, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("dependents of %s  (%d, across all ingested repos)\n", res.FQN, len(res.Dependents))
	fmt.Printf("%-16s  %-12s  %s\n", "REPO", "EDGE", "FQN")
	for _, d := range res.Dependents {
		fmt.Printf("%-16s  %-12s  %s\n", d.Repo, d.EdgeKind, d.FQN)
	}
	return nil
}

// ---------- hypothesize ----------

func cmdHypothesize(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("hypothesize needs two args: <base-snapshot> <scratch-repo-path>")
	}
	base, scratchPath := args[0], args[1]
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	stats, err := doIngest(ctx, sp, scratchPath, ingestOpts{kind: "hypothetical", baseSnapID: base}, cliProgress())
	if err != nil {
		return err
	}
	fmt.Printf("hypothetical snapshot created: %s\n  repo=%s base=%s symbols=%d edges=%d\n",
		stats.SnapshotID, stats.Repo, base, stats.Symbols, stats.Edges)
	fmt.Printf("  diff/neighbors/drift now work against snapshot %s\n", stats.SnapshotID)
	return nil
}

// ---------- annotations ----------

func cmdAnnotations(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("annotations", flag.ExitOnError)
	atype := fs.String("type", "", "filter by annotation type (e.g. sentinel.raise)")
	spec := fs.String("spec", "", "filter by spec_section value")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	rows, err := doAnnotations(ctx, sp, repo, snap, *atype, *spec)
	if err != nil {
		return err
	}
	fmt.Printf("%-20s  %-34s  %s\n", "TYPE", "SPEC_SECTION", "FQN / name")
	for _, r := range rows {
		spec := r.SpecSection
		if spec == "" {
			spec = "-"
		}
		fmt.Printf("%-20s  %-34s  %s  [%s]\n", r.Type, spec, r.FQN, r.Name)
	}
	fmt.Printf("(%d annotations)\n", len(rows))
	return nil
}

// ---------- symbols ----------

func cmdSymbols(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("symbols", flag.ExitOnError)
	kind := fs.String("kind", "", "filter by kind (func/method/type/field/var/const/package/file)")
	match := fs.String("match", "", "filter by FQN regular expression")
	orphans := fs.Bool("orphans", false, "only orphans — zero incoming non-DECLARES edges, excluding entrypoints")
	limit := fs.Int("limit", 0, "max rows (0 = all)")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	rows, err := doListSymbols(ctx, sp, repo, snap, *kind, *match, *orphans, *limit)
	if err != nil {
		return err
	}
	fmt.Printf("%-7s  %-10s  %s\n", "KIND", "ORPHAN", "FQN  (file:line)")
	for _, r := range rows {
		orphan := ""
		if r.Orphan {
			orphan = "ORPHAN"
			if r.TestOnly {
				orphan = "TEST-ONLY" // dead in production, reached only by a test
			}
		}
		fmt.Printf("%-7s  %-10s  %s  (%s:%d)\n", r.Kind, orphan, r.FQN, r.FilePath, r.LineStart)
	}
	fmt.Printf("(%d symbols)\n", len(rows))
	return nil
}

// ---------- coverage ----------

func cmdCoverage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("coverage", flag.ExitOnError)
	untested := fs.Bool("untested", false, "list only production functions no test reaches")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	rep, err := doCoverage(ctx, sp, repo, snap, *untested)
	if err != nil {
		return err
	}
	fmt.Printf("reachability coverage: %d/%d production funcs reached by a test (%.1f%%) — %d test funcs\n",
		rep.Covered, rep.ProductionFuncs, rep.CoveragePct, rep.TestFuncs)
	if len(rep.HollowTests) > 0 {
		fmt.Printf("\n%d hollow test(s) — reach no production code:\n", len(rep.HollowTests))
		for _, h := range rep.HollowTests {
			fmt.Printf("  %s  (%s)\n", h.FQN, h.FilePath)
		}
	}
	fmt.Printf("\n%-9s  %-7s  %s\n", "COVERED", "KIND", "FQN  (file:line)")
	for _, s := range rep.Symbols {
		mark := "yes"
		if !s.Covered {
			mark = "NO"
		}
		fmt.Printf("%-9s  %-7s  %s  (%s:%d)\n", mark, s.Kind, s.FQN, s.FilePath, s.LineStart)
	}
	fmt.Printf("(%d symbols)\n", len(rep.Symbols))
	return nil
}

// ---------- list-commits ----------

func cmdListCommits(ctx context.Context, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("list-commits takes no arguments")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	out, err := doListCommits(ctx, sp)
	if err != nil {
		return err
	}
	fmt.Printf("%-14s  %-40s  %-12s  %-7s  %-30s  %s\n", "REPO", "SNAPSHOT", "KIND", "SYMBOLS", "BRANCH", "INGESTED_AT")
	for _, c := range out {
		branch := c.Branch
		if branch == "" {
			branch = "-"
		}
		fmt.Printf("%-14s  %-40s  %-12s  %-7d  %-30s  %s\n", c.Repo, c.SnapshotID, c.Kind, c.SymbolCount, branch, c.IngestedAt)
	}
	return nil
}

// ---------- prune ----------

// deleteSnapshotData removes a public logical snapshot. Immutable generations
// are retained because a reader may already have resolved the old pointer; a
// later lifecycle policy may reclaim generations that are proven unreachable.
// Legacy snapshots have interleaved children under their own logical ID, so
// retain their bounded partitioned-delete path before removing the parent.
func deleteSnapshotData(ctx context.Context, sp *spanner.Client, repo, snap string) error {
	row, err := sp.Single().ReadRow(ctx, "Snapshots", spanner.Key{repo, snap},
		[]string{"kind", "storage_snapshot_id"})
	if err != nil {
		return err
	}
	var kind string
	var storage spanner.NullString
	if err := row.Columns(&kind, &storage); err != nil {
		return err
	}
	if kind == "staging" {
		return fmt.Errorf("snapshot %q for repo %q is private staging data", snap, repo)
	}
	if !storage.Valid || storage.StringVal == "" {
		return deleteSnapshotGenerationData(ctx, sp, repo, snap)
	}
	if _, err := sp.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Snapshots", spanner.Key{repo, snap}),
	}); err != nil {
		return fmt.Errorf("delete snapshot row: %w", err)
	}
	return nil
}

// deleteSnapshotGenerationData removes child partitions before their owning
// snapshot row. Callers use it for an unpublished private generation or a
// legacy public snapshot whose child rows share its logical ID.
func deleteSnapshotGenerationData(ctx context.Context, sp *spanner.Client, repo, snap string) error {
	params := map[string]interface{}{"repo": repo, "snap": snap}
	for _, t := range []string{"SnapshotEdges", "SnapshotMembers", "SnapshotFiles", "GoFileChunks"} {
		if _, err := sp.PartitionedUpdate(ctx, spanner.Statement{
			SQL:    "DELETE FROM " + t + " WHERE repo = @repo AND snapshot_id = @snap",
			Params: params,
		}); err != nil {
			return fmt.Errorf("delete %s: %w", t, err)
		}
	}
	if _, err := sp.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Snapshots", spanner.Key{repo, snap}),
	}); err != nil {
		return fmt.Errorf("delete snapshot row: %w", err)
	}
	return nil
}

func cmdPrune(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("prune needs two args: <repo> <snapshot-id>")
	}
	repo, snap := args[0], args[1]
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	if err := deleteSnapshotData(ctx, sp, repo, snap); err != nil {
		return err
	}
	fmt.Printf("pruned: repo=%s snapshot=%s\n", repo, snap)
	return nil
}

// ---------- get ----------

func cmdGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("get needs one positional arg: <fqn>")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	s, err := doGetInRepo(ctx, sp, fs.Arg(0), repo, snap)
	if err != nil {
		return err
	}
	fmt.Printf("fqn:        %s\n", s.FQN)
	fmt.Printf("kind:       %s\n", s.Kind)
	fmt.Printf("repo:       %s\n", s.Repo)
	fmt.Printf("snapshot:   %s\n", s.SnapshotID)
	fmt.Printf("file:       %s:%d-%d\n", s.FilePath, s.LineStart, s.LineEnd)
	if s.Signature != "" {
		fmt.Printf("\nsignature:\n%s\n", s.Signature)
	}
	if s.DocComment != "" {
		fmt.Printf("\ndoc:\n%s\n", s.DocComment)
	}
	if s.Body != "" {
		fmt.Printf("\nbody:\n%s\n", s.Body)
	}
	return nil
}
