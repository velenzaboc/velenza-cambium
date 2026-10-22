package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// ---------- structural query (symbols) ----------

// SymbolRow is one symbol in a snapshot, for deterministic enumeration.
type SymbolRow struct {
	FQN       string `json:"fqn"`
	Kind      string `json:"kind"`
	FilePath  string `json:"file_path"`
	LineStart int64  `json:"line_start"`
	Orphan    bool   `json:"orphan"`
	// TestOnly is set on a production symbol that has no production caller but
	// IS reached by test code — dead in production, kept alive only by a test.
	TestOnly bool `json:"test_only,omitempty"`
}

// isTestFile reports whether a path is a Go test file. Test-ness is derived
// from the path suffix, so no schema column is needed to mark test symbols.
func isTestFile(path string) bool { return strings.HasSuffix(path, "_test.go") }

// isTestFuncName reports whether an FQN's leaf is a Go test entry point —
// Test*/Benchmark*/Fuzz*/Example*. A lowercase helper in a _test.go file is
// test code but not itself a test, so it is not a coverage root.
func isTestFuncName(fqn string) bool {
	leaf := fqn
	if i := strings.LastIndex(fqn, "."); i >= 0 {
		leaf = fqn[i+1:]
	}
	for _, p := range []string{"Test", "Benchmark", "Fuzz", "Example"} {
		if strings.HasPrefix(leaf, p) {
			return true
		}
	}
	return false
}

// doListSymbols enumerates the symbols of a snapshot — the deterministic
// counterpart to semantic search. Filterable by kind, by fqn regex, and by
// orphan status (zero incoming non-DECLARES edges, excluding entrypoints).
func doListSymbols(ctx context.Context, sp *spanner.Client, repo, snapshotID, kindFilter, matchRegex string, orphansOnly bool, limit int) ([]SymbolRow, error) {
	ref, err := resolveSnapshotRef(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", snapshotID, repo, err)
	}
	var re *regexp.Regexp
	if matchRegex != "" {
		re, err = regexp.Compile(matchRegex)
		if err != nil {
			return nil, fmt.Errorf("bad --match regex: %w", err)
		}
	}

	// Classify each symbol's incoming non-DECLARES edges by whether the SOURCE
	// is production or test code. A production symbol is orphan when nothing in
	// production reaches it; a test symbol is orphan when nothing reaches it at
	// all. Splitting the two means "production-dead" and "test-only" no longer
	// look identical — which is exactly the distinction test ingestion buys.
	edges, err := loadSnapshotEdgesFromGeneration(ctx, sp, repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	meta, err := snapshotMemberMetaFromGeneration(ctx, sp, repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	prodIncoming := make(map[string]struct{}, len(edges))
	testIncoming := make(map[string]struct{}, len(edges))
	for _, e := range edges {
		if e.Kind == "DECLARES" {
			continue
		}
		if isTestFile(meta[e.Src].FilePath) {
			testIncoming[e.Dst] = struct{}{}
		} else {
			prodIncoming[e.Dst] = struct{}{}
		}
	}

	sql := `SELECT symbol_id, fqn, kind, file_path, line_start
	        FROM SnapshotMembers
	        WHERE repo = @repo AND snapshot_id = @snap`
	params := map[string]interface{}{"repo": repo, "snap": ref.StorageID}
	if kindFilter != "" {
		sql += ` AND kind = @kind`
		params["kind"] = kindFilter
	}
	sql += ` ORDER BY fqn`
	iter := sp.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()

	var out []SymbolRow
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var symbolID, fqn, kind, fp string
		var line int64
		if err := row.Columns(&symbolID, &fqn, &kind, &fp, &line); err != nil {
			return nil, err
		}
		if re != nil && !re.MatchString(fqn) {
			continue
		}
		_, prod := prodIncoming[symbolID]
		_, test := testIncoming[symbolID]
		selfTest := isTestFile(fp)
		// A test symbol is reached legitimately by test code; a production
		// symbol is only "alive" if production reaches it.
		incoming := prod || (selfTest && test)
		orphan := isContentKind(kind) && !isEntrypoint(fqn) && !incoming
		testOnly := orphan && !selfTest && test
		if orphansOnly && !orphan {
			continue
		}
		out = append(out, SymbolRow{FQN: fqn, Kind: kind, FilePath: fp, LineStart: line, Orphan: orphan, TestOnly: testOnly})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func isContentKind(kind string) bool {
	switch kind {
	case "func", "method", "type", "var", "const":
		return true
	}
	return false
}

// isEntrypoint reports whether an FQN's leaf name is a Go entrypoint that
// legitimately has no in-repo caller (main, init, test/benchmark/fuzz/example).
func isEntrypoint(fqn string) bool {
	leaf := fqn
	if i := strings.LastIndex(fqn, "."); i >= 0 {
		leaf = fqn[i+1:]
	}
	if leaf == "main" || leaf == "init" || strings.HasPrefix(leaf, "init@") {
		return true
	}
	for _, p := range []string{"Test", "Benchmark", "Fuzz", "Example"} {
		if strings.HasPrefix(leaf, p) {
			return true
		}
	}
	return false
}

// ---------- coverage ----------

// CoverageSymbol is one production function/method and whether a test reaches
// it transitively through CALLS edges.
type CoverageSymbol struct {
	FQN       string `json:"fqn"`
	Kind      string `json:"kind"`
	FilePath  string `json:"file_path"`
	LineStart int64  `json:"line_start"`
	Covered   bool   `json:"covered"`
}

// HollowTest is a test function whose CALLS closure reaches no production code
// at all — it runs, but it exercises nothing.
type HollowTest struct {
	FQN      string `json:"fqn"`
	FilePath string `json:"file_path"`
}

// CoverageReport is graph-derived REACHABILITY coverage: which production
// functions are reachable from some test through the CALLS graph. This is not
// execution coverage (`go test -coverprofile`) — it cannot say a line ran or
// an assertion fired. What it proves is structural: a symbol no test reaches is
// definitely untested, and a test reaching no production code exercises nothing.
type CoverageReport struct {
	ProductionFuncs int              `json:"production_funcs"`
	Covered         int              `json:"covered"`
	CoveragePct     float64          `json:"coverage_pct"`
	TestFuncs       int              `json:"test_funcs"`
	HollowTests     []HollowTest     `json:"hollow_tests"`
	Symbols         []CoverageSymbol `json:"symbols"`
}

func doCoverage(ctx context.Context, sp *spanner.Client, repo, snapshotID string, untestedOnly bool) (*CoverageReport, error) {
	ref, err := resolveSnapshotRef(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", snapshotID, repo, err)
	}

	type memberRow struct {
		fqn, kind, file string
		line            int64
	}
	members := map[string]memberRow{}
	iter := sp.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT symbol_id, fqn, kind, file_path, line_start FROM SnapshotMembers WHERE repo=@repo AND snapshot_id=@snap`,
		Params: map[string]interface{}{"repo": repo, "snap": ref.StorageID},
	})
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return nil, err
		}
		var id, fqn, kind, file string
		var line int64
		if err := row.Columns(&id, &fqn, &kind, &file, &line); err != nil {
			iter.Stop()
			return nil, err
		}
		members[id] = memberRow{fqn, kind, file, line}
	}
	iter.Stop()

	edges, err := loadSnapshotEdgesFromGeneration(ctx, sp, repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	callsOut := map[string][]string{}
	for _, e := range edges {
		if e.Kind == "CALLS" {
			callsOut[e.Src] = append(callsOut[e.Src], e.Dst)
		}
	}

	isCallable := func(kind string) bool { return kind == "func" || kind == "method" }
	// A production target is a callable not in a test file and not an
	// entrypoint (main/init are not unit-test units).
	isProdTarget := func(m memberRow) bool {
		return isCallable(m.kind) && !isTestFile(m.file) && !isEntrypoint(m.fqn)
	}

	// Test functions = real Test*/Benchmark*/Fuzz*/Example* entry points in a
	// _test.go file. Lowercase helpers in test files are test code but not
	// tests — they are reached transitively, never used as a BFS root.
	var testFuncs []string
	for id, m := range members {
		if isCallable(m.kind) && isTestFile(m.file) && isTestFuncName(m.fqn) {
			testFuncs = append(testFuncs, id)
		}
	}

	// Global BFS from every test function over CALLS edges → reached set.
	reached := map[string]struct{}{}
	var queue []string
	for _, id := range testFuncs {
		if _, seen := reached[id]; !seen {
			reached[id] = struct{}{}
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, d := range callsOut[n] {
			if _, seen := reached[d]; !seen {
				reached[d] = struct{}{}
				queue = append(queue, d)
			}
		}
	}

	// Per-test BFS: a test is hollow if its closure reaches no production target.
	var hollow []HollowTest
	for _, t := range testFuncs {
		seen := map[string]struct{}{t: {}}
		q := []string{t}
		exercises := false
		for len(q) > 0 && !exercises {
			n := q[0]
			q = q[1:]
			for _, d := range callsOut[n] {
				if _, ok := seen[d]; ok {
					continue
				}
				seen[d] = struct{}{}
				if isProdTarget(members[d]) {
					exercises = true
					break
				}
				q = append(q, d)
			}
		}
		if !exercises {
			hollow = append(hollow, HollowTest{FQN: members[t].fqn, FilePath: members[t].file})
		}
	}
	sort.Slice(hollow, func(i, j int) bool { return hollow[i].FQN < hollow[j].FQN })

	var syms []CoverageSymbol
	covered, total := 0, 0
	for id, m := range members {
		if !isProdTarget(m) {
			continue
		}
		total++
		_, cov := reached[id]
		if cov {
			covered++
		}
		if untestedOnly && cov {
			continue
		}
		syms = append(syms, CoverageSymbol{FQN: m.fqn, Kind: m.kind, FilePath: m.file, LineStart: m.line, Covered: cov})
	}
	sort.Slice(syms, func(i, j int) bool { return syms[i].FQN < syms[j].FQN })

	pct := 0.0
	if total > 0 {
		pct = float64(covered) / float64(total) * 100
	}
	return &CoverageReport{
		ProductionFuncs: total,
		Covered:         covered,
		CoveragePct:     pct,
		TestFuncs:       len(testFuncs),
		HollowTests:     hollow,
		Symbols:         syms,
	}, nil
}

// Shared query helpers — used by both CLI commands and MCP tool handlers.
// All queries run against the Phase 1 schema: SymbolVersions (immutable spine),
// Snapshots / SnapshotMembers / SnapshotEdges (per-commit sets), SymbolBlobs
// (embedding cache).

type SearchResult struct {
	FQN       string  `json:"fqn"`
	Kind      string  `json:"kind"`
	FilePath  string  `json:"file_path"`
	LineStart int64   `json:"line_start"`
	Distance  float64 `json:"distance"`
}

type NeighborResult struct {
	Depth     int    `json:"depth"`
	Direction string `json:"direction"` // "in" | "out"
	Kind      string `json:"kind"`
	FQN       string `json:"fqn"`
	FilePath  string `json:"file_path,omitempty"`
	LineStart int64  `json:"line_start,omitempty"`
}

type SymbolDetail struct {
	FQN        string `json:"fqn"`
	Kind       string `json:"kind"`
	Repo       string `json:"repo"`
	SnapshotID string `json:"snapshot_id"`
	FilePath   string `json:"file_path"`
	LineStart  int64  `json:"line_start"`
	LineEnd    int64  `json:"line_end"`
	Signature  string `json:"signature,omitempty"`
	DocComment string `json:"doc_comment,omitempty"`
	Body       string `json:"body,omitempty"`
}

// ---------- search ----------

// doSearch is snapshot-coherent: results are restricted to symbols that are
// members of (repo, snapshotID). It over-fetches blobs from the ANN index then
// keeps only those present in the snapshot.
func doSearch(ctx context.Context, sp *spanner.Client, query, repo, snapshotID string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 15
	}
	ref, err := resolveSnapshotRef(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	vecs, err := callEmbed(ctx, []string{query}, "RETRIEVAL_QUERY")
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("expected 1 query embedding, got %d", len(vecs))
	}
	// Vector-search SymbolBlobs, then JOIN only to members of the pinned
	// snapshot. Over-fetch (limit*20) so enough survive the snapshot filter.
	stmt := spanner.Statement{
		SQL: `WITH top_blobs AS (
		        SELECT content_hash,
		               APPROX_COSINE_DISTANCE(embedding, @q, options => JSON '{"num_leaves_to_search": 400}') AS distance
		        FROM SymbolBlobs
		        WHERE embedding IS NOT NULL
		        ORDER BY distance
		        LIMIT @overfetch
		      )
		      SELECT ANY_VALUE(sm.fqn)        AS fqn,
		             ANY_VALUE(sm.kind)       AS kind,
		             ANY_VALUE(sm.file_path)  AS file_path,
		             ANY_VALUE(sm.line_start) AS line_start,
		             t.distance
		      FROM top_blobs AS t
		      JOIN SnapshotMembers AS sm ON sm.content_hash = t.content_hash
		      WHERE sm.repo = @repo AND sm.snapshot_id = @snap
		      GROUP BY t.content_hash, t.distance
		      ORDER BY t.distance
		      LIMIT @lim`,
		Params: map[string]interface{}{
			"q": vecs[0], "repo": repo, "snap": ref.StorageID,
			"overfetch": int64(limit * 20), "lim": int64(limit),
		},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []SearchResult
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var r SearchResult
		if err := row.Columns(&r.FQN, &r.Kind, &r.FilePath, &r.LineStart, &r.Distance); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// ---------- resolve ----------

// resolveFQN finds the symbol_id + repo + snapshot_id for an FQN. If pinSnap is
// non-empty the symbol is resolved within exactly that snapshot; otherwise the
// most-recently-committed snapshot containing it is used.
func resolveFQN(ctx context.Context, sp *spanner.Client, fqn, pinSnap string) (symbolID, repo, snapshotID string, err error) {
	return resolveFQNInRepo(ctx, sp, fqn, "", pinSnap)
}

func resolveFQNInRepo(ctx context.Context, sp *spanner.Client, fqn, pinRepo, pinSnap string) (symbolID, repo, snapshotID string, err error) {
	symbolID, ref, err := resolveFQNRefInRepo(ctx, sp, fqn, pinRepo, pinSnap)
	if err != nil {
		return "", "", "", err
	}
	return symbolID, ref.Repo, ref.LogicalID, nil
}

func resolveFQNRefInRepo(ctx context.Context, sp *spanner.Client, fqn, pinRepo, pinSnap string) (string, snapshotRef, error) {
	params := map[string]interface{}{"fqn": fqn}
	repoClause := ""
	snapshotClause := ""
	if pinRepo != "" {
		repoClause = " AND s.repo = @repo"
		params["repo"] = pinRepo
	}
	if pinSnap != "" {
		snapshotClause = " AND s.snapshot_id = @snap"
		params["snap"] = pinSnap
	}
	stmt := spanner.Statement{
		SQL: `SELECT sm.symbol_id, s.repo, s.snapshot_id, sm.snapshot_id
		      FROM Snapshots AS s
		      JOIN SnapshotMembers AS sm
		        ON sm.repo = s.repo
		       AND ((s.storage_snapshot_id IS NULL AND sm.snapshot_id = s.snapshot_id)
		         OR sm.snapshot_id = s.storage_snapshot_id)
		      WHERE sm.fqn = @fqn AND s.kind != 'staging'` + repoClause + snapshotClause + `
		      ORDER BY s.commit_time DESC, s.repo`,
		Params: params,
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		if pinSnap != "" {
			return "", snapshotRef{}, fmt.Errorf("no symbol %q in snapshot %q", fqn, pinSnap)
		}
		return "", snapshotRef{}, fmt.Errorf("no symbol matches FQN %q — try code_graph_search first", fqn)
	}
	if err != nil {
		return "", snapshotRef{}, err
	}
	var symbolID, repo, snapshotID, storageID string
	if err := row.Columns(&symbolID, &repo, &snapshotID, &storageID); err != nil {
		return "", snapshotRef{}, err
	}
	if pinSnap != "" && pinRepo == "" {
		if _, nextErr := iter.Next(); nextErr == nil {
			return "", snapshotRef{}, fmt.Errorf("symbol %q in snapshot %q is ambiguous across repositories", fqn, pinSnap)
		} else if nextErr != iterator.Done {
			return "", snapshotRef{}, nextErr
		}
	} else if pinSnap == "" && pinRepo == "" {
		for {
			next, nextErr := iter.Next()
			if nextErr == iterator.Done {
				break
			}
			if nextErr != nil {
				return "", snapshotRef{}, nextErr
			}
			var nextSymbolID, nextRepo, nextSnapshot, nextStorage string
			if err := next.Columns(&nextSymbolID, &nextRepo, &nextSnapshot, &nextStorage); err != nil {
				return "", snapshotRef{}, err
			}
			if nextRepo != repo {
				return "", snapshotRef{}, fmt.Errorf("symbol %q is ambiguous across repositories; pin a repository context", fqn)
			}
		}
	}
	return symbolID, snapshotRef{Repo: repo, LogicalID: snapshotID, StorageID: storageID}, nil
}

// ---------- edges ----------

// EdgeRow is one directed edge within a snapshot.
type EdgeRow struct {
	Src  string
	Dst  string
	Kind string
}

// loadSnapshotEdges returns every edge of a snapshot. New ingests carry an
// EDGE_SET_COMPLETE marker and keep all derived edges in SnapshotEdges because
// whole-program resolution is snapshot-dependent. Snapshots written before
// that marker retain read compatibility with legacy SymbolEdges.
func loadSnapshotEdges(ctx context.Context, sp *spanner.Client, repo, snapshotID string) ([]EdgeRow, error) {
	storageID, err := resolveSnapshotStorageID(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	return loadSnapshotEdgesFromGeneration(ctx, sp, repo, storageID)
}

func loadSnapshotEdgesFromGeneration(ctx context.Context, sp *spanner.Client, repo, storageID string) ([]EdgeRow, error) {
	var out []EdgeRow
	complete := false
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, dst_symbol_id, kind
		      FROM SnapshotEdges
		      WHERE repo = @repo AND snapshot_id = @snap`,
		Params: map[string]interface{}{"repo": repo, "snap": storageID},
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
		var e EdgeRow
		if err := row.Columns(&e.Src, &e.Dst, &e.Kind); err != nil {
			iter.Stop()
			return nil, err
		}
		if e.Kind == snapshotEdgeSetMarkerKind && e.Src == snapshotEdgeSetMarkerID && e.Dst == snapshotEdgeSetMarkerID {
			complete = true
			continue
		}
		out = append(out, e)
	}
	iter.Stop()
	if complete {
		return out, nil
	}
	legacyStmt := spanner.Statement{
		SQL: `SELECT e.src_symbol_id, e.dst_symbol_id, e.kind
		      FROM SymbolEdges AS e
		      JOIN SnapshotMembers AS sm
		        ON sm.symbol_id = e.src_symbol_id AND sm.content_hash = e.src_content_hash
		      WHERE sm.repo = @repo AND sm.snapshot_id = @snap`,
		Params: map[string]interface{}{"repo": repo, "snap": storageID},
	}
	iter = sp.Single().Query(ctx, legacyStmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var e EdgeRow
		if err := row.Columns(&e.Src, &e.Dst, &e.Kind); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// snapshotMemberMeta returns fqn/file/line per symbol_id for a snapshot.
func snapshotMemberMeta(ctx context.Context, sp *spanner.Client, repo, snapshotID string) (map[string]NeighborResult, error) {
	storageID, err := resolveSnapshotStorageID(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	return snapshotMemberMetaFromGeneration(ctx, sp, repo, storageID)
}

func snapshotMemberMetaFromGeneration(ctx context.Context, sp *spanner.Client, repo, storageID string) (map[string]NeighborResult, error) {
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, fqn, file_path, line_start
		      FROM SnapshotMembers WHERE repo = @repo AND snapshot_id = @snap`,
		Params: map[string]interface{}{"repo": repo, "snap": storageID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	out := map[string]NeighborResult{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var sid, fqn, fp string
		var line int64
		if err := row.Columns(&sid, &fqn, &fp, &line); err != nil {
			return nil, err
		}
		out[sid] = NeighborResult{FQN: fqn, FilePath: fp, LineStart: line}
	}
	return out, nil
}

// ---------- neighbors ----------

func doNeighbors(ctx context.Context, sp *spanner.Client, fqn string, depth int, pinSnap string) ([]NeighborResult, error) {
	return doNeighborsInRepo(ctx, sp, fqn, depth, "", pinSnap)
}

func doNeighborsInRepo(ctx context.Context, sp *spanner.Client, fqn string, depth int, pinRepo, pinSnap string) ([]NeighborResult, error) {
	if depth <= 0 {
		depth = 1
	}
	if depth > 3 {
		depth = 3 // guard against runaway fanout
	}
	rootID, ref, err := resolveFQNRefInRepo(ctx, sp, fqn, pinRepo, pinSnap)
	if err != nil {
		return nil, err
	}
	edges, err := loadSnapshotEdgesFromGeneration(ctx, sp, ref.Repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	meta, err := snapshotMemberMetaFromGeneration(ctx, sp, ref.Repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	// Adjacency.
	type adj struct{ id, kind string }
	outAdj := map[string][]adj{}
	inAdj := map[string][]adj{}
	for _, e := range edges {
		outAdj[e.Src] = append(outAdj[e.Src], adj{e.Dst, e.Kind})
		inAdj[e.Dst] = append(inAdj[e.Dst], adj{e.Src, e.Kind})
	}

	visited := map[string]struct{}{rootID: {}}
	frontier := []string{rootID}
	var results []NeighborResult

	for d := 1; d <= depth; d++ {
		if len(frontier) == 0 {
			break
		}
		nextFrontier := map[string]struct{}{}
		emit := func(dir string, neighborID, kind string) {
			m, ok := meta[neighborID]
			if !ok {
				return // edge points outside this snapshot (stdlib / external pkg)
			}
			results = append(results, NeighborResult{
				Depth: d, Direction: dir, Kind: kind,
				FQN: m.FQN, FilePath: m.FilePath, LineStart: m.LineStart,
			})
			if _, seen := visited[neighborID]; !seen {
				visited[neighborID] = struct{}{}
				nextFrontier[neighborID] = struct{}{}
			}
		}
		for _, id := range frontier {
			for _, a := range outAdj[id] {
				emit("out", a.id, a.kind)
			}
			for _, a := range inAdj[id] {
				emit("in", a.id, a.kind)
			}
		}
		frontier = frontier[:0]
		for id := range nextFrontier {
			frontier = append(frontier, id)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Depth != results[j].Depth {
			return results[i].Depth < results[j].Depth
		}
		if results[i].Direction != results[j].Direction {
			return results[i].Direction < results[j].Direction
		}
		if results[i].Kind != results[j].Kind {
			return results[i].Kind < results[j].Kind
		}
		return results[i].FQN < results[j].FQN
	})
	return results, nil
}

// ---------- get ----------

// doGet returns a symbol's full record. If pinSnap is non-empty the symbol is
// read AS IT IS in that snapshot; otherwise the most-recent snapshot wins.
func doGet(ctx context.Context, sp *spanner.Client, fqn, pinSnap string) (*SymbolDetail, error) {
	return doGetInRepo(ctx, sp, fqn, "", pinSnap)
}

func doGetInRepo(ctx context.Context, sp *spanner.Client, fqn, pinRepo, pinSnap string) (*SymbolDetail, error) {
	symbolID, ref, err := resolveFQNRefInRepo(ctx, sp, fqn, pinRepo, pinSnap)
	if err != nil {
		return nil, err
	}
	stmt := spanner.Statement{
		SQL: `SELECT sm.fqn, sm.kind, sm.repo,
		             sm.file_path, sm.line_start, sm.line_end,
		             sv.signature, sv.doc_comment, sv.body
		      FROM SnapshotMembers AS sm
		      LEFT JOIN SymbolVersions AS sv
		        ON sv.symbol_id = sm.symbol_id AND sv.content_hash = sm.content_hash
		      WHERE sm.repo = @repo AND sm.snapshot_id = @snap AND sm.symbol_id = @symbol`,
		Params: map[string]interface{}{"repo": ref.Repo, "snap": ref.StorageID, "symbol": symbolID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return nil, fmt.Errorf("resolved symbol %q disappeared from %s@%s", fqn, ref.Repo, ref.LogicalID)
	}
	if err != nil {
		return nil, err
	}
	var s SymbolDetail
	var sig, doc, body spanner.NullString
	if err := row.Columns(&s.FQN, &s.Kind, &s.Repo, &s.FilePath, &s.LineStart, &s.LineEnd,
		&sig, &doc, &body); err != nil {
		return nil, err
	}
	s.SnapshotID = ref.LogicalID
	if sig.Valid {
		s.Signature = sig.StringVal
	}
	if doc.Valid {
		s.DocComment = doc.StringVal
	}
	if body.Valid {
		s.Body = body.StringVal
	}
	return &s, nil
}

// ---------- list snapshots ----------

type CommitInfo struct {
	Repo        string `json:"repo"`
	SnapshotID  string `json:"snapshot_id"`
	CommitSHA   string `json:"commit_sha"`
	Kind        string `json:"kind"`
	Branch      string `json:"branch,omitempty"`
	SymbolCount int64  `json:"symbol_count"`
	CommitTime  string `json:"commit_time,omitempty"`
	IngestedAt  string `json:"ingested_at"`
}

func doListCommits(ctx context.Context, sp *spanner.Client) ([]CommitInfo, error) {
	stmt := spanner.Statement{
		SQL: `SELECT repo, snapshot_id, commit_sha, kind, branch, symbol_count, commit_time, ingested_at
		      FROM Snapshots
		      WHERE kind != 'staging'
		      ORDER BY ingested_at DESC`,
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []CommitInfo
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var c CommitInfo
		var branch spanner.NullString
		var commitTime, ingestedAt spanner.NullTime
		if err := row.Columns(&c.Repo, &c.SnapshotID, &c.CommitSHA, &c.Kind, &branch, &c.SymbolCount,
			&commitTime, &ingestedAt); err != nil {
			return nil, err
		}
		if branch.Valid {
			c.Branch = branch.StringVal
		}
		if commitTime.Valid {
			c.CommitTime = commitTime.Time.UTC().Format("2006-01-02T15:04:05Z")
		}
		if ingestedAt.Valid {
			c.IngestedAt = ingestedAt.Time.UTC().Format("2006-01-02T15:04:05Z")
		}
		out = append(out, c)
	}
	return out, nil
}

// snapshotExists reports whether a Snapshots row exists for (repo, snapshot_id).
func snapshotExists(ctx context.Context, sp *spanner.Client, repo, snapshotID string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT 1 FROM Snapshots WHERE repo = @repo AND snapshot_id = @snap AND kind != 'staging' LIMIT 1`,
		Params: map[string]interface{}{"repo": repo, "snap": snapshotID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ---------- annotations (//volt:) ----------

// AnnotationRow is one //volt: annotation found on a symbol in a snapshot.
type AnnotationRow struct {
	FQN         string `json:"fqn"`
	Type        string `json:"type"`                   // first token, e.g. "sentinel.raise"
	Name        string `json:"name,omitempty"`         // name= value, if present
	SpecSection string `json:"spec_section,omitempty"` // spec_section= value, if present
	Raw         string `json:"raw"`                    // the full annotation text (after "//volt:")
}

// doAnnotations returns every //volt: annotation in a snapshot, optionally
// filtered by annotation type and/or spec_section. Makes sentinel-coverage
// audits queryable.
func doAnnotations(ctx context.Context, sp *spanner.Client, repo, snapshotID, filterType, filterSpec string) ([]AnnotationRow, error) {
	ref, err := resolveSnapshotRef(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", snapshotID, repo, err)
	}
	stmt := spanner.Statement{
		SQL: `SELECT sm.fqn, sv.volt_annotations
		      FROM SnapshotMembers AS sm
		      JOIN SymbolVersions AS sv
		        ON sv.symbol_id = sm.symbol_id AND sv.content_hash = sm.content_hash
		      WHERE sm.repo = @repo AND sm.snapshot_id = @snap
		        AND sv.volt_annotations IS NOT NULL
		      ORDER BY sm.fqn`,
		Params: map[string]interface{}{"repo": repo, "snap": ref.StorageID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []AnnotationRow
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var fqn string
		var anns []string
		if err := row.Columns(&fqn, &anns); err != nil {
			return nil, err
		}
		for _, raw := range anns {
			a := parseVoltAnnotation(fqn, raw)
			if filterType != "" && a.Type != filterType {
				continue
			}
			if filterSpec != "" && a.SpecSection != filterSpec {
				continue
			}
			out = append(out, a)
		}
	}
	return out, nil
}

// parseVoltAnnotation parses "sentinel.raise name=X raise_site=Y spec_section=Z"
// into its type and key=value fields.
func parseVoltAnnotation(fqn, raw string) AnnotationRow {
	a := AnnotationRow{FQN: fqn, Raw: raw}
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return a
	}
	a.Type = fields[0]
	for _, f := range fields[1:] {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		switch k {
		case "name":
			a.Name = v
		case "spec_section":
			a.SpecSection = v
		}
	}
	return a
}

// ---------- dependents (cross-repo) ----------

type Dependent struct {
	Repo     string `json:"repo"`
	FQN      string `json:"fqn"`
	EdgeKind string `json:"edge_kind"`
}

type DependentsResult struct {
	FQN        string      `json:"fqn"`
	Dependents []Dependent `json:"dependents"`
}

// doDependents finds everything that calls or references a symbol, across ALL
// ingested repos, in each repo's most-recent snapshot. Cross-repo works for
// free: symbol_id = uuidv5(fqn) is global, so an edge from repo B into repo
// A's exported function already points at the same symbol_id.
func doDependents(ctx context.Context, sp *spanner.Client, fqn string) (*DependentsResult, error) {
	stmt := spanner.Statement{SQL: `SELECT repo, snapshot_id
		FROM Snapshots WHERE kind = 'ingested'
		ORDER BY repo, commit_time DESC`}
	iter := sp.Single().Query(ctx, stmt)
	latest := map[string]string{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return nil, err
		}
		var repo, snapshot string
		if err := row.Columns(&repo, &snapshot); err != nil {
			iter.Stop()
			return nil, err
		}
		if _, ok := latest[repo]; !ok {
			latest[repo] = snapshot
		}
	}
	iter.Stop()
	res := &DependentsResult{FQN: fqn}
	targetID := uuidFromFQN(fqn)
	seen := map[Dependent]struct{}{}
	for repo, snapshot := range latest {
		ref, err := resolveSnapshotRef(ctx, sp, repo, snapshot)
		if err != nil {
			return nil, err
		}
		edges, err := loadSnapshotEdgesFromGeneration(ctx, sp, repo, ref.StorageID)
		if err != nil {
			return nil, err
		}
		meta, err := snapshotMemberMetaFromGeneration(ctx, sp, repo, ref.StorageID)
		if err != nil {
			return nil, err
		}
		for _, edge := range edges {
			if edge.Dst != targetID || (edge.Kind != "CALLS" && edge.Kind != "REFERENCES" && edge.Kind != "IMPLEMENTS" && edge.Kind != "EMBEDS") {
				continue
			}
			source, ok := meta[edge.Src]
			if !ok {
				continue
			}
			seen[Dependent{Repo: repo, FQN: source.FQN, EdgeKind: edge.Kind}] = struct{}{}
		}
	}
	for dependent := range seen {
		res.Dependents = append(res.Dependents, dependent)
	}
	sort.Slice(res.Dependents, func(i, j int) bool {
		if res.Dependents[i].Repo != res.Dependents[j].Repo {
			return res.Dependents[i].Repo < res.Dependents[j].Repo
		}
		if res.Dependents[i].FQN != res.Dependents[j].FQN {
			return res.Dependents[i].FQN < res.Dependents[j].FQN
		}
		return res.Dependents[i].EdgeKind < res.Dependents[j].EdgeKind
	})
	return res, nil
}

// ---------- diff ----------

type DiffEntry struct {
	FQN         string `json:"fqn"`
	Kind        string `json:"kind"`
	Status      string `json:"status"` // "added" | "removed" | "modified"
	FilePathOld string `json:"file_path_old,omitempty"`
	FilePathNew string `json:"file_path_new,omitempty"`
	LineOld     int64  `json:"line_old,omitempty"`
	LineNew     int64  `json:"line_new,omitempty"`
}

type DiffResult struct {
	Repo     string      `json:"repo"`
	Before   string      `json:"before_commit"`
	After    string      `json:"after_commit"`
	Added    []DiffEntry `json:"added"`
	Removed  []DiffEntry `json:"removed"`
	Modified []DiffEntry `json:"modified"`
}

type snapshotDiffMember struct {
	symbolID    string
	fqn         string
	kind        string
	filePath    string
	line        int64
	contentHash spanner.NullString
}

func loadSnapshotDiffMembers(ctx context.Context, sp *spanner.Client, repo, snapshotID string) (map[string]snapshotDiffMember, error) {
	storageID, err := resolveSnapshotStorageID(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	return loadSnapshotDiffMembersFromGeneration(ctx, sp, repo, storageID)
}

func loadSnapshotDiffMembersFromGeneration(ctx context.Context, sp *spanner.Client, repo, storageID string) (map[string]snapshotDiffMember, error) {
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, fqn, kind, file_path, line_start, content_hash
		      FROM SnapshotMembers
		      WHERE repo = @repo AND snapshot_id = @snap`,
		Params: map[string]interface{}{"repo": repo, "snap": storageID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	out := map[string]snapshotDiffMember{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var member snapshotDiffMember
		if err := row.Columns(&member.symbolID, &member.fqn, &member.kind, &member.filePath, &member.line, &member.contentHash); err != nil {
			return nil, err
		}
		out[member.symbolID] = member
	}
	return out, nil
}

func sameContentHash(a, b spanner.NullString) bool {
	return a.Valid == b.Valid && (!a.Valid || a.StringVal == b.StringVal)
}

func doDiff(ctx context.Context, sp *spanner.Client, repo, before, after string) (*DiffResult, error) {
	beforeRef, err := resolveSnapshotRef(ctx, sp, repo, before)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", before, repo, err)
	}
	afterRef, err := resolveSnapshotRef(ctx, sp, repo, after)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", after, repo, err)
	}
	beforeMembers, err := loadSnapshotDiffMembersFromGeneration(ctx, sp, repo, beforeRef.StorageID)
	if err != nil {
		return nil, err
	}
	afterMembers, err := loadSnapshotDiffMembersFromGeneration(ctx, sp, repo, afterRef.StorageID)
	if err != nil {
		return nil, err
	}
	res := &DiffResult{Repo: repo, Before: before, After: after}
	allIDs := map[string]struct{}{}
	for id := range beforeMembers {
		allIDs[id] = struct{}{}
	}
	for id := range afterMembers {
		allIDs[id] = struct{}{}
	}
	for id := range allIDs {
		oldMember, hadOld := beforeMembers[id]
		newMember, hadNew := afterMembers[id]
		if hadOld && hadNew && sameContentHash(oldMember.contentHash, newMember.contentHash) {
			continue
		}
		e := DiffEntry{
			FQN: oldMember.fqn, Kind: oldMember.kind,
			FilePathOld: oldMember.filePath, LineOld: oldMember.line,
			FilePathNew: newMember.filePath, LineNew: newMember.line,
		}
		if !hadOld {
			e.FQN, e.Kind, e.Status = newMember.fqn, newMember.kind, "added"
		} else if !hadNew {
			e.Status = "removed"
		} else {
			e.Status = "modified"
		}
		switch e.Status {
		case "added":
			res.Added = append(res.Added, e)
		case "removed":
			res.Removed = append(res.Removed, e)
		case "modified":
			res.Modified = append(res.Modified, e)
		}
	}
	for _, entries := range [][]DiffEntry{res.Added, res.Removed, res.Modified} {
		sort.Slice(entries, func(i, j int) bool { return entries[i].FQN < entries[j].FQN })
	}
	return res, nil
}
