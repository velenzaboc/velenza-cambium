package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	mcpNewSpannerClient = newSpannerClient
	mcpLatestSnapshot   = latestSnapshot
	mcpRunServer        = func(ctx context.Context, server *mcp.Server) error {
		return server.Run(ctx, &mcp.StdioTransport{})
	}
	mcpSnapshotRepo   = snapshotRepo
	mcpDoSearch       = doSearch
	mcpDoNeighbors    = doNeighbors
	mcpDoGet          = doGet
	mcpDoIngest       = doIngest
	mcpDoListCommits  = doListCommits
	mcpDoListSymbols  = doListSymbols
	mcpDoAnnotations  = doAnnotations
	mcpDoCoverage     = doCoverage
	mcpDoDiff         = doDiff
	mcpDoLineage      = doLineage
	mcpDoDrift        = doDrift
	mcpDoFindings     = doFindings
	mcpDoHazards      = doHazards
	mcpDoDependents   = doDependents
	mcpNotifyProgress = func(ctx context.Context, req *mcp.CallToolRequest, params *mcp.ProgressNotificationParams) error {
		return req.Session.NotifyProgress(ctx, params)
	}
)

// MCP server exposing the code-graph to LLM agents.
// Run as a stdio subprocess: `code-graph mcp`. Register in ~/.claude/settings.json
// under mcpServers; see README.md.

type searchInput struct {
	Query      string `json:"query" jsonschema:"natural-language description of the code you're looking for"`
	Limit      int    `json:"limit,omitempty" jsonschema:"top-K results (default 15, max 50)"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot for this call"`
}

type neighborsInput struct {
	FQN        string `json:"fqn" jsonschema:"fully-qualified symbol name (e.g. github.com/foo/bar.PkgName.TypeName.Method)"`
	Depth      int    `json:"depth,omitempty" jsonschema:"hops to traverse, 1-3 (default 1)"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"pin traversal to this snapshot; default is the latest snapshot containing the symbol"`
}

type getInput struct {
	FQN        string `json:"fqn" jsonschema:"fully-qualified symbol name"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"read the symbol as it is in this snapshot; default is the latest"`
}

type diffInput struct {
	Repo   string `json:"repo" jsonschema:"repository name as recorded at ingest time (e.g. velenza-cambium)"`
	Before string `json:"before_commit" jsonschema:"full 40-char commit SHA — the baseline"`
	After  string `json:"after_commit" jsonschema:"full 40-char commit SHA — the comparison target"`
}

type ingestInput struct {
	RepoPath string `json:"repo_path" jsonschema:"absolute path to a Go repository on the local filesystem (must contain a go.mod and a .git directory)"`
}

type listCommitsInput struct{}

type listCommitsOutput struct {
	Commits []CommitInfo `json:"commits"`
}

type symbolsInput struct {
	Kind       string `json:"kind,omitempty" jsonschema:"filter by kind: func/method/type/field/var/const/package/file"`
	Match      string `json:"match,omitempty" jsonschema:"filter by a regular expression matched against the FQN"`
	Orphans    bool   `json:"orphans,omitempty" jsonschema:"return only orphans — symbols with zero incoming non-DECLARES edges, excluding entrypoints"`
	Limit      int    `json:"limit,omitempty" jsonschema:"max rows (0 = all)"`
	Repo       string `json:"repo,omitempty" jsonschema:"override the current snapshot's repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot"`
}

type symbolsOutput struct {
	Symbols []SymbolRow `json:"symbols"`
}

type annotationsInput struct {
	Type       string `json:"type,omitempty" jsonschema:"filter by annotation type, e.g. sentinel.raise"`
	Spec       string `json:"spec,omitempty" jsonschema:"filter by spec_section value"`
	Repo       string `json:"repo,omitempty" jsonschema:"override the current snapshot's repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot"`
}

type annotationsOutput struct {
	Annotations []AnnotationRow `json:"annotations"`
}

type coverageInput struct {
	Untested   bool   `json:"untested,omitempty" jsonschema:"return only production functions no test reaches"`
	Repo       string `json:"repo,omitempty" jsonschema:"override the current snapshot's repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot"`
}

type lineageInput struct {
	FQN string `json:"fqn" jsonschema:"fully-qualified symbol name"`
}

type driftInput struct {
	Repo   string `json:"repo" jsonschema:"repository name as recorded at ingest time"`
	Before string `json:"before_commit" jsonschema:"baseline commit SHA"`
	After  string `json:"after_commit" jsonschema:"comparison-target commit SHA"`
}

type findingsInput struct {
	Limit      int    `json:"limit,omitempty" jsonschema:"max findings per category (default 50)"`
	Repo       string `json:"repo,omitempty" jsonschema:"override the current snapshot's repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot"`
}

type hazardsInput struct {
	Category   string `json:"category,omitempty" jsonschema:"filter by hazard category (e.g. MAP_ESCAPE_UNDER_LOCK, LOCK_ACROSS_IO, UNBOUNDED_MAP)"`
	FilePrefix string `json:"file_prefix,omitempty" jsonschema:"filter by relative file path prefix"`
	Limit      int    `json:"limit,omitempty" jsonschema:"max hazards to return (0 = all)"`
	Repo       string `json:"repo,omitempty" jsonschema:"override the current snapshot's repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"override the current snapshot"`
	RepoPath   string `json:"repo_path,omitempty" jsonschema:"optional local repository path to analyze directly via SSA/VTA without Spanner"`
}

type dependentsInput struct {
	FQN string `json:"fqn" jsonschema:"fully-qualified symbol name to find callers of"`
}

type hypothesizeInput struct {
	BaseSnapshot    string `json:"base_snapshot" jsonschema:"commit SHA of the snapshot this hypothesis derives from"`
	ScratchRepoPath string `json:"scratch_repo_path" jsonschema:"absolute path to a Go worktree where a proposed change has been applied"`
}

type useInput struct {
	Repo       string `json:"repo,omitempty" jsonschema:"repository to pin; omit to use the newest ingested repo"`
	SnapshotID string `json:"snapshot_id,omitempty" jsonschema:"snapshot to pin; omit to pin that repo's newest snapshot"`
}

func cmdMCP(ctx context.Context, args []string) error {
	// One persistent Spanner client for the server's lifetime.
	sp, err := mcpNewSpannerClient(ctx)
	if err != nil {
		return fmt.Errorf("spanner client: %w", err)
	}
	if sp != nil {
		defer sp.Close()
	}

	// The server holds ONE current snapshot. Every read operates within it, so
	// the agent never gets results smeared across builds. Resolved at startup
	// to the newest ingested snapshot; switchable via code_graph_use.
	cur := &cgContext{}
	if c, e := mcpLatestSnapshot(ctx, sp, ""); e == nil {
		*cur = c
	}
	// resolveSnap returns (repo, snapshot): the per-call override if given,
	// else the held current snapshot.
	resolveSnap := func(repoOverride, snapOverride string) (string, string, error) {
		if snapOverride != "" {
			r := repoOverride
			if r == "" {
				rr, err := mcpSnapshotRepo(ctx, sp, snapOverride)
				if err != nil {
					return "", "", err
				}
				r = rr
			}
			return r, snapOverride, nil
		}
		if cur.SnapshotID == "" {
			return "", "", fmt.Errorf("no current snapshot — call code_graph_use, or ingest a repo first")
		}
		return cur.Repo, cur.SnapshotID, nil
	}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "code-graph",
		Version: "v0.1.0",
	}, nil)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_use",
			Description: `Pin the current snapshot. EVERY other read tool (search, neighbors, get, symbols, annotations, findings) operates within the current snapshot — so they are coherent, never smeared across builds. Call this once to choose which build you are inspecting. Omit snapshot_id to pin a repo's newest snapshot; omit both to report the current pin. Individual tools still accept a per-call snapshot_id override.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in useInput) (*mcp.CallToolResult, cgContext, error) {
			if in.Repo == "" && in.SnapshotID == "" {
				return textResult(*cur), *cur, nil
			}
			if in.SnapshotID != "" {
				repo := in.Repo
				if repo == "" {
					r, err := mcpSnapshotRepo(ctx, sp, in.SnapshotID)
					if err != nil {
						return nil, cgContext{}, err
					}
					repo = r
				}
				*cur = cgContext{Repo: repo, SnapshotID: in.SnapshotID}
			} else {
				c, err := mcpLatestSnapshot(ctx, sp, in.Repo)
				if err != nil {
					return nil, cgContext{}, err
				}
				*cur = c
			}
			return textResult(*cur), *cur, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_search",
			Description: `Semantic search over Go symbols (functions, methods, types, vars, consts) ingested into the code graph. Returns top-K symbols ranked by cosine distance against the query embedding. Use to find symbols by intent ("what writes to Spanner?") rather than exact name. FQNs returned can be fed into code_graph_neighbors or code_graph_get.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in searchInput) (*mcp.CallToolResult, mcpSearchOutput, error) {
			limit := in.Limit
			if limit <= 0 {
				limit = 15
			}
			if limit > 50 {
				limit = 50
			}
			repo, snap, err := resolveSnap("", in.SnapshotID)
			if err != nil {
				return nil, mcpSearchOutput{}, err
			}
			results, err := mcpDoSearch(ctx, sp, in.Query, repo, snap, limit)
			if err != nil {
				return nil, mcpSearchOutput{}, err
			}
			return textResult(results), mcpSearchOutput{Results: results}, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_neighbors",
			Description: `Graph traversal: returns the incoming and outgoing edges of a symbol up to N hops. Edge kinds include CALLS (function calls, with interface dispatch resolved via CHA), REFERENCES (uses of types/vars/consts), IMPLEMENTS (interface satisfaction), EMBEDS (struct embedding), DECLARES (parent contains child), IMPORTS. Use after code_graph_search to understand the surrounding call graph and impact radius before editing.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in neighborsInput) (*mcp.CallToolResult, mcpNeighborsOutput, error) {
			depth := in.Depth
			if depth <= 0 {
				depth = 1
			}
			_, snap, err := resolveSnap("", in.SnapshotID)
			if err != nil {
				return nil, mcpNeighborsOutput{}, err
			}
			results, err := mcpDoNeighbors(ctx, sp, in.FQN, depth, snap)
			if err != nil {
				return nil, mcpNeighborsOutput{}, err
			}
			return textResult(results), mcpNeighborsOutput{Results: results}, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_get",
			Description: `Returns the full record for a symbol: kind, file:line range, signature, doc comment, and source body. Use to read a specific function or type without opening the file, particularly when code_graph_neighbors has identified a relevant symbol. Body may be truncated to 50KB for very large functions.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in getInput) (*mcp.CallToolResult, *SymbolDetail, error) {
			_, snap, err := resolveSnap("", in.SnapshotID)
			if err != nil {
				return nil, nil, err
			}
			s, err := mcpDoGet(ctx, sp, in.FQN, snap)
			if err != nil {
				return nil, nil, err
			}
			return textResult(s), s, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_ingest",
			Description: `Ingest a Go repository at its current HEAD commit into the code graph. Walks all .go files, extracts symbols (funcs, methods, types, vars, consts), resolves call edges including interface dispatch via CHA, embeds each unique content via Vertex AI, writes to Spanner. Subsequent ingests of overlapping commits hit the content-hash cache; typically only 5-50 symbols need re-embedding after a small commit. First-time ingest of a medium repo is ~10 minutes; cache-warm re-ingest is ~30 seconds. Streams notifications/progress as it runs.`,
		},
		func(ctx context.Context, req *mcp.CallToolRequest, in ingestInput) (*mcp.CallToolResult, *IngestStats, error) {
			progress := mcpProgressFunc(ctx, req)
			stats, err := mcpDoIngest(ctx, sp, in.RepoPath, ingestOpts{}, progress)
			if err != nil {
				return nil, nil, err
			}
			return textResult(stats), stats, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_list_commits",
			Description: `List every (repo, commit_sha) currently ingested in the code graph, with symbol count and ingest timestamp. Call this before code_graph_diff to confirm both commits are available, or before code_graph_ingest to check whether a repo+commit is already done.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, _ listCommitsInput) (*mcp.CallToolResult, listCommitsOutput, error) {
			out, err := mcpDoListCommits(ctx, sp)
			if err != nil {
				return nil, listCommitsOutput{}, err
			}
			return textResult(out), listCommitsOutput{Commits: out}, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_symbols",
			Description: `Deterministic enumeration of a snapshot's symbols — the structural counterpart to the semantic code_graph_search. Filter by kind, by a regular expression over the FQN, and by orphan status. An orphan is a content-bearing symbol with zero incoming non-DECLARES edges, excluding entrypoints (main/init/Test*) — i.e. dead code or an unwired registration. Use for exhaustive sweeps ("every opcode", "all symbols matching X") and dead-code audits that semantic search cannot do completely.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in symbolsInput) (*mcp.CallToolResult, symbolsOutput, error) {
			repo, snap, err := resolveSnap(in.Repo, in.SnapshotID)
			if err != nil {
				return nil, symbolsOutput{}, err
			}
			rows, err := mcpDoListSymbols(ctx, sp, repo, snap, in.Kind, in.Match, in.Orphans, in.Limit)
			if err != nil {
				return nil, symbolsOutput{}, err
			}
			return textResult(rows), symbolsOutput{Symbols: rows}, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_annotations",
			Description: `List the //volt: and //spec: annotations in a snapshot — the repository's sentinel-raise-site and normative-spec-link conventions (e.g. "sentinel.raise name=... spec_section=..." or "spec.implements doc_id=... section=..."). Each result carries the owning symbol's FQN, the annotation type, and the parsed name / spec_section. Filter by type or spec_section.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in annotationsInput) (*mcp.CallToolResult, annotationsOutput, error) {
			repo, snap, err := resolveSnap(in.Repo, in.SnapshotID)
			if err != nil {
				return nil, annotationsOutput{}, err
			}
			rows, err := mcpDoAnnotations(ctx, sp, repo, snap, in.Type, in.Spec)
			if err != nil {
				return nil, annotationsOutput{}, err
			}
			return textResult(rows), annotationsOutput{Annotations: rows}, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_coverage",
			Description: `Graph-derived REACHABILITY coverage of a snapshot: which production functions are reachable from a test function through the CALLS graph. This is not execution coverage (go test -coverprofile) — it cannot say a line ran or an assertion fired. It proves two structural facts no line-coverage report surfaces cleanly: (1) production functions that NO test reaches (definitely untested), and (2) hollow tests — test functions whose call closure reaches no production code at all (they run but exercise nothing). Set untested=true to list only the unreached production functions. Use to find untested code and fake/hollow tests.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in coverageInput) (*mcp.CallToolResult, *CoverageReport, error) {
			repo, snap, err := resolveSnap(in.Repo, in.SnapshotID)
			if err != nil {
				return nil, nil, err
			}
			rep, err := mcpDoCoverage(ctx, sp, repo, snap, in.Untested)
			if err != nil {
				return nil, nil, err
			}
			return textResult(rep), rep, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_diff",
			Description: `Structural diff between two ingested commits of a repo. Returns the symbols added, removed, or modified, identified by content-hash comparison (not text diff). Modified means the function or type body changed since the previous commit. Use during PR review to get an exact impact list, then feed each modified symbol into code_graph_neighbors to assess blast radius. Both commits must have been ingested.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in diffInput) (*mcp.CallToolResult, *DiffResult, error) {
			res, err := mcpDoDiff(ctx, sp, in.Repo, in.Before, in.After)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_lineage",
			Description: `The version history of a single symbol across all ingested commits. Returns each distinct body the symbol has had, oldest first, with the semantic delta (embedding cosine distance) from the prior version. A large semantic delta between consecutive versions marks a meaning change; small deltas across many versions mark a stable symbol. Survives nothing yet across renames — see code_graph_aliases once rename-stitching lands.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in lineageInput) (*mcp.CallToolResult, *LineageResult, error) {
			res, err := mcpDoLineage(ctx, sp, in.FQN)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_drift",
			Description: `Classify every symbol that changed between two commits by textual delta vs semantic delta. Quadrants: 'behavior-shift' (small text change, large meaning change — the dangerous, bug-hiding kind, review closely), 'refactor' (large text change, behavior preserved — low risk), 'rewrite' (both large), 'cosmetic' (both small). Results are ranked by semantic delta descending, so the riskiest changes are first. Use during PR review to focus attention by semantic surprise instead of line count.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in driftInput) (*mcp.CallToolResult, *DriftResult, error) {
			res, err := mcpDoDrift(ctx, sp, in.Repo, in.Before, in.After)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_findings",
			Description: `Mine a snapshot for places where graph topology and embedding semantics disagree. Returns two finding types: 'duplication' (symbols that are semantically near-identical but have no edge connecting them — undiscovered duplication or a missing abstraction) and 'misuse' (symbols connected by a CALLS/REFERENCES edge but semantically far apart — a possible leaky abstraction or misuse). Use as a periodic code-health audit; triage the top entries.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in findingsInput) (*mcp.CallToolResult, *FindingsResult, error) {
			repo, snap, err := resolveSnap(in.Repo, in.SnapshotID)
			if err != nil {
				return nil, nil, err
			}
			res, err := mcpDoFindings(ctx, sp, repo, snap, in.Limit)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_hazards",
			Description: `Detect 12 categories of production concurrency, control-flow, and resource-lifecycle hazards extracted via Go SSA + VTA + AST analysis: MAP_ESCAPE_UNDER_LOCK, SLICE_ESCAPE_UNDER_LOCK, UNBOUNDED_MAP, LOCK_ACROSS_IO (1-to-4 hop CALLS_UNDER_LOCK -> CALLS* -> IO_SINK), CYCLIC_LOCK_ORDER, TXN_CLOSURE_OUTER_MUTATION, DEFER_IN_LOOP, TIMER_IN_LOOP, DETACHES_CONTEXT, SPAWNS_UNRECOVERED_GO, UNBOUNDED_SHIFT, and UNCLOSED_RESOURCE. Pass repo_path to analyze a local Go worktree directly, or omit repo_path to query the pinned Spanner snapshot.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in hazardsInput) (*mcp.CallToolResult, *HazardsReport, error) {
			opts := HazardQueryOpts{
				Category:   in.Category,
				FilePrefix: in.FilePrefix,
				Limit:      in.Limit,
				Repo:       in.Repo,
				SnapshotID: in.SnapshotID,
				RepoPath:   in.RepoPath,
			}
			if in.RepoPath != "" {
				res, err := mcpDoHazards(ctx, sp, "local", "working-tree", opts)
				if err != nil {
					return nil, nil, err
				}
				return textResult(res), res, nil
			}
			repo, snap, err := resolveSnap(in.Repo, in.SnapshotID)
			if err != nil {
				return nil, nil, err
			}
			res, err := mcpDoHazards(ctx, sp, repo, snap, opts)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_dependents",
			Description: `Find everything that calls or references a symbol, across ALL ingested repositories, in each repo's most-recent snapshot. Unlike code_graph_neighbors (which stays within one snapshot), this spans repo boundaries — use it to answer "what breaks in other repos if I change this exported function?" before editing a shared API.`,
		},
		func(ctx context.Context, _ *mcp.CallToolRequest, in dependentsInput) (*mcp.CallToolResult, *DependentsResult, error) {
			res, err := mcpDoDependents(ctx, sp, in.FQN)
			if err != nil {
				return nil, nil, err
			}
			return textResult(res), res, nil
		},
	)

	mcp.AddTool(server,
		&mcp.Tool{
			Name:        "code_graph_hypothesize",
			Description: `Ingest a scratch Go worktree (where a proposed change has been applied) as a 'hypothetical' snapshot parented to a base commit. The hypothetical snapshot is a first-class snapshot: code_graph_diff, code_graph_neighbors, and code_graph_drift all work against it. Use to test the structural consequences of a change — new edges, dropped edges, semantic drift — before committing it. Streams notifications/progress.`,
		},
		func(ctx context.Context, req *mcp.CallToolRequest, in hypothesizeInput) (*mcp.CallToolResult, *IngestStats, error) {
			progress := mcpProgressFunc(ctx, req)
			stats, err := mcpDoIngest(ctx, sp, in.ScratchRepoPath,
				ingestOpts{kind: "hypothetical", baseSnapID: in.BaseSnapshot}, progress)
			if err != nil {
				return nil, nil, err
			}
			return textResult(stats), stats, nil
		},
	)

	return mcpRunServer(ctx, server)
}

// Output wrappers so the structured-output schema is self-describing.
type mcpSearchOutput struct {
	Results []SearchResult `json:"results"`
}
type mcpNeighborsOutput struct {
	Results []NeighborResult `json:"results"`
}

// mcpProgressFunc returns a ProgressFunc that emits notifications/progress on
// the active session whenever the client supplied a progressToken in _meta.
// Falls back to a no-op (silent) if the client didn't request progress.
func mcpProgressFunc(ctx context.Context, req *mcp.CallToolRequest) ProgressFunc {
	token := req.Params.GetProgressToken()
	if token == nil {
		return func(string, int, int, string) {}
	}
	return func(stage string, done, total int, msg string) {
		params := &mcp.ProgressNotificationParams{
			ProgressToken: token,
			Progress:      float64(done),
			Message:       stage + " " + msg,
		}
		if total > 0 {
			params.Total = float64(total)
		}
		_ = mcpNotifyProgress(ctx, req, params)
	}
}

// textResult attaches a pretty-printed JSON copy of the structured output as a
// text-content block, so agents (and humans tailing the transport) see it
// directly. The structured output is the authoritative form.
func textResult(v interface{}) *mcp.CallToolResult {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(b)}},
	}
}
