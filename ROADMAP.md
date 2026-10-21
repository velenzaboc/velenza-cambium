# code-graph ROADMAP

Path from today's state to the `VISION.md` state. Seven phases. Phase 1 is a
breaking schema change (drop + recreate + re-ingest); every later phase is
additive. Order is dependency-driven: Phase 1 unblocks everything.

## Status (2026-05-15)

- **Phases 1-6: BUILT and verified.** Phase 3's rename detection proven with a
  real `ComputeTotal`→`AggregateValues` rename; Phase 4 found a genuine
  duplicate pair (`formatCommitTimestamp` / `formatCommitTS`).
- **History backfill: BUILT** — three follow-on capabilities:
  1. *Symbol-version edge storage* — content-sourced edges live in `SymbolEdges`
     keyed by `(src_symbol_id, src_content_hash)`, written once per version and
     reused by every snapshot. A re-ingest writes only changed symbols' edges.
     Structural edges (DECLARES/IMPORTS) stay per-snapshot in `SnapshotEdges`.
  2. *Branch DAG* — `Snapshots.parent_snapshot_ids` records the true commit DAG;
     `branch` labels the ref. `lineage` reports, per version, the first commit
     **and branch** it appeared on.
  3. *Filtered replay* — `code-graph replay` walks `git rev-list --all`, skips
     commits that touch no `.go` file, supports `--every`/`--max` sampling, and
     ingests through one reusable worktree. The caches make it affordable.
- **Known tradeoff:** CHA interface-dispatch edges are frozen at first ingest of
  the source symbol-version. If the type lattice gains an implementer but the
  caller's body is unchanged, the caller's dispatch edges go slightly stale
  until its body next changes. Accepted for replay affordability.
- **Phase 7 (change streams): DEFERRED** to the Arbor stack event bus integration — the
  natural point to wire `SymbolVersions` change events into pub/sub.

## Current state (baseline)

- Tables: `SymbolBlobs` (content-addressed embeddings), `CodeSymbols`
  (per-`(repo,commit)` snapshot), `CodeEdges` (interleaved), `CodeGraph`
  property graph.
- Binary subcommands: `ingest`, `list-commits`, `search`, `neighbors`, `get`,
  `diff`, `prune`, `mcp`. Six MCP tools.
- `symbol_id = uuidv5(fqn)` (stable). `content_hash = sha256(sig+doc+body)`.
- Content-hash embedding cache verified (0 Vertex calls on cache-warm ingest).

The baseline already has axes 1-3. The roadmap adds axis 4 (time) as a
first-class dimension and the cross-axis capabilities.

---

## Phase 1 — SymbolVersions spine + Snapshot model  *(keystone)*

**Goal.** Decouple symbol-version identity from the commit. The atom becomes
`(symbol_id, content_hash)`; a commit becomes a named *set* of symbol-versions.

**Schema (drop + recreate `code-graph` DB).**
- `SymbolVersions (symbol_id, content_hash)` PK → `fqn, kind, signature,
  doc_comment, body, first_seen TIMESTAMP`. Immutable, append-only. One row per
  distinct body a symbol has ever had. `InsertOrUpdate` keeps it idempotent.
- `Snapshots (repo, snapshot_id)` PK → `label STRING, commit_sha, ingested_at,
  symbol_count, kind STRING` (`'ingested'` now; `'hypothetical'` used in P6).
- `SnapshotMembers (repo, snapshot_id, symbol_id)` PK → `fqn, content_hash,
  kind, file_path, line_start, line_end`. Interleaved in `Snapshots`
  `ON DELETE CASCADE`. (file/line are snapshot-scoped — a body can be identical
  yet sit at a different line.) `content_hash` is the FK into `SymbolVersions`.
- `SnapshotEdges (repo, snapshot_id, symbol_id, dst_symbol_id, kind)` PK,
  interleaved in `Snapshots`.
- `SymbolBlobs` unchanged (still the embedding store, keyed by `content_hash`).
- `CodeGraph` property graph redefined over `SnapshotMembers` nodes +
  `SnapshotEdges` edges.

**Code.**
- `doIngest` writes four things: `SymbolVersions` (insert-or-update per
  symbol-version), one `Snapshots` row, `SnapshotMembers`, `SnapshotEdges`.
- `diff` → join `SnapshotMembers` of two snapshots on `symbol_id`, compare
  `content_hash` (logic identical to today, table names change).
- `search` → vector-search `SymbolBlobs` → `content_hash` → join
  `SymbolVersions` / latest `SnapshotMembers` for file:line.
- `neighbors` / `get` / `list-commits` / `prune` retargeted to new tables.

**New verbs/tools.** None — this phase preserves all six existing tools.

**Verification.** Re-ingest velenza-cambium at two commits; all six MCP tools
produce output identical (modulo table rename) to today's verified results;
`diff` cross-checks against `git diff --name-status`.

**Depends on.** Nothing. Unblocks all later phases.

---

## Phase 2 — Lineage + drift verbs  *(the time axis)*

**Goal.** Make symbol history queryable. Ship the bug-hiding-change detector
(VISION #2).

**Schema.** None — `SymbolVersions` already holds the lineage.

**Code.**
- `doLineage(fqn)` — all `SymbolVersions` for `symbol_id = uuidv5(fqn)` ordered
  by `first_seen`; for each consecutive pair compute the **semantic delta** =
  `COSINE_DISTANCE` (exact) between the two versions' `SymbolBlobs` embeddings.
  Return `[{content_hash, first_seen, semantic_delta_from_prev}]`.
- `doDrift(snapA, snapB)` — for every symbol with a different `content_hash`
  between the two snapshots, compute (a) **semantic delta** = exact cosine
  between the two embeddings, (b) **textual delta** = normalized Levenshtein
  ratio over the bodies. Classify into quadrants:
  `refactor` (textual high, semantic low), `behavior-shift` (textual low,
  semantic high — the dangerous one), `rewrite` (both high),
  `cosmetic` (both low). Rank by semantic delta descending.
- New CLI verbs `lineage`, `drift`; MCP tools `code_graph_lineage`,
  `code_graph_drift`.

**New verbs/tools.** `lineage`, `drift` (CLI + MCP).

**Verification.** `lineage` of a function modified between the two test
commits shows two versions with a non-zero delta; `drift` between the commits
ranks the 73 modified symbols and the `behavior-shift` quadrant is non-empty.

**Depends on.** Phase 1.

---

## Phase 3 — Rename / move identity stitching

**Goal.** Lineage survives renames, moves, repackaging (VISION #1).

**Schema.** `SymbolAliases (repo, from_symbol_id, to_symbol_id)` PK →
`from_fqn, to_fqn, embedding_distance FLOAT64, caller_overlap FLOAT64,
detected_at`.

**Code.**
- After ingesting snapshot B, compare against the latest prior snapshot A:
  for each symbol *removed* in B and each symbol *added* in B, compute
  embedding distance (exact cosine) + caller-set overlap (Jaccard over incoming
  `SnapshotEdges` source sets). If distance < 0.05 **and** caller overlap > 0.7,
  record an alias `from_symbol_id (A) → to_symbol_id (B)`.
- `doLineage` follows alias edges transitively so a renamed function presents
  one continuous history.

**New verbs/tools.** `lineage` gains alias-following (no new verb); optional
`code_graph_aliases` MCP tool to list detected renames.

**Verification.** In a scratch worktree, rename a function + commit; ingest;
confirm `SymbolAliases` records the rename and `lineage` stitches across it.

**Depends on.** Phase 2.

---

## Phase 4 — Signal-disagreement mining

**Goal.** Generate duplication / missing-abstraction / misuse findings
(VISION #5).

**Schema.** Optional `Findings (repo, snapshot_id, finding_id)` cache table;
can also be computed on demand.

**Code.**
- `doFindings(repo, snapshot_id)`:
  - **duplication** — self-join `SymbolBlobs` on `APPROX_COSINE_DISTANCE < 0.08`
    restricted to symbols in the snapshot; drop pairs that have a connecting
    edge or a common caller.
  - **misuse / leaky abstraction** — `SnapshotEdges` whose endpoints have
    embedding distance > 0.6.
- New CLI verb `findings`; MCP tool `code_graph_findings`.

**New verbs/tools.** `findings` (CLI + MCP).

**Verification.** A known duplicate pair in velenza-cambium surfaces in the
duplication list; spot-check 5 misuse candidates by hand.

**Depends on.** Phase 1 (embeddings already present at baseline).

---

## Phase 5 — Cross-repo unified graph

**Goal.** Edges and queries span repositories (VISION #7).

**Schema.** None. `fqn` already carries the full module path, so
`symbol_id = uuidv5(fqn)` is globally unique; an edge from repo B into repo A's
exported function already resolves to A's `SymbolVersions` row once both are
ingested.

**Code.**
- Confirm multi-repo ingest writes distinct `Snapshots` per repo while sharing
  `SymbolVersions` / `SymbolBlobs`.
- New query `dependents <fqn>` — incoming `CALLS`/`REFERENCES` edges across
  *all* ingested repos, not filtered by repo.
- `neighbors` gains an `--all-repos` flag.

**New verbs/tools.** `dependents` (CLI + MCP `code_graph_dependents`).

**Verification.** Ingest velenza-canon + velenza-cambium; `dependents` on an
exported symbol lists cross-repo callers.

**Depends on.** Phase 1.

---

## Phase 6 — Hypothetical graph states

**Goal.** An agent tests a change before writing it to the real tree
(VISION #3).

**Schema.** Uses `Snapshots.kind = 'hypothetical'` from Phase 1.

**Code.**
- `hypothesize(base_snapshot, scratch_repo_path)` — ingest a scratch worktree
  (where the agent has applied a proposed change) as a snapshot tagged
  `kind='hypothetical'`, parented to `base_snapshot`. This reuses the entire
  ingest path, so edges are *correctly* recomputed (full type analysis), not
  approximated.
- `diff`, `neighbors`, `drift` already work on any snapshot, so they work on a
  hypothetical one for free.
- v2 refinement (later): in-memory synthetic version + single-symbol edge
  recompute, to avoid needing a scratch worktree. Deferred — the worktree path
  is correct and simple.

**New verbs/tools.** `hypothesize` (CLI + MCP `code_graph_hypothesize`).

**Verification.** Apply a one-function change in a scratch worktree;
`hypothesize` it; `diff base hypothetical` reports exactly that one symbol.

**Depends on.** Phases 1, 2.

---

## Phase 7 — Change-stream event source

**Goal.** The graph emits events when symbols change (VISION #8).

**Schema.** `CREATE CHANGE STREAM SymbolVersionsStream FOR SymbolVersions`.

**Code.**
- A `watch` subcommand consuming the change stream via the Spanner change
  stream reader API; emits structured events
  (`{fqn, old_hash, new_hash, semantic_delta}`).
- Optional: publish events to a Pub/Sub topic so downstream Arbor services can react
  (re-run validator, recompute blast radius, page review).

**New verbs/tools.** `watch` (CLI, long-running).

**Verification.** Ingest a new version of one symbol; observe the corresponding
event on the stream.

**Depends on.** Phase 1.

---

## Sequencing summary

```
Phase 1  (keystone, breaking)  ──┬── Phase 2 ──┬── Phase 3
                                 │             └── Phase 6
                                 ├── Phase 4
                                 ├── Phase 5
                                 └── Phase 7
```

Build order: 1 → 2 → 3 → 4 → 5 → 6 → 7. Phases 4, 5, 7 have no
inter-dependency and could be parallelized after Phase 1; kept linear here for
review simplicity.
