# velenza-cambium (`code-graph`)

**Layer 1 of the Velenza Arbor Stack** (`velenza-inarch` → `velenza-cambium` → `velenza-canon` → `velenza-trellis`).

`velenza-cambium` (compatible CLI/MCP alias: `code-graph`, Unified CLI: `inr cambium` / `inr code`) is a semantic, structural, and neuro-symbolic SSA/VTA Code Property Graph in Spanner. It ingests Go repositories (both native `.inarch` workspaces and `.git` repositories), splits them at symbol boundaries, resolves the call graph (including interface dispatch), extracts SSA/VTA production concurrency & lifecycle hazards (`hazards` / `code_graph_hazards`), embeds every symbol for semantic search, and stores everything as a versioned, queryable graph. It is exposed as a CLI (`velenza-cambium` / `code-graph` / `arbor-info`) and as an MCP server for AI agents.

**This README is the complete reference.** Companion docs: [VISION.md](VISION.md) (the
conceptual model and capability rationale), [ROADMAP.md](ROADMAP.md) (build phases),
[AGENT_DESIGN.md](AGENT_DESIGN.md) (the agent-first design principle), [RUNBOOK.md](RUNBOOK.md) (operator
steps for a full-history replay), [FIDELITY_PLAN.md](FIDELITY_PLAN.md) and [FIDELITY_PHASE_B.md](FIDELITY_PHASE_B.md) (lossless structural master and round-trip verification), and [CONTRIBUTING.md](CONTRIBUTING.md) (open-source hygiene and development workflow).

---

## The Velenza Arbor Stack & Cross-Layer Integration

`velenza-cambium` is **Layer 1** of the open-source **Velenza Arbor Stack**, a 4-layer neuro-symbolic software engineering stack for concurrent AI coding agents:

| Layer | Repository | Canonical Binary | Compatibility Alias | Umbrella CLI | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Layer 0** | [`velenzaboc/velenza-inarch`](https://github.com/velenzaboc/velenza-inarch) | `velenza-inarch` | `inr` | `inr` | **The Graft Union & Rootstock** — Checkout-free content-addressed VCS (`BonsaiChangeset`, 64-char BLAKE3 changeset IDs, client-side rebase, JIT leases) |
| **Layer 1** | [`velenzaboc/velenza-cambium`](https://github.com/velenzaboc/velenza-cambium) | `velenza-cambium` | `code-graph` | `inr cambium` / `inr code` | **The Living Vascular Code Tissue** — CPG/SSA/VTA semantic code graph and deterministic production hazard analyzer (`code_graph_hazards`) |
| **Layer 2** | [`velenzaboc/velenza-canon`](https://github.com/velenzaboc/velenza-canon) | `velenza-canon` | `spec-graph` | `inr canon` / `inr spec` | **The Normative Root Canon** — Versioned architectural specification graph and requirement-to-symbol governance (`trace-code`) |
| **Layer 3** | [`velenzaboc/velenza-trellis`](https://github.com/velenzaboc/velenza-trellis) | `velenza-trellis` | `task-graph` | `inr trellis` / `inr task` | **The Structural Growth Trellis** — Multi-agent execution DAG, TDD receipts, and cryptographic worklink provenance (`fg-validator-runner`) |

### How `velenza-cambium` Integrates Across the Stack

1. **With Layer 0 ([`velenza-inarch`](https://github.com/velenzaboc/velenza-inarch)):**
   - **Native `.inarch` Workspace Ingestion:** `velenza-cambium ingest` automatically detects `.inarch/refs/` (via `gitMeta` fallback) and ingests 64-character BLAKE3 `BonsaiChangeset` IDs into `Snapshots.commit_sha` (`STRING(64)`) alongside standard 40-character Git SHA-1 commits.
   - **Umbrella CLI Delegation & Shared Env:** Invoking `inr cambium <subcommand>` (or `inr code <subcommand>`) delegates directly to `velenza-cambium` and propagates `ARBOR_GCP_PROJECT` and `ARBOR_SPANNER_INSTANCE` to `VELENZA_CAMBIUM_*` / `CODE_GRAPH_*`.
2. **With Layer 2 ([`velenza-canon`](https://github.com/velenzaboc/velenza-canon)):**
   - **Spec & Sentinel Annotation Extraction:** During AST/SSA/VTA ingestion (`walkRepo`), `velenza-cambium` extracts `//spec:<REQ-ID>` and `//volt:<SENTINEL>` comments on functions, methods, and types into `SnapshotMembers` / `annotations` (`code_graph_annotations`), enabling `velenza-canon trace-code` (`spec_graph_trace_code`) to verify bidirectional requirement-to-symbol coverage.
3. **With Layer 3 ([`velenza-trellis`](https://github.com/velenzaboc/velenza-trellis)):**
   - **Hazard-Gated Task Delivery & Worklink Provenance:** Before an agent marks a task complete or binds a changeset worklink in `velenza-trellis`, `velenza-cambium hazards --analyze <repo>` (`code_graph_hazards`) verifies zero SSA/VTA lock-order, goroutine-leak, or map-escape-under-lock hazards on the target snapshot.

### Arbor Descriptor (`velenza-cambium arbor-info --json`)

Running `velenza-cambium arbor-info --json` (or `code-graph arbor-info --json` / `inr cambium arbor-info --json`) emits the canonical Layer 1 descriptor from `BuildCambiumArborDescriptor()` in `cambium_arbor.go`:

```json
{
  "layer": 1,
  "name": "velenza-cambium",
  "alias": "code-graph",
  "canonical_binary": "velenza-cambium",
  "legacy_alias": "code-graph",
  "umbrella_subcommands": [
    "inr cambium",
    "inr code"
  ],
  "inarch_native_vcs": true,
  "config_env_vars": [
    "VELENZA_CAMBIUM_GCP_PROJECT",
    "VELENZA_CAMBIUM_PROJECT",
    "VELENZA_CAMBIUM_SPANNER_INSTANCE",
    "VELENZA_CAMBIUM_INSTANCE",
    "VELENZA_CAMBIUM_SPANNER_DATABASE",
    "VELENZA_CAMBIUM_DATABASE",
    "CODE_GRAPH_GCP_PROJECT",
    "CODE_GRAPH_SPANNER_INSTANCE",
    "CODE_GRAPH_SPANNER_DATABASE",
    "ARBOR_GCP_PROJECT",
    "ARBOR_SPANNER_INSTANCE"
  ],
  "vcs_backends": [
    "inarch",
    "git"
  ],
  "hazard_category_count": 12
}
```

---

## 1. Concept

The unit of versioning is the **symbol-version**: an `(symbol_id, content_hash)`
pair. Every distinct body a function/type/var has ever had is one immutable
row. A git commit is demoted to a **snapshot** — a named *set* of
symbol-versions. This makes the codebase a 4-dimensional object:

| Axis | What | Stored in |
|---|---|---|
| Symbols | every func/method/type/field/var/const | `SymbolVersions` |
| Topology | typed edges (CALLS/REFERENCES/IMPLEMENTS/EMBEDS/DECLARES/IMPORTS) | `SymbolEdges` + `SnapshotEdges` |
| Semantics | a 3072-dim embedding per unique symbol body | `SymbolBlobs` |
| Time | the sequence of content-hashes an FQN has carried | `SymbolVersions` × `Snapshots` |

`symbol_id = uuidv5(fqn)` — globally stable, so edges resolve across repos and
re-ingest is idempotent. `content_hash = sha256(signature + doc + body)`.

---

## 2. Quick start

```bash
# Build (needs Go 1.27+)
go build -o bin/velenza-cambium .
go build -o bin/code-graph .

# Auth: exact owner-qualified service-account identity, keylessly impersonated.
# Provision and verify the identity and grants through owner-managed infrastructure
# before use; this source configuration does not prove live IAM state.
export VELENZA_CAMBIUM_GCP_PROJECT='your-gcp-project-id'
export VELENZA_CAMBIUM_SPANNER_INSTANCE='your-spanner-instance-id'
export VELENZA_CAMBIUM_SPANNER_DATABASE='code-graph'
export CODE_GRAPH_GCP_PROJECT="$VELENZA_CAMBIUM_GCP_PROJECT"
export CODE_GRAPH_SPANNER_INSTANCE="$VELENZA_CAMBIUM_SPANNER_INSTANCE"
export CODE_GRAPH_SPANNER_DATABASE="$VELENZA_CAMBIUM_SPANNER_DATABASE"
export CODE_GRAPH_CREDENTIALS_SOURCE=application_default
export CODE_GRAPH_CREDENTIALS_PROFILE=impersonated_service_account
export CODE_GRAPH_CREDENTIALS_IDENTITY="code-graph-operator@${CODE_GRAPH_GCP_PROJECT}.iam.gserviceaccount.com"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE CLOUDSDK_CONFIG \
  EXPERIMENTAL_GOOGLE_API_USE_S2A GCE_METADATA_HOST GRPC_BINARY_LOG_FILTER \
  GRPC_ENFORCE_ALPN_ENABLED HTTP_PROXY HTTPS_PROXY NO_PROXY \
  GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB GOOGLE_API_GO_EXPERIMENTAL_ENABLE_NEW_AUTH_LIB \
  GOOGLE_API_CERTIFICATE_CONFIG GOOGLE_API_USE_CLIENT_CERTIFICATE \
  GOOGLE_API_USE_MTLS GOOGLE_API_USE_MTLS_ENDPOINT \
  GOOGLE_AUTH_TRUST_BOUNDARY_ENABLED GOOGLE_CLOUD_DISABLE_DIRECT_PATH \
  GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS GOOGLE_CLOUD_QUOTA_PROJECT GOOGLE_CLOUD_UNIVERSE_DOMAIN \
  GOOGLE_SPANNER_DISABLE_DIRECT_ACCESS_BOUND_TOKEN GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS \
  GOOGLE_SPANNER_ENABLE_GCP_FALLBACK GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API \
  SSL_CERT_DIR SSL_CERT_FILE SPANNER_EMULATOR_HOST SPANNER_MONITORING_HOST \
  http_proxy https_proxy no_proxy
gcloud auth application-default login \
  --impersonate-service-account="$CODE_GRAPH_CREDENTIALS_IDENTITY"

# After rejecting launch-time overrides, code-graph pins
# GOOGLE_API_USE_CLIENT_CERTIFICATE=false and GOOGLE_API_USE_MTLS_ENDPOINT=never.
# That also disables the pinned transport's automatic
# ~/.config/gcloud/certificate_config.json discovery path.
# GODEBUG may retain unrelated Go runtime tuning, but it must not contain
# http2debug=, tlsmlkem=0, or tlssecpmlkem=0. The binary rejects those values
# before resolving credentials so bearer material cannot enter HTTP/2 debug
# logs and the Go 1.27 default hybrid ML-KEM key exchanges cannot be disabled.

# The ordinary-operator contract excludes database create/drop permissions.
# `create-db` is an owner-governed bootstrap command requiring a separately
# qualified schema-administration identity; normal ingestion starts here:

# Ingest a Go repo at its current HEAD
bin/code-graph ingest /path/to/go/repo

# Pin the build you're inspecting — every query then operates within it
bin/code-graph use velenza-cambium              # newest snapshot of the repo
bin/code-graph use velenza-cambium <commit-sha> # a specific build

# Query (all run against the pinned snapshot)
bin/code-graph search "what writes to the database"
bin/code-graph neighbors <fqn>

# Zero-Spanner local SSA/VTA hazard audit on any Go workspace
bin/velenza-cambium hazards --analyze /path/to/go/repo
```

**Snapshot coherence.** Every read — `search`, `neighbors`, `get`, `symbols`,
`annotations`, `findings`, `hazards` — operates within ONE current snapshot. There is no
per-symbol "latest" (which would smear results across builds). The current
snapshot is the explicit `--snapshot` override, else the `use`-pinned snapshot,
else the newest ingested one. `diff`/`drift`/`lineage` are inherently
multi-snapshot and take explicit commits.

Requirements: Go 1.27+, `inr` or `git`, network to `*.googleapis.com`. The target
Spanner database is configured via `VELENZA_CAMBIUM_GCP_PROJECT`,
`VELENZA_CAMBIUM_SPANNER_INSTANCE`, and `VELENZA_CAMBIUM_SPANNER_DATABASE` (or the
compatibility aliases `CODE_GRAPH_GCP_PROJECT`, `CODE_GRAPH_SPANNER_INSTANCE`,
`CODE_GRAPH_SPANNER_DATABASE`, `ARBOR_GCP_PROJECT`, and `ARBOR_SPANNER_INSTANCE`).

---

## 3. Data model

Eleven tables (`schema.sql`) — 7 core symbol-graph tables plus 4 Fidelity Layer tables (see [FIDELITY_PLAN.md](FIDELITY_PLAN.md) and [FIDELITY_PHASE_B.md](FIDELITY_PHASE_B.md)). GoogleSQL dialect.

### SymbolBlobs — content-addressed embedding cache
| Column | Type | Notes |
|---|---|---|
| `content_hash` | STRING(64) PK | sha256 of the embed text |
| `embedding` | ARRAY<FLOAT32>(3072) | nullable; vector index lives here |
| `created_at` | TIMESTAMP | |

Global, deduplicated across all repos and commits. A body embedded once is
never embedded again. Owns `SymbolBlobsEmbeddingIdx` (COSINE vector index).

### SymbolVersions — the immutable spine
| Column | Type | Notes |
|---|---|---|
| `symbol_id` | STRING(36) PK | uuidv5(fqn) |
| `content_hash` | STRING(64) PK | |
| `fqn`, `kind` | STRING | kind ∈ func/method/type/field/const/var, plus the non-Go kinds table/column/index/conformance-pass |
| `signature`, `doc_comment`, `body` | STRING(MAX) | |
| `created_at` | TIMESTAMP | |

One row per distinct body a symbol has had. Content-bearing symbols only
(packages/files have no body and live only in `SnapshotMembers`).

### SymbolEdges — content-sourced edges, version-keyed
| Column | Type | Notes |
|---|---|---|
| `src_symbol_id` | STRING(36) PK | |
| `src_content_hash` | STRING(64) PK | |
| `dst_symbol_id` | STRING(36) PK | uuidv5 of the callee/referent FQN |
| `kind` | STRING(32) PK | CALLS / REFERENCES / IMPLEMENTS / EMBEDS |

Edges are keyed by the *source symbol-version* so a snapshot's edge set is just
`SymbolEdges` joined to that snapshot's members. `SymbolEdgesByDst` indexes the
reverse direction.

**Edges are recomputed and rewritten on every ingest — there is no edge cache.**
An edge set is *not* a pure function of the source body bytes: call-graph edges
depend on the whole-program type lattice, and AST edges depend on the package
type-checking cleanly. A cache keyed on `(src_symbol_id, src_content_hash)`
therefore froze stale/degraded edge sets — an ingest that hit a partial
type-check cached an incomplete set, and every later ingest skipped that version
because its body hash was unchanged, producing **false orphans**. `writeSymbolEdges`
deletes each touched source-version's rows and re-inserts the freshly-derived
set every ingest. Only `SymbolBlobs` (embeddings — a genuine pure function of
body text) is cached for replay affordability.

### Snapshots — a named set of symbol-versions
| Column | Type | Notes |
|---|---|---|
| `repo`, `snapshot_id` | STRING PK | snapshot_id = commit SHA (ingested) or generated (hypothetical) |
| `label`, `commit_sha` | STRING | |
| `commit_time` | TIMESTAMP | git committer time — drives lineage ordering |
| `kind` | STRING(20) | `ingested` or `hypothetical` |
| `base_snapshot_id` | STRING(64) | parent for hypothetical snapshots |
| `parent_snapshot_ids` | ARRAY<STRING> | the git commit DAG (2+ for a merge) |
| `branch` | STRING(256) | ref the snapshot was ingested under |
| `symbol_count`, `ingested_at` | | |

### SnapshotMembers — which symbol-version of each FQN is in a snapshot
Interleaved in `Snapshots` (cascade delete). Columns: `repo`, `snapshot_id`,
`symbol_id`, `fqn`, `content_hash` (nullable — null for package/file), `kind`,
`file_path`, `line_start`, `line_end`. Indexes: by fqn, by content_hash, by
symbol_id.

### SnapshotEdges — structural edges, snapshot-scoped
Interleaved in `Snapshots`. Holds only `DECLARES` and `IMPORTS` — edges whose
source is a package or file (no content body, so they cannot be version-keyed).
Small (~5k/snapshot). Columns: `repo`, `snapshot_id`, `symbol_id`,
`dst_symbol_id`, `kind`.

### SymbolAliases — detected rename/move links
Columns: `repo`, `from_symbol_id`, `to_symbol_id`, `from_fqn`, `to_fqn`,
`embedding_distance`, `caller_overlap`, `detected_at`. Populated by
rename-detection at ingest (see §6).

### Fidelity Layer Tables (`FileContents`, `SnapshotFiles`, `DeclSource`, `GoFileChunks`)
- `FileContents` — Content-addressed chunked raw file bytes (`(content_sha, chunk_index)` PK, 4 MiB chunks) for non-Go files and the Phase A byte-exact ingest oracle.
- `SnapshotFiles` — Interleaved in `Snapshots`; maps `(repo, snapshot_id, file_path)` to `(content_sha, mode, size_bytes)`.
- `DeclSource` — Content-addressed exact top-level Go declaration/chunk text (`content_sha` PK) for Phase B structural editing.
- `GoFileChunks` — Interleaved in `Snapshots`; ordered top-level `.go` chunks (`package`, `import`, `decl`, `comment`, `raw`) per file for gofmt-canonical structural rendering (`manifest --from-structure` / `verify --from-structure`). See [FIDELITY_PLAN.md](FIDELITY_PLAN.md) and [FIDELITY_PHASE_B.md](FIDELITY_PHASE_B.md).

**A snapshot's full edge set** = `SymbolEdges` joined to that snapshot's members
on `(symbol_id, content_hash)` ∪ `SnapshotEdges` for that snapshot.

---

## 4. The ingest pipeline

`ingest <repo-path>` runs `doIngest`, which:

1. **Resolve** — repo name from the shared git dir (`git rev-parse
   --git-common-dir`, so worktrees of one repo agree); commit SHA, commit time,
   parent SHAs, branch.
2. **Walk** — `golang.org/x/tools/go/packages.Load` type-checks the whole
   module with `Tests:true`, so `_test.go` files are walked too. Each `*ast`
   declaration becomes a `Symbol`; `printer.Fprint` renders signature and body.
   `Tests:true` returns several variants per testable package (`foo`,
   `foo [foo.test]`, `foo_test`, the synthesized `foo.test` main) — exactly one
   per `PkgPath` is chosen (the test-augmented variant when present, synthesized
   `*.test` mains skipped), so each physical file is ingested once. Test-ness is
   derived from the `_test.go` path suffix — no schema column.
   Struct fields are ingested too: each named field of a `struct` type becomes a
   `field` symbol (FQN `pkg.Type.field`), so "is this field wired / read", "what
   writes `X`" are graph queries — the layer that makes generated-code (ASG→Go)
   verification possible.
3. **Edges**:
   - AST pass — `DECLARES`, `IMPORTS` (file → imported package, so "which files
     import X" is a query), `EMBEDS`, plus `CALLS`/`REFERENCES` from each
     function body via `types.Info.Uses`. Field accesses produce a
     `REFERENCES` edge from the enclosing function to the `field` symbol:
     selector reads/writes `x.f` via `types.Info.Selections` (FieldVal), and
     keyed struct literals `T{F: v}` via the composite-literal keys. (v1 emits
     one `REFERENCES` kind for any field use — a read/write split is future
     work; a promoted field access attributes to the outer type, not the inner
     declaring type — the edge dangles for promoted accesses.)
   - `REFERENCES`/`CALLS` edges resolve only to *package-scoped* symbols —
     function-local variables, parameters and named results are below the
     symbol boundary and produce no edge (emitting one would be a dangling,
     name-colliding false edge).
   - `IMPLEMENTS` — `types.Implements` over every named type vs every interface.
   - **VTA pass** — `golang.org/x/tools/go/callgraph/vta` builds SSA and
     resolves the call graph including interface and function-value dispatch.
     VTA (Variable Type Analysis) refines a `cha.CallGraph` bootstrap: CHA alone
     is sound but resolves a call through a func value or interface to *every*
     type-compatible target; VTA narrows that with value-flow, dropping the
     spurious edges. Edges whose source is not a first-party declared symbol
     (stdlib callers) are filtered out.
4. **Content hash** — `content_hash = sha256(buildEmbedText)` where
   `buildEmbedText = signature + doc + body`, clipped to 6000 chars.
5. **Embedding cache check** — `selectMissingBlobs` queries `SymbolBlobs` for
   hashes already present; only misses are embedded (Vertex AI, batches of 100,
   `gemini-embedding-2-preview`, 3072 dims, `RETRIEVAL_DOCUMENT`). Embeds are written
   to `SymbolBlobs`.
6. **Edge split** — content-sourced edges → `SymbolEdges`; structural edges
   → `SnapshotEdges`. Edges are recomputed and rewritten every ingest
   (delete-then-insert per source-version) — there is no edge cache; see §5.
7. **Write** — `Snapshots` row first (interleaved-parent requirement), then
   `SymbolVersions`, `SnapshotMembers`, `SymbolEdges`, `SnapshotEdges`.
8. **Rename detection** — `detectAliases` (§6), for `kind=ingested` only.

Re-ingesting an unchanged repo re-embeds nothing (the embedding cache hits) but
always rewrites edges fresh.

---

## 5. The embedding cache — why replay is affordable

| Cache | Keyed by | Effect |
|---|---|---|
| `SymbolBlobs` | `content_hash` | a body is embedded once, ever, globally |

The embedding is the expensive part of ingest — a Vertex AI call, rate-limited.
It is cached because an embedding genuinely *is* a pure function of the symbol's
own body text: same body bytes → same vector, always. Re-ingesting commit N+1
after commit N only embeds symbols whose body changed; a 5-commits-apart ingest
of velenza-cambium hit 3,295/3,445 blob-cache.

**Edges are deliberately NOT cached.** An edge set is not a pure function of the
source body: call-graph interface-dispatch edges depend on the whole-program type
lattice (a new implementer changes a caller's dispatch edges with its body
unchanged), and AST edges depend on the package type-checking cleanly (a partial
type-check yields an incomplete set). An earlier version *did* cache edges keyed
on `(src_symbol_id, src_content_hash)` — and a degraded ingest froze an
incomplete edge set that every later ingest skipped over, producing false
orphans (`EnforcePolicy` flagged ORPHAN despite a static call at
`cmd/server.run`). `writeSymbolEdges` now deletes and re-derives every touched
source-version's edges on every ingest. Edge compute (the AST + VTA walk) runs
every ingest regardless of any cache, so the only thing dropped is the stale
Spanner rows — correctness for free.

---

## 6. Rename / move detection (`detectAliases`)

After ingesting a snapshot, it is compared to the prior snapshot (greatest
`commit_time` strictly earlier). For each symbol *removed* and each symbol
*added*, two independent signals are computed:

- **embedding distance** — exact cosine between the two bodies' embeddings.
- **caller overlap** — Jaccard of the two symbols' caller-id sets.

A pair is recorded as an alias in `SymbolAliases` when
`embedding_distance < 0.25` **and** `caller_overlap > 0.70`. The embedding
bound is deliberately loose (a rename changes a few identifiers); caller
overlap is the strong discriminator. `lineage` follows alias chains so a
renamed symbol's history stays continuous.

---

## 7. CLI reference

Every command requires exactly one validated Google credential selector:
exactly one carrier — `CODE_GRAPH_CREDENTIALS_SOURCE=application_default` (the
well-known gcloud ADC location) or `CODE_GRAPH_CREDENTIALS_FILE=<absolute
path>` (an explicit impersonated-ADC document, for a machine whose single
well-known slot belongs to another tool) — one supported profile
(`impersonated_service_account` for local operation or
`attached_service_account` on Google-hosted compute), and the exact
`CODE_GRAPH_CREDENTIALS_IDENTITY` service-account email. Setting both carriers
is refused, and `attached_service_account` refuses the file carrier entirely.
Both carriers receive byte-identical validation; the explicit one is not a
relaxed arm. Exportable service account keys and ambient credential-file
overrides (`GOOGLE_APPLICATION_CREDENTIALS`, `CLOUDSDK_CONFIG`,
`CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`) are rejected on both. See RUNBOOK §
"Sharing a machine with another impersonating tool".

| Command | Purpose |
|---|---|
| `create-db [--drop] [schema.sql]` | Owner-governed bootstrap only: create the Spanner database from a schema file. `--drop` recreates. It requires a separately qualified schema-administration identity; the ordinary-operator contract excludes create/drop. |
| `migrate <ddl-file>` | Apply additive DDL (ADD COLUMN / CREATE TABLE / CREATE INDEX) to the existing database — no drop, no data loss. |
| `use <repo> [<snapshot>]` | Pin the current snapshot (all reads operate within it). Snapshot omitted = the repo's newest. |
| `context` | Show the currently-pinned snapshot. |
| `ingest <repo-path>` | Ingest a Go repo at its current HEAD. |
| `replay [flags] <repo-path>` | Ingest a filtered subset of a repo's full history. See §10. |
| `update <repo-path>` | Compare every branch tip to the graph and ingest only the missing tips. The keep-current tool — O(branches), near-instant when nothing changed. |
| `list-commits` | List every ingested (repo, snapshot) with kind, symbol count, timestamps. |
| `search [--limit N] "<query>"` | Semantic search over symbols. Default limit 15. |
| `neighbors [--depth N] [--snapshot S] <fqn>` | Graph traversal, 1–3 hops. Incoming + outgoing edges. `--snapshot` pins traversal to a specific build. |
| `get [--snapshot S] <fqn>` | Full record for a symbol: signature, doc, body, file:line. `--snapshot` reads it as it is in a specific build. |
| `symbols [--kind K] [--match RE] [--orphans] [--limit N] [--snapshot S]` | Deterministic symbol enumeration — filter by kind, FQN regex, orphan status. An orphan with a test caller but no production caller is marked `TEST-ONLY`. Runs against the current snapshot. |
| `annotations [--type T] [--spec S] [--snapshot S]` | List `//volt:` annotations (sentinel raise sites) with owning symbol, name, spec_section. |
| `coverage [--untested] [--snapshot S]` | Reachability coverage — which production functions a test reaches through the CALLS graph, plus hollow tests (tests reaching no production code). `--untested` lists only the unreached. |
| `manifest <repo> <snapshot> <out-dir>` | Fidelity layer — recreate a snapshot's source tree on disk from the captured file content (or `--from-structure` from `GoFileChunks`). |
| `verify <repo> <snapshot> <original-dir>` | Fidelity round-trip check — diff a snapshot's captured content against an on-disk tree (build outputs excluded; supports `--from-structure`). |
| `serve [--addr :8080] [--oidc-audience URL]` | Run the HTTP service exposing `/ingest` and `/manifest` with OIDC authentication. |
| `findings [--limit N] [--snapshot S]` | Duplication + misuse mining over production symbols (test files excluded) of the current snapshot. |
| `hazards [--repo-path\|--analyze\|--dir <path>] [--category C] [--file-prefix P] [--limit N] [--snapshot S]` | Neuro-symbolic SSA/VTA production hazard query over the current Spanner snapshot, or local zero-Spanner static analysis when `--repo-path` / `--analyze` / `--dir` is supplied. |
| `arbor-info [--json]` | Emit the canonical Velenza Arbor Layer 1 JSON descriptor (`velenza-cambium`). |
| `diff <repo> <before> <after>` | Structural diff between two snapshots: added/removed/modified. |
| `drift <repo> <before> <after>` | Changed symbols ranked by semantic surprise, quadrant-classified. |
| `lineage <fqn>` | Version history of a symbol; follows rename aliases. |
| `dependents <fqn>` | Cross-repo callers/referencers of a symbol. |
| `hypothesize <base-snapshot> <scratch-repo-path>` | Ingest a scratch worktree as a hypothetical snapshot. |
| `prune <repo> <snapshot-id>` | Delete a snapshot (partitioned DML). |
| `mcp` | Run the stdio MCP server. |

Flags must precede positional arguments (Go `flag` stops at the first non-flag).

---

## 8. MCP reference

`velenza-cambium mcp` (or `code-graph mcp`) runs a stdio MCP server (`github.com/modelcontextprotocol/go-sdk`).
Register it in `~/.claude/settings.json` under `mcpServers`. Sixteen tools,
each returns structured JSON:

| Tool | Input | Returns |
|---|---|---|
| `code_graph_use` | `repo?`, `snapshot_id?` | pins the current snapshot for the session — all reads then operate within it |
| `code_graph_ingest` | `repo_path` | IngestStats. Streams `notifications/progress`. |
| `code_graph_list_commits` | — | all ingested snapshots |
| `code_graph_search` | `query`, `limit?`, `snapshot_id?` | top-K symbols: fqn, kind, file:line, distance |
| `code_graph_neighbors` | `fqn`, `depth?` | incoming + outgoing edges with kind |
| `code_graph_get` | `fqn` | full symbol record |
| `code_graph_diff` | `repo`, `before_commit`, `after_commit` | added / removed / modified |
| `code_graph_drift` | `repo`, `before_commit`, `after_commit` | changed symbols, quadrant-classified |
| `code_graph_lineage` | `fqn` | version history with semantic deltas |
| `code_graph_findings` | `repo`, `snapshot_id`, `limit?` | duplication + misuse |
| `code_graph_dependents` | `fqn` | cross-repo callers |
| `code_graph_symbols` | `repo`, `snapshot_id`, `kind?`, `match?`, `orphans?`, `limit?` | deterministic symbol enumeration |
| `code_graph_annotations` | `repo`, `snapshot_id`, `type?`, `spec?` | `//volt:` and `//spec:` annotations |
| `code_graph_coverage` | `repo`, `snapshot_id`, `untested?` | reachability coverage: production funcs reached by a test + hollow tests |
| `code_graph_hazards` | `repo?`, `snapshot_id?`, `repo_path?`, `category?`, `file_prefix?`, `limit?` | 12-category SSA/VTA production concurrency, transaction, and resource-lifecycle hazard findings |
| `code_graph_hypothesize` | `base_snapshot`, `scratch_repo_path` | IngestStats. Streams progress. |

`code_graph_get` and `code_graph_neighbors` also take an optional `snapshot_id` to pin the read to a specific build.

`create-db`, `prune`, `replay` are CLI-only (admin / long-batch operations).

---

## 9. Capabilities in depth

### search
Embeds the query (`RETRIEVAL_QUERY`), vector-searches `SymbolBlobs`
(`APPROX_COSINE_DISTANCE`, ANN index), then expands top-K content hashes to
`SnapshotMembers` for fqn/file/line. Finds symbols by meaning, not name.

### neighbors
Loads the snapshot's full edge set (union of the two edge tables) and member
metadata, then BFS in memory for `depth` hops (max 3). `CALLS` edges are
VTA-resolved — they include interface and function-value dispatch callees a
text search cannot find. Edge kinds: `CALLS`, `REFERENCES`, `IMPLEMENTS`,
`EMBEDS`, `DECLARES`, `IMPORTS`.

### diff
`FULL OUTER JOIN` of two snapshots' members on `symbol_id`, comparing
`content_hash`. Identity-based, not text-based — a function moved between files
unchanged shows as neither added nor removed.

### drift
For each modified symbol, two deltas:
- **semantic delta** — exact cosine between the old and new embeddings.
- **textual delta** — `1 − tokenJaccard(oldBody, newBody)`.

Quadrants (thresholds: semantic 0.15, textual 0.30):
| Quadrant | textual | semantic | meaning |
|---|---|---|---|
| `behavior-shift` | low | high | small edit, big meaning change — the bug-hiding kind |
| `refactor` | high | low | large edit, behavior preserved — low risk |
| `rewrite` | high | high | |
| `cosmetic` | low | low | |

Results are ranked by semantic delta descending.

### lineage
Resolves the symbol, follows `SymbolAliases` chains transitively, gathers every
snapshot containing any symbol in the chain, orders by `commit_time`, collapses
consecutive identical content-hashes into versions. Per version: first commit,
first branch, semantic delta from the previous version, snapshot count.

### findings
Two signal-disagreement queries over a snapshot. Both operate on **production
symbols only** — `_test.go` symbols are excluded, because table-driven tests are
near-identical by nature and would otherwise flood the duplication ranking.
- **duplication** — symbol pairs with embedding distance `< 0.08` and **no**
  connecting edge. Brute-force O(n²) cosine, parallelized across cores.
- **misuse** — `CALLS`/`REFERENCES` edges whose endpoints have embedding
  distance `> 0.60` (a function calling something semantically unrelated).

### dependents
Cross-repo: callers/referencers of a symbol across every ingested repo's most
recent snapshot. Works because `symbol_id` is global — an edge from repo B into
repo A's exported function already targets the same `symbol_id`.

### hypothesize
Ingests a scratch worktree (where a proposed change has been applied) as a
`kind=hypothetical` snapshot parented to a base. `diff`/`neighbors`/`drift` then
work against it — test a change's structural consequences before committing it.

---

## 9b. Non-Go evidence

Ingest also captures evidence beyond Go source, as first-class snapshot symbols:

- **DDL** — `.sql` files are parsed into `table` / `column` / `index` symbols
  (FQN prefix `ddl:`). A table's body is its `CREATE TABLE` statement; an
  index's body is its `CREATE INDEX`. Edges: `table —DECLARES→ column`,
  `index —INDEXES→ table`. Lets the graph answer schema-shape questions —
  `get ddl:index:<name>` shows uniqueness / `NULL_FILTERED` / columns;
  `symbols --kind index` enumerates them.
- **Conformance manifest** — `conformance_manifest.json` is parsed into one
  `conformance-pass` symbol per pass (FQN prefix `conformance:`); the body is
  the pass's conformance facts.

These flow through the same `SnapshotMembers` / `SymbolVersions` model — so
`get`, `search`, `symbols`, `diff`, `drift`, `lineage` all work on schema and
manifest entries exactly as on Go symbols.

## 10. replay — history backfill

`replay [flags] <repo-path>` ingests a filtered subset of a repo's full commit
history across all branches.

| Flag | Default | Meaning |
|---|---|---|
| `--go-only` | true | only commits that touch a `.go` file |
| `--every N` | 1 | sample every Nth commit |
| `--max N` | 0 | keep only the most recent N sampled commits |
| `--workers N` | 0 (auto) | max concurrent ingests |
| `--min-free-gb F` | 4.0 | governor: pause new ingests below this free RAM |
| `--shard i/n` | — | process shard i of n — for splitting across **machines** |

It enumerates `git rev-list --reverse --all`, samples, and ingests through an
in-process **worker pool**. Already-ingested commits are skipped, so replay is
**resumable** — re-run the same command to continue.

**The governor.** Each ingest holds ~3 GB RAM (the whole typed program + SSA).
Concurrency is therefore memory-bound, not core-bound:
- `--workers 0` auto-picks `min(cores, freeRAM / 3.5GB)`.
- Before each ingest, `admitIngest` blocks while free RAM is below
  `--min-free-gb`; admission is serialized so a burst of workers cannot
  collectively OOM the machine. One ingest is always allowed (no deadlock).

Realistic concurrency: 16 GB → ~3 workers; 32 GB → ~7-8; 64 GB → ~14-16.

**`--shard` is for multiple machines**, not within one — the worker pool
already parallelizes within a machine. Each shard takes a contiguous block so
its caches warm internally.

See `RUNBOOK.md` for the full operator procedure.

---

## 11. Parameters reference

| Parameter | Value | Where |
|---|---|---|
| Embedding model | `gemini-embedding-2-preview`, 3072 dims | Vertex AI |
| Embed task types | `RETRIEVAL_DOCUMENT` (symbols), `RETRIEVAL_QUERY` (search) | |
| Embed batch size | 100 | |
| Embed retry | 7 attempts, exponential backoff + jitter, on 429/5xx | `callEmbed` |
| Embed input clip | 6000 chars | `embedMaxChars` |
| Body store clip | 50000 bytes | `bodyMaxBytes` |
| Spanner mutation batch | 200 | `mutationBatch` |
| Alias: max embedding distance | 0.25 | `aliasMaxEmbeddingDistance` |
| Alias: min caller overlap | 0.70 | `aliasMinCallerOverlap` |
| Drift: semantic-high threshold | 0.15 | `semanticHighThreshold` |
| Drift: textual-high threshold | 0.30 | `textualHighThreshold` |
| Findings: duplication max distance | 0.08 | `duplicationMaxDistance` |
| Findings: misuse min distance | 0.60 | `misuseMinDistance` |
| Replay: per-ingest RAM estimate | 3.5 GB | `perIngestGB` |
| `symbol_id` | uuidv5(fqn), namespace `c0de9aaf-…0001` | `uuidFromFQN` |
| `content_hash` | sha256(signature + doc + body) | `sha256Hex` |

---

## 12. Known limitations

- **Go only.** The walker uses `go/packages`; non-Go files are not ingested.
- **Reachability, not execution.** `coverage` proves a test *can reach* a
  symbol through the call graph — not that a line ran or an assertion is
  correct. It finds untested code and hollow tests; it does not replace
  `go test -coverprofile` for branch-level execution coverage.
- **VTA is sound-leaning, not exact.** VTA narrows CHA's over-approximation of
  interface/function-value dispatch but is still a static over-approximation;
  a CALLS edge it reports through dynamic dispatch is possible, not certain.
- **Field reachability is static-only.** `field` symbols and field-access edges
  capture selector and composite-literal uses, but NOT reflective population
  (`json.Unmarshal`, Spanner row decode, `reflect`). A field set only
  reflectively shows zero incoming edges — so `field` is deliberately excluded
  from the default `--orphans` sweep (it would false-positive on every
  JSON/Spanner-decoded struct). Ask `neighbors <pkg.Type.field>` directly to
  judge a specific field; do not bulk-sweep fields for deadness.
- **Locals are not symbols.** Function-local variables are below the symbol
  boundary and not ingested — `go build` already makes an unused local a
  compile error, so the graph need not track them.
- **Snapshot vs working tree.** The graph reflects ingested snapshots. For the
  exact uncommitted working-tree state, read files directly.
- **Old-commit fidelity.** Commits whose dependencies no longer resolve cleanly
  type-check partially; `walkRepo` continues on partial loads, so old snapshots
  may have reduced fidelity rather than failing.
- **Read-only comprehension.** The graph is for navigation/analysis; code edits
  go through normal file editing.

---

## 13. Source layout

| File | Contents |
|---|---|
| `main.go` | Entrypoint, CLI dispatch, ingest pipeline, AST/VTA walk, embedding, Spanner writes |
| `cambium_arbor.go` | Velenza Arbor Layer 1 descriptor (`arbor-info`) and native `.inarch` VCS workspace resolver |
| `hazards.go` | Neuro-symbolic SSA/VTA/CFG hazard analyzer (12 hazard categories) and Spanner env config resolution |
| `query.go` | `search`, `neighbors`, `get`, `diff`, `dependents`, `symbols`, `annotations`, `coverage`, edge loading |
| `lineage.go` | `lineage`, `drift`, the repo DAG, cosine/Jaccard helpers |
| `aliases.go` | Rename/move detection, alias-chain resolution |
| `findings.go` | Duplication + misuse mining |
| `replay.go` | History backfill (`replay`, `update`), worker pool, memory governor, sharding |
| `mcp.go` | MCP stdio server, 16 tool registrations (including `code_graph_hazards`) |
| `credentials.go` | Strict keyless service-account impersonation and transport validation |
| `context.go` | Snapshot pinning (`use`, `context`) and current-snapshot resolution |
| `admin.go` | `create-db` and `migrate` (Spanner Database Admin API) |
| `ddl.go` | SQL DDL (`CREATE TABLE` / `CREATE INDEX`) and conformance manifest symbol extraction |
| `fidelity.go` | Phase A byte-exact source capture (`FileContents`, `SnapshotFiles`) |
| `chunks.go` | Phase B structural Go declaration chunk decomposition (`DeclSource`, `GoFileChunks`) |
| `manifest.go` | `manifest` and `verify` (byte-exact and `--from-structure` round-trip verification) |
| `serve.go` | Authenticated HTTP service (`/ingest`, `/manifest`) |
| `schema.sql` | Complete 11-table Spanner GoogleSQL schema (`STRING(64)` commit SHAs for `.inarch` + `.git`) |
