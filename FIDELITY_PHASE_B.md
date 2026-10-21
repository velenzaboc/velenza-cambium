# Fidelity Phase B — the structural master

Detailed design for Phase B of FIDELITY_PLAN.md: the editable structural master
for `.go` files. Phase A proved capture is lossless (the byte oracle); Phase B
makes the app *editable as structure* and renders it back, with the Phase A
oracle as the falsifiable test.

## 1. The decision: chunked-declaration master, not a full-AST node graph

**Considered and rejected — full AST as graph rows.** Every `ast` node (every
`IfStmt`, `CallExpr`, `Ident`) as a typed row with parent/child edges. Rejected:
it reimplements `go/ast` as a Spanner schema (~40 node types, interface-typed
fields, `token.Pos` bookkeeping); reconstructing a valid `*ast.File` from rows
faithfully — especially comment placement — is the known-hard problem of
programmatic Go editing, and getting it wrong is silent corruption. It buys
*sub-statement queryability* — which the editing workflow ("rewrite functions,
rip out pieces") does not require. If sub-statement *analysis* is ever wanted,
that is an additive overlay on the analysis graph, not the master's storage form.

**Chosen — the master is a `.go` file decomposed into ordered chunks.** A file
is an ordered list of top-level **chunks**; each chunk is one top-level unit —
the package clause, an import block, a declaration, or a floating comment group
— stored as its **exact source text**. Lossless by construction (text is text).
Editable: a chunk is the edit unit. Structural editing *inside* a chunk is
available on demand — a declaration chunk is a parseable Go fragment; parse it,
edit its AST, `go/printer` it back, store the new text. You edit the real thing
at full fidelity (the 4K master); you decompose to AST only for the chunk you
are touching, which also bounds the comment-placement risk to that one chunk.

## 2. The fidelity boundary — Phase A is byte-exact, Phase B is gofmt-exact

Phase B renders by `gofmt`-ing reassembled chunks (§3), so its output is
gofmt-canonical **by construction**. A file that is already gofmt-canonical
round-trips byte-identically. A file that is NOT — for example, **167 of 699
repository `.go` files in a large production benchmark that were not gofmt-canonical
under Go 1.27 `gofmt`** (some genuine, some toolchain-version
skew) — renders as its gofmt'd form, which differs from the original input.

This is deliberate and consistent with the agreed design ([FIDELITY_PLAN.md](FIDELITY_PLAN.md) §2:
"gofmt-canonical output"): the editable structural master normalizes to canonical
Go. The two layers split cleanly:

- **Phase A — byte-exact.** Raw file bytes; `verify` is a byte diff. The
  byte-for-byte backstop and the carrier for non-Go files.
- **Phase B — gofmt-exact.** The structural master; `verify --from-structure`
  diffs `gofmt(render)` against `gofmt(original)` — gofmt on **both** sides, or
  the 167 non-canonical files throw false failures.

Manifesting the repository through Phase B therefore reformats those 167 files to
canonical Go — an intended normalization, not a fidelity loss. (That 24% of the
target codebase is not gofmt-canonical is itself worth a separate code-hygiene look.)

## 3. Decomposition (ingest)

Per `.go` file, parsed with `go/parser` + `parser.ParseComments` (build-tag
independent — every file, every platform):

- **Package chunk** — byte 0 through the end of the line containing the
  `package X` clause. Captures `//go:build` constraints, the package doc
  comment, and the package clause as exact text. Its span is `[0, endOfPackageLine]`.
- **One chunk per top-level declaration** — `*ast.GenDecl` (import / const / var
  / type) and `*ast.FuncDecl`. Span = `decl.Doc.Pos()` if a doc comment is
  present (Go attaches contiguous `//go:` directives to `Doc`), else
  `decl.Pos()`, through `decl.End()`.
- **One chunk per floating comment group** — a `*ast.CommentGroup` in
  `file.Comments` whose position lies **outside the package chunk span AND
  outside every declaration chunk span** (comments between decls, trailing file
  comments). The package-span exclusion is required — a build constraint and the
  package doc are in `file.Comments` too, and are already inside the package
  chunk; without it they would be captured twice.
- Chunks are ordered by source position; `chunk_index` is that order.
- **The blank line before each chunk IS captured** (`blank_before`). `gofmt`
  does NOT canonicalize blank lines between top-level declarations — it
  *preserves* the author's choice (0 stays 0, 1 stays 1, only 2+ collapses to
  1). So whether a chunk was preceded by a blank line is fidelity-bearing.
  (Discovered by the B2 round-trip: without it, three adjacent `const`s
  rendered with a blank line between each — 26 files failed `verify`.)
  Everything else `gofmt` will not invent (code, comments, directives) lives
  inside a chunk; the inter-chunk gap is otherwise pure whitespace `gofmt`
  regenerates.
- **Parse-failure fallback** — if `go/parser` cannot parse a `.go` file (an
  intentionally-malformed `testdata/` fixture), it is captured as a single
  `raw` chunk holding the whole file's bytes. It still round-trips losslessly
  (verbatim, no gofmt); it is simply not decomposed or structurally editable.

## 4. Render (manifest from structure)

Per `.go` file: fetch its chunks ordered by `chunk_index`, concatenate the chunk
texts separated by `\n\n`, run `gofmt`. A `raw`-chunk file is emitted verbatim
(no gofmt). Non-Go files: raw bytes from Phase A's `FileContents`.
`gofmt(joined chunks) == gofmt(original)` holds because every gofmt-preserved
byte is inside a chunk and `gofmt` is deterministic + idempotent — and `verify`
is the proof, not the assumption. The `\n\n` separator is safe: it can only
over-separate, and `gofmt` collapses blank runs; a doc comment is never its own
chunk (it lives inside its declaration's chunk), so `\n\n` can never detach one.

## 5. Schema additions

- `DeclSource` — content-addressed exact chunk text. PK `content_sha STRING(64)`.
  Column `source STRING(MAX)` (a single top-level chunk is far under 10 MiB).
  Deduplicated across snapshots — an unchanged declaration costs nothing on
  re-ingest, exactly like `SymbolBlobs` / `FileContents`.
- `GoFileChunks` — the per-file ordered chunk list, interleaved in `Snapshots`.
  PK `(repo STRING(128), snapshot_id STRING(64), file_path STRING(1024),
  chunk_index INT64)` — `file_path` is `STRING(1024)`, NOT `STRING(MAX)`: a
  Spanner primary-key column cannot be `STRING(MAX)` (as `SnapshotFiles` already
  is). Columns: `chunk_kind` (`package` / `import` / `decl` / `comment` / `raw`),
  `symbol_fqns ARRAY<STRING(MAX)>` (the FQNs a `decl` chunk declares — empty for
  import/comment/raw, one for most decls, **N for a grouped `var (...)` /
  `const (...)` block** — the analysis-graph link), `content_sha`
  (→ `DeclSource`), `blank_before BOOL` (a blank line preceded this chunk).
- `deleteSnapshotData` (the re-ingest replace helper) gains `GoFileChunks` in its
  explicit partitioned-delete list — it is a 4th interleaved child of `Snapshots`.

The analysis graph (`SymbolVersions`, edges, embeddings) is unchanged — derived,
read-only navigation. `GoFileChunks` is the master for `.go` files; `SnapshotFiles`
(Phase A) remains the master for non-Go files.

## 6. Edit model (the handoff to Phase C)

An edit is a graph operation on `GoFileChunks`:
- **Replace** a chunk — point it at a new `DeclSource`. New text is hand-written
  or printed from an edited sub-AST.
- **Insert / delete / reorder** chunks — operations on `chunk_index` (Phase C
  picks the renumbering scheme — gap indices or a renumber pass).
- **Structural edit inside a declaration** — for a `decl` chunk only: load the
  chunk text, parse `"package p\n" + text`, edit the AST, `go/printer` it, drop
  the synthetic package line, store as a new `DeclSource`. (`package` / `comment`
  / `raw` chunks are edited as text, not via this trick.)
After any edit the analysis graph for that snapshot is recomputed from the
master (it is a projection — it never drifts).

## 7. Phase B scope and acceptance

- **B1 — decomposition + capture.** At ingest, decompose every `.go` file into
  `GoFileChunks` + `DeclSource` (raw fallback for unparseable files).
- **B2 — structural manifest + verify.** `manifest --from-structure` renders
  `.go` files from chunks (§4); `verify --from-structure` diffs `gofmt(render)`
  against `gofmt(original)` — gofmt on both sides.
- **Acceptance (the completeness proof):** `verify --from-structure` is CLEAN
  across the whole repository — the structural master reproduces every `.go` file,
  gofmt-canonical, with no dropped comment, directive, or declaration. Until that
  passes, the master is not yet lossless and Phase B is not done.

Editing operations (§6) are Phase C; B delivers the master and proves it lossless.
