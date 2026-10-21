# code-graph Fidelity Layer — ingest → edit → manifest, one master

## 0. Goal

Ingest a Go app into the ASG, **heavily modify it inside the ASG** (rip out
pieces, rewrite functions, change anything), and **manifest** it back out as
valid, gofmt-clean Go.

This is **gene editing**, not photocopying. The Xerox framing was wrong — a
copier doesn't edit. You take the gene in, change it, and the output must be
*a valid gene* — correct by the rules of what Go is, reflecting your edits.

And it is editing **in 4K**: you edit the full-fidelity master directly. There
is no downscaled proxy you edit and hope the master conforms to.

## 1. ONE master — the non-negotiable principle

There is exactly **one** representation of the app you can edit: the **master**
— the complete structural form (the full Go AST as data: every declaration,
statement, expression, comment, `//go:` directive, import). You edit the
master. `manifest` renders the master. Nothing else is an editable copy, so
nothing can drift.

The earlier design stored raw bytes *and* structure and manifested unmodified
files from the bytes. That is a two-version design — the bytes are stale the
instant you edit. **Killed.** One master.

## 2. The three things stored — and why only one is a "version"

| Thing | Role | Editable? | Can it drift? |
|---|---|---|---|
| **The master** — full AST as data | the app; the editing medium; the manifest source | YES — this is what you edit | it IS the source of truth — nothing to drift from |
| **Ingest-time oracle** — raw bytes of source files, kept only to run the completeness check | proves the master captured everything, ONCE, at ingest | no | consulted once at ingest, never again — not a parallel copy |
| **Analysis/symbol graph** — symbols, edges, embeddings | read-only *navigation* index (the 1080p preview) | no — never edited | no — it is *recomputed* from the master; a downstream projection cannot disagree with its source |

Only the master is a version. The oracle is a one-shot test input. The
analysis graph is a derived index. Neither is a second copy of the app.

## 3. Manifest

`manifest` **always** renders Go source from the master — for an unedited app
and a heavily-edited one alike. There is no raw-byte fast path. Output is
gofmt-canonical (Go has one canonical format; `go/printer` produces it
deterministically).

## 4. The completeness proof

`ingest(unedited app) → manifest → diff(gofmt(original), gofmt(output)) == empty`

This is not a "we can copy" feature. It is the **proof that the master lost
nothing on the way in** — every comment, directive, import, blank-line group.
Once it passes, you can rip the app apart in the ASG knowing manifest will not
silently drop anything. For an *edited* master the standard is the gene
standard: the output is valid Go that compiles and reflects the edits.

## 5. Capture scope — source, never build output

The master is built from the app's **source**: `.go` files (→ AST master) and
non-Go **source** files (`go.mod`, `go.sum`, configs, `//go:embed` assets,
`testdata/`, scripts, docs — carried as bytes; they have no AST).

**Build outputs are NOT captured** — compiled binaries (`engine.test`,
`service-binary`), anything that is regenerated from source. They are the export,
not the project; storing them is stale-render drift. Detection: executable
magic bytes (Mach-O / ELF / PE) are skipped at capture.

## 6. Architecture — the master IS the editing medium

- **Ingest** — parse every `.go` file (`go/parser`, build-tag-independent) into
  the AST master, stored as data in the ASG. Non-Go source files captured as
  bytes. Build outputs skipped.
- **Edit** — modification happens *on the master* in the ASG — structural
  operations on AST nodes (replace a function, delete a declaration, rewrite a
  statement). This is the 4K edit: full fidelity, no proxy.
- **Manifest** — render the (edited) master → gofmt Go.
- **Navigate** — the symbol/analysis graph is recomputed from the master and
  used read-only to find your way around. Editing never touches it.

## 7. Phases

### Phase A — non-Go source capture + the completeness oracle + verify harness
Capture non-Go source files as bytes; capture `.go` file bytes as the
ingest-time oracle only. `manifest` / `verify` commands stood up. Skip build
outputs (executable magic). Large files chunked (no size ceiling — losslessness
admits no arbitrary cap). *Status: capture code exists (`fidelity.go`); needs
the executable-skip + chunking fix; see §8.*

### Phase B — the structural master *(the product)*
Full-AST capture as data in the ASG — every declaration, statement, expression,
comment, directive (detailed in [FIDELITY_PHASE_B.md](FIDELITY_PHASE_B.md)). `manifest` renders the master to gofmt Go. The completeness
proof (§4) must pass on the whole repository: unedited ingest → manifest → empty
diff. This is the bulk of the build and the hard part (faithful AST round-trip
with comment/directive placement).

### Phase C — edit operations on the master
The ASG operations that modify the master — replace / delete / insert / rewrite
AST nodes — so "modify within the ASG" is a real workflow. Manifest renders the
result. Acceptance: edited master manifests to valid, compiling Go.

## 8. Immediate fix (Phase A capture, resolved)

The initial raw-byte capture hit Spanner's 10 MiB column limit on committed
build binaries. Resolved via:
- **Skip build outputs** — detect executable magic bytes; do not capture.
- **Chunk `FileContents`** — `(content_sha, chunk_index)` so a genuinely large
  *source* file (a big `testdata` fixture) has no size ceiling.

## 9. What does NOT change

The analysis graph — `SymbolVersions`, edges, embeddings, coverage, `--orphans`,
`findings`, `hazards`, the 16 MCP tools — is untouched. It is the derived navigation index,
recomputed from the master, never edited.
