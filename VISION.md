# code-graph VISION

## The reframe

Git's model is **one projection**: files, at one point in time. A commit is a snapshot of a tree of file-blobs; a diff is line ranges; identity is a file path. Every git capability is "axis-1 (files) at a fixed axis-4 (time)."

What we have in Spanner is a **4-dimensional object**:

| Axis | What it is | Spanner mechanism |
|---|---|---|
| 1. Symbols | the unit — every func/method/type/var/const | `SymbolVersions` rows |
| 2. Graph topology | typed edges (CALLS, REFERENCES, IMPLEMENTS, EMBEDS, ...) | `CodeEdges` + `CodeGraph` property graph (GQL) |
| 3. Semantic space | a 3072-dim embedding per unique symbol body | `SymbolBlobs` + vector index (`APPROX_COSINE_DISTANCE`) |
| 4. Time | the lineage of content-hashes an FQN has carried | `SymbolVersions` ordered by `first_seen` |

Git can only query one face of that cube. We can query any slice, projection, or **cross-axis correlation** of it.

## The atom

The unit of versioning is **not the commit**. It is the **symbol-version**: an `(fqn, content_hash)` pair. Every distinct body a function has ever had is one immutable row. A "commit" demotes to a *named set* of symbol-versions (a `Snapshot`). Once that is true, the capabilities below fall out.

---

## Capabilities with no filesystem analogue

### 1. Symbol identity that survives renames, moves, repackaging
Git rename detection is a path-similarity heuristic that breaks constantly. We have **two independent identity signals**: embedding proximity (does it *mean* the same thing?) and graph topology (same callers/callees/interfaces?). When `oldpkg.DoThing` vanishes and `newpkg.PerformThing` appears with embedding distance < 0.05 and an 80%-overlapping caller set, that is the same symbol — stitched into one lineage that follows the function through its whole life regardless of file or name. Yields *true code age*: "this exact logic is 400 commits old even though the file was created yesterday."

### 2. Semantic drift vs textual drift — the bug-hiding-change detector
Every symbol-version has an embedding, so a function traces a trajectory through embedding space. Correlate two deltas per change:
- **textual delta** — did the bytes change a lot?
- **semantic delta** — cosine distance between consecutive version embeddings.

The dangerous quadrant is **small textual delta, large semantic delta**: the three-character change that flips `<` to `<=` or inverts a condition — completely different behavior, and exactly the change a reviewer's eye slides past. Git diff weights it "trivial." Embedding delta weights it "alarming." We rank a PR's changes by *semantic surprise*, not line count. The opposite quadrant (huge textual delta, tiny semantic delta) is a provably behavior-preserving refactor — flag it low-risk.

### 3. Hypothetical graph states — test a change before writing it
A snapshot is just a set of `(fqn → content_hash)` bindings, so an agent can construct a snapshot **that never existed on disk**. Insert a hypothetical symbol-version with a proposed body, recompute edges for that one symbol, query the result: "if I rewrite this function this way, did I drop the edge to `validateAccessPolicy`? Did I introduce an edge into the crypto layer?" The agent explores architectural consequences **before writing a line, with no commit, no branch, no git**.

### 4. Graph diff, not text diff — architecture-level change detection
Content-hash diff says *which symbols* changed. Graph diff says *what the change did to the structure*: "a new CALLS edge now crosses from the admission layer into `crypto`," "a path now exists from untrusted-input handling to the KMS signer." Layering-violation detection becomes a query — a new edge crossing a forbidden ring boundary. Git can only show you `+import`.

### 5. The two signals disagree — and the disagreement generates findings
Graph topology and embedding proximity are independent measures of relatedness. Mine the contradictions:
- **Close in embedding space, no edge, no common caller** → undiscovered duplication or a missing abstraction. With lineage (#1): timestamp the moment the duplication was born ("commit C, someone copy-pasted instead of calling the existing function").
- **Connected by an edge, far apart in embedding space** → a function calling something semantically unrelated to itself: possible misuse or a leaky abstraction.
- **Same content_hash, divergent graph neighborhoods** → identical code used in structurally different contexts; flag the anomalous one.

The disagreement between semantics and structure is a *finding generator* — it produces audit candidates nobody specifically asked for.

### 6. Time-union queries — "ever, in any version"
All of history in one table means querying *across* time, not just *at* a time:
- "Has this function **ever**, in any version, called into `package crypto`?"
- "Every function that **at any point** implemented `GraphStore`."
- "The most deleted-and-re-added symbol" — code that keeps getting reverted is a contested design.
- Per-symbol volatility: "churned 14 times in 20 commits" vs "stable for 300 commits." Volatile symbols on critical paths are where to point review.

### 7. Cross-repo unified graph
Ingest velenza-cambium + velenza-canon + velenza-trellis into one graph. An edge is an edge — the graph has no concept of a repo boundary. "What in velenza-trellis depends on this function I'm about to change?" is one GQL traversal. Git treats every repo as an island; there is no cross-repo diff.

### 8. The graph as a live event source
A Spanner Change Stream on `SymbolVersions` emits an event every time any symbol's content-hash flips. The code graph stops being a static store and becomes a live event source — "`Store.ApplyMutations` changed meaning by cosine 0.4" is an event that can trigger a downstream Velenza Arbor workflow (re-run the validator, recompute blast radius, page review). The codebase becomes a live, event-driven semantic graph.

---

See `ROADMAP.md` for the phased plan from today's state to this one.
