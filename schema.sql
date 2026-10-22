-- SymbolBlobs: content-addressed embedding cache. One row per unique
-- (signature + doc + body) tuple, globally deduplicated. Owns the vector index.
CREATE TABLE SymbolBlobs (
  content_hash STRING(64)  NOT NULL,
  embedding    ARRAY<FLOAT32>(vector_length=>3072),
  created_at   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_hash);

CREATE VECTOR INDEX SymbolBlobsEmbeddingIdx
  ON SymbolBlobs(embedding)
  WHERE embedding IS NOT NULL
  OPTIONS (distance_type='COSINE', tree_depth=3, num_leaves=1000);

-- SymbolVersions: the immutable spine. One row per distinct body a symbol has
-- ever had. Identity is (symbol_id, content_hash) — independent of any commit.
CREATE TABLE SymbolVersions (
  symbol_id        STRING(36)  NOT NULL,
  content_hash     STRING(64)  NOT NULL,
  fqn              STRING(MAX) NOT NULL,
  kind             STRING(32)  NOT NULL,
  signature        STRING(MAX),
  doc_comment      STRING(MAX),
  body             STRING(MAX),
  volt_annotations ARRAY<STRING(MAX)>,
  created_at       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (symbol_id, content_hash);

-- SymbolEdges: edges whose SOURCE is a content-bearing symbol, keyed by the
-- source symbol-VERSION. An edge from a function is a pure function of that
-- function's body + resolution context (symbol_id encodes the package), so it
-- is written once per (src_symbol_id, src_content_hash) and reused by every
-- snapshot that contains that version — the cache that makes history replay
-- affordable. (CHA interface-dispatch edges are frozen at first ingest of the
-- source version; see ROADMAP.)
CREATE TABLE SymbolEdges (
  src_symbol_id    STRING(36) NOT NULL,
  src_content_hash STRING(64) NOT NULL,
  dst_symbol_id    STRING(36) NOT NULL,
  kind             STRING(32) NOT NULL,
) PRIMARY KEY (src_symbol_id, src_content_hash, dst_symbol_id, kind);

CREATE INDEX SymbolEdgesByDst ON SymbolEdges(dst_symbol_id);

-- Snapshots: a named set of symbol-versions. parent_snapshot_ids records the
-- git DAG (a merge has 2+ parents); branch is the ref it was ingested under.
CREATE TABLE Snapshots (
  repo                STRING(128) NOT NULL,
  snapshot_id         STRING(64)  NOT NULL,
  label               STRING(256) NOT NULL,
  commit_sha      STRING(64)  NOT NULL,
  commit_time         TIMESTAMP,
  kind                STRING(20)  NOT NULL,
  base_snapshot_id    STRING(64),
  parent_snapshot_ids ARRAY<STRING(64)>,
  branch              STRING(256),
  symbol_count        INT64       NOT NULL,
  ingested_at         TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (repo, snapshot_id);

-- SnapshotMembers: which symbol-version of each FQN is in a snapshot.
CREATE TABLE SnapshotMembers (
  repo         STRING(128) NOT NULL,
  snapshot_id  STRING(64)  NOT NULL,
  symbol_id    STRING(36)  NOT NULL,
  fqn          STRING(MAX) NOT NULL,
  content_hash STRING(64),
  kind         STRING(32)  NOT NULL,
  file_path    STRING(MAX) NOT NULL,
  line_start   INT64       NOT NULL,
  line_end     INT64       NOT NULL,
) PRIMARY KEY (repo, snapshot_id, symbol_id),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;

CREATE INDEX SnapshotMembersByFqn
  ON SnapshotMembers(repo, snapshot_id, fqn);

CREATE INDEX SnapshotMembersByContentHash
  ON SnapshotMembers(content_hash);

CREATE INDEX SnapshotMembersBySymbol
  ON SnapshotMembers(symbol_id);

-- SnapshotEdges: the complete derived edge set for one repository snapshot.
-- Resolution depends on the whole analysis context, so even an unchanged
-- source body may have different CALLS/IMPLEMENTS edges in another snapshot.
CREATE TABLE SnapshotEdges (
  repo          STRING(128) NOT NULL,
  snapshot_id   STRING(64)  NOT NULL,
  symbol_id     STRING(36)  NOT NULL,
  dst_symbol_id STRING(36)  NOT NULL,
  kind          STRING(32)  NOT NULL,
) PRIMARY KEY (repo, snapshot_id, symbol_id, dst_symbol_id, kind),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;

-- FileContents: content-addressed raw file content, chunked. Spanner caps a
-- single column value at 10 MiB, so content is split into ordered chunks
-- (reassembled by chunk_index). One (content_sha, chunk_index) row per chunk,
-- deduplicated across snapshots. The fidelity layer's ingest-time completeness
-- oracle + carrier for non-Go source files.
CREATE TABLE FileContents (
  content_sha STRING(64) NOT NULL,
  chunk_index INT64      NOT NULL,
  bytes       BYTES(MAX) NOT NULL,
  created_at  TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_sha, chunk_index);

-- SnapshotFiles: per-snapshot file manifest — which file content is at which
-- path in a given ingest. Interleaved in Snapshots (cascade delete).
CREATE TABLE SnapshotFiles (
  repo        STRING(128)  NOT NULL,
  snapshot_id STRING(64)   NOT NULL,
  file_path   STRING(1024) NOT NULL,
  content_sha STRING(64)   NOT NULL,
  is_go       BOOL         NOT NULL,
  file_order  INT64        NOT NULL,
) PRIMARY KEY (repo, snapshot_id, file_path),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;

-- DeclSource: content-addressed exact chunk text — the fidelity layer's
-- structural master (phase B). Deduplicated across snapshots.
CREATE TABLE DeclSource (
  content_sha STRING(64)  NOT NULL,
  source      STRING(MAX) NOT NULL,
  created_at  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_sha);

-- GoFileChunks: per .go file, the ordered list of top-level chunks (package /
-- import / decl / comment / raw). Interleaved in Snapshots (cascade delete).
-- file_path STRING(1024) — a Spanner PK column cannot be STRING(MAX).
-- blank_before: whether a blank line preceded this chunk in the source. gofmt
-- preserves the author's 0-or-1 blank-line choice between top-level decls, so
-- this bit is fidelity-bearing.
CREATE TABLE GoFileChunks (
  repo         STRING(128)        NOT NULL,
  snapshot_id  STRING(64)         NOT NULL,
  file_path    STRING(1024)       NOT NULL,
  chunk_index  INT64              NOT NULL,
  chunk_kind   STRING(16)         NOT NULL,
  symbol_fqns  ARRAY<STRING(MAX)>,
  content_sha  STRING(64)         NOT NULL,
  blank_before BOOL,
) PRIMARY KEY (repo, snapshot_id, file_path, chunk_index),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;

-- SymbolAliases: detected rename/move identity links.
CREATE TABLE SymbolAliases (
  repo               STRING(128) NOT NULL,
  from_symbol_id     STRING(36)  NOT NULL,
  to_symbol_id       STRING(36)  NOT NULL,
  from_fqn           STRING(MAX) NOT NULL,
  to_fqn             STRING(MAX) NOT NULL,
  embedding_distance FLOAT64     NOT NULL,
  caller_overlap     FLOAT64     NOT NULL,
  detected_at        TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (repo, from_symbol_id, to_symbol_id);
