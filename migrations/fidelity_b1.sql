-- Fidelity layer, phase B1 — the structural master for .go files.
-- Each .go file is decomposed into ordered chunks (package clause / import
-- block / declaration / floating comment / raw); each chunk's exact source
-- text is content-addressed in DeclSource; GoFileChunks is the per-file
-- ordered chunk list. See FIDELITY_PHASE_B.md.

-- DeclSource: content-addressed exact chunk text, deduplicated across snapshots.
CREATE TABLE DeclSource (
  content_sha STRING(64)  NOT NULL,
  source      STRING(MAX) NOT NULL,
  created_at  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_sha);

-- GoFileChunks: the per-file ordered chunk list. Interleaved in Snapshots
-- (cascade delete). file_path is STRING(1024) — a Spanner PK column cannot be
-- STRING(MAX). symbol_fqns holds the FQNs a `decl` chunk declares (empty for
-- import/comment/raw, N for a grouped var(...)/const(...) block).
CREATE TABLE GoFileChunks (
  repo        STRING(128)        NOT NULL,
  snapshot_id STRING(64)         NOT NULL,
  file_path   STRING(1024)       NOT NULL,
  chunk_index INT64              NOT NULL,
  chunk_kind  STRING(16)         NOT NULL,
  symbol_fqns ARRAY<STRING(MAX)>,
  content_sha STRING(64)         NOT NULL,
) PRIMARY KEY (repo, snapshot_id, file_path, chunk_index),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;
