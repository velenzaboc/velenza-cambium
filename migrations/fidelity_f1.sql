-- Fidelity layer, phase F1 — lossless file capture.
-- Additive migration: apply with `code-graph migrate migrations/fidelity_f1.sql`.
-- These tables also live in schema.sql so a fresh `create-db` includes them.

-- FileContents: content-addressed raw file bytes. One row per unique file
-- content, globally deduplicated across snapshots — the lossless backstop of
-- the fidelity layer. Holds every file of an ingested app, .go and non-Go
-- alike (go.mod, go.sum, //go:embed assets, testdata). Bytes always regenerate.
CREATE TABLE FileContents (
  content_sha STRING(64) NOT NULL,
  bytes       BYTES(MAX) NOT NULL,
  size        INT64      NOT NULL,
  created_at  TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_sha);

-- SnapshotFiles: the per-snapshot file manifest — which file content sits at
-- which path in a given ingest. Interleaved in Snapshots (cascade delete).
-- file_path is the git-relative path; file_order is its lexical rank for a
-- deterministic manifest walk.
CREATE TABLE SnapshotFiles (
  repo        STRING(128)  NOT NULL,
  snapshot_id STRING(64)   NOT NULL,
  file_path   STRING(1024) NOT NULL,
  content_sha STRING(64)   NOT NULL,
  is_go       BOOL         NOT NULL,
  file_order  INT64        NOT NULL,
) PRIMARY KEY (repo, snapshot_id, file_path),
  INTERLEAVE IN PARENT Snapshots ON DELETE CASCADE;
