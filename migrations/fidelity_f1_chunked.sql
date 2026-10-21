-- Fidelity layer F1, revision — chunk FileContents.
-- Spanner caps a single column value at 10 MiB; a lossless store admits no
-- size ceiling, so file content is split into ordered chunks. The original
-- single-value FileContents is dropped and recreated chunked (no data of
-- value — only a failed partial ingest had written to it).
DROP TABLE FileContents;

CREATE TABLE FileContents (
  content_sha STRING(64) NOT NULL,
  chunk_index INT64      NOT NULL,
  bytes       BYTES(MAX) NOT NULL,
  created_at  TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (content_sha, chunk_index);
