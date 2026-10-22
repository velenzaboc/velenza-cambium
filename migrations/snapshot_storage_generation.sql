-- Logical snapshots atomically select one complete immutable child generation.
-- NULL preserves the legacy layout where children use snapshot_id directly.
ALTER TABLE Snapshots ADD COLUMN storage_snapshot_id STRING(64);
