-- Fidelity B2 — capture the inter-chunk blank line.
-- gofmt does NOT canonicalize blank lines between top-level declarations; it
-- preserves the author's 0-or-1 choice (collapsing only 2+ to 1). So whether a
-- chunk was preceded by a blank line is fidelity-bearing and must be stored.
ALTER TABLE GoFileChunks ADD COLUMN blank_before BOOL;
