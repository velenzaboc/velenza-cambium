package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Phase 3: rename / move identity stitching. After a snapshot is ingested,
// compare it to the prior snapshot of the same repo: a symbol that disappeared
// and a near-identical one that appeared are the same symbol, renamed or moved.
// Match on TWO independent signals — embedding proximity and caller-set overlap.

const (
	// A rename changes 1-3 identifiers, which moves the embedding more than a
	// whitespace edit but far less than a real rewrite. Caller-set overlap is
	// the strong discriminator, so the embedding bound is deliberately loose.
	aliasMaxEmbeddingDistance = 0.25
	aliasMinCallerOverlap     = 0.70
)

type memberLite struct {
	symbolID    string
	fqn         string
	contentHash string
}

// detectAliases runs as the final step of doIngest. It is best-effort: a
// failure here is logged via progress but does not fail the ingest.
func detectAliases(ctx context.Context, sp *spanner.Client, repo, newSnapshotID string, progress ProgressFunc) error {
	prior, err := priorSnapshot(ctx, sp, repo, newSnapshotID)
	if err != nil {
		return err
	}
	if prior == "" {
		progress("aliases", 0, 0, "no prior snapshot — nothing to stitch")
		return nil
	}

	priorMembers, err := contentMembers(ctx, sp, repo, prior)
	if err != nil {
		return err
	}
	newMembers, err := contentMembers(ctx, sp, repo, newSnapshotID)
	if err != nil {
		return err
	}
	priorByID := map[string]memberLite{}
	for _, m := range priorMembers {
		priorByID[m.symbolID] = m
	}
	newByID := map[string]memberLite{}
	for _, m := range newMembers {
		newByID[m.symbolID] = m
	}

	var removed, added []memberLite
	for _, m := range priorMembers {
		if _, ok := newByID[m.symbolID]; !ok {
			removed = append(removed, m)
		}
	}
	for _, m := range newMembers {
		if _, ok := priorByID[m.symbolID]; !ok {
			added = append(added, m)
		}
	}
	if len(removed) == 0 || len(added) == 0 {
		progress("aliases", 0, 0, "no removed/added pair to consider")
		return nil
	}

	// Embeddings for every candidate body.
	var hashes []string
	for _, m := range append(append([]memberLite{}, removed...), added...) {
		if m.contentHash != "" {
			hashes = append(hashes, m.contentHash)
		}
	}
	embByHash, err := loadSymbolEmbeddings(ctx, sp, hashes)
	if err != nil {
		return err
	}

	// Caller sets: who calls each removed symbol (in prior) / added symbol (in new).
	removedCallers, err := callerSets(ctx, sp, repo, prior, idsOf(removed))
	if err != nil {
		return err
	}
	addedCallers, err := callerSets(ctx, sp, repo, newSnapshotID, idsOf(added))
	if err != nil {
		return err
	}

	var muts []*spanner.Mutation
	bestDist := 2.0 // diagnostic: closest pair that also clears the caller-overlap bar
	for _, r := range removed {
		rEmb := embByHash[r.contentHash]
		if rEmb == nil {
			continue
		}
		for _, a := range added {
			aEmb := embByHash[a.contentHash]
			if aEmb == nil {
				continue
			}
			dist := cosineDistance(rEmb, aEmb)
			overlap := jaccard(removedCallers[r.symbolID], addedCallers[a.symbolID])
			if overlap >= aliasMinCallerOverlap && dist < bestDist {
				bestDist = dist
			}
			if dist > aliasMaxEmbeddingDistance {
				continue
			}
			if overlap < aliasMinCallerOverlap {
				continue
			}
			muts = append(muts, spanner.InsertOrUpdate("SymbolAliases",
				[]string{"repo", "from_symbol_id", "to_symbol_id", "from_fqn", "to_fqn",
					"embedding_distance", "caller_overlap", "detected_at"},
				[]interface{}{repo, r.symbolID, a.symbolID, r.fqn, a.fqn,
					dist, overlap, spanner.CommitTimestamp},
			))
		}
	}
	if len(muts) > 0 {
		for start := 0; start < len(muts); start += mutationBatch {
			end := start + mutationBatch
			if end > len(muts) {
				end = len(muts)
			}
			if _, err := sp.Apply(ctx, muts[start:end]); err != nil {
				return err
			}
		}
	}
	diag := ""
	if len(muts) == 0 && bestDist < 2.0 {
		diag = fmt.Sprintf(" (closest caller-matched pair: distance %.4f)", bestDist)
	}
	progress("aliases", len(muts), len(muts), fmt.Sprintf("%d rename/move links detected%s", len(muts), diag))
	return nil
}

// priorSnapshot returns the ingested snapshot of repo with the greatest
// commit_time strictly earlier than the given snapshot's commit_time.
func priorSnapshot(ctx context.Context, sp *spanner.Client, repo, snapshotID string) (string, error) {
	stmt := spanner.Statement{
		SQL: `SELECT prev.snapshot_id
		      FROM Snapshots AS cur
		      JOIN Snapshots AS prev
		        ON prev.repo = cur.repo AND prev.kind = 'ingested'
		       AND prev.commit_time < cur.commit_time
		      WHERE cur.repo = @repo AND cur.snapshot_id = @snap
		      ORDER BY prev.commit_time DESC
		      LIMIT 1`,
		Params: map[string]interface{}{"repo": repo, "snap": snapshotID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var prev string
	if err := row.Columns(&prev); err != nil {
		return "", err
	}
	return prev, nil
}

// contentMembers returns the content-bearing symbols of a snapshot.
func contentMembers(ctx context.Context, sp *spanner.Client, repo, snapshotID string) ([]memberLite, error) {
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, fqn, content_hash
		      FROM SnapshotMembers
		      WHERE repo = @repo AND snapshot_id = @snap AND content_hash IS NOT NULL`,
		Params: map[string]interface{}{"repo": repo, "snap": snapshotID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []memberLite
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var m memberLite
		var hash spanner.NullString
		if err := row.Columns(&m.symbolID, &m.fqn, &hash); err != nil {
			return nil, err
		}
		m.contentHash = hash.StringVal
		out = append(out, m)
	}
	return out, nil
}

// callerSets returns, per target symbol_id, the set of symbol_ids that call or
// reference it within the given snapshot.
func callerSets(ctx context.Context, sp *spanner.Client, repo, snapshotID string, targets []string) (map[string]map[string]struct{}, error) {
	out := map[string]map[string]struct{}{}
	if len(targets) == 0 {
		return out, nil
	}
	const chunk = 1000
	for start := 0; start < len(targets); start += chunk {
		end := start + chunk
		if end > len(targets) {
			end = len(targets)
		}
		stmt := spanner.Statement{
			SQL: `SELECT e.dst_symbol_id, e.src_symbol_id
			      FROM SymbolEdges AS e
			      JOIN SnapshotMembers AS sm
			        ON sm.symbol_id = e.src_symbol_id AND sm.content_hash = e.src_content_hash
			      WHERE sm.repo = @repo AND sm.snapshot_id = @snap
			        AND e.kind IN ('CALLS', 'REFERENCES')
			        AND e.dst_symbol_id IN UNNEST(@targets)`,
			Params: map[string]interface{}{"repo": repo, "snap": snapshotID, "targets": targets[start:end]},
		}
		iter := sp.Single().Query(ctx, stmt)
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return nil, err
			}
			var dst, src string
			if err := row.Columns(&dst, &src); err != nil {
				iter.Stop()
				return nil, err
			}
			if out[dst] == nil {
				out[dst] = map[string]struct{}{}
			}
			out[dst][src] = struct{}{}
		}
		iter.Stop()
	}
	return out, nil
}

func idsOf(ms []memberLite) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.symbolID
	}
	return out
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if _, ok := b[k]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

// resolveAliasChain returns the full set of symbol_ids transitively linked to
// the given one via SymbolAliases (either direction) — the symbol's identity
// across all its renames.
func resolveAliasChain(ctx context.Context, sp *spanner.Client, repo, symbolID string) (map[string]struct{}, error) {
	chain := map[string]struct{}{symbolID: {}}
	frontier := []string{symbolID}
	for len(frontier) > 0 {
		stmt := spanner.Statement{
			SQL: `SELECT from_symbol_id, to_symbol_id
			      FROM SymbolAliases
			      WHERE repo = @repo
			        AND (from_symbol_id IN UNNEST(@ids) OR to_symbol_id IN UNNEST(@ids))`,
			Params: map[string]interface{}{"repo": repo, "ids": frontier},
		}
		iter := sp.Single().Query(ctx, stmt)
		var next []string
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return nil, err
			}
			var from, to string
			if err := row.Columns(&from, &to); err != nil {
				iter.Stop()
				return nil, err
			}
			for _, id := range []string{from, to} {
				if _, ok := chain[id]; !ok {
					chain[id] = struct{}{}
					next = append(next, id)
				}
			}
		}
		iter.Stop()
		frontier = next
	}
	return chain, nil
}
