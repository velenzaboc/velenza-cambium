package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// loadRepoDAG loads every ingested snapshot of a repo with its parent SHAs and
// branch label — the commit DAG, for lineage ordering and merge-aware queries.
func loadRepoDAG(ctx context.Context, sp *spanner.Client, repo string) (map[string]*snapMeta, error) {
	stmt := spanner.Statement{
		SQL: `SELECT snapshot_id, commit_sha, commit_time, branch, parent_snapshot_ids
		      FROM Snapshots
		      WHERE repo = @repo AND kind = 'ingested'`,
		Params: map[string]interface{}{"repo": repo},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	dag := map[string]*snapMeta{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var sm snapMeta
		var ct spanner.NullTime
		var branch spanner.NullString
		var parents []string
		if err := row.Columns(&sm.id, &sm.commitSHA, &ct, &branch, &parents); err != nil {
			return nil, err
		}
		if ct.Valid {
			sm.commitTime = ct.Time
		}
		if branch.Valid {
			sm.branch = branch.StringVal
		}
		sm.parents = parents
		dag[sm.id] = &sm
	}
	return dag, nil
}

// Phase 2: the time axis. lineage walks a symbol's version history; drift
// classifies the changes between two snapshots by textual vs semantic delta.

// ---------- lineage ----------

type LineageVersion struct {
	ContentHash     string  `json:"content_hash"`
	FirstCommitSHA  string  `json:"first_commit_sha"`
	FirstCommitTime string  `json:"first_commit_time"`
	FirstBranch     string  `json:"first_branch,omitempty"`
	SnapshotCount   int     `json:"snapshot_count"`
	SemanticDelta   float64 `json:"semantic_delta_from_prev"`
}

type LineageResult struct {
	FQN      string           `json:"fqn"`
	Repo     string           `json:"repo"`
	Versions []LineageVersion `json:"versions"`
}

// snapMeta holds the DAG-relevant fields of a snapshot.
type snapMeta struct {
	id         string
	commitSHA  string
	commitTime time.Time
	branch     string
	parents    []string
}

func doLineage(ctx context.Context, sp *spanner.Client, fqn string) (*LineageResult, error) {
	symbolID, repo, _, err := resolveFQN(ctx, sp, fqn, "") // lineage spans all snapshots
	if err != nil {
		return nil, err
	}
	// Follow rename/move aliases so the lineage is continuous across renames.
	chain, err := resolveAliasChain(ctx, sp, repo, symbolID)
	if err != nil {
		return nil, err
	}
	sids := keysOf(chain)

	// Load the repo's snapshots (the DAG: each carries its parent SHAs and
	// branch label). Lineage is ordered by commit_time; the stored parent DAG
	// remains available for merge-aware analysis.
	dag, err := loadRepoDAG(ctx, sp, repo)
	if err != nil {
		return nil, err
	}

	// Which snapshots contain a symbol in the alias chain, and at what version.
	stmt := spanner.Statement{
		SQL: `SELECT s.snapshot_id, sm.content_hash
		      FROM Snapshots AS s
		      JOIN SnapshotMembers AS sm
		        ON sm.repo = s.repo
		       AND ((s.storage_snapshot_id IS NULL AND sm.snapshot_id = s.snapshot_id)
		         OR sm.snapshot_id = s.storage_snapshot_id)
		      WHERE s.repo = @repo AND sm.symbol_id IN UNNEST(@sids) AND s.kind = 'ingested'`,
		Params: map[string]interface{}{"repo": repo, "sids": sids},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()

	type occ struct {
		snap *snapMeta
		hash string
	}
	var occs []occ
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var snapID string
		var hash spanner.NullString
		if err := row.Columns(&snapID, &hash); err != nil {
			return nil, err
		}
		sm := dag[snapID]
		if sm == nil {
			continue
		}
		occs = append(occs, occ{sm, hash.StringVal})
	}

	// Order oldest commit first.
	sort.Slice(occs, func(i, j int) bool {
		return occs[i].snap.commitTime.Before(occs[j].snap.commitTime)
	})

	// Collapse consecutive identical content_hashes into distinct versions.
	var versions []LineageVersion
	for _, o := range occs {
		if n := len(versions); n > 0 && versions[n-1].ContentHash == o.hash {
			versions[n-1].SnapshotCount++
			continue
		}
		versions = append(versions, LineageVersion{
			ContentHash:     o.hash,
			FirstCommitSHA:  o.snap.commitSHA,
			FirstCommitTime: o.snap.commitTime.UTC().Format("2006-01-02T15:04:05Z"),
			FirstBranch:     o.snap.branch,
			SnapshotCount:   1,
		})
	}

	// Semantic delta between consecutive versions = cosine distance of embeddings.
	embByHash, err := loadSymbolEmbeddings(ctx, sp, versionHashes(versions))
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(versions); i++ {
		prev := embByHash[versions[i-1].ContentHash]
		cur := embByHash[versions[i].ContentHash]
		if prev != nil && cur != nil {
			versions[i].SemanticDelta = cosineDistance(prev, cur)
		}
	}
	return &LineageResult{FQN: fqn, Repo: repo, Versions: versions}, nil
}

func versionHashes(vs []LineageVersion) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, v := range vs {
		if v.ContentHash == "" {
			continue
		}
		if _, ok := seen[v.ContentHash]; ok {
			continue
		}
		seen[v.ContentHash] = struct{}{}
		out = append(out, v.ContentHash)
	}
	return out
}

// ---------- drift ----------

type DriftEntry struct {
	FQN           string  `json:"fqn"`
	Kind          string  `json:"kind"`
	SemanticDelta float64 `json:"semantic_delta"`
	TextualDelta  float64 `json:"textual_delta"`
	Quadrant      string  `json:"quadrant"`
}

type DriftResult struct {
	Repo    string       `json:"repo"`
	Before  string       `json:"before"`
	After   string       `json:"after"`
	Added   int          `json:"added"`
	Removed int          `json:"removed"`
	Changed []DriftEntry `json:"changed"`
}

// Quadrant thresholds. Embeddings are 3072-dim; semantically-identical code
// sits well under 0.10 cosine distance, so 0.15 is a conservative "meaning
// changed" line. Textual delta is token-set Jaccard distance.
const (
	semanticHighThreshold = 0.15
	textualHighThreshold  = 0.30
)

func doDrift(ctx context.Context, sp *spanner.Client, repo, before, after string) (*DriftResult, error) {
	for _, snap := range []string{before, after} {
		ok, err := snapshotExists(ctx, sp, repo, snap)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("snapshot %q for repo %q is not ingested — call code_graph_ingest first", snap, repo)
		}
	}
	var mods []changedSymbol
	res := &DriftResult{Repo: repo, Before: before, After: after}
	hashSet := map[string]struct{}{}
	beforeMembers, err := loadSnapshotDiffMembers(ctx, sp, repo, before)
	if err != nil {
		return nil, err
	}
	afterMembers, err := loadSnapshotDiffMembers(ctx, sp, repo, after)
	if err != nil {
		return nil, err
	}
	allIDs := map[string]struct{}{}
	for id := range beforeMembers {
		allIDs[id] = struct{}{}
	}
	for id := range afterMembers {
		allIDs[id] = struct{}{}
	}
	for symbolID := range allIDs {
		oldMember, hadOld := beforeMembers[symbolID]
		newMember, hadNew := afterMembers[symbolID]
		if hadOld && hadNew && sameContentHash(oldMember.contentHash, newMember.contentHash) {
			continue
		}
		switch {
		case !hadOld:
			res.Added++
		case !hadNew:
			res.Removed++
		default:
			if oldMember.contentHash.Valid && newMember.contentHash.Valid {
				mods = append(mods, changedSymbol{symbolID, oldMember.fqn, oldMember.kind, oldMember.contentHash.StringVal, newMember.contentHash.StringVal})
				hashSet[oldMember.contentHash.StringVal] = struct{}{}
				hashSet[newMember.contentHash.StringVal] = struct{}{}
			}
		}
	}
	if len(mods) == 0 {
		return res, nil
	}

	hashes := make([]string, 0, len(hashSet))
	for h := range hashSet {
		hashes = append(hashes, h)
	}
	embByHash, err := loadSymbolEmbeddings(ctx, sp, hashes)
	if err != nil {
		return nil, err
	}
	bodyByKey, err := fetchBodies(ctx, sp, mods)
	if err != nil {
		return nil, err
	}

	for _, m := range mods {
		e := DriftEntry{FQN: m.fqn, Kind: m.kind}
		oldEmb, newEmb := embByHash[m.oldHash], embByHash[m.newHash]
		if oldEmb != nil && newEmb != nil {
			e.SemanticDelta = cosineDistance(oldEmb, newEmb)
		}
		oldBody := bodyByKey[m.symbolID+"|"+m.oldHash]
		newBody := bodyByKey[m.symbolID+"|"+m.newHash]
		e.TextualDelta = tokenJaccardDistance(oldBody, newBody)
		e.Quadrant = classifyQuadrant(e.TextualDelta, e.SemanticDelta)
		res.Changed = append(res.Changed, e)
	}
	// Rank by semantic surprise — the dangerous changes float to the top.
	sort.Slice(res.Changed, func(i, j int) bool {
		return res.Changed[i].SemanticDelta > res.Changed[j].SemanticDelta
	})
	return res, nil
}

func classifyQuadrant(textual, semantic float64) string {
	tHigh := textual > textualHighThreshold
	sHigh := semantic > semanticHighThreshold
	switch {
	case !tHigh && sHigh:
		return "behavior-shift" // small edit, big meaning change — review closely
	case tHigh && !sHigh:
		return "refactor" // big edit, behavior preserved — low risk
	case tHigh && sHigh:
		return "rewrite"
	default:
		return "cosmetic"
	}
}

// ---------- shared helpers ----------

func fetchEmbeddings(ctx context.Context, sp *spanner.Client, hashes []string) (map[string][]float32, error) {
	out := map[string][]float32{}
	if len(hashes) == 0 {
		return out, nil
	}
	stmt := spanner.Statement{
		SQL:    `SELECT content_hash, embedding FROM SymbolBlobs WHERE content_hash IN UNNEST(@h) AND embedding IS NOT NULL`,
		Params: map[string]interface{}{"h": hashes},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var h string
		var emb []float32
		if err := row.Columns(&h, &emb); err != nil {
			return nil, err
		}
		out[h] = emb
	}
	return out, nil
}

// loadSymbolEmbeddings is the external-data boundary used by lineage, drift,
// and alias stitching. Keeping the matcher independent of the concrete Spanner
// vector representation lets its identity decisions be exercised deterministically.
var loadSymbolEmbeddings = fetchEmbeddings

// changedSymbol is one symbol that differs between two snapshots.
type changedSymbol struct {
	symbolID, fqn, kind, oldHash, newHash string
}

// fetchBodies returns a map keyed "symbol_id|content_hash" → body text.
func fetchBodies(ctx context.Context, sp *spanner.Client, mods []changedSymbol) (map[string]string, error) {
	sidSet, hashSet := map[string]struct{}{}, map[string]struct{}{}
	for _, m := range mods {
		sidSet[m.symbolID] = struct{}{}
		hashSet[m.oldHash] = struct{}{}
		hashSet[m.newHash] = struct{}{}
	}
	sids := keysOf(sidSet)
	hashes := keysOf(hashSet)
	out := map[string]string{}
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, content_hash, body FROM SymbolVersions
		      WHERE symbol_id IN UNNEST(@sids) AND content_hash IN UNNEST(@hashes)`,
		Params: map[string]interface{}{"sids": sids, "hashes": hashes},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var sid, hash string
		var body spanner.NullString
		if err := row.Columns(&sid, &hash, &body); err != nil {
			return nil, err
		}
		out[sid+"|"+hash] = body.StringVal
	}
	return out, nil
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func cosineDistance(a, b []float32) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	sim := dot / (math.Sqrt(na) * math.Sqrt(nb))
	return 1 - sim
}

// tokenJaccardDistance is a cheap O(n) textual-change proxy: 1 minus the
// Jaccard similarity of the two bodies' token sets. A three-character change
// barely moves it; a rewrite moves it a lot.
func tokenJaccardDistance(a, b string) float64 {
	ta, tb := tokenSet(a), tokenSet(b)
	if len(ta) == 0 && len(tb) == 0 {
		return 0
	}
	inter := 0
	for t := range ta {
		if _, ok := tb[t]; ok {
			inter++
		}
	}
	union := len(ta) + len(tb) - inter
	if union == 0 {
		return 0
	}
	return 1 - float64(inter)/float64(union)
}

func tokenSet(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, tok := range strings.FieldsFunc(s, func(r rune) bool {
		return !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) {
		out[tok] = struct{}{}
	}
	return out
}
