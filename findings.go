package main

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Phase 4: signal-disagreement mining. Graph topology and embedding proximity
// are independent measures of relatedness; where they disagree, something is
// worth a human's attention.
//   duplication — semantically near-identical, but NOT connected by an edge:
//                 undiscovered duplication or a missing abstraction.
//   misuse      — connected by an edge, but semantically far apart:
//                 a function calling something unrelated to itself.

const (
	duplicationMaxDistance = 0.08
	misuseMinDistance      = 0.60
)

type DuplicationFinding struct {
	FQNA     string  `json:"fqn_a"`
	FQNB     string  `json:"fqn_b"`
	Distance float64 `json:"embedding_distance"`
}

type MisuseFinding struct {
	SrcFQN   string  `json:"src_fqn"`
	DstFQN   string  `json:"dst_fqn"`
	EdgeKind string  `json:"edge_kind"`
	Distance float64 `json:"embedding_distance"`
}

type FindingsResult struct {
	Repo        string               `json:"repo"`
	Snapshot    string               `json:"snapshot_id"`
	Duplication []DuplicationFinding `json:"duplication"`
	Misuse      []MisuseFinding      `json:"misuse"`
}

type findingSymbol struct {
	symbolID    string
	fqn         string
	contentHash string
}

func doFindings(ctx context.Context, sp *spanner.Client, repo, snapshotID string, limit int) (*FindingsResult, error) {
	if limit <= 0 {
		limit = 50
	}
	ref, err := resolveSnapshotRef(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot %q for repo %q is not ingested: %w", snapshotID, repo, err)
	}

	syms, err := findingSymbolsFromGeneration(ctx, sp, repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(syms))
	for _, s := range syms {
		hashes = append(hashes, s.contentHash)
	}
	embByHash, err := loadSymbolEmbeddings(ctx, sp, dedupStrings(hashes))
	if err != nil {
		return nil, err
	}
	// Keep only symbols whose embedding we actually have.
	var withEmb []findingSymbol
	for _, s := range syms {
		if embByHash[s.contentHash] != nil {
			withEmb = append(withEmb, s)
		}
	}

	edgeList, err := loadSnapshotEdgesFromGeneration(ctx, sp, repo, ref.StorageID)
	if err != nil {
		return nil, err
	}
	edgePairs := make(map[string]struct{}, len(edgeList))
	for _, e := range edgeList {
		edgePairs[edgeKey(e.Src, e.Dst)] = struct{}{}
	}

	res := &FindingsResult{Repo: repo, Snapshot: snapshotID}
	res.Duplication = mineDuplication(withEmb, embByHash, edgePairs, limit)
	res.Misuse = mineMisuse(withEmb, embByHash, edgeList, limit)
	return res, nil
}

// mineDuplication brute-forces the O(n^2) pairwise cosine across content
// symbols, parallelized across cores. Exact (not ANN) — n is a few thousand,
// so the full scan is a couple of seconds and needs no per-row index lookup.
func mineDuplication(syms []findingSymbol, embByHash map[string][]float32, edgePairs map[string]struct{}, limit int) []DuplicationFinding {
	n := len(syms)
	workers := runtime.NumCPU()
	var mu sync.Mutex
	var found []DuplicationFinding
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			var local []DuplicationFinding
			for i := start; i < n; i += workers {
				ei := embByHash[syms[i].contentHash]
				for j := i + 1; j < n; j++ {
					if syms[i].contentHash == syms[j].contentHash {
						continue // identical body — not a finding, it IS the same code
					}
					d := cosineDistance(ei, embByHash[syms[j].contentHash])
					if d > duplicationMaxDistance {
						continue
					}
					if _, connected := edgePairs[edgeKey(syms[i].symbolID, syms[j].symbolID)]; connected {
						continue // a real edge already relates them
					}
					local = append(local, DuplicationFinding{
						FQNA: syms[i].fqn, FQNB: syms[j].fqn, Distance: d,
					})
				}
			}
			mu.Lock()
			found = append(found, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()

	sort.Slice(found, func(i, j int) bool { return found[i].Distance < found[j].Distance })
	if len(found) > limit {
		found = found[:limit]
	}
	return found
}

func mineMisuse(syms []findingSymbol, embByHash map[string][]float32, edges []EdgeRow, limit int) []MisuseFinding {
	hashByID := map[string]string{}
	fqnByID := map[string]string{}
	for _, s := range syms {
		hashByID[s.symbolID] = s.contentHash
		fqnByID[s.symbolID] = s.fqn
	}
	var found []MisuseFinding
	for _, e := range edges {
		if e.Kind != "CALLS" && e.Kind != "REFERENCES" {
			continue
		}
		srcH, srcOK := hashByID[e.Src]
		dstH, dstOK := hashByID[e.Dst]
		if !srcOK || !dstOK {
			continue
		}
		srcE, dstE := embByHash[srcH], embByHash[dstH]
		if srcE == nil || dstE == nil {
			continue
		}
		d := cosineDistance(srcE, dstE)
		if d < misuseMinDistance {
			continue
		}
		found = append(found, MisuseFinding{
			SrcFQN: fqnByID[e.Src], DstFQN: fqnByID[e.Dst], EdgeKind: e.Kind, Distance: d,
		})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].Distance > found[j].Distance })
	if len(found) > limit {
		found = found[:limit]
	}
	return found
}

// findingSymbols returns the PRODUCTION content-bearing symbols of a snapshot.
// Test symbols (_test.go) are excluded: duplication mining is about production
// code health, and table-driven/parameterized tests are near-identical by
// nature — including them floods the duplication ranking and buries the real
// production pairs. Excluding them at the source cleans both miners at once.
func findingSymbols(ctx context.Context, sp *spanner.Client, repo, snapshotID string) ([]findingSymbol, error) {
	storageID, err := resolveSnapshotStorageID(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	return findingSymbolsFromGeneration(ctx, sp, repo, storageID)
}

func findingSymbolsFromGeneration(ctx context.Context, sp *spanner.Client, repo, storageID string) ([]findingSymbol, error) {
	stmt := spanner.Statement{
		SQL: `SELECT symbol_id, fqn, content_hash, file_path
		      FROM SnapshotMembers
		      WHERE repo = @repo AND snapshot_id = @snap AND content_hash IS NOT NULL`,
		Params: map[string]interface{}{"repo": repo, "snap": storageID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []findingSymbol
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var s findingSymbol
		var hash spanner.NullString
		var filePath string
		if err := row.Columns(&s.symbolID, &s.fqn, &hash, &filePath); err != nil {
			return nil, err
		}
		if isTestFile(filePath) {
			continue
		}
		s.contentHash = hash.StringVal
		out = append(out, s)
	}
	return out, nil
}

// edgeKey is order-independent so an edge in either direction counts as
// "connected" for the duplication filter.
func edgeKey(a, b string) string {
	if a < b {
		return a + "|" + b
	}
	return b + "|" + a
}

func dedupStrings(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
