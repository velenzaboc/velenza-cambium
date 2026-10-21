package main

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Conformance-manifest ingestion: a conformance_manifest.json in the repo
// becomes one `conformance-pass` symbol per pass entry, so the manifest's
// conformance facts are queryable alongside the Go symbols. FQNs are prefixed
// `conformance:`.

// scanManifest walks repoPath for conformance manifest JSON files and turns
// each pass entry into a symbol.
func scanManifest(repoPath, repoName, snapshotID string) ([]Symbol, []Edge, error) {
	var syms []Symbol
	err := filepath.WalkDir(repoPath, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		base := strings.ToLower(d.Name())
		if !strings.Contains(base, "conformance") || !strings.Contains(base, "manifest") || !strings.HasSuffix(base, ".json") {
			return nil
		}
		raw, e := os.ReadFile(p)
		if e != nil {
			return nil
		}
		rel, _ := filepath.Rel(repoPath, p)
		syms = append(syms, parseManifest(raw, rel, repoName, snapshotID)...)
		return nil
	})
	return syms, nil, err
}

func parseManifest(raw []byte, relPath, repoName, snapshotID string) []Symbol {
	var doc struct {
		Passes map[string]json.RawMessage `json:"passes"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return nil
	}
	names := make([]string, 0, len(doc.Passes))
	for n := range doc.Passes {
		names = append(names, n)
	}
	sort.Strings(names)
	var syms []Symbol
	for _, name := range names {
		fqn := "conformance:" + name
		pretty, _ := json.MarshalIndent(json.RawMessage(doc.Passes[name]), "", "  ")
		syms = append(syms, Symbol{
			ID: uuidFromFQN(fqn), Repo: repoName, SnapshotID: snapshotID,
			FQN: fqn, Kind: "conformance-pass", FilePath: relPath,
			Signature: "conformance pass " + name,
			Body:      clip(string(pretty), bodyMaxBytes),
		})
	}
	return syms
}
