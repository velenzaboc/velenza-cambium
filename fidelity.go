package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Fidelity layer — lossless source capture for the ingest → edit → manifest
// pipeline. See FIDELITY_PLAN.md.
//
// The app's editable master is the structural form (the AST as data — phase
// B). This file is phase A: it captures the raw bytes of every SOURCE file as
// (1) the ingest-time completeness oracle that proves the master lost nothing,
// and (2) the carrier for non-Go source files (go.mod, embeds, testdata) which
// have no AST. It is NOT a second editable copy — manifest renders the master.
//
// Build outputs (compiled binaries) are skipped: they are the export, not the
// project. File content is content-addressed (dedup across snapshots) and
// chunked (Spanner caps a column value at 10 MiB; losslessness admits no cap).

const (
	fileChunkSize       = 4 << 20 // 4 MiB per chunk — well under Spanner's 10 MiB column cap
	fidelityCommitBytes = 8 << 20 // flush FileContents mutations once ~8 MiB has accumulated
)

// isBuildOutput reports whether b begins with a compiled-executable magic
// number — ELF, Mach-O (32/64, both byte orders), Mach-O fat/universal, or
// PE/DOS. Such files are build output, not app source, and are not captured.
func isBuildOutput(b []byte) bool {
	if len(b) < 4 {
		return false
	}
	if b[0] == 0x7F && b[1] == 'E' && b[2] == 'L' && b[3] == 'F' {
		return true // ELF
	}
	if b[0] == 'M' && b[1] == 'Z' {
		return true // PE / DOS
	}
	switch binary.BigEndian.Uint32(b[:4]) {
	case 0xFEEDFACE, 0xFEEDFACF, 0xCEFAEDFE, 0xCFFAEDFE, 0xCAFEBABE, 0xBEBAFECA:
		return true // Mach-O 32/64 BE/LE, fat/universal
	}
	return false
}

type capturedFile struct {
	path  string
	sha   string
	bytes []byte
	isGo  bool
}

// captureFiles records every tracked SOURCE file of repoPath into the
// fidelity tables for this snapshot. Supports both native Velenza Inarch
// (.inarch) workspaces and Git (.git) repositories; compiled build outputs are skipped.
func captureFiles(ctx context.Context, sp *spanner.Client, repoPath, repoName, snapshotID string, progress ProgressFunc) error {
	paths, err := listTrackedFiles(repoPath)
	if err != nil {
		return err
	}

	var files []capturedFile
	shaSet := map[string]struct{}{}
	skipped := 0
	for _, p := range paths {
		if p == "" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(repoPath, p))
		if err != nil {
			continue // tracked in the index but absent on disk — skip
		}
		if isBuildOutput(b) {
			skipped++
			continue // compiled binary — build output, not app source
		}
		sha := sha256Hex(string(b))
		files = append(files, capturedFile{path: p, sha: sha, bytes: b, isGo: strings.HasSuffix(p, ".go")})
		shaSet[sha] = struct{}{}
	}

	present, err := selectPresentShas(ctx, sp, shaSet)
	if err != nil {
		return err
	}

	// FileContents — insert chunked content for shas not already stored.
	var muts []*spanner.Mutation
	pending := 0
	flush := func() error {
		if len(muts) == 0 {
			return nil
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		muts = muts[:0]
		pending = 0
		return nil
	}
	seen := map[string]struct{}{}
	newContent := 0
	for _, f := range files {
		if _, ok := present[f.sha]; ok {
			continue
		}
		if _, ok := seen[f.sha]; ok {
			continue
		}
		seen[f.sha] = struct{}{}
		newContent++
		for ci, off := 0, 0; off < len(f.bytes) || ci == 0; ci, off = ci+1, off+fileChunkSize {
			end := off + fileChunkSize
			if end > len(f.bytes) {
				end = len(f.bytes)
			}
			chunk := f.bytes[off:end] // ci==0 with an empty file yields a single empty chunk
			muts = append(muts, spanner.InsertOrUpdate("FileContents",
				[]string{"content_sha", "chunk_index", "bytes", "created_at"},
				[]interface{}{f.sha, int64(ci), chunk, spanner.CommitTimestamp},
			))
			pending += len(chunk)
			if pending >= fidelityCommitBytes {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// SnapshotFiles — the per-snapshot path→content manifest.
	for i, f := range files {
		muts = append(muts, spanner.InsertOrUpdate("SnapshotFiles",
			[]string{"repo", "snapshot_id", "file_path", "content_sha", "is_go", "file_order"},
			[]interface{}{repoName, snapshotID, f.path, f.sha, f.isGo, int64(i)},
		))
		if len(muts) >= mutationBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if progress != nil {
		progress("files", len(files), len(files),
			fmt.Sprintf("%d source files captured (%d new content blobs, %d build outputs skipped)",
				len(files), newContent, skipped))
	}
	return nil
}

// selectPresentShas returns which content_shas already have content stored.
// Chunks of a sha are always written together, so chunk 0's presence implies
// the whole file is present.
func selectPresentShas(ctx context.Context, sp *spanner.Client, shaSet map[string]struct{}) (map[string]struct{}, error) {
	shas := make([]string, 0, len(shaSet))
	for s := range shaSet {
		shas = append(shas, s)
	}
	present := map[string]struct{}{}
	const chunk = 1000
	for start := 0; start < len(shas); start += chunk {
		end := start + chunk
		if end > len(shas) {
			end = len(shas)
		}
		iter := sp.Single().Query(ctx, spanner.Statement{
			SQL:    `SELECT content_sha FROM FileContents WHERE chunk_index = 0 AND content_sha IN UNNEST(@shas)`,
			Params: map[string]interface{}{"shas": shas[start:end]},
		})
		err := func() error {
			defer iter.Stop()
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					return nil
				}
				if err != nil {
					return err
				}
				var s string
				if err := row.Columns(&s); err != nil {
					return err
				}
				present[s] = struct{}{}
			}
		}()
		if err != nil {
			return nil, err
		}
	}
	return present, nil
}

// snapshotFileRow is one file of a snapshot with its full reassembled content.
type snapshotFileRow struct {
	path  string
	sha   string
	isGo  bool
	bytes []byte
}

// loadSnapshotFiles returns every file of a snapshot, content reassembled from
// its chunks, ordered by file_order.
func loadSnapshotFiles(ctx context.Context, sp *spanner.Client, repo, snapshotID string) ([]snapshotFileRow, error) {
	stmt := spanner.Statement{
		SQL: `SELECT sf.file_path, sf.content_sha, sf.is_go, sf.file_order, fc.chunk_index, fc.bytes
		      FROM SnapshotFiles AS sf
		      JOIN FileContents AS fc ON fc.content_sha = sf.content_sha
		      WHERE sf.repo = @repo AND sf.snapshot_id = @snap
		      ORDER BY sf.file_order, fc.chunk_index`,
		Params: map[string]interface{}{"repo": repo, "snap": snapshotID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	var out []snapshotFileRow
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var path, sha string
		var isGo bool
		var fileOrder, chunkIndex int64
		var chunk []byte
		if err := row.Columns(&path, &sha, &isGo, &fileOrder, &chunkIndex, &chunk); err != nil {
			return nil, err
		}
		// file_order makes a file's chunk rows contiguous; chunk_index orders
		// them within. Append to the current file, or start a new one.
		if len(out) > 0 && out[len(out)-1].path == path {
			out[len(out)-1].bytes = append(out[len(out)-1].bytes, chunk...)
		} else {
			out = append(out, snapshotFileRow{path: path, sha: sha, isGo: isGo, bytes: append([]byte(nil), chunk...)})
		}
	}
	return out, nil
}

// doManifest writes every file of a snapshot to outDir, recreating the tree.
// fromStructure renders .go files from the Phase B structural master (chunks,
// gofmt-canonical); otherwise every file is written from Phase A raw bytes.
func doManifest(ctx context.Context, sp *spanner.Client, repo, snapshotID, outDir string, fromStructure bool) (int, error) {
	ok, err := snapshotExists(ctx, sp, repo, snapshotID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("snapshot %q for repo %q is not ingested", snapshotID, repo)
	}
	files, err := loadSnapshotFiles(ctx, sp, repo, snapshotID)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("snapshot %q has no captured files — it was ingested before the fidelity layer; re-ingest", snapshotID)
	}
	var chunks map[string][]chunkText
	if fromStructure {
		if chunks, err = loadGoFileChunks(ctx, sp, repo, snapshotID); err != nil {
			return 0, err
		}
	}
	for _, f := range files {
		content := f.bytes
		if fromStructure && f.isGo {
			cs, ok := chunks[f.path]
			if !ok {
				return 0, fmt.Errorf("no structural chunks for .go file %s — re-ingest", f.path)
			}
			rendered, err := renderGoFile(cs)
			if err != nil {
				return 0, fmt.Errorf("render %s: %w", f.path, err)
			}
			content = rendered
		}
		dst := filepath.Join(outDir, filepath.FromSlash(f.path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return 0, err
		}
		if err := os.WriteFile(dst, content, 0o644); err != nil {
			return 0, err
		}
	}
	return len(files), nil
}

// VerifyResult is the outcome of a fidelity round-trip check.
type VerifyResult struct {
	Files      int      `json:"files"`
	Differing  []string `json:"differing"`
	OnlyInSnap []string `json:"only_in_snapshot"`
	OnlyOnDisk []string `json:"only_on_disk"`
}

// Clean reports whether the snapshot reproduces the on-disk tree exactly.
func (v VerifyResult) Clean() bool {
	return len(v.Differing) == 0 && len(v.OnlyInSnap) == 0 && len(v.OnlyOnDisk) == 0
}

// doVerify diffs a snapshot against an on-disk git tree — the fidelity
// round-trip check. Without fromStructure it byte-compares Phase A raw bytes
// (byte-exact). With fromStructure it renders .go files from the Phase B
// structural master and compares gofmt-canonical forms — gofmt on both sides
// (the original is gofmt'd too, since 24% of the kernel is not yet canonical);
// non-Go files still byte-compare. Build outputs are excluded (not captured).
func doVerify(ctx context.Context, sp *spanner.Client, repo, snapshotID, originalDir string, fromStructure bool) (*VerifyResult, error) {
	files, err := loadSnapshotFiles(ctx, sp, repo, snapshotID)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("snapshot %q has no captured files — re-ingest with the fidelity layer", snapshotID)
	}
	var chunks map[string][]chunkText
	if fromStructure {
		if chunks, err = loadGoFileChunks(ctx, sp, repo, snapshotID); err != nil {
			return nil, err
		}
	}
	trackedPaths, err := listTrackedFiles(originalDir)
	if err != nil {
		return nil, err
	}
	disk := map[string]struct{}{}
	for _, p := range trackedPaths {
		if p == "" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(originalDir, filepath.FromSlash(p)))
		if err != nil || isBuildOutput(b) {
			continue // unreadable or a build output — not part of the captured set
		}
		disk[p] = struct{}{}
	}
	res := &VerifyResult{Files: len(files)}
	for _, f := range files {
		if _, ok := disk[f.path]; !ok {
			res.OnlyInSnap = append(res.OnlyInSnap, f.path)
			continue
		}
		delete(disk, f.path)
		onDisk, err := os.ReadFile(filepath.Join(originalDir, filepath.FromSlash(f.path)))
		if err != nil {
			res.Differing = append(res.Differing, f.path)
			continue
		}
		if !verifyMatch(f, onDisk, fromStructure, chunks) {
			res.Differing = append(res.Differing, f.path)
		}
	}
	for p := range disk {
		res.OnlyOnDisk = append(res.OnlyOnDisk, p)
	}
	sort.Strings(res.Differing)
	sort.Strings(res.OnlyInSnap)
	sort.Strings(res.OnlyOnDisk)
	return res, nil
}

// verifyMatch reports whether a snapshot file reproduces its on-disk original.
// fromStructure .go files: render from chunks and compare gofmt-canonical forms
// (a `raw` file compares verbatim). Everything else: Phase A byte compare.
func verifyMatch(f snapshotFileRow, onDisk []byte, fromStructure bool, chunks map[string][]chunkText) bool {
	if !fromStructure || !f.isGo {
		return bytes.Equal(f.bytes, onDisk)
	}
	cs, ok := chunks[f.path]
	if !ok {
		return false
	}
	rendered, err := renderGoFile(cs)
	if err != nil {
		return false
	}
	if isRawChunks(cs) {
		return bytes.Equal(rendered, onDisk)
	}
	diskFmt, err := gofmtSource(onDisk)
	if err != nil {
		return false
	}
	return bytes.Equal(rendered, diskFmt)
}

// ---------- CLI commands ----------

func cmdManifest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("manifest", flag.ExitOnError)
	fromStruct := fs.Bool("from-structure", false, "render .go files from the Phase B structural master (gofmt-canonical) instead of Phase A raw bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 3 {
		return fmt.Errorf("manifest needs three args: [--from-structure] <repo> <snapshot-id> <out-dir>")
	}
	repo, snap, outDir := fs.Arg(0), fs.Arg(1), fs.Arg(2)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	n, err := doManifest(ctx, sp, repo, snap, outDir, *fromStruct)
	if err != nil {
		return err
	}
	mode := "Phase A raw bytes"
	if *fromStruct {
		mode = "structural master"
	}
	fmt.Printf("manifested %d files from %s@%s into %s (%s)\n", n, repo, snap, outDir, mode)
	return nil
}

func cmdVerify(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	fromStruct := fs.Bool("from-structure", false, "render .go files from the structural master and gofmt-diff (instead of a Phase A raw-byte diff)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 3 {
		return fmt.Errorf("verify needs three args: [--from-structure] <repo> <snapshot-id> <original-dir>")
	}
	repo, snap, dir := fs.Arg(0), fs.Arg(1), fs.Arg(2)
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	res, err := doVerify(ctx, sp, repo, snap, dir, *fromStruct)
	if err != nil {
		return err
	}
	if res.Clean() {
		std := "byte-for-byte"
		if *fromStruct {
			std = "gofmt-canonical (structural master)"
		}
		fmt.Printf("VERIFY CLEAN — %d files, snapshot reproduces %s %s\n", res.Files, dir, std)
		return nil
	}
	fmt.Printf("VERIFY MISMATCH — %d files checked\n", res.Files)
	report := func(label string, items []string) {
		if len(items) == 0 {
			return
		}
		fmt.Printf("  %s (%d):\n", label, len(items))
		for _, p := range items {
			fmt.Printf("    %s\n", p)
		}
	}
	report("content differs", res.Differing)
	report("in snapshot, not on disk", res.OnlyInSnap)
	report("on disk, not in snapshot", res.OnlyOnDisk)
	return fmt.Errorf("verify failed: %d differing, %d snapshot-only, %d disk-only",
		len(res.Differing), len(res.OnlyInSnap), len(res.OnlyOnDisk))
}
