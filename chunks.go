package main

import (
	"context"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Fidelity phase B1 — the structural master. Each .go file is decomposed into
// an ordered list of top-level chunks (package clause / import block /
// declaration / floating comment group), each stored as its EXACT source text.
// A file that fails to parse (a malformed testdata fixture) becomes one `raw`
// chunk. See FIDELITY_PHASE_B.md. B1 captures; B2 manifests + verifies.

// goChunk is one top-level unit of a .go file, exact source text preserved.
type goChunk struct {
	kind        string // package | import | decl | comment | raw
	text        string
	fqns        []string // FQNs a `decl` chunk declares — empty otherwise
	blankBefore bool     // a blank line preceded this chunk in the source
}

// decomposeGoFile splits a .go file into ordered chunks. pkgPath is the file's
// import path, used to build the symbol FQNs a declaration chunk declares.
func decomposeGoFile(path string, src []byte, pkgPath string) []goChunk {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return []goChunk{{kind: "raw", text: string(src)}}
	}
	off := func(p token.Pos) int {
		pos := fset.Position(p)
		if !pos.IsValid() {
			return 0
		}
		return pos.Offset
	}
	// eol advances o past the end of its line (newline included) — so a chunk
	// captures any trailing line comment on a declaration's last line.
	eol := func(o int) int {
		for o < len(src) && src[o] != '\n' {
			o++
		}
		if o < len(src) {
			o++
		}
		return o
	}
	type span struct {
		start, end int
		c          goChunk
	}
	var spans []span

	// Package chunk: byte 0 through the end of the `package X` clause line —
	// captures //go:build constraints, the package doc, and the clause.
	pkgEnd := eol(off(f.Name.End()))
	spans = append(spans, span{0, pkgEnd, goChunk{kind: "package", text: string(src[:pkgEnd])}})

	// One chunk per top-level declaration; span includes the doc comment.
	for _, d := range f.Decls {
		start := off(d.Pos())
		if doc := declDoc(d); doc != nil {
			if ds := off(doc.Pos()); ds < start {
				start = ds
			}
		}
		end := eol(off(d.End()))
		if start < 0 || end > len(src) || start >= end {
			continue
		}
		kind := "decl"
		if gd, ok := d.(*ast.GenDecl); ok && gd.Tok == token.IMPORT {
			kind = "import"
		}
		spans = append(spans, span{start, end, goChunk{
			kind: kind, text: string(src[start:end]), fqns: declFQNs(d, pkgPath),
		}})
	}

	// Floating comment chunks: comment groups outside the package span AND
	// outside every declaration span (the package-span exclusion stops
	// build constraints / package doc being captured twice).
	declSpans := len(spans)
	for _, cg := range f.Comments {
		cs := off(cg.Pos())
		contained := false
		for i := 0; i < declSpans; i++ {
			if cs >= spans[i].start && cs < spans[i].end {
				contained = true
				break
			}
		}
		if contained {
			continue
		}
		ce := eol(off(cg.End()))
		if cs < 0 || ce > len(src) || cs >= ce {
			continue
		}
		spans = append(spans, span{cs, ce, goChunk{kind: "comment", text: string(src[cs:ce])}})
	}

	sort.SliceStable(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	out := make([]goChunk, len(spans))
	for i := range spans {
		out[i] = spans[i].c
		// The gap between consecutive chunks is pure whitespace (a comment in
		// it would be its own chunk). A newline in that gap means a blank line
		// separated them — gofmt preserves that, so it must be captured.
		if i > 0 {
			prevEnd, start := spans[i-1].end, spans[i].start
			if prevEnd <= start && strings.Contains(string(src[prevEnd:start]), "\n") {
				out[i].blankBefore = true
			}
		}
	}
	return out
}

func declDoc(d ast.Decl) *ast.CommentGroup {
	switch x := d.(type) {
	case *ast.FuncDecl:
		return x.Doc
	case *ast.GenDecl:
		return x.Doc
	}
	return nil
}

// declFQNs returns the package-scoped symbol FQNs a declaration declares —
// one for a func/method, one per type, one per var/const name (a grouped
// var(...)/const(...) declares several), none for an import.
func declFQNs(d ast.Decl, pkgPath string) []string {
	var out []string
	switch x := d.(type) {
	case *ast.FuncDecl:
		out = append(out, funcFQN(pkgPath, x))
	case *ast.GenDecl:
		for _, spec := range x.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				out = append(out, pkgPath+"."+s.Name.Name)
			case *ast.ValueSpec:
				for _, n := range s.Names {
					out = append(out, pkgPath+"."+n.Name)
				}
			}
		}
	}
	return out
}

// modulePath reads the `module` line of repoPath/go.mod.
func modulePath(repoPath string) string {
	b, err := os.ReadFile(filepath.Join(repoPath, "go.mod"))
	if err != nil {
		return ""
	}
	for _, ln := range strings.Split(string(b), "\n") {
		if t := strings.TrimSpace(ln); strings.HasPrefix(t, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(t, "module"))
		}
	}
	return ""
}

// pkgPathForFile derives a file's import path from the module path and the
// file's directory relative to the module root.
func pkgPathForFile(modPath, relPath string) string {
	dir := filepath.ToSlash(filepath.Dir(relPath))
	if dir == "." || dir == "" {
		return modPath
	}
	return modPath + "/" + dir
}

// captureGoChunks decomposes every git-tracked .go file of repoPath into
// GoFileChunks + DeclSource for this snapshot.
func captureGoChunks(ctx context.Context, sp *spanner.Client, repoPath, repoName, snapshotID string, progress ProgressFunc) error {
	modPath := modulePath(repoPath)
	out, err := exec.Command("git", "-C", repoPath, "ls-files", "-z", "*.go").Output()
	if err != nil {
		return fmt.Errorf("git ls-files go: %w", err)
	}
	paths := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
	sort.Strings(paths)

	type chunkRow struct {
		file  string
		idx   int
		kind  string
		fqns  []string
		sha   string
		blank bool
	}
	var rows []chunkRow
	sources := map[string]string{} // content_sha -> exact text
	files, rawFiles := 0, 0
	for _, p := range paths {
		if p == "" {
			continue
		}
		src, err := os.ReadFile(filepath.Join(repoPath, p))
		if err != nil {
			continue
		}
		files++
		chunks := decomposeGoFile(p, src, pkgPathForFile(modPath, p))
		if len(chunks) == 1 && chunks[0].kind == "raw" {
			rawFiles++
		}
		for i, ch := range chunks {
			sha := sha256Hex(ch.text)
			sources[sha] = ch.text
			rows = append(rows, chunkRow{p, i, ch.kind, ch.fqns, sha, ch.blankBefore})
		}
	}

	// DeclSource — insert only content not already stored (global dedup).
	shaSet := make(map[string]struct{}, len(sources))
	for s := range sources {
		shaSet[s] = struct{}{}
	}
	present, err := selectPresentDeclShas(ctx, sp, shaSet)
	if err != nil {
		return err
	}
	var muts []*spanner.Mutation
	flush := func() error {
		if len(muts) == 0 {
			return nil
		}
		if _, err := sp.Apply(ctx, muts); err != nil {
			return err
		}
		muts = muts[:0]
		return nil
	}
	for sha, text := range sources {
		if _, ok := present[sha]; ok {
			continue
		}
		muts = append(muts, spanner.InsertOrUpdate("DeclSource",
			[]string{"content_sha", "source", "created_at"},
			[]interface{}{sha, text, spanner.CommitTimestamp},
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

	// GoFileChunks — the per-file ordered chunk list.
	for _, r := range rows {
		muts = append(muts, spanner.InsertOrUpdate("GoFileChunks",
			[]string{"repo", "snapshot_id", "file_path", "chunk_index", "chunk_kind", "symbol_fqns", "content_sha", "blank_before"},
			[]interface{}{repoName, snapshotID, r.file, int64(r.idx), r.kind, r.fqns, r.sha, r.blank},
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
		progress("chunks", len(rows), len(rows),
			fmt.Sprintf("%d chunks over %d .go files (%d unparseable→raw)", len(rows), files, rawFiles))
	}
	return nil
}

// ---------- B2: render the structural master back to Go ----------

// chunkText is one chunk of a .go file with its exact source — the render unit.
type chunkText struct {
	kind        string
	text        string
	blankBefore bool
}

// loadGoFileChunks returns, per .go file_path, the snapshot's ordered chunk
// texts (GoFileChunks joined to DeclSource).
func loadGoFileChunks(ctx context.Context, sp *spanner.Client, repo, snapshotID string) (map[string][]chunkText, error) {
	stmt := spanner.Statement{
		SQL: `SELECT c.file_path, c.chunk_index, c.chunk_kind, c.blank_before, d.source
		      FROM GoFileChunks AS c
		      JOIN DeclSource AS d ON d.content_sha = c.content_sha
		      WHERE c.repo = @repo AND c.snapshot_id = @snap
		      ORDER BY c.file_path, c.chunk_index`,
		Params: map[string]interface{}{"repo": repo, "snap": snapshotID},
	}
	iter := sp.Single().Query(ctx, stmt)
	defer iter.Stop()
	out := map[string][]chunkText{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var path, kind, source string
		var idx int64
		var blank spanner.NullBool
		if err := row.Columns(&path, &idx, &kind, &blank, &source); err != nil {
			return nil, err
		}
		out[path] = append(out[path], chunkText{kind: kind, text: source, blankBefore: blank.Valid && blank.Bool})
	}
	return out, nil
}

// isRawChunks reports whether a file was captured as a single unparseable
// `raw` chunk — it is rendered verbatim, not gofmt'd.
func isRawChunks(chunks []chunkText) bool {
	return len(chunks) == 1 && chunks[0].kind == "raw"
}

// gofmtSource is gofmt as a library call — used to put the verify comparison's
// disk side into the same gofmt-canonical form the structural render produces.
func gofmtSource(src []byte) ([]byte, error) {
	return format.Source(src)
}

// renderGoFile reassembles a .go file from its ordered chunks. A `raw` file is
// emitted verbatim; otherwise the chunk texts are joined with one blank line
// between and run through gofmt — the output is gofmt-canonical by construction
// (the Phase B fidelity standard; see FIDELITY_PHASE_B.md §2).
func renderGoFile(chunks []chunkText) ([]byte, error) {
	if isRawChunks(chunks) {
		return []byte(chunks[0].text), nil
	}
	var b strings.Builder
	for i, ch := range chunks {
		if i > 0 {
			// Separate chunks by a single newline (terminating the previous
			// chunk's last line) plus a blank line only where the source had
			// one. gofmt then keeps that 0-or-1 choice; forcing a blank
			// everywhere would space out adjacent declarations.
			if ch.blankBefore {
				b.WriteString("\n\n")
			} else {
				b.WriteString("\n")
			}
		}
		b.WriteString(strings.TrimRight(ch.text, "\n"))
	}
	b.WriteString("\n")
	formatted, err := format.Source([]byte(b.String()))
	if err != nil {
		return nil, fmt.Errorf("gofmt render: %w", err)
	}
	return formatted, nil
}

func selectPresentDeclShas(ctx context.Context, sp *spanner.Client, shaSet map[string]struct{}) (map[string]struct{}, error) {
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
			SQL:    `SELECT content_sha FROM DeclSource WHERE content_sha IN UNNEST(@shas)`,
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
