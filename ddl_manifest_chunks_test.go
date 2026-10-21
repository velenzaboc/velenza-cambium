package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSplitDDLStatementsAndColumns(t *testing.T) {
	raw := "\n  CREATE TABLE `Widgets` (\n  `id` STRING(36) NOT NULL,\n  score NUMERIC(20, 4),\n  CONSTRAINT valid_score CHECK (score > 0),\n  PRIMARY KEY (id)\n);\n\nALTER TABLE Widgets ADD COLUMN note STRING(MAX)"
	statements := splitDDLStatements(raw)
	if len(statements) != 2 {
		t.Fatalf("statements = %#v, want 2", statements)
	}
	if statements[0].line != 2 || statements[1].line != 9 {
		t.Fatalf("statement lines = %d/%d, want 2/9", statements[0].line, statements[1].line)
	}
	if got, want := parseTableColumns(statements[0].text), []string{"id", "score"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
	if got := parseTableColumns("CREATE TABLE Broken"); got != nil {
		t.Fatalf("columns without parenthesis = %v, want nil", got)
	}
	for _, r := range []rune{' ', '\t', '\n', '\r'} {
		if !isSpace(r) {
			t.Fatalf("isSpace(%q) = false", r)
		}
	}
	if isSpace('x') {
		t.Fatal("isSpace accepted non-whitespace")
	}
}

func TestParseAndScanDDL(t *testing.T) {
	raw := "CREATE TABLE Things (id STRING(36), payload ARRAY<STRING(MAX)>, FOREIGN KEY (id) REFERENCES Other (id)) PRIMARY KEY (id);\n" +
		"CREATE UNIQUE NULL_FILTERED INDEX ThingsByPayload ON Things(payload);\n" +
		"ALTER TABLE Things ADD COLUMN created_at TIMESTAMP;\nSELECT 1;"
	var symbols []Symbol
	var edges []Edge
	parseDDLFile(raw, "schema.sql", "repo", "snap", &symbols, &edges)
	gotKinds := map[string]string{}
	for _, symbol := range symbols {
		gotKinds[symbol.FQN] = symbol.Kind
		if symbol.Repo != "repo" || symbol.SnapshotID != "snap" || symbol.FilePath != "schema.sql" {
			t.Fatalf("symbol lost provenance: %+v", symbol)
		}
	}
	wantKinds := map[string]string{
		"ddl:Things": "table", "ddl:Things.id": "column", "ddl:Things.payload": "column",
		"ddl:index:ThingsByPayload": "index", "ddl:Things.created_at": "column",
	}
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("symbols = %v, want %v", gotKinds, wantKinds)
	}
	if len(edges) != 4 {
		t.Fatalf("edges = %d, want 4: %+v", len(edges), edges)
	}
	if symbols[0].ID != uuidFromFQN(symbols[0].FQN) || symbols[0].LineStart != 1 {
		t.Fatalf("table identity/line = %+v", symbols[0])
	}

	repo := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repo, "nested", "schema.sql"), raw)
	mustWriteTestFile(t, filepath.Join(repo, "ignored.txt"), "CREATE TABLE Ignored (id INT64);")
	mustWriteTestFile(t, filepath.Join(repo, ".git", "hidden.sql"), "CREATE TABLE Hidden (id INT64);")
	scanned, scannedEdges, err := scanDDL(repo, "repo", "snap")
	if err != nil {
		t.Fatalf("scan DDL: %v", err)
	}
	if len(scanned) != len(symbols) || len(scannedEdges) != len(edges) {
		t.Fatalf("scan counts = %d/%d, want %d/%d", len(scanned), len(scannedEdges), len(symbols), len(edges))
	}
	for _, symbol := range scanned {
		if strings.Contains(symbol.FQN, "Ignored") || strings.Contains(symbol.FQN, "Hidden") {
			t.Fatalf("scan admitted ignored source: %+v", symbol)
		}
	}
}

func TestParseAndScanManifest(t *testing.T) {
	raw := []byte(`{"passes":{"z":{"ok":true},"a":{"count":2}}}`)
	symbols := parseManifest(raw, "conformance_manifest.json", "repo", "snap")
	if len(symbols) != 2 || symbols[0].FQN != "conformance:a" || symbols[1].FQN != "conformance:z" {
		t.Fatalf("sorted manifest symbols = %+v", symbols)
	}
	if symbols[0].ID != uuidFromFQN("conformance:a") || !strings.Contains(symbols[0].Body, `"count": 2`) {
		t.Fatalf("manifest symbol = %+v", symbols[0])
	}
	if got := parseManifest([]byte("{"), "bad.json", "repo", "snap"); got != nil {
		t.Fatalf("invalid manifest = %+v, want nil", got)
	}

	repo := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repo, "Conformance_Manifest.JSON"), string(raw))
	mustWriteTestFile(t, filepath.Join(repo, "manifest.json"), string(raw))
	mustWriteTestFile(t, filepath.Join(repo, ".git", "conformance_manifest.json"), string(raw))
	scanned, edges, err := scanManifest(repo, "repo", "snap")
	if err != nil {
		t.Fatalf("scan manifest: %v", err)
	}
	if len(scanned) != 2 || len(edges) != 0 {
		t.Fatalf("scan manifest symbols/edges = %d/%d", len(scanned), len(edges))
	}
}

func TestDecomposeAndRenderGoFile(t *testing.T) {
	source := []byte("//go:build linux\n\n// Package sample documents the package.\npackage sample\n\nimport (\n\t\"fmt\"\n)\n\n// T documents T.\ntype T struct{}\n\n// floating\n\nfunc (T) Method() { fmt.Println(\"ok\") } // trailing\n\nvar (\n\tA = 1\n\tB = 2\n)\n")
	chunks := decomposeGoFile("sample.go", source, "example.test/sample")
	if len(chunks) != 6 {
		t.Fatalf("chunk count = %d, want 6: %#v", len(chunks), chunks)
	}
	wantKinds := []string{"package", "import", "decl", "comment", "decl", "decl"}
	for i, want := range wantKinds {
		if chunks[i].kind != want {
			t.Fatalf("chunk %d kind = %q, want %q", i, chunks[i].kind, want)
		}
	}
	if got, want := chunks[2].fqns, []string{"example.test/sample.T"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("type FQNs = %v, want %v", got, want)
	}
	if got, want := chunks[4].fqns, []string{"example.test/sample.T.Method"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("method FQNs = %v, want %v", got, want)
	}
	if got, want := chunks[5].fqns, []string{"example.test/sample.A", "example.test/sample.B"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("value FQNs = %v, want %v", got, want)
	}
	texts := make([]chunkText, len(chunks))
	for i, chunk := range chunks {
		texts[i] = chunkText{kind: chunk.kind, text: chunk.text, blankBefore: chunk.blankBefore}
	}
	rendered, err := renderGoFile(texts)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	formatted, err := gofmtSource(source)
	if err != nil {
		t.Fatalf("gofmt source: %v", err)
	}
	if string(rendered) != string(formatted) {
		t.Fatalf("render mismatch\n--- got ---\n%s\n--- want ---\n%s", rendered, formatted)
	}

	broken := []byte("package broken\nfunc {")
	raw := decomposeGoFile("broken.go", broken, "example.test/broken")
	if len(raw) != 1 || raw[0].kind != "raw" || raw[0].text != string(broken) {
		t.Fatalf("raw decomposition = %#v", raw)
	}
	rawRendered, err := renderGoFile([]chunkText{{kind: "raw", text: string(broken)}})
	if err != nil || string(rawRendered) != string(broken) {
		t.Fatalf("raw render = %q, %v", rawRendered, err)
	}
	if !isRawChunks([]chunkText{{kind: "raw"}}) || isRawChunks(nil) || isRawChunks([]chunkText{{kind: "decl"}}) {
		t.Fatal("raw chunk classification is incorrect")
	}
	if _, err := gofmtSource(broken); err == nil {
		t.Fatal("invalid Go source unexpectedly formatted")
	}
	if _, err := renderGoFile([]chunkText{{kind: "decl", text: "func {"}}); err == nil || !strings.Contains(err.Error(), "gofmt render") {
		t.Fatalf("invalid render error = %v", err)
	}
}

func TestDeclarationHelpersAndModulePaths(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", `package p
import "fmt"
// F doc.
func F() { fmt.Println() }
type T int
const A, B = 1, 2
`, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	if declDoc(file.Decls[0]) != nil {
		t.Fatal("import unexpectedly has declaration doc")
	}
	if declDoc(file.Decls[1]) == nil {
		t.Fatal("function doc was not found")
	}
	var fqns []string
	for _, declaration := range file.Decls {
		fqns = append(fqns, declFQNs(declaration, "example.test/p")...)
	}
	want := []string{"example.test/p.F", "example.test/p.T", "example.test/p.A", "example.test/p.B"}
	if !reflect.DeepEqual(fqns, want) {
		t.Fatalf("declaration FQNs = %v, want %v", fqns, want)
	}
	if got := declFQNs(&ast.GenDecl{Tok: token.IMPORT}, "example.test/p"); len(got) != 0 {
		t.Fatalf("import FQNs = %v", got)
	}

	repo := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repo, "go.mod"), "// lead\nmodule example.test/root\n\ngo 1.27\n")
	if got := modulePath(repo); got != "example.test/root" {
		t.Fatalf("module path = %q", got)
	}
	if got := modulePath(filepath.Join(repo, "missing")); got != "" {
		t.Fatalf("missing module path = %q", got)
	}
	if got := pkgPathForFile("example.test/root", "root.go"); got != "example.test/root" {
		t.Fatalf("root package path = %q", got)
	}
	if got := pkgPathForFile("example.test/root", filepath.Join("internal", "thing.go")); got != "example.test/root/internal" {
		t.Fatalf("nested package path = %q", got)
	}
}

func mustWriteTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
