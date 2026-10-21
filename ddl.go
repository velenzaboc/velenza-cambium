package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// DDL ingestion: .sql files in a repo become table / column / index symbols in
// the snapshot, so the graph can answer schema-shape questions (e.g. "does this
// table have the required UNIQUE NULL_FILTERED index"). FQNs are prefixed
// `ddl:` so they are easy to enumerate with `symbols --match '^ddl:'`.

var (
	reCreateTable = regexp.MustCompile(`(?is)^\s*CREATE\s+TABLE\s+` + "`?" + `(\w+)` + "`?")
	reCreateIndex = regexp.MustCompile(`(?is)^\s*CREATE\s+(?:UNIQUE\s+)?(?:NULL_FILTERED\s+)?(?:SEARCH\s+|VECTOR\s+)?INDEX\s+` + "`?" + `(\w+)` + "`?" + `\s+ON\s+` + "`?" + `(\w+)`)
	reAlterAddCol = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+` + "`?" + `(\w+)` + "`?" + `\s+ADD\s+COLUMN\s+` + "`?" + `(\w+)`)
	ddlConstraint = map[string]bool{
		"PRIMARY": true, "FOREIGN": true, "CONSTRAINT": true,
		"INTERLEAVE": true, "CHECK": true, "ROW": true,
	}
)

// scanDDL walks repoPath for .sql files and parses their DDL.
func scanDDL(repoPath, repoName, snapshotID string) ([]Symbol, []Edge, error) {
	var syms []Symbol
	var edges []Edge
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
		if !strings.HasSuffix(p, ".sql") {
			return nil
		}
		raw, e := os.ReadFile(p)
		if e != nil {
			return nil
		}
		rel, _ := filepath.Rel(repoPath, p)
		parseDDLFile(string(raw), rel, repoName, snapshotID, &syms, &edges)
		return nil
	})
	return syms, edges, err
}

// ddlStatement is one ; -terminated statement with the file line it began on.
type ddlStatement struct {
	text string
	line int
}

func splitDDLStatements(raw string) []ddlStatement {
	var out []ddlStatement
	var buf strings.Builder
	line, stmtLine := 1, 0
	for _, r := range raw {
		if r == '\n' {
			line++
		}
		if stmtLine == 0 && !isSpace(r) {
			stmtLine = line
		}
		if r == ';' {
			if t := strings.TrimSpace(buf.String()); t != "" {
				out = append(out, ddlStatement{t, stmtLine})
			}
			buf.Reset()
			stmtLine = 0
			continue
		}
		buf.WriteRune(r)
	}
	if t := strings.TrimSpace(buf.String()); t != "" {
		out = append(out, ddlStatement{t, stmtLine})
	}
	return out
}

func isSpace(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' }

func parseDDLFile(raw, relPath, repoName, snapshotID string, syms *[]Symbol, edges *[]Edge) {
	addEdge := func(srcFQN, dstFQN, kind string) {
		*edges = append(*edges, Edge{
			Repo: repoName, SnapshotID: snapshotID,
			SrcID: uuidFromFQN(srcFQN), DstID: uuidFromFQN(dstFQN), Kind: kind,
		})
	}
	for _, st := range splitDDLStatements(raw) {
		switch {
		case reCreateTable.MatchString(st.text):
			m := reCreateTable.FindStringSubmatch(st.text)
			table := m[1]
			tableFQN := "ddl:" + table
			*syms = append(*syms, Symbol{
				ID: uuidFromFQN(tableFQN), Repo: repoName, SnapshotID: snapshotID,
				FQN: tableFQN, Kind: "table", FilePath: relPath,
				LineStart: st.line, LineEnd: st.line + strings.Count(st.text, "\n"),
				Signature: "table " + table, Body: clip(st.text, bodyMaxBytes),
			})
			for _, col := range parseTableColumns(st.text) {
				colFQN := tableFQN + "." + col
				*syms = append(*syms, Symbol{
					ID: uuidFromFQN(colFQN), Repo: repoName, SnapshotID: snapshotID,
					FQN: colFQN, Kind: "column", FilePath: relPath, LineStart: st.line, LineEnd: st.line,
					Signature: "column " + table + "." + col,
				})
				addEdge(tableFQN, colFQN, "DECLARES")
			}
		case reCreateIndex.MatchString(st.text):
			m := reCreateIndex.FindStringSubmatch(st.text)
			idx, table := m[1], m[2]
			idxFQN := "ddl:index:" + idx
			*syms = append(*syms, Symbol{
				ID: uuidFromFQN(idxFQN), Repo: repoName, SnapshotID: snapshotID,
				FQN: idxFQN, Kind: "index", FilePath: relPath,
				LineStart: st.line, LineEnd: st.line + strings.Count(st.text, "\n"),
				Signature: "index " + idx + " ON " + table, Body: clip(st.text, bodyMaxBytes),
			})
			addEdge(idxFQN, "ddl:"+table, "INDEXES")
		case reAlterAddCol.MatchString(st.text):
			m := reAlterAddCol.FindStringSubmatch(st.text)
			table, col := m[1], m[2]
			tableFQN, colFQN := "ddl:"+table, "ddl:"+table+"."+col
			*syms = append(*syms, Symbol{
				ID: uuidFromFQN(colFQN), Repo: repoName, SnapshotID: snapshotID,
				FQN: colFQN, Kind: "column", FilePath: relPath, LineStart: st.line, LineEnd: st.line,
				Signature: "column " + table + "." + col + " (ALTER ADD)",
			})
			addEdge(tableFQN, colFQN, "DECLARES")
		}
	}
}

// parseTableColumns extracts column names from a CREATE TABLE statement —
// the depth-1 comma-separated entries inside the outer (...), skipping
// constraint clauses (PRIMARY KEY, FOREIGN KEY, CONSTRAINT, ...).
func parseTableColumns(stmt string) []string {
	open := strings.Index(stmt, "(")
	if open < 0 {
		return nil
	}
	depth := 0
	var body strings.Builder
	for _, r := range stmt[open:] {
		switch r {
		case '(':
			depth++
			if depth == 1 {
				continue
			}
		case ')':
			depth--
			if depth == 0 {
				goto done
			}
		}
		body.WriteRune(r)
	}
done:
	var cols []string
	depth = 0
	var part strings.Builder
	flush := func() {
		f := strings.Fields(strings.TrimSpace(part.String()))
		part.Reset()
		if len(f) == 0 {
			return
		}
		if ddlConstraint[strings.ToUpper(f[0])] {
			return
		}
		cols = append(cols, strings.Trim(f[0], "`"))
	}
	for _, r := range body.String() {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				flush()
				continue
			}
		}
		part.WriteRune(r)
	}
	flush()
	return cols
}
