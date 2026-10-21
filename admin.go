package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var spannerInstance = strings.TrimSuffix(spannerDB, "/databases/code-graph")

// These narrow call seams keep the command's DDL admission behavior testable
// without making test code construct live Google Cloud clients.
var (
	newCodeGraphDatabaseAdmin   = newDatabaseAdminClient
	closeCodeGraphDatabaseAdmin = func(client *database.DatabaseAdminClient) error {
		return client.Close()
	}
	dropCodeGraphDatabase = func(ctx context.Context, client *database.DatabaseAdminClient, req *databasepb.DropDatabaseRequest) error {
		return client.DropDatabase(ctx, req)
	}
	createCodeGraphDatabase = func(ctx context.Context, client *database.DatabaseAdminClient, req *databasepb.CreateDatabaseRequest) (func(context.Context) error, error) {
		op, err := client.CreateDatabase(ctx, req)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { _, err := op.Wait(ctx); return err }, nil
	}
	updateCodeGraphDatabaseDDL = func(ctx context.Context, client *database.DatabaseAdminClient, req *databasepb.UpdateDatabaseDdlRequest) (func(context.Context) error, error) {
		op, err := client.UpdateDatabaseDdl(ctx, req)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { return op.Wait(ctx) }, nil
	}
)

// cmdCreateDB creates the code-graph database and applies schema.sql.
// With --drop it first drops any existing database (full recreate).
func cmdCreateDB(ctx context.Context, args []string) error {
	drop := false
	schemaPath := "schema.sql"
	for _, a := range args {
		switch {
		case a == "--drop":
			drop = true
		case strings.HasSuffix(a, ".sql"):
			schemaPath = a
		default:
			return fmt.Errorf("unknown arg %q (expected --drop and/or a path to schema.sql)", a)
		}
	}

	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	stmts := splitDDL(string(raw))
	if len(stmts) == 0 {
		return fmt.Errorf("schema %q produced no statements", schemaPath)
	}

	adm, err := newCodeGraphDatabaseAdmin(ctx)
	if err != nil {
		return err
	}
	defer closeCodeGraphDatabaseAdmin(adm)

	dbPath := resolveSpannerDBPath()
	instPath := resolveSpannerInstancePath()
	dbName := resolveDefaultSpannerDatabase()

	if drop {
		if err := dropCodeGraphDatabase(ctx, adm, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
			if status.Code(err) != codes.NotFound {
				return fmt.Errorf("drop: %w", err)
			}
		} else {
			fmt.Println("dropped existing database")
		}
	}

	wait, err := createCodeGraphDatabase(ctx, adm, &databasepb.CreateDatabaseRequest{
		Parent:          instPath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
		ExtraStatements: stmts,
	})
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	fmt.Printf("creating database + applying %d DDL statements...\n", len(stmts))
	if err := wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	fmt.Println("database code-graph is READY")
	return nil
}

// cmdMigrate applies DDL statements (from a .sql file) to the existing
// database via UpdateDatabaseDdl — for additive schema changes (ADD COLUMN,
// CREATE TABLE, CREATE INDEX) that must not drop existing data.
func cmdMigrate(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("migrate needs one arg: <ddl-file>")
	}
	raw, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("read ddl: %w", err)
	}
	stmts := splitDDL(string(raw))
	if len(stmts) == 0 {
		return fmt.Errorf("%q produced no statements", args[0])
	}
	adm, err := newCodeGraphDatabaseAdmin(ctx)
	if err != nil {
		return err
	}
	defer closeCodeGraphDatabaseAdmin(adm)
	wait, err := updateCodeGraphDatabaseDDL(ctx, adm, &databasepb.UpdateDatabaseDdlRequest{
		Database:   resolveSpannerDBPath(),
		Statements: stmts,
	})
	if err != nil {
		return fmt.Errorf("update ddl: %w", err)
	}
	fmt.Printf("applying %d DDL statement(s)...\n", len(stmts))
	if err := wait(ctx); err != nil {
		return fmt.Errorf("ddl wait: %w", err)
	}
	fmt.Println("migration applied")
	return nil
}

// splitDDL splits a .sql file into individual statements: drops full-line
// comments, splits on ';', trims whitespace.
func splitDDL(raw string) []string {
	var noComments []string
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		noComments = append(noComments, line)
	}
	var out []string
	for _, s := range strings.Split(strings.Join(noComments, "\n"), ";") {
		if t := strings.TrimSpace(s); t != "" {
			out = append(out, t)
		}
	}
	return out
}
