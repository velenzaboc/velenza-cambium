package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Snapshot coherence: every read operates within ONE snapshot. There is no
// per-symbol "latest" — that produced reads smeared across builds. The current
// snapshot comes from (in priority order): an explicit --snapshot override, the
// saved CLI context, or the newest ingested snapshot.

type cgContext struct {
	Repo       string `json:"repo"`
	SnapshotID string `json:"snapshot_id"`
}

func contextFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.TempDir()
	}
	return filepath.Join(home, ".code-graph-context.json")
}

func loadContext() (cgContext, bool) {
	raw, err := os.ReadFile(contextFilePath())
	if err != nil {
		return cgContext{}, false
	}
	var c cgContext
	if json.Unmarshal(raw, &c) != nil || c.SnapshotID == "" {
		return cgContext{}, false
	}
	return c, true
}

func saveContext(c cgContext) error {
	raw, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(contextFilePath(), raw, 0o644)
}

// latestSnapshot resolves the newest ingested snapshot — for a given repo if
// repo is set, else the newest across all repos (by commit time).
func latestSnapshot(ctx context.Context, sp *spanner.Client, repo string) (cgContext, error) {
	sql := `SELECT repo, snapshot_id FROM Snapshots WHERE kind = 'ingested'`
	params := map[string]interface{}{}
	if repo != "" {
		sql += ` AND repo = @repo`
		params["repo"] = repo
	}
	sql += ` ORDER BY commit_time DESC LIMIT 1`
	iter := sp.Single().Query(ctx, spanner.Statement{SQL: sql, Params: params})
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return cgContext{}, fmt.Errorf("no ingested snapshot found%s", repoSuffix(repo))
	}
	if err != nil {
		return cgContext{}, err
	}
	var c cgContext
	if err := row.Columns(&c.Repo, &c.SnapshotID); err != nil {
		return cgContext{}, err
	}
	return c, nil
}

func repoSuffix(repo string) string {
	if repo == "" {
		return ""
	}
	return " for repo " + repo
}

// snapshotRepo returns the repo a snapshot id belongs to.
func snapshotRepo(ctx context.Context, sp *spanner.Client, snapshotID string) (string, error) {
	iter := sp.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT repo FROM Snapshots WHERE snapshot_id = @snap AND kind != 'staging' ORDER BY repo`,
		Params: map[string]interface{}{"snap": snapshotID},
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err == iterator.Done {
		return "", fmt.Errorf("snapshot %q is not ingested", snapshotID)
	}
	if err != nil {
		return "", err
	}
	var repo string
	if err := row.Columns(&repo); err != nil {
		return "", err
	}
	if _, err := iter.Next(); err == nil {
		return "", fmt.Errorf("snapshot %q is ambiguous across repositories; pin it with `use <repo> <snapshot>`", snapshotID)
	} else if err != iterator.Done {
		return "", err
	}
	return repo, nil
}

// resolveQuerySnapshot picks the single snapshot a CLI query runs against:
// explicit override > saved context > newest ingested snapshot. It returns the
// repo + snapshot and a short note describing where the snapshot came from.
func resolveQuerySnapshot(ctx context.Context, sp *spanner.Client, explicitSnap string) (repo, snapshotID, note string, err error) {
	if explicitSnap != "" {
		r, err := snapshotRepo(ctx, sp, explicitSnap)
		if err != nil {
			return "", "", "", err
		}
		return r, explicitSnap, "snapshot: " + shortSHA(explicitSnap) + " (--snapshot)", nil
	}
	if c, ok := loadContext(); ok {
		if ok2, _ := snapshotExists(ctx, sp, c.Repo, c.SnapshotID); ok2 {
			return c.Repo, c.SnapshotID, "snapshot: " + shortSHA(c.SnapshotID) + " (" + c.Repo + ", pinned via `use`)", nil
		}
	}
	c, err := latestSnapshot(ctx, sp, "")
	if err != nil {
		return "", "", "", err
	}
	return c.Repo, c.SnapshotID, "snapshot: " + shortSHA(c.SnapshotID) + " (" + c.Repo + ", newest — run `use` to pin)", nil
}

// ---------- use / context commands ----------

func cmdUse(ctx context.Context, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("use needs <repo> [<snapshot>]")
	}
	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	var c cgContext
	if len(args) == 2 {
		ok, err := snapshotExists(ctx, sp, args[0], args[1])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshot %q for repo %q is not ingested", args[1], args[0])
		}
		c = cgContext{Repo: args[0], SnapshotID: args[1]}
	} else {
		c, err = latestSnapshot(ctx, sp, args[0])
		if err != nil {
			return err
		}
	}
	if err := saveContext(c); err != nil {
		return err
	}
	fmt.Printf("current snapshot pinned: repo=%s snapshot=%s\n", c.Repo, c.SnapshotID)
	fmt.Println("all queries now operate within this build until `use` is run again")
	return nil
}

func cmdContext(ctx context.Context, args []string) error {
	c, ok := loadContext()
	if !ok {
		fmt.Println("no current snapshot pinned — queries use the newest ingested snapshot; run `use <repo>` to pin")
		return nil
	}
	fmt.Printf("current snapshot: repo=%s snapshot=%s\n", c.Repo, c.SnapshotID)
	return nil
}
