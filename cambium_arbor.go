package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type CambiumArborDescriptor struct {
	Layer               int      `json:"layer"`
	Name                string   `json:"name"`
	Alias               string   `json:"alias"`
	CanonicalBinary     string   `json:"canonical_binary"`
	LegacyAlias         string   `json:"legacy_alias"`
	UmbrellaSubcommands []string `json:"umbrella_subcommands"`
	InarchNativeVCS     bool     `json:"inarch_native_vcs"`
	ConfigEnvVars       []string `json:"config_env_vars"`
	VCSBackends         []string `json:"vcs_backends"`
	HazardCategoryCount int      `json:"hazard_category_count"`
}

func BuildCambiumArborDescriptor() CambiumArborDescriptor {
	return CambiumArborDescriptor{
		Layer:               1,
		Name:                "velenza-cambium",
		Alias:               "code-graph",
		CanonicalBinary:     "velenza-cambium",
		LegacyAlias:         "code-graph",
		UmbrellaSubcommands: []string{"inr cambium", "inr code"},
		InarchNativeVCS:     true,
		ConfigEnvVars: []string{
			"VELENZA_CAMBIUM_GCP_PROJECT",
			"VELENZA_CAMBIUM_PROJECT",
			"VELENZA_CAMBIUM_SPANNER_INSTANCE",
			"VELENZA_CAMBIUM_INSTANCE",
			"VELENZA_CAMBIUM_SPANNER_DATABASE",
			"VELENZA_CAMBIUM_DATABASE",
			"CODE_GRAPH_GCP_PROJECT",
			"CODE_GRAPH_SPANNER_INSTANCE",
			"CODE_GRAPH_SPANNER_DATABASE",
			"ARBOR_GCP_PROJECT",
			"ARBOR_SPANNER_INSTANCE",
		},
		VCSBackends:         []string{"inarch", "git"},
		HazardCategoryCount: 12,
	}
}

func runArborInfoCLI(_ context.Context, args []string, out io.Writer) (int, error) {
	for _, a := range args {
		if a == "--json" {
			continue
		}
		return 2, fmt.Errorf("unknown arbor-info argument %q", a)
	}
	desc := BuildCambiumArborDescriptor()
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(desc); err != nil {
		return 1, err
	}
	return 0, nil
}

func hasGitDir(repoPath string) bool {
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); err == nil {
		return true
	}
	return false
}

func hasInarchDir(repoPath string) bool {
	info, err := os.Stat(filepath.Join(repoPath, ".inarch"))
	return err == nil && info.IsDir()
}

func inarchDeriveRepoName(repoPath string) (string, error) {
	cfgPath := filepath.Join(repoPath, ".inarch", "config.toml")
	if raw, err := os.ReadFile(cfgPath); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			key, val, ok := strings.Cut(trimmed, "=")
			if !ok || strings.TrimSpace(key) != "repo_id" {
				continue
			}
			val = strings.TrimSpace(val)
			val = strings.Trim(val, `"'`)
			val = strings.TrimSuffix(val, ".git")
			if idx := strings.LastIndex(val, "/"); idx >= 0 && idx+1 < len(val) {
				val = val[idx+1:]
			}
			if val != "" {
				return val, nil
			}
		}
	}
	absDir, err := filepath.Abs(repoPath)
	if err != nil {
		return "", err
	}
	return filepath.Base(absDir), nil
}

func inarchHead(repoPath string) (string, error) {
	inarchDir := filepath.Join(repoPath, ".inarch")
	headBytes, err := os.ReadFile(filepath.Join(inarchDir, "HEAD"))
	if err == nil {
		headStr := strings.TrimSpace(string(headBytes))
		if strings.HasPrefix(headStr, "ref:") {
			refRel := strings.TrimSpace(strings.TrimPrefix(headStr, "ref:"))
			refBytes, refErr := os.ReadFile(filepath.Join(inarchDir, filepath.FromSlash(refRel)))
			if refErr == nil {
				if resolved := strings.TrimSpace(string(refBytes)); resolved != "" {
					return resolved, nil
				}
			}
		} else if headStr != "" {
			return headStr, nil
		}
	}
	leaseBytes, err := os.ReadFile(filepath.Join(inarchDir, "jit_lease.json"))
	if err == nil {
		var lease map[string]any
		if json.Unmarshal(leaseBytes, &lease) == nil {
			for _, k := range []string{"head_changeset_id", "base_changeset_id", "changeset_id"} {
				if v, ok := lease[k].(string); ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v), nil
				}
			}
		}
	}
	return "", fmt.Errorf("unable to resolve .inarch HEAD in %s", repoPath)
}

func inarchBranch(repoPath string) string {
	inarchDir := filepath.Join(repoPath, ".inarch")
	if headBytes, err := os.ReadFile(filepath.Join(inarchDir, "HEAD")); err == nil {
		headStr := strings.TrimSpace(string(headBytes))
		if strings.HasPrefix(headStr, "ref:") {
			refRel := strings.TrimSpace(strings.TrimPrefix(headStr, "ref:"))
			refRel = strings.TrimPrefix(refRel, "refs/heads/")
			refRel = strings.TrimPrefix(refRel, "refs/bookmarks/")
			if refRel != "" {
				return refRel
			}
		}
	}
	if leaseBytes, err := os.ReadFile(filepath.Join(inarchDir, "jit_lease.json")); err == nil {
		var lease map[string]any
		if json.Unmarshal(leaseBytes, &lease) == nil {
			for _, k := range []string{"bookmark", "branch"} {
				if v, ok := lease[k].(string); ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	return "main"
}

func inarchCommitTime(repoPath, _ string) (time.Time, error) {
	inarchDir := filepath.Join(repoPath, ".inarch")
	if info, err := os.Stat(filepath.Join(inarchDir, "HEAD")); err == nil {
		return info.ModTime().UTC(), nil
	}
	if info, err := os.Stat(inarchDir); err == nil {
		return info.ModTime().UTC(), nil
	}
	return time.Now().UTC(), nil
}

func inarchParents(repoPath, sha string) ([]string, error) {
	metaPath := filepath.Join(repoPath, ".inarch", "commits", sha+".json")
	if raw, err := os.ReadFile(metaPath); err == nil {
		var meta struct {
			Parents []string `json:"parents"`
		}
		if json.Unmarshal(raw, &meta) == nil {
			return meta.Parents, nil
		}
	}
	return []string{}, nil
}

func listTrackedFiles(repoPath string) ([]string, error) {
	if hasGitDir(repoPath) {
		if out, err := exec.Command("git", "-C", repoPath, "ls-files", "-z").Output(); err == nil {
			rawPaths := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
			var paths []string
			for _, p := range rawPaths {
				if p != "" {
					paths = append(paths, p)
				}
			}
			sort.Strings(paths)
			return paths, nil
		}
	}
	if !hasInarchDir(repoPath) {
		out, err := exec.Command("git", "-C", repoPath, "ls-files", "-z").Output()
		if err != nil {
			return nil, fmt.Errorf("git ls-files: %w", err)
		}
		rawPaths := strings.Split(strings.TrimRight(string(out), "\x00"), "\x00")
		var paths []string
		for _, p := range rawPaths {
			if p != "" {
				paths = append(paths, p)
			}
		}
		sort.Strings(paths)
		return paths, nil
	}

	var paths []string
	err := filepath.WalkDir(repoPath, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".inarch" || name == ".git" || name == "vendor" || name == "node_modules" || name == "target" {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(repoPath, p)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}
