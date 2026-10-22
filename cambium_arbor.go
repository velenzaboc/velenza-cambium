package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
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
	if head, err := inarchWorkspaceChangeset(repoPath); err == nil && head != "" {
		return head, nil
	}
	// Native Inarch refs contain raw 32-byte hashes and hex-encode ref names.
	refPath := filepath.Join(inarchDir, "store", "meta", "refs", hex.EncodeToString([]byte("main")))
	if raw, err := os.ReadFile(refPath); err == nil {
		if len(raw) != 32 {
			return "", fmt.Errorf("invalid Inarch main ref length %d", len(raw))
		}
		return hex.EncodeToString(raw), nil
	}
	// Retain the former HEAD layout as read compatibility for older workspaces.
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
	if lease, err := readInarchLease(filepath.Join(inarchDir, "jit_lease.json")); err == nil {
		for _, k := range []string{"head_changeset_id", "base_changeset_id", "changeset_id"} {
			if v := strings.TrimSpace(lease[k]); v != "" {
				return v, nil
			}
		}
	}
	return "", fmt.Errorf("unable to resolve .inarch HEAD in %s", repoPath)
}

func inarchWorkspaceChangeset(repoPath string) (string, error) {
	values, err := readSimpleTOML(filepath.Join(repoPath, ".inarch", "workspace_state.toml"))
	if err != nil {
		return "", err
	}
	head := strings.TrimSpace(values["changeset_id"])
	if len(head) != 64 {
		return "", fmt.Errorf("invalid Inarch workspace changeset_id %q", head)
	}
	if _, err := hex.DecodeString(head); err != nil {
		return "", fmt.Errorf("invalid Inarch workspace changeset_id: %w", err)
	}
	return strings.ToLower(head), nil
}

func readSimpleTOML(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	return out, nil
}

// Inarch writes the lease as TOML despite its historical .json filename.
// Older workspaces did write JSON, so retain that format as read compatibility.
func readInarchLease(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		var legacy map[string]json.RawMessage
		if err := json.Unmarshal(raw, &legacy); err != nil {
			return nil, err
		}
		values := make(map[string]string)
		for _, key := range []string{"head_changeset_id", "base_changeset_id", "changeset_id", "bookmark", "branch"} {
			var value string
			if encoded, ok := legacy[key]; ok && json.Unmarshal(encoded, &value) == nil {
				values[key] = value
			}
		}
		return values, nil
	}
	return readSimpleTOML(path)
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
	if lease, err := readInarchLease(filepath.Join(inarchDir, "jit_lease.json")); err == nil {
		for _, k := range []string{"bookmark", "branch"} {
			if v := strings.TrimSpace(lease[k]); v != "" {
				return v
			}
		}
	}
	return "main"
}

func inarchCommitTime(repoPath, sha string) (time.Time, error) {
	inarchDir := filepath.Join(repoPath, ".inarch")
	if info, err := os.Stat(inarchChangesetPath(repoPath, sha)); err == nil {
		return info.ModTime().UTC(), nil
	}
	if info, err := os.Stat(filepath.Join(inarchDir, "workspace_state.toml")); err == nil {
		return info.ModTime().UTC(), nil
	}
	if info, err := os.Stat(filepath.Join(inarchDir, "HEAD")); err == nil {
		return info.ModTime().UTC(), nil
	}
	if info, err := os.Stat(inarchDir); err == nil {
		return info.ModTime().UTC(), nil
	}
	return time.Now().UTC(), nil
}

func inarchParents(repoPath, sha string) ([]string, error) {
	raw, err := os.ReadFile(inarchChangesetPath(repoPath, sha))
	if err == nil {
		if len(raw) < 12 || string(raw[:8]) != "INRCBCS1" {
			return nil, fmt.Errorf("invalid Inarch changeset header for %s", sha)
		}
		count := int(binary.LittleEndian.Uint32(raw[8:12]))
		if count > (len(raw)-12)/32 {
			return nil, fmt.Errorf("truncated Inarch changeset parent list for %s", sha)
		}
		parents := make([]string, count)
		for i := 0; i < count; i++ {
			start := 12 + i*32
			parents[i] = hex.EncodeToString(raw[start : start+32])
		}
		return parents, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	// Read compatibility for the former JSON metadata layout.
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

func inarchChangesetPath(repoPath, sha string) string {
	if len(sha) < 2 {
		return filepath.Join(repoPath, ".inarch", "store", "meta", "changesets", sha)
	}
	return filepath.Join(repoPath, ".inarch", "store", "meta", "changesets", sha[:2], sha)
}

// validateTrackedFileBoundaries refuses tracked symlinks that leave the
// checkout before any parser, embedder, or fidelity reader can dereference
// them. Symlinks whose resolved target remains inside the checkout are valid.
func validateTrackedFileBoundaries(repoPath string) error {
	paths, err := listTrackedFiles(repoPath)
	if err != nil {
		return err
	}
	root, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	for _, rel := range paths {
		full := filepath.Join(repoPath, filepath.FromSlash(rel))
		_, err := os.Lstat(full)
		if err != nil {
			return fmt.Errorf("tracked file %q: %w", rel, err)
		}
		resolved, err := filepath.EvalSymlinks(full)
		if err != nil {
			return fmt.Errorf("tracked file symlink path %q: %w", rel, err)
		}
		resolved, err = filepath.Abs(resolved)
		if err != nil {
			return fmt.Errorf("tracked file symlink path %q: %w", rel, err)
		}
		within, err := filepath.Rel(root, resolved)
		if err != nil || within == ".." || strings.HasPrefix(within, ".."+string(filepath.Separator)) || filepath.IsAbs(within) {
			return fmt.Errorf("tracked file symlink path %q escapes repository checkout", rel)
		}
	}
	return nil
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
