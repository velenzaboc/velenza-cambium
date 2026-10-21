package main

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLineageMathAndClassificationHelpers(t *testing.T) {
	versions := []LineageVersion{{ContentHash: "a"}, {ContentHash: ""}, {ContentHash: "b"}, {ContentHash: "a"}}
	hashes := versionHashes(versions)
	if !reflect.DeepEqual(hashes, []string{"a", "b"}) {
		t.Fatalf("version hashes = %v", hashes)
	}
	keys := keysOf(map[string]struct{}{"b": {}, "a": {}})
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("keys = %v", keys)
	}

	for _, test := range []struct {
		textual, semantic float64
		want              string
	}{
		{0.1, 0.2, "behavior-shift"},
		{0.4, 0.1, "refactor"},
		{0.4, 0.2, "rewrite"},
		{0.1, 0.1, "cosmetic"},
		{textualHighThreshold, semanticHighThreshold, "cosmetic"},
	} {
		if got := classifyQuadrant(test.textual, test.semantic); got != test.want {
			t.Fatalf("classifyQuadrant(%v,%v) = %q, want %q", test.textual, test.semantic, got, test.want)
		}
	}

	if got := cosineDistance([]float32{1, 0}, []float32{1, 0}); math.Abs(got) > 1e-12 {
		t.Fatalf("identical cosine distance = %v", got)
	}
	if got := cosineDistance([]float32{1, 0}, []float32{0, 1}); math.Abs(got-1) > 1e-12 {
		t.Fatalf("orthogonal cosine distance = %v", got)
	}
	for _, pair := range [][2][]float32{{nil, nil}, {{1}, {1, 2}}, {{0}, {1}}} {
		if got := cosineDistance(pair[0], pair[1]); got != 0 {
			t.Fatalf("invalid cosine distance = %v", got)
		}
	}
	if got := tokenJaccardDistance("alpha beta", "alpha beta"); got != 0 {
		t.Fatalf("identical token distance = %v", got)
	}
	if got := tokenJaccardDistance("alpha beta", "beta gamma"); math.Abs(got-(2.0/3.0)) > 1e-12 {
		t.Fatalf("partial token distance = %v", got)
	}
	if got := tokenJaccardDistance("!!!", "---"); got != 0 {
		t.Fatalf("empty token distance = %v", got)
	}
	if got := tokenSet("alpha_beta-42 alpha_beta"); len(got) != 2 {
		t.Fatalf("token set = %v", got)
	}
}

func TestAliasSetHelpers(t *testing.T) {
	members := []memberLite{{symbolID: "one"}, {symbolID: "two"}}
	if got := idsOf(members); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("ids = %v", got)
	}
	a := map[string]struct{}{"one": {}, "two": {}}
	b := map[string]struct{}{"two": {}, "three": {}}
	if got := jaccard(a, b); math.Abs(got-(1.0/3.0)) > 1e-12 {
		t.Fatalf("jaccard = %v", got)
	}
	if got := jaccard(nil, nil); got != 0 {
		t.Fatalf("empty jaccard = %v", got)
	}
	if got := jaccard(a, a); got != 1 {
		t.Fatalf("identity jaccard = %v", got)
	}
}

func TestReplaySelectionAndShardHelpers(t *testing.T) {
	commits := []string{"a", "b", "c", "d", "latest"}
	if got := sampleCommits(nil, 1, 0); got != nil {
		t.Fatalf("empty sample = %v", got)
	}
	if got := sampleCommits(commits, 2, 0); !reflect.DeepEqual(got, []string{"a", "c", "latest"}) {
		t.Fatalf("sample = %v", got)
	}
	if got := sampleCommits(commits, 0, 2); !reflect.DeepEqual(got, []string{"d", "latest"}) {
		t.Fatalf("capped sample = %v", got)
	}
	if got := shortSHA("123456789012345"); got != "123456789012" {
		t.Fatalf("short SHA = %q", got)
	}
	if got := shortSHA("short"); got != "short" {
		t.Fatalf("short SHA passthrough = %q", got)
	}

	i, n, err := parseShard(" 1 / 3 ")
	if err != nil || i != 1 || n != 3 {
		t.Fatalf("parse shard = %d/%d, %v", i, n, err)
	}
	for _, bad := range []string{"1", "x/2", "-1/2", "2/2", "0/0"} {
		if _, _, err := parseShard(bad); err == nil {
			t.Fatalf("invalid shard %q accepted", bad)
		}
	}
	if got := contiguousShard(commits, 0, 2); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("first shard = %v", got)
	}
	if got := contiguousShard(commits, 1, 2); !reflect.DeepEqual(got, []string{"d", "latest"}) {
		t.Fatalf("second shard = %v", got)
	}
	if got := contiguousShard(commits, 3, 3); got != nil {
		t.Fatalf("empty tail shard = %v", got)
	}
	if got := autoWorkers(); got < 1 || got > runtime.NumCPU() {
		t.Fatalf("auto workers = %d, CPUs = %d", got, runtime.NumCPU())
	}
	var gate sync.Mutex
	var running int64
	admitIngest(&gate, &running, ^uint64(0))
	if got := atomic.LoadInt64(&running); got != 1 {
		t.Fatalf("governor reservation = %d", got)
	}
}

func TestGitHistoryHelpers(t *testing.T) {
	repo := t.TempDir()
	mustRunGit(t, repo, "init", "-b", "main")
	mustRunGit(t, repo, "config", "user.email", "coverage@example.invalid")
	mustRunGit(t, repo, "config", "user.name", "Coverage Test")
	mustWriteTestFile(t, filepath.Join(repo, "main.go"), "package sample\n")
	mustRunGit(t, repo, "add", "main.go")
	mustRunGit(t, repo, "commit", "-m", "go one")
	first := strings.TrimSpace(mustRunGit(t, repo, "rev-parse", "HEAD"))
	mustWriteTestFile(t, filepath.Join(repo, "README.md"), "docs\n")
	mustRunGit(t, repo, "add", "README.md")
	mustRunGit(t, repo, "commit", "-m", "docs")
	mustWriteTestFile(t, filepath.Join(repo, "main.go"), "package sample\n\nconst Version = 2\n")
	mustRunGit(t, repo, "add", "main.go")
	mustRunGit(t, repo, "commit", "-m", "go two")
	last := strings.TrimSpace(mustRunGit(t, repo, "rev-parse", "HEAD"))

	all, err := gitRevListAll(repo, false)
	if err != nil || len(all) != 3 || all[0] != first || all[2] != last {
		t.Fatalf("all commits = %v, %v", all, err)
	}
	goOnly, err := gitRevListAll(repo, true)
	if err != nil || !reflect.DeepEqual(goOnly, []string{first, last}) {
		t.Fatalf("Go commits = %v, %v", goOnly, err)
	}
	tips, err := gitBranchTips(repo)
	if err != nil || !reflect.DeepEqual(tips, []string{last}) {
		t.Fatalf("branch tips = %v, %v", tips, err)
	}
	if _, err := gitRevListAll(filepath.Join(repo, "missing"), false); err == nil {
		t.Fatal("missing repo rev-list unexpectedly succeeded")
	}
	if _, err := gitBranchTips(filepath.Join(repo, "missing")); err == nil {
		t.Fatal("missing repo branch listing unexpectedly succeeded")
	}
}

func mustRunGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return string(out)
}
