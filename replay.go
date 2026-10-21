package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/shirou/gopsutil/v4/mem"
)

// Phase: filtered history backfill. replay walks a repo's commit DAG across all
// branches and ingests a filtered subset of commits as snapshots. The
// content-hash caches (SymbolBlobs for embeddings, SymbolEdges for edges) make
// every commit after the first cheap — only genuinely new symbol-versions cost
// anything. Commits that touch no .go file are skipped by default, since they
// produce a snapshot indistinguishable from their parent.

// perIngestGB is the conservative peak RAM of one ingest (packages.Load + CHA
// hold the whole typed program in memory). It sizes the worker pool and the
// governor — getting this wrong toward "too high" is what OOM-kills a machine.
const perIngestGB = 3.5

// replayService is the narrow persistence boundary used by the replay commands.
// Keeping command orchestration on this interface lets the command be tested
// against real Git worktrees without requiring a live Spanner instance.
type replayService interface {
	Close()
	SnapshotExists(context.Context, string, string) (bool, error)
	Ingest(context.Context, string) (*IngestStats, error)
}

type liveReplayService struct{ client *spanner.Client }

func (s *liveReplayService) Close() { s.client.Close() }

func (s *liveReplayService) SnapshotExists(ctx context.Context, repo, snapshot string) (bool, error) {
	return snapshotExists(ctx, s.client, repo, snapshot)
}

func (s *liveReplayService) Ingest(ctx context.Context, repoPath string) (*IngestStats, error) {
	return doIngest(ctx, s.client, repoPath, ingestOpts{}, nil)
}

var newReplayService = func(ctx context.Context) (replayService, error) {
	client, err := newSpannerClient(ctx)
	if err != nil {
		return nil, err
	}
	return &liveReplayService{client: client}, nil
}

func cmdReplay(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	goOnly := fs.Bool("go-only", true, "only commits that touch .go files")
	every := fs.Int("every", 1, "sample every Nth commit (1 = every commit)")
	max := fs.Int("max", 0, "cap to the most recent N sampled commits (0 = no cap)")
	shard := fs.String("shard", "", "process only shard i of n, e.g. 3/10 — for splitting across MACHINES")
	workers := fs.Int("workers", 0, "max concurrent ingests (0 = auto from RAM + cores)")
	minFreeGB := fs.Float64("min-free-gb", 4.0, "governor: pause launching a new ingest while free RAM is below this")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("replay needs one arg: <repo-path>")
	}
	repoPath, err := filepath.Abs(fs.Arg(0))
	if err != nil {
		return err
	}
	repoName, err := deriveRepoName(repoPath)
	if err != nil {
		return fmt.Errorf("derive repo name: %w", err)
	}

	commits, err := gitRevListAll(repoPath, *goOnly)
	if err != nil {
		return fmt.Errorf("enumerate commits: %w", err)
	}
	if len(commits) == 0 {
		return fmt.Errorf("no commits matched")
	}
	selected := sampleCommits(commits, *every, *max)

	// --shard splits the commit list across MACHINES (run shard 0/2 on one box,
	// 1/2 on another). WITHIN a machine, the governed worker pool below handles
	// parallelism. Contiguous blocks so each shard's content caches warm.
	wtSuffix := ""
	if *shard != "" {
		i, n, err := parseShard(*shard)
		if err != nil {
			return err
		}
		selected = contiguousShard(selected, i, n)
		wtSuffix = fmt.Sprintf("-shard%d-of-%d", i, n)
		fmt.Printf("replay: repo=%s  shard %d/%d  %d commits in this shard (of %d matched)\n",
			repoName, i, n, len(selected), len(commits))
	} else {
		fmt.Printf("replay: repo=%s  %d commits matched, %d selected (go-only=%v every=%d max=%d)\n",
			repoName, len(commits), len(selected), *goOnly, *every, *max)
	}
	if len(selected) == 0 {
		fmt.Println("replay: nothing to do")
		return nil
	}

	nWorkers := *workers
	if nWorkers <= 0 {
		nWorkers = autoWorkers()
	}
	minFreeBytes := uint64(*minFreeGB * 1e9)
	fmt.Printf("replay: %d concurrent workers, governor holds new ingests below %.1fGB free RAM\n",
		nWorkers, *minFreeGB)

	sp, err := newReplayService(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()

	var (
		ingested, skipped, failed int64
		running                   int64      // ingests currently in their heavy phase
		gateMu                    sync.Mutex // serializes the governor's admit decision
		wtMu                      sync.Mutex // serializes `git worktree add/remove`
		wg                        sync.WaitGroup
	)
	jobs := make(chan int)
	go func() {
		for i := range selected {
			jobs <- i
		}
		close(jobs)
	}()

	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Each worker owns one detached worktree for its whole run.
			wt := filepath.Join(os.TempDir(), fmt.Sprintf("cg-replay-%s%s-w%d", repoName, wtSuffix, workerID))
			wtMu.Lock()
			_ = exec.Command("git", "-C", repoPath, "worktree", "remove", "--force", wt).Run()
			out, err := exec.Command("git", "-C", repoPath, "worktree", "add", "--detach", wt, selected[0]).CombinedOutput()
			wtMu.Unlock()
			if err != nil {
				fmt.Printf("  worker %d: worktree add failed: %s\n", workerID, strings.TrimSpace(string(out)))
				return
			}
			defer func() {
				wtMu.Lock()
				_ = exec.Command("git", "-C", repoPath, "worktree", "remove", "--force", wt).Run()
				wtMu.Unlock()
			}()

			for idx := range jobs {
				sha := selected[idx]
				exists, err := sp.SnapshotExists(ctx, repoName, sha)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					fmt.Printf("  [%d/%d] %s — snapshot check failed: %v\n", idx+1, len(selected), shortSHA(sha), err)
					continue
				}
				if exists {
					atomic.AddInt64(&skipped, 1)
					continue
				}
				// Governor: block here until the machine has headroom for one
				// more ingest. Admission is serialized so workers don't all
				// pass at once and stampede memory.
				admitIngest(&gateMu, &running, minFreeBytes)
				if out, err := exec.Command("git", "-C", wt, "checkout", "--detach", "--force", sha).CombinedOutput(); err != nil {
					atomic.AddInt64(&running, -1)
					atomic.AddInt64(&failed, 1)
					fmt.Printf("  [%d/%d] %s — checkout failed: %s\n", idx+1, len(selected), shortSHA(sha), strings.TrimSpace(string(out)))
					continue
				}
				stats, err := sp.Ingest(ctx, wt)
				atomic.AddInt64(&running, -1)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					fmt.Printf("  [%d/%d] %s — ingest failed: %v\n", idx+1, len(selected), shortSHA(sha), err)
					continue
				}
				atomic.AddInt64(&ingested, 1)
				fmt.Printf("  [%d/%d] %s  symbols=%d edges=%d cache=%d/%d  (w%d)\n",
					idx+1, len(selected), shortSHA(sha), stats.Symbols, stats.Edges,
					stats.CacheHits, stats.UniqueHashes, workerID)
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("replay done: %d ingested, %d already present, %d failed\n", ingested, skipped, failed)
	return nil
}

// autoWorkers sizes the pool by the smaller of (cores) and (free RAM / per-
// ingest footprint). The live governor handles variance on top of this.
func autoWorkers() int {
	n := runtime.NumCPU()
	if vm, err := mem.VirtualMemory(); err == nil {
		byRAM := int(float64(vm.Available) / (perIngestGB * 1e9))
		if byRAM < n {
			n = byRAM
		}
	}
	if n < 1 {
		n = 1
	}
	return n
}

// admitIngest is the live memory governor. It blocks until free RAM is above
// the threshold, then reserves a slot (running++). The gate mutex serializes
// the admit decision so a burst of idle workers cannot all pass at once and
// collectively OOM the machine. One ingest is always allowed even under
// pressure (running == 0) so the run cannot deadlock.
func admitIngest(gateMu *sync.Mutex, running *int64, minFreeBytes uint64) {
	gateMu.Lock()
	defer gateMu.Unlock()
	for {
		if atomic.LoadInt64(running) == 0 {
			break
		}
		vm, err := mem.VirtualMemory()
		if err != nil || vm.Available >= minFreeBytes {
			break
		}
		fmt.Printf("  [governor] %.1fGB free < %.1fGB threshold, %d ingests running — holding\n",
			float64(vm.Available)/1e9, float64(minFreeBytes)/1e9, atomic.LoadInt64(running))
		time.Sleep(4 * time.Second)
	}
	atomic.AddInt64(running, 1)
}

// cmdUpdate is the keep-current counterpart to replay. It compares every
// branch tip in the local repo to the graph and ingests only the tips that are
// missing — O(branches), not O(all-history). Near-instant when nothing changed.
// It does NOT backfill intermediate commits between an old tip and a new one;
// that is replay's job. update keeps the graph current with the latest state
// of each branch.
func cmdUpdate(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("update needs one arg: <repo-path>")
	}
	repoPath, err := filepath.Abs(args[0])
	if err != nil {
		return err
	}
	repoName, err := deriveRepoName(repoPath)
	if err != nil {
		return fmt.Errorf("derive repo name: %w", err)
	}
	tips, err := gitBranchTips(repoPath)
	if err != nil {
		return fmt.Errorf("enumerate branch tips: %w", err)
	}

	sp, err := newReplayService(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()

	var missing []string
	for _, sha := range tips {
		exists, err := sp.SnapshotExists(ctx, repoName, sha)
		if err != nil {
			return err
		}
		if !exists {
			missing = append(missing, sha)
		}
	}
	fmt.Printf("update: repo=%s  %d branch tips  %d already in graph  %d to ingest\n",
		repoName, len(tips), len(tips)-len(missing), len(missing))
	if len(missing) == 0 {
		fmt.Println("graph is current — nothing to do")
		return nil
	}

	// One reusable detached worktree; ingest each missing tip in turn (serial —
	// update expects a small number of changed tips).
	wt := filepath.Join(os.TempDir(), "cg-update-"+repoName)
	_ = exec.Command("git", "-C", repoPath, "worktree", "remove", "--force", wt).Run()
	if out, err := exec.Command("git", "-C", repoPath, "worktree", "add", "--detach", wt, missing[0]).CombinedOutput(); err != nil {
		return fmt.Errorf("worktree add: %v: %s", err, out)
	}
	defer func() {
		_ = exec.Command("git", "-C", repoPath, "worktree", "remove", "--force", wt).Run()
	}()

	ingested, failed := 0, 0
	for i, sha := range missing {
		if out, err := exec.Command("git", "-C", wt, "checkout", "--detach", "--force", sha).CombinedOutput(); err != nil {
			fmt.Printf("  [%d/%d] %s — checkout failed: %s\n", i+1, len(missing), shortSHA(sha), strings.TrimSpace(string(out)))
			failed++
			continue
		}
		stats, err := sp.Ingest(ctx, wt)
		if err != nil {
			fmt.Printf("  [%d/%d] %s — ingest failed: %v\n", i+1, len(missing), shortSHA(sha), err)
			failed++
			continue
		}
		ingested++
		fmt.Printf("  [%d/%d] %s ingested  symbols=%d edges=%d\n",
			i+1, len(missing), shortSHA(sha), stats.Symbols, stats.Edges)
	}
	fmt.Printf("update done: %d ingested, %d failed\n", ingested, failed)
	return nil
}

// gitBranchTips returns the unique tip commit SHAs of every local and
// remote-tracking branch.
func gitBranchTips(repoPath string) ([]string, error) {
	out, err := exec.Command("git", "-C", repoPath, "for-each-ref",
		"--format=%(objectname)", "refs/heads", "refs/remotes").Output()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var tips []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		sha := strings.TrimSpace(line)
		if sha == "" {
			continue
		}
		if _, ok := seen[sha]; ok {
			continue
		}
		seen[sha] = struct{}{}
		tips = append(tips, sha)
	}
	return tips, nil
}

// gitRevListAll returns commit SHAs reachable from every ref, oldest first so
// that parent snapshots are ingested before their children (alias detection
// and DAG ordering both want this).
func gitRevListAll(repoPath string, goOnly bool) ([]string, error) {
	cmdArgs := []string{"-C", repoPath, "rev-list", "--reverse", "--all"}
	if goOnly {
		cmdArgs = append(cmdArgs, "--", "*.go")
	}
	out, err := exec.Command("git", cmdArgs...).Output()
	if err != nil {
		return nil, err
	}
	var commits []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			commits = append(commits, s)
		}
	}
	return commits, nil
}

// sampleCommits takes every Nth commit, then optionally keeps only the most
// recent `max`. The newest commit is always retained so the latest state is
// captured even under sampling.
func sampleCommits(commits []string, every, max int) []string {
	if len(commits) == 0 {
		return nil
	}
	if every < 1 {
		every = 1
	}
	var out []string
	for i := 0; i < len(commits); i++ {
		if i%every == 0 {
			out = append(out, commits[i])
		}
	}
	if last := commits[len(commits)-1]; len(out) == 0 || out[len(out)-1] != last {
		out = append(out, last)
	}
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

func shortSHA(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// parseShard parses "i/n" (0 <= i < n).
func parseShard(s string) (int, int, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("shard must be i/n, got %q", s)
	}
	i, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	n, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || n < 1 || i < 0 || i >= n {
		return 0, 0, fmt.Errorf("invalid shard %q (need 0 <= i < n, n >= 1)", s)
	}
	return i, n, nil
}

// contiguousShard returns shard i of n as a contiguous block, so the content
// caches warm within the shard.
func contiguousShard(all []string, i, n int) []string {
	blockSize := (len(all) + n - 1) / n // ceil
	start := i * blockSize
	if start >= len(all) {
		return nil
	}
	end := start + blockSize
	if end > len(all) {
		end = len(all)
	}
	return all[start:end]
}
