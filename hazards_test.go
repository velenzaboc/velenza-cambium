package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeHazardFixtureModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	goMod := "module example.com/hazardfixture\n\ngo 1.23\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	src := `package hazardfixture

import (
	"context"
	"maps"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"
)

//spec:SPEC-HAZARD-FIXTURE-001
//volt:VOLT-HAZARD-FIXTURE-001
type Service struct {
	mu        sync.RWMutex
	muA       sync.Mutex
	muB       sync.Mutex
	cache     map[string]string
	bounded   map[string]string
	items     []string
	httpCli   *http.Client
}

// 1. MAP_ESCAPE_UNDER_LOCK (Unsafe)
//spec:SPEC-HAZARD-FIXTURE-001
//volt:VOLT-HAZARD-FIXTURE-001
func (s *Service) UnsafeMapEscape() map[string]string {
	//spec:SPEC-HAZARD-FIXTURE-001
	//volt:VOLT-HAZARD-FIXTURE-001
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache
}

// 1b. Safe Map Clone Under Lock (Negative control)
func (s *Service) SafeMapClone() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.cache)
}

// 2. SLICE_ESCAPE_UNDER_LOCK (Unsafe)
func (s *Service) UnsafeSliceEscape() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items
}

// 2b. Safe Slice Clone Under Lock (Negative control)
func (s *Service) SafeSliceClone() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.items)
}

// 3. UNBOUNDED_MAP: cache has MAP_INSERT in PutCache but never MAP_DELETE anywhere
func (s *Service) PutCache(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[k] = v
}

// 3b. Bounded map negative control: bounded has both insert and delete
func (s *Service) PutBounded(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bounded[k] = v
}

func (s *Service) EvictBounded(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bounded, k)
}

// 4. Inter-procedural LOCK_ACROSS_IO (HandleLockedIO -> midHop -> performRemoteIO -> http.Get)
func (s *Service) HandleLockedIO(ctx context.Context, url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.midHop(ctx, url)
}

func (s *Service) midHop(ctx context.Context, url string) error {
	return s.performRemoteIO(ctx, url)
}

func (s *Service) performRemoteIO(ctx context.Context, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// 4b. Safe unlocked I/O negative control (unlocks before calling midHop)
func (s *Service) HandleUnlockedIO(ctx context.Context, url string) error {
	s.mu.Lock()
	_ = len(s.items)
	s.mu.Unlock()
	return s.midHop(ctx, url)
}

// 5. CYCLIC_LOCK_ORDER (AB-BA deadlock between muA and muB)
func (s *Service) LockOrderAB() {
	s.muA.Lock()
	s.muB.Lock()
	s.muB.Unlock()
	s.muA.Unlock()
}

func (s *Service) LockOrderBA() {
	s.muB.Lock()
	s.muA.Lock()
	s.muA.Unlock()
	s.muB.Unlock()
}

// 6. TXN_CLOSURE_OUTER_MUTATION
func RunInReadWriteTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (s *Service) UnsafeTxnClosureMutation(ctx context.Context, shouldMutate bool) (bool, error) {
	var terminalized bool
	err := RunInReadWriteTransaction(ctx, func(txCtx context.Context) error {
		if shouldMutate {
			terminalized = true
		}
		return nil
	})
	return terminalized, err
}

// 6b. Safe Txn Closure (resets outer variable in entry block)
func (s *Service) SafeTxnClosureReset(ctx context.Context, shouldMutate bool) (bool, error) {
	var terminalized bool
	err := RunInReadWriteTransaction(ctx, func(txCtx context.Context) error {
		terminalized = false
		if shouldMutate {
			terminalized = true
		}
		return nil
	})
	return terminalized, err
}

// 7. DEFER_IN_LOOP
func (s *Service) UnsafeDeferInLoop(files []string) {
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			continue
		}
		defer f.Close()
	}
}

// 8. TIMER_IN_LOOP
func (s *Service) UnsafeTimerInLoop(ctx context.Context, rounds int) {
	for i := 0; i < rounds; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// 9. DETACHES_CONTEXT
func (s *Service) UnsafeDetachedContext(ctx context.Context) context.Context {
	detached := context.WithoutCancel(ctx)
	return detached
}

// 9b. Safe detached context with timeout negative control
func (s *Service) SafeDetachedContextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	return context.WithTimeout(detached, 5*time.Second)
}

// 10. SPAWNS_UNRECOVERED_GO
func (s *Service) UnsafeSpawnsGoroutine() {
	go func() {
		s.PutBounded("bg", "val")
	}()
}

// 10b. Safe recovered goroutine negative control
func (s *Service) SafeSpawnsRecoveredGoroutine() {
	go func() {
		defer func() {
			_ = recover()
		}()
		s.PutBounded("bg", "safe")
	}()
}

// 11. UNBOUNDED_SHIFT
func (s *Service) UnsafeBackoffShift(attempt int) time.Duration {
	return time.Duration(1 << attempt)
}

// 11b. Safe bounded shift negative control
func (s *Service) SafeBackoffShift(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 30 {
		attempt = 30
	}
	return time.Duration(1 << attempt)
}

// 12. UNCLOSED_RESOURCE
func (s *Service) UnsafeUnclosedFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	_ = f.Name()
	return nil
}

// 12b. Safe closed file negative control
func (s *Service) SafeClosedFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}
`
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(src), 0o644); err != nil {
		t.Fatalf("write fixture.go: %v", err)
	}
	return dir
}

func TestAnalyzePackageHazardsDetectsAllClassesAndRespectsSafeControls(t *testing.T) {
	dir := writeHazardFixtureModule(t)

	symbols, edges, findings, err := analyzePackageHazards(dir)
	if err != nil {
		t.Fatalf("analyzePackageHazards failed: %v", err)
	}
	if len(symbols) == 0 {
		t.Fatalf("expected non-empty symbols from analyzePackageHazards, got 0")
	}
	if len(edges) == 0 {
		t.Fatalf("expected non-empty edges from analyzePackageHazards, got 0")
	}
	if len(findings) == 0 {
		t.Fatalf("expected hazard findings from analyzePackageHazards, got 0")
	}

	report := evaluateHazardsFromGraph(symbols, edges, "", "")
	if report.Total == 0 {
		t.Fatalf("expected non-zero report.Total from evaluateHazardsFromGraph, got 0")
	}

	byCategory := make(map[string][]HazardFinding)
	for _, h := range report.Hazards {
		byCategory[h.Category] = append(byCategory[h.Category], h)
	}

	assertHasFindingInFunc := func(category, fnSubstr string) HazardFinding {
		t.Helper()
		list := byCategory[category]
		for _, item := range list {
			if strings.Contains(item.FunctionFQN, fnSubstr) || strings.Contains(item.TargetFQN, fnSubstr) {
				return item
			}
		}
		t.Fatalf("expected hazard category %q referencing %q, got findings: %+v", category, fnSubstr, list)
		return HazardFinding{}
	}

	assertNoFindingInFunc := func(category, fnSubstr string) {
		t.Helper()
		for _, item := range byCategory[category] {
			if strings.Contains(item.FunctionFQN, fnSubstr) {
				t.Fatalf("unexpected false-positive hazard %q in safe function %q: %+v", category, fnSubstr, item)
			}
		}
	}

	// 1. MAP_ESCAPE_UNDER_LOCK
	assertHasFindingInFunc("MAP_ESCAPE_UNDER_LOCK", "UnsafeMapEscape")
	assertNoFindingInFunc("MAP_ESCAPE_UNDER_LOCK", "SafeMapClone")

	// 2. SLICE_ESCAPE_UNDER_LOCK
	assertHasFindingInFunc("SLICE_ESCAPE_UNDER_LOCK", "UnsafeSliceEscape")
	assertNoFindingInFunc("SLICE_ESCAPE_UNDER_LOCK", "SafeSliceClone")

	// 3. UNBOUNDED_MAP
	assertHasFindingInFunc("UNBOUNDED_MAP", "Service.cache")
	for _, item := range byCategory["UNBOUNDED_MAP"] {
		if strings.Contains(item.TargetFQN, "Service.bounded") || strings.Contains(item.FunctionFQN, "Service.bounded") {
			t.Fatalf("unexpected UNBOUNDED_MAP on bounded field Service.bounded: %+v", item)
		}
	}

	// 4. LOCK_ACROSS_IO (multi-hop HandleLockedIO -> midHop -> performRemoteIO)
	lockIO := assertHasFindingInFunc("LOCK_ACROSS_IO", "HandleLockedIO")
	if len(lockIO.HopPath) < 3 {
		t.Fatalf("expected LOCK_ACROSS_IO HopPath of length >= 3 (HandleLockedIO -> midHop -> performRemoteIO), got %v", lockIO.HopPath)
	}
	assertNoFindingInFunc("LOCK_ACROSS_IO", "HandleUnlockedIO")

	// 5. CYCLIC_LOCK_ORDER
	assertHasFindingInFunc("CYCLIC_LOCK_ORDER", "LockOrder")

	// 6. TXN_CLOSURE_OUTER_MUTATION
	assertHasFindingInFunc("TXN_CLOSURE_OUTER_MUTATION", "UnsafeTxnClosureMutation")
	assertNoFindingInFunc("TXN_CLOSURE_OUTER_MUTATION", "SafeTxnClosureReset")

	// 7. DEFER_IN_LOOP
	assertHasFindingInFunc("DEFER_IN_LOOP", "UnsafeDeferInLoop")

	// 8. TIMER_IN_LOOP
	assertHasFindingInFunc("TIMER_IN_LOOP", "UnsafeTimerInLoop")

	// 9. DETACHES_CONTEXT
	assertHasFindingInFunc("DETACHES_CONTEXT", "UnsafeDetachedContext")
	assertNoFindingInFunc("DETACHES_CONTEXT", "SafeDetachedContextWithTimeout")

	// 10. SPAWNS_UNRECOVERED_GO
	assertHasFindingInFunc("SPAWNS_UNRECOVERED_GO", "UnsafeSpawnsGoroutine")
	assertNoFindingInFunc("SPAWNS_UNRECOVERED_GO", "SafeSpawnsRecoveredGoroutine")

	// 11. UNBOUNDED_SHIFT
	assertHasFindingInFunc("UNBOUNDED_SHIFT", "UnsafeBackoffShift")
	assertNoFindingInFunc("UNBOUNDED_SHIFT", "SafeBackoffShift")

	// 12. UNCLOSED_RESOURCE
	assertHasFindingInFunc("UNCLOSED_RESOURCE", "UnsafeUnclosedFile")
	assertNoFindingInFunc("UNCLOSED_RESOURCE", "SafeClosedFile")

	// Verify filtering by category and file_prefix in evaluateHazardsFromGraph
	filtered := evaluateHazardsFromGraph(symbols, edges, "MAP_ESCAPE_UNDER_LOCK", "fixture.go")
	if filtered.Total != 1 || len(filtered.Hazards) != 1 {
		t.Fatalf("expected exactly 1 filtered MAP_ESCAPE_UNDER_LOCK finding, got %+v", filtered)
	}
}
