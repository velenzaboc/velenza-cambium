package main

import (
	"context"
	"flag"
	"fmt"
	"go/token"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	"google.golang.org/api/iterator"
)

// HazardEdgeKinds enumerates all SSA/VTA/AST hazard edges stored in SymbolEdges / SnapshotEdges.
var HazardEdgeKinds = []string{
	"MAP_ESCAPE_UNDER_LOCK",
	"SLICE_ESCAPE_UNDER_LOCK",
	"MAP_INSERT",
	"MAP_DELETE",
	"ACQUIRES_LOCK",
	"ACQUIRES_NESTED_LOCK",
	"LOCK_ORDER_PRECEDES",
	"CALLS_UNDER_LOCK",
	"IO_SINK",
	"TXN_CLOSURE_OUTER_MUTATION",
	"DEFER_IN_LOOP",
	"TIMER_IN_LOOP",
	"DETACHES_CONTEXT",
	"SPAWNS_UNRECOVERED_GO",
	"UNBOUNDED_SHIFT",
	"UNCLOSED_RESOURCE",
}

// HazardFinding represents a single production reliability or concurrency hazard
// discovered via SSA/VTA/AST analysis or Spanner property-graph traversal.
type HazardFinding struct {
	Category    string   `json:"category"`
	Severity    string   `json:"severity"`
	FunctionFQN string   `json:"function_fqn"`
	TargetFQN   string   `json:"target_fqn,omitempty"`
	FilePath    string   `json:"file_path"`
	LineStart   int      `json:"line_start"`
	LineEnd     int      `json:"line_end,omitempty"`
	EdgeKind    string   `json:"edge_kind,omitempty"`
	HopPath     []string `json:"hop_path,omitempty"`
	Description string   `json:"description"`
}

// HazardsReport is the structured output of `code_graph_hazards` and `code-graph hazards`.
type HazardsReport struct {
	Repo       string          `json:"repo"`
	Snapshot   string          `json:"snapshot"`
	Total      int             `json:"total"`
	ByCategory map[string]int  `json:"by_category"`
	Hazards    []HazardFinding `json:"hazards"`
}

// HazardQueryOpts configures filtering for hazard queries.
type HazardQueryOpts struct {
	Category   string `json:"category,omitempty"`
	FilePrefix string `json:"file_prefix,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Repo       string `json:"repo,omitempty"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	RepoPath   string `json:"repo_path,omitempty"`
}

func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func resolveDefaultGCPProject() string {
	if v := firstNonEmptyEnv("VELENZA_CAMBIUM_GCP_PROJECT", "VELENZA_CAMBIUM_PROJECT", "CODE_GRAPH_GCP_PROJECT", "ARBOR_GCP_PROJECT", "GOOGLE_CLOUD_PROJECT"); v != "" {
		return v
	}
	return gcpProject
}

func resolveDefaultSpannerInstance() string {
	if v := firstNonEmptyEnv("VELENZA_CAMBIUM_SPANNER_INSTANCE", "VELENZA_CAMBIUM_INSTANCE", "CODE_GRAPH_SPANNER_INSTANCE", "ARBOR_SPANNER_INSTANCE"); v != "" {
		return v
	}
	// Derive default instance ID from spannerDB constant without duplicating literals.
	parts := strings.Split(spannerDB, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func resolveDefaultSpannerDatabase() string {
	if v := firstNonEmptyEnv("VELENZA_CAMBIUM_SPANNER_DATABASE", "VELENZA_CAMBIUM_DATABASE", "CODE_GRAPH_SPANNER_DATABASE"); v != "" {
		return v
	}
	parts := strings.Split(spannerDB, "/")
	if len(parts) >= 6 && parts[5] != "" {
		return parts[5]
	}
	return "code-graph"
}

func resolveSpannerInstancePath() string {
	if v := firstNonEmptyEnv("VELENZA_CAMBIUM_SPANNER_INSTANCE_PATH", "CODE_GRAPH_SPANNER_INSTANCE_PATH"); v != "" {
		return v
	}
	if firstNonEmptyEnv(
		"VELENZA_CAMBIUM_GCP_PROJECT", "VELENZA_CAMBIUM_PROJECT", "CODE_GRAPH_GCP_PROJECT", "ARBOR_GCP_PROJECT", "GOOGLE_CLOUD_PROJECT",
		"VELENZA_CAMBIUM_SPANNER_INSTANCE", "VELENZA_CAMBIUM_INSTANCE", "CODE_GRAPH_SPANNER_INSTANCE", "ARBOR_SPANNER_INSTANCE",
	) != "" {
		return fmt.Sprintf("projects/%s/instances/%s", resolveDefaultGCPProject(), resolveDefaultSpannerInstance())
	}
	return spannerInstance
}

func resolveSpannerDBPath() string {
	if v := firstNonEmptyEnv("VELENZA_CAMBIUM_SPANNER_DB", "CODE_GRAPH_SPANNER_DB"); v != "" {
		return v
	}
	if firstNonEmptyEnv(
		"VELENZA_CAMBIUM_GCP_PROJECT", "VELENZA_CAMBIUM_PROJECT", "CODE_GRAPH_GCP_PROJECT", "ARBOR_GCP_PROJECT", "GOOGLE_CLOUD_PROJECT",
		"VELENZA_CAMBIUM_SPANNER_INSTANCE", "VELENZA_CAMBIUM_INSTANCE", "CODE_GRAPH_SPANNER_INSTANCE", "ARBOR_SPANNER_INSTANCE",
		"VELENZA_CAMBIUM_SPANNER_DATABASE", "VELENZA_CAMBIUM_DATABASE", "CODE_GRAPH_SPANNER_DATABASE",
	) != "" {
		return fmt.Sprintf("%s/databases/%s", resolveSpannerInstancePath(), resolveDefaultSpannerDatabase())
	}
	return spannerDB
}

// analyzePackageHazards loads the Go packages at repoPath, builds SSA + VTA,
// extracts all symbol and hazard edges, and evaluates the 12 hazard categories.
func analyzePackageHazards(repoPath string) ([]Symbol, []Edge, []HazardFinding, error) {
	absPath, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, nil, nil, err
	}
	syms, edges, err := walkRepo(absPath, "local", "working-tree", func(string, int, int, string) {})
	if err != nil {
		return nil, nil, nil, err
	}
	rep := evaluateHazardsFromGraph(syms, edges, "", "")
	return syms, edges, rep.Hazards, nil
}

// collectSSAHazardEdges inspects the built SSA program and VTA call graph during
// walkRepo to emit hazard edges and synthetic sink symbols without requiring any
// Spanner schema changes.
func collectSSAHazardEdges(
	pkgs []*packages.Package,
	prog *ssa.Program,
	cg *callgraph.Graph,
	repoPath, repoName, snapshotID string,
	addSym func(Symbol),
	addEdge func(string, string, string),
) {
	if prog == nil {
		return
	}

	chosenPkgPaths := make(map[string]struct{}, len(pkgs))
	for _, p := range pkgs {
		if p != nil && p.PkgPath != "" {
			chosenPkgPaths[p.PkgPath] = struct{}{}
		}
	}

	sinkAdded := make(map[string]struct{})
	ensureSink := func(sinkFQN, sig string, caller *ssa.Function) string {
		if _, ok := sinkAdded[sinkFQN]; !ok {
			sinkAdded[sinkFQN] = struct{}{}
			var rel string
			var line int
			if caller != nil && prog.Fset != nil && caller.Pos().IsValid() {
				pos := prog.Fset.Position(caller.Pos())
				if r, err := filepath.Rel(repoPath, pos.Filename); err == nil {
					rel = r
				} else {
					rel = pos.Filename
				}
				line = pos.Line
			}
			addSym(Symbol{
				ID:          uuidFromFQN(sinkFQN),
				Repo:        repoName,
				SnapshotID:  snapshotID,
				FQN:         sinkFQN,
				Kind:        "sink",
				FilePath:    rel,
				LineStart:   line,
				LineEnd:     line,
				Signature:   sig,
				ContentHash: "",
			})
		}
		return sinkFQN
	}

	allFns := ssautilAllFunctions(prog)
	for fn := range allFns {
		if fn == nil || len(fn.Blocks) == 0 {
			continue
		}
		ownerFn := fn
		for ownerFn.Parent() != nil {
			ownerFn = ownerFn.Parent()
		}
		if ownerFn.Pkg == nil || ownerFn.Pkg.Pkg == nil {
			continue
		}
		pkgPath := ownerFn.Pkg.Pkg.Path()
		if _, inChosen := chosenPkgPaths[pkgPath]; !inChosen {
			continue
		}
		callerFQN := ssaFuncFQN(ownerFn)
		if callerFQN == "" {
			continue
		}

		// Only run top-level function analyses once per named function/method;
		// closures are still inspected for MAP_INSERT/MAP_DELETE/IO_SINK attributed to callerFQN.
		isTopLevel := fn.Parent() == nil

		acquiresLock := false
		if isTopLevel {
			acquiresLock = fnAcquiresAnyLock(fn)
		}

		hasShiftBound := fnHasShiftUpperBound(fn)
		acceptsCtx := fnAcceptsContext(ownerFn)

		// Compute loop membership for basic blocks in fn.
		inLoop := make(map[*ssa.BasicBlock]bool, len(fn.Blocks))
		for _, b := range fn.Blocks {
			if blockCanReachSelf(b) {
				inLoop[b] = true
			}
		}

		// Track active locks held across instructions (explicit Lock/RLock vs explicit Unlock/RUnlock).
		var heldLocks []string

		for _, b := range fn.Blocks {
			loopBlock := inLoop[b]
			for _, instr := range b.Instrs {
				switch ins := instr.(type) {
				case *ssa.MapUpdate:
					if field := resolveStructFieldAddr(ins.Map); field != "" {
						addEdge(callerFQN, field, "MAP_INSERT")
					}

				case *ssa.Return:
					if isTopLevel && acquiresLock {
						for _, res := range ins.Results {
							if field, isMap, isSlice := resolveDirectFieldLoad(res, fn); field != "" {
								if isMap {
									addEdge(callerFQN, field, "MAP_ESCAPE_UNDER_LOCK")
								} else if isSlice {
									addEdge(callerFQN, field, "SLICE_ESCAPE_UNDER_LOCK")
								}
							}
						}
					}

				case *ssa.Defer:
					if loopBlock {
						sink := ensureSink("sink:defer_in_loop:"+callerFQN, "defer in loop", ownerFn)
						addEdge(callerFQN, sink, "DEFER_IN_LOOP")
					}

				case *ssa.Go:
					targetFn := resolveCallTargetFunc(&ins.Call)
					if targetFn != nil && !funcHasRecoverDefer(targetFn) {
						sink := ensureSink("sink:unrecovered_go:"+callerFQN, "unrecovered goroutine", ownerFn)
						addEdge(callerFQN, sink, "SPAWNS_UNRECOVERED_GO")
					}

				case *ssa.BinOp:
					if ins.Op == token.SHL {
						if _, isConst := ins.Y.(*ssa.Const); !isConst && !hasShiftBound {
							sink := ensureSink("sink:unbounded_shift:"+callerFQN, "unbounded shift", ownerFn)
							addEdge(callerFQN, sink, "UNBOUNDED_SHIFT")
						}
					}

				case *ssa.Call:
					// 1. Check builtin delete(m, k)
					if builtin, ok := ins.Call.Value.(*ssa.Builtin); ok {
						if builtin.Name() == "delete" && len(ins.Call.Args) >= 1 {
							if field := resolveStructFieldAddr(ins.Call.Args[0]); field != "" {
								addEdge(callerFQN, field, "MAP_DELETE")
							}
						}
						if builtin.Name() == "clear" && len(ins.Call.Args) >= 1 {
							if field := resolveStructFieldAddr(ins.Call.Args[0]); field != "" {
								addEdge(callerFQN, field, "MAP_DELETE")
							}
						}
						continue
					}

					calleeFQN := resolveSSACallFQN(&ins.Call)

					// 2. Check explicit Lock / RLock acquisition
					if lockField, isLock := detectLockAcquire(&ins.Call, callerFQN); isLock {
						addEdge(callerFQN, lockField, "ACQUIRES_LOCK")
						for _, prevLock := range heldLocks {
							if prevLock != lockField {
								addEdge(prevLock, lockField, "LOCK_ORDER_PRECEDES")
								addEdge(callerFQN, lockField, "ACQUIRES_NESTED_LOCK")
							}
						}
						heldLocks = append(heldLocks, lockField)
						continue
					}

					// 3. Check explicit Unlock / RUnlock release (not deferred)
					if unlockField, isUnlock := detectLockRelease(&ins.Call, callerFQN); isUnlock {
						if len(heldLocks) > 0 {
							updated := heldLocks[:0]
							removed := false
							for _, h := range heldLocks {
								if !removed && h == unlockField {
									removed = true
									continue
								}
								updated = append(updated, h)
							}
							if !removed && len(updated) > 0 {
								updated = updated[:len(updated)-1]
							}
							heldLocks = updated
						}
						continue
					}

					// 4. sync.Map Store / Delete
					if strings.HasSuffix(calleeFQN, "sync.Map.Store") && len(ins.Call.Args) >= 1 {
						if field := resolveStructFieldAddr(ins.Call.Args[0]); field != "" {
							addEdge(callerFQN, field, "MAP_INSERT")
						}
					}
					if (strings.HasSuffix(calleeFQN, "sync.Map.Delete") || strings.HasSuffix(calleeFQN, "maps.DeleteFunc")) && len(ins.Call.Args) >= 1 {
						if field := resolveStructFieldAddr(ins.Call.Args[0]); field != "" {
							addEdge(callerFQN, field, "MAP_DELETE")
						}
					}

					// 5. Calls while holding a lock
					if len(heldLocks) > 0 && calleeFQN != "" {
						addEdge(callerFQN, calleeFQN, "CALLS_UNDER_LOCK")
						// Also add VTA targets if available
						if cg != nil {
							if node := cg.Nodes[fn]; node != nil {
								for _, outEdge := range node.Out {
									if outEdge.Site == ins && outEdge.Callee != nil && outEdge.Callee.Func != nil {
										if vtaCallee := ssaFuncFQN(outEdge.Callee.Func); vtaCallee != "" {
											addEdge(callerFQN, vtaCallee, "CALLS_UNDER_LOCK")
										}
									}
								}
							}
						}
					}

					// 6. External I/O sink detection
					if isExternalIOSink(calleeFQN) {
						sink := ensureSink("sink:io:"+calleeFQN, "io sink "+calleeFQN, ownerFn)
						addEdge(callerFQN, sink, "IO_SINK")
						if len(heldLocks) > 0 {
							addEdge(callerFQN, sink, "CALLS_UNDER_LOCK")
						}
					}

					// 7. Timer in loop
					if loopBlock && isTimerAllocation(calleeFQN) {
						sink := ensureSink("sink:timer_in_loop:"+callerFQN, "timer in loop "+calleeFQN, ownerFn)
						addEdge(callerFQN, sink, "TIMER_IN_LOOP")
					}

					// 8. Detached context without timeout/deadline
					if acceptsCtx && isContextDetachCall(calleeFQN) {
						if !detachedContextHasTimeout(ins, fn) {
							sink := ensureSink("sink:detaches_context:"+callerFQN, "detached context without timeout", ownerFn)
							addEdge(callerFQN, sink, "DETACHES_CONTEXT")
						}
					}

					// 9. ReadWriteTransaction retry-closure outer variable mutation
					if isReadWriteTxnCall(calleeFQN) {
						for _, arg := range ins.Call.Args {
							if mc, ok := arg.(*ssa.MakeClosure); ok {
								if closureFn, ok := mc.Fn.(*ssa.Function); ok {
									if closureMutatesOuterWithoutEntryReset(closureFn) {
										sink := ensureSink("sink:txn_closure:"+callerFQN, "txn closure outer mutation", ownerFn)
										addEdge(callerFQN, sink, "TXN_CLOSURE_OUTER_MUTATION")
									}
								}
							}
						}
					}

					// 10. Unclosed file/resource detection
					if isFileOpenCall(calleeFQN) {
						if !openedResourceIsClosedOrEscapes(ins, fn) {
							sink := ensureSink("sink:unclosed_resource:"+callerFQN, "unclosed resource "+calleeFQN, ownerFn)
							addEdge(callerFQN, sink, "UNCLOSED_RESOURCE")
						}
					}
				}
			}
		}
	}
}

func ssautilAllFunctions(prog *ssa.Program) map[*ssa.Function]bool {
	out := ssautil.AllFunctions(prog)
	if out == nil {
		out = make(map[*ssa.Function]bool)
	}
	var visit func(fn *ssa.Function)
	visit = func(fn *ssa.Function) {
		if fn == nil || out[fn] {
			return
		}
		out[fn] = true
		for _, anon := range fn.AnonFuncs {
			visit(anon)
		}
	}
	for _, pkg := range prog.AllPackages() {
		if pkg == nil {
			continue
		}
		for _, mem := range pkg.Members {
			switch m := mem.(type) {
			case *ssa.Function:
				visit(m)
			case *ssa.Type:
				if m.Object() != nil {
					t := m.Object().Type()
					ms := prog.MethodSets.MethodSet(t)
					for i := 0; i < ms.Len(); i++ {
						if fnObj, ok := ms.At(i).Obj().(*types.Func); ok {
							visit(prog.FuncValue(fnObj))
						}
					}
					pms := prog.MethodSets.MethodSet(types.NewPointer(t))
					for i := 0; i < pms.Len(); i++ {
						if fnObj, ok := pms.At(i).Obj().(*types.Func); ok {
							visit(prog.FuncValue(fnObj))
						}
					}
				}
			}
		}
	}
	return out
}

func derefType(t types.Type) types.Type {
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		return ptr.Elem()
	}
	if ptr, ok := t.(*types.Pointer); ok {
		return ptr.Elem()
	}
	return t
}

func resolveStructFieldAddr(v ssa.Value) string {
	for v != nil {
		switch x := v.(type) {
		case *ssa.UnOp:
			if x.Op == token.MUL {
				v = x.X
				continue
			}
			return ""
		case *ssa.FieldAddr:
			baseType := derefType(x.X.Type())
			st, ok := baseType.Underlying().(*types.Struct)
			if !ok || x.Field < 0 || x.Field >= st.NumFields() {
				return ""
			}
			fld := st.Field(x.Field)
			return fieldFQN(x.X.Type(), fld.Name())
		default:
			return ""
		}
	}
	return ""
}

func resolveDirectFieldLoad(v ssa.Value, fn *ssa.Function) (field string, isMap bool, isSlice bool) {
	return resolveDirectFieldLoadVisited(v, fn, make(map[ssa.Value]bool))
}

func resolveDirectFieldLoadVisited(v ssa.Value, fn *ssa.Function, visited map[ssa.Value]bool) (field string, isMap bool, isSlice bool) {
	if v == nil || visited[v] {
		return "", false, false
	}
	visited[v] = true

	switch x := v.(type) {
	case *ssa.Phi:
		for _, edge := range x.Edges {
			if f, m, s := resolveDirectFieldLoadVisited(edge, fn, visited); f != "" {
				return f, m, s
			}
		}
	case *ssa.UnOp:
		if x.Op != token.MUL {
			return "", false, false
		}
		if fa, ok := x.X.(*ssa.FieldAddr); ok {
			fFQN := resolveStructFieldAddr(fa)
			if fFQN == "" {
				return "", false, false
			}
			switch x.Type().Underlying().(type) {
			case *types.Map:
				return fFQN, true, false
			case *types.Slice:
				return fFQN, false, true
			}
			return "", false, false
		}
		// In functions with `defer`, go/ssa stores the return expression into a
		// local result *ssa.Alloc before RunDefers and then loads `*alloc` at Return.
		if alloc, ok := x.X.(*ssa.Alloc); ok && fn != nil {
			for _, b := range fn.Blocks {
				for _, instr := range b.Instrs {
					if st, ok := instr.(*ssa.Store); ok && st.Addr == alloc {
						if f, m, s := resolveDirectFieldLoadVisited(st.Val, fn, visited); f != "" {
							return f, m, s
						}
					}
				}
			}
		}
	}
	return "", false, false
}

func resolveSSACallFQN(common *ssa.CallCommon) string {
	if common == nil {
		return ""
	}
	if common.IsInvoke() {
		if common.Method != nil {
			if recv := common.Value.Type(); recv != nil {
				if tf := typeFQN(recv); tf != "" {
					return tf + "." + common.Method.Name()
				}
			}
			return common.Method.FullName()
		}
		return ""
	}
	if fn := common.StaticCallee(); fn != nil {
		return ssaFuncFQN(fn)
	}
	return ""
}

func resolveCallTargetFunc(common *ssa.CallCommon) *ssa.Function {
	if common == nil {
		return nil
	}
	if fn := common.StaticCallee(); fn != nil {
		return fn
	}
	if mc, ok := common.Value.(*ssa.MakeClosure); ok {
		if fn, ok := mc.Fn.(*ssa.Function); ok {
			return fn
		}
	}
	return nil
}

func detectLockAcquire(common *ssa.CallCommon, callerFQN string) (string, bool) {
	callee := resolveSSACallFQN(common)
	if callee == "" {
		return "", false
	}
	if !(strings.HasSuffix(callee, "sync.Mutex.Lock") ||
		strings.HasSuffix(callee, "sync.RWMutex.Lock") ||
		strings.HasSuffix(callee, "sync.RWMutex.RLock") ||
		strings.HasSuffix(callee, "sync.Locker.Lock")) {
		return "", false
	}
	if !common.IsInvoke() && len(common.Args) >= 1 {
		if field := resolveStructFieldAddr(common.Args[0]); field != "" {
			return field, true
		}
	}
	return callerFQN + "#lock", true
}

func detectLockRelease(common *ssa.CallCommon, callerFQN string) (string, bool) {
	callee := resolveSSACallFQN(common)
	if callee == "" {
		return "", false
	}
	if !(strings.HasSuffix(callee, "sync.Mutex.Unlock") ||
		strings.HasSuffix(callee, "sync.RWMutex.Unlock") ||
		strings.HasSuffix(callee, "sync.RWMutex.RUnlock") ||
		strings.HasSuffix(callee, "sync.Locker.Unlock")) {
		return "", false
	}
	if !common.IsInvoke() && len(common.Args) >= 1 {
		if field := resolveStructFieldAddr(common.Args[0]); field != "" {
			return field, true
		}
	}
	return callerFQN + "#lock", true
}

func fnAcquiresAnyLock(fn *ssa.Function) bool {
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			if call, ok := instr.(*ssa.Call); ok {
				if _, ok := detectLockAcquire(&call.Call, ""); ok {
					return true
				}
			}
		}
	}
	return false
}

func blockCanReachSelf(start *ssa.BasicBlock) bool {
	if start == nil {
		return false
	}
	visited := make(map[*ssa.BasicBlock]bool)
	queue := append([]*ssa.BasicBlock(nil), start.Succs...)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == start {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		queue = append(queue, cur.Succs...)
	}
	return false
}

func funcHasRecoverDefer(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			def, ok := instr.(*ssa.Defer)
			if !ok {
				continue
			}
			target := resolveCallTargetFunc(&def.Call)
			if funcCallsBuiltinRecover(target) {
				return true
			}
		}
	}
	return false
}

func funcCallsBuiltinRecover(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			call, ok := instr.(*ssa.Call)
			if !ok {
				continue
			}
			if builtin, ok := call.Call.Value.(*ssa.Builtin); ok && builtin.Name() == "recover" {
				return true
			}
		}
	}
	return false
}

func fnHasShiftUpperBound(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			bin, ok := instr.(*ssa.BinOp)
			if !ok {
				continue
			}
			switch bin.Op {
			case token.LSS, token.LEQ, token.GTR, token.GEQ:
				if isSmallPositiveShiftConst(bin.X) || isSmallPositiveShiftConst(bin.Y) {
					return true
				}
			}
		}
	}
	return false
}

func isSmallPositiveShiftConst(v ssa.Value) bool {
	c, ok := v.(*ssa.Const)
	if !ok || c.Value == nil {
		return false
	}
	n, err := strconv.ParseInt(c.Value.ExactString(), 10, 64)
	if err != nil {
		return false
	}
	return n >= 1 && n <= 63
}

func fnAcceptsContext(fn *ssa.Function) bool {
	if fn == nil || fn.Signature == nil {
		return false
	}
	params := fn.Signature.Params()
	for i := 0; i < params.Len(); i++ {
		t := params.At(i).Type().String()
		if t == "context.Context" || strings.HasSuffix(t, "net/http.Request") {
			return true
		}
	}
	return false
}

func isExternalIOSink(calleeFQN string) bool {
	if calleeFQN == "" {
		return false
	}
	ioSuffixes := []string{
		"net/http.Client.Do",
		"net/http.Get",
		"net/http.Post",
		"net.Conn.Read",
		"net.Conn.Write",
		"net.Dial",
		"net.Dialer.DialContext",
		"os.ReadFile",
		"os.WriteFile",
		"os.Open",
		"os.OpenFile",
		"os.Create",
		"exec.Cmd.Run",
		"exec.Cmd.Output",
		"exec.Cmd.CombinedOutput",
	}
	for _, suf := range ioSuffixes {
		if calleeFQN == suf || strings.HasSuffix(calleeFQN, "."+suf) {
			return true
		}
	}
	if strings.Contains(calleeFQN, "cloud.google.com/go/spanner.") ||
		strings.Contains(calleeFQN, "cloud.google.com/go/kms") ||
		strings.Contains(calleeFQN, "google.golang.org/grpc.") {
		return true
	}
	return false
}

func isTimerAllocation(calleeFQN string) bool {
	return calleeFQN == "time.After" || calleeFQN == "time.NewTicker" || calleeFQN == "time.Tick"
}

func isContextDetachCall(calleeFQN string) bool {
	return calleeFQN == "context.WithoutCancel" || calleeFQN == "context.Background" || calleeFQN == "context.TODO"
}

func detachedContextHasTimeout(detachCall *ssa.Call, fn *ssa.Function) bool {
	if detachCall != nil && detachCall.Referrers() != nil {
		for _, ref := range *detachCall.Referrers() {
			if c, ok := ref.(*ssa.Call); ok {
				callee := resolveSSACallFQN(&c.Call)
				if callee == "context.WithTimeout" || callee == "context.WithDeadline" ||
					callee == "context.WithTimeoutCause" || callee == "context.WithDeadlineCause" {
					return true
				}
			}
		}
	}
	if fn != nil {
		for _, b := range fn.Blocks {
			for _, instr := range b.Instrs {
				if c, ok := instr.(*ssa.Call); ok {
					callee := resolveSSACallFQN(&c.Call)
					if callee == "context.WithTimeout" || callee == "context.WithDeadline" ||
						callee == "context.WithTimeoutCause" || callee == "context.WithDeadlineCause" {
						for _, arg := range c.Call.Args {
							if arg == detachCall {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}

func isReadWriteTxnCall(calleeFQN string) bool {
	return strings.HasSuffix(calleeFQN, ".ReadWriteTransaction") ||
		strings.HasSuffix(calleeFQN, ".RunInReadWriteTransaction")
}

func closureMutatesOuterWithoutEntryReset(closureFn *ssa.Function) bool {
	if closureFn == nil || len(closureFn.Blocks) == 0 || len(closureFn.FreeVars) == 0 {
		return false
	}
	entryBlock := closureFn.Blocks[0]
	resetAtEntry := make(map[*ssa.FreeVar]bool)
	loadedAtEntry := make(map[*ssa.FreeVar]bool)
	for _, instr := range entryBlock.Instrs {
		switch ins := instr.(type) {
		case *ssa.UnOp:
			if ins.Op == token.MUL {
				if fv, ok := ins.X.(*ssa.FreeVar); ok {
					loadedAtEntry[fv] = true
				}
			}
		case *ssa.Store:
			if fv, ok := ins.Addr.(*ssa.FreeVar); ok && !loadedAtEntry[fv] {
				if _, isConst := ins.Val.(*ssa.Const); isConst {
					resetAtEntry[fv] = true
				}
			}
		}
	}

	for _, b := range closureFn.Blocks {
		for _, instr := range b.Instrs {
			if st, ok := instr.(*ssa.Store); ok {
				if fv, ok := st.Addr.(*ssa.FreeVar); ok {
					if !resetAtEntry[fv] {
						return true
					}
				}
			}
		}
	}
	return false
}

func isFileOpenCall(calleeFQN string) bool {
	return calleeFQN == "os.Open" || calleeFQN == "os.OpenFile" || calleeFQN == "os.Create"
}

func openedResourceIsClosedOrEscapes(openCall *ssa.Call, fn *ssa.Function) bool {
	if openCall == nil || openCall.Referrers() == nil {
		return true
	}
	var fileVal ssa.Value
	for _, ref := range *openCall.Referrers() {
		if ext, ok := ref.(*ssa.Extract); ok && ext.Index == 0 {
			fileVal = ext
			break
		}
	}
	if fileVal == nil || fileVal.Referrers() == nil {
		return true
	}
	for _, ref := range *fileVal.Referrers() {
		switch r := ref.(type) {
		case *ssa.Return:
			return true
		case *ssa.Store:
			return true
		case *ssa.Defer:
			callee := resolveSSACallFQN(&r.Call)
			if strings.HasSuffix(callee, ".Close") {
				return true
			}
			if target := resolveCallTargetFunc(&r.Call); target != nil && funcCallsClose(target) {
				return true
			}
		case *ssa.Call:
			callee := resolveSSACallFQN(&r.Call)
			if strings.HasSuffix(callee, ".Close") {
				return true
			}
		case *ssa.MakeClosure:
			if target, ok := r.Fn.(*ssa.Function); ok && funcCallsClose(target) {
				return true
			}
		}
	}
	return false
}

func funcCallsClose(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}
	for _, b := range fn.Blocks {
		for _, instr := range b.Instrs {
			if c, ok := instr.(*ssa.Call); ok {
				if strings.HasSuffix(resolveSSACallFQN(&c.Call), ".Close") {
					return true
				}
			}
		}
	}
	return false
}

// evaluateHazardsFromGraph computes all 12 hazard categories from the extracted
// symbol and edge sets (either in-memory from walkRepo or queried from Spanner).
func evaluateHazardsFromGraph(syms []Symbol, edges []Edge, categoryFilter, filePrefix string) HazardsReport {
	rep := evaluateHazardsFromGraphWithOpts("local", "working-tree", syms, edges, HazardQueryOpts{
		Category:   categoryFilter,
		FilePrefix: filePrefix,
	})
	return *rep
}

func evaluateHazardsFromGraphWithOpts(repo, snapshotID string, syms []Symbol, edges []Edge, opts HazardQueryOpts) *HazardsReport {
	symByID := make(map[string]Symbol, len(syms))
	for _, s := range syms {
		symByID[s.ID] = s
	}

	var findings []HazardFinding
	addFinding := func(f HazardFinding) {
		if opts.Category != "" && !strings.EqualFold(f.Category, opts.Category) {
			return
		}
		if opts.FilePrefix != "" && !strings.HasPrefix(f.FilePath, opts.FilePrefix) {
			return
		}
		findings = append(findings, f)
	}

	// Index edges by kind and source/destination.
	edgesByKind := make(map[string][]Edge)
	callsFrom := make(map[string][]string)
	ioSinksFrom := make(map[string][]string)
	nestedLockCallers := make(map[string][]string) // dstLockID -> []callerSymbolID
	for _, e := range edges {
		edgesByKind[e.Kind] = append(edgesByKind[e.Kind], e)
		if e.Kind == "CALLS" || e.Kind == "CALLS_UNDER_LOCK" {
			callsFrom[e.SrcID] = append(callsFrom[e.SrcID], e.DstID)
		}
		if e.Kind == "IO_SINK" {
			ioSinksFrom[e.SrcID] = append(ioSinksFrom[e.SrcID], e.DstID)
		}
		if e.Kind == "ACQUIRES_NESTED_LOCK" {
			nestedLockCallers[e.DstID] = append(nestedLockCallers[e.DstID], e.SrcID)
		}
	}

	// 1. Direct single-edge hazards
	directKinds := []struct {
		edgeKind string
		category string
		severity string
		descFmt  string
	}{
		{"MAP_ESCAPE_UNDER_LOCK", "MAP_ESCAPE_UNDER_LOCK", "HIGH", "%s returns map field %s under lock without cloning"},
		{"SLICE_ESCAPE_UNDER_LOCK", "SLICE_ESCAPE_UNDER_LOCK", "HIGH", "%s returns slice field %s under lock without cloning"},
		{"TXN_CLOSURE_OUTER_MUTATION", "TXN_CLOSURE_OUTER_MUTATION", "HIGH", "%s mutates outer captured state inside ReadWriteTransaction closure without entry reset (%s)"},
		{"DEFER_IN_LOOP", "DEFER_IN_LOOP", "MEDIUM", "%s executes defer inside a loop cycle (%s)"},
		{"TIMER_IN_LOOP", "TIMER_IN_LOOP", "MEDIUM", "%s allocates timer/ticker inside a loop cycle (%s)"},
		{"DETACHES_CONTEXT", "DETACHES_CONTEXT", "MEDIUM", "%s detaches context without timeout or deadline (%s)"},
		{"SPAWNS_UNRECOVERED_GO", "SPAWNS_UNRECOVERED_GO", "MEDIUM", "%s spawns goroutine without panic recovery defer (%s)"},
		{"UNBOUNDED_SHIFT", "UNBOUNDED_SHIFT", "MEDIUM", "%s performs left bit-shift without upper-bound guard (%s)"},
		{"UNCLOSED_RESOURCE", "UNCLOSED_RESOURCE", "MEDIUM", "%s opens file/resource without closing or returning it (%s)"},
	}

	for _, dk := range directKinds {
		for _, e := range edgesByKind[dk.edgeKind] {
			src := symByID[e.SrcID]
			dst := symByID[e.DstID]
			if src.FQN == "" {
				continue
			}
			addFinding(HazardFinding{
				Category:    dk.category,
				Severity:    dk.severity,
				FunctionFQN: src.FQN,
				TargetFQN:   dst.FQN,
				FilePath:    src.FilePath,
				LineStart:   src.LineStart,
				LineEnd:     src.LineEnd,
				EdgeKind:    dk.edgeKind,
				Description: fmt.Sprintf(dk.descFmt, src.FQN, dst.FQN),
			})
		}
	}

	// 2. UNBOUNDED_MAP: struct map fields with MAP_INSERT > 0 and MAP_DELETE == 0
	insertersByField := make(map[string][]string)
	deletesByField := make(map[string]int)
	for _, e := range edgesByKind["MAP_INSERT"] {
		insertersByField[e.DstID] = append(insertersByField[e.DstID], e.SrcID)
	}
	for _, e := range edgesByKind["MAP_DELETE"] {
		deletesByField[e.DstID]++
	}
	for fieldID, inserters := range insertersByField {
		if deletesByField[fieldID] > 0 || len(inserters) == 0 {
			continue
		}
		fieldSym := symByID[fieldID]
		callerSym := symByID[inserters[0]]
		if callerSym.FQN == "" && fieldSym.FQN == "" {
			continue
		}
		filePath := fieldSym.FilePath
		lineStart := fieldSym.LineStart
		lineEnd := fieldSym.LineEnd
		if filePath == "" {
			filePath = callerSym.FilePath
			lineStart = callerSym.LineStart
			lineEnd = callerSym.LineEnd
		}
		addFinding(HazardFinding{
			Category:    "UNBOUNDED_MAP",
			Severity:    "MEDIUM",
			FunctionFQN: callerSym.FQN,
			TargetFQN:   fieldSym.FQN,
			FilePath:    filePath,
			LineStart:   lineStart,
			LineEnd:     lineEnd,
			EdgeKind:    "MAP_INSERT",
			Description: fmt.Sprintf("map field %s has %d insertion path(s) (e.g. %s) and 0 deletion/eviction paths", fieldSym.FQN, len(inserters), callerSym.FQN),
		})
	}

	// 3. Multi-hop LOCK_ACROSS_IO: CALLS_UNDER_LOCK -> CALLS* (up to 4 hops) -> IO_SINK
	seenLockIO := make(map[string]bool)
	for _, e := range edgesByKind["CALLS_UNDER_LOCK"] {
		callerSym := symByID[e.SrcID]
		if callerSym.FQN == "" {
			continue
		}
		if path := findIOSinkPath(e.SrcID, e.DstID, callsFrom, ioSinksFrom, symByID); len(path) >= 2 {
			key := callerSym.FQN + "->" + path[len(path)-1]
			if seenLockIO[key] {
				continue
			}
			seenLockIO[key] = true
			addFinding(HazardFinding{
				Category:    "LOCK_ACROSS_IO",
				Severity:    "HIGH",
				FunctionFQN: callerSym.FQN,
				TargetFQN:   path[len(path)-1],
				FilePath:    callerSym.FilePath,
				LineStart:   callerSym.LineStart,
				LineEnd:     callerSym.LineEnd,
				EdgeKind:    "CALLS_UNDER_LOCK",
				HopPath:     path,
				Description: fmt.Sprintf("%s holds lock across %d-hop call path to I/O sink: %s", callerSym.FQN, len(path)-1, strings.Join(path, " -> ")),
			})
		}
	}

	// 4. CYCLIC_LOCK_ORDER: cycles in LOCK_ORDER_PRECEDES
	lockAdj := make(map[string][]string)
	for _, e := range edgesByKind["LOCK_ORDER_PRECEDES"] {
		lockAdj[e.SrcID] = append(lockAdj[e.SrcID], e.DstID)
	}
	seenLockCycle := make(map[string]bool)
	for lockA, nexts := range lockAdj {
		for _, lockB := range nexts {
			if canReachLock(lockB, lockA, lockAdj) {
				pairKey := lockA + "|" + lockB
				if lockB < lockA {
					pairKey = lockB + "|" + lockA
				}
				if seenLockCycle[pairKey] {
					continue
				}
				seenLockCycle[pairKey] = true
				symA := symByID[lockA]
				symB := symByID[lockB]
				var callerSym Symbol
				if callers := nestedLockCallers[lockB]; len(callers) > 0 {
					callerSym = symByID[callers[0]]
				} else if callers := nestedLockCallers[lockA]; len(callers) > 0 {
					callerSym = symByID[callers[0]]
				} else {
					callerSym = symA
				}
				addFinding(HazardFinding{
					Category:    "CYCLIC_LOCK_ORDER",
					Severity:    "HIGH",
					FunctionFQN: callerSym.FQN,
					TargetFQN:   fmt.Sprintf("%s <-> %s", symA.FQN, symB.FQN),
					FilePath:    callerSym.FilePath,
					LineStart:   callerSym.LineStart,
					LineEnd:     callerSym.LineEnd,
					EdgeKind:    "LOCK_ORDER_PRECEDES",
					HopPath:     []string{symA.FQN, symB.FQN, symA.FQN},
					Description: fmt.Sprintf("cyclic lock acquisition order between %s and %s (in %s)", symA.FQN, symB.FQN, callerSym.FQN),
				})
			}
		}
	}

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Category != findings[j].Category {
			return findings[i].Category < findings[j].Category
		}
		if findings[i].FunctionFQN != findings[j].FunctionFQN {
			return findings[i].FunctionFQN < findings[j].FunctionFQN
		}
		return findings[i].TargetFQN < findings[j].TargetFQN
	})

	if opts.Limit > 0 && len(findings) > opts.Limit {
		findings = findings[:opts.Limit]
	}

	byCat := make(map[string]int)
	for _, f := range findings {
		byCat[f.Category]++
	}

	return &HazardsReport{
		Repo:       repo,
		Snapshot:   snapshotID,
		Total:      len(findings),
		ByCategory: byCat,
		Hazards:    findings,
	}
}

func findIOSinkPath(callerID, startCalleeID string, callsFrom, ioSinksFrom map[string][]string, symByID map[string]Symbol) []string {
	callerFQN := symByID[callerID].FQN
	startSym := symByID[startCalleeID]
	if startSym.Kind == "sink" && strings.HasPrefix(startSym.FQN, "sink:io:") {
		return []string{callerFQN, startSym.FQN}
	}

	type bfsNode struct {
		id   string
		path []string
	}
	queue := []bfsNode{{id: startCalleeID, path: []string{callerFQN, startSym.FQN}}}
	visited := map[string]bool{callerID: true, startCalleeID: true}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		if sinks := ioSinksFrom[cur.id]; len(sinks) > 0 {
			sinkFQN := symByID[sinks[0]].FQN
			if sinkFQN == "" {
				sinkFQN = "sink:io"
			}
			return append(cur.path, sinkFQN)
		}
		if len(cur.path) >= 5 { // up to 4 call hops + sink
			continue
		}
		for _, nextID := range callsFrom[cur.id] {
			if visited[nextID] {
				continue
			}
			nextSym := symByID[nextID]
			if nextSym.FQN == "" {
				continue
			}
			visited[nextID] = true
			nextPath := append(append([]string(nil), cur.path...), nextSym.FQN)
			if nextSym.Kind == "sink" && strings.HasPrefix(nextSym.FQN, "sink:io:") {
				return nextPath
			}
			queue = append(queue, bfsNode{id: nextID, path: nextPath})
		}
	}
	return nil
}

func canReachLock(start, target string, adj map[string][]string) bool {
	visited := make(map[string]bool)
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == target {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		queue = append(queue, adj[cur]...)
	}
	return false
}

// doHazards queries Spanner (or analyzes a local repo path when opts.RepoPath != "")
// and returns the structured HazardsReport.
func doHazards(ctx context.Context, sp *spanner.Client, repo, snap string, opts HazardQueryOpts) (*HazardsReport, error) {
	if opts.RepoPath != "" {
		absPath, err := filepath.Abs(opts.RepoPath)
		if err != nil {
			return nil, err
		}
		syms, edges, err := walkRepo(absPath, "local", "working-tree", func(string, int, int, string) {})
		if err != nil {
			return nil, err
		}
		return evaluateHazardsFromGraphWithOpts("local", "working-tree", syms, edges, opts), nil
	}
	if sp == nil {
		return nil, fmt.Errorf("spanner client is required when repo_path is not provided")
	}

	var syms []Symbol
	memStmt := spanner.Statement{
		SQL: `SELECT symbol_id, fqn, content_hash, kind, file_path, line_start, line_end
		      FROM SnapshotMembers
		      WHERE repo = @repo AND snapshot_id = @snap`,
		Params: map[string]interface{}{"repo": repo, "snap": snap},
	}
	iter := sp.Single().Query(ctx, memStmt)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return nil, err
		}
		var s Symbol
		var ch spanner.NullString
		var ls, le int64
		if err := row.Columns(&s.ID, &s.FQN, &ch, &s.Kind, &s.FilePath, &ls, &le); err != nil {
			iter.Stop()
			return nil, err
		}
		s.ContentHash = ch.StringVal
		s.Repo = repo
		s.SnapshotID = snap
		s.LineStart = int(ls)
		s.LineEnd = int(le)
		syms = append(syms, s)
	}
	iter.Stop()

	var edges []Edge
	edgeQueries := []string{
		`SELECT e.src_symbol_id, e.dst_symbol_id, e.kind
		 FROM SnapshotMembers AS m
		 JOIN SymbolEdges AS e
		   ON m.symbol_id = e.src_symbol_id AND m.content_hash = e.src_content_hash
		 WHERE m.repo = @repo AND m.snapshot_id = @snap`,
		`SELECT symbol_id AS src_symbol_id, dst_symbol_id, kind
		 FROM SnapshotEdges
		 WHERE repo = @repo AND snapshot_id = @snap`,
	}
	for _, q := range edgeQueries {
		eIter := sp.Single().Query(ctx, spanner.Statement{
			SQL:    q,
			Params: map[string]interface{}{"repo": repo, "snap": snap},
		})
		for {
			row, err := eIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				eIter.Stop()
				return nil, err
			}
			var e Edge
			if err := row.Columns(&e.SrcID, &e.DstID, &e.Kind); err != nil {
				eIter.Stop()
				return nil, err
			}
			e.Repo = repo
			e.SnapshotID = snap
			edges = append(edges, e)
		}
		eIter.Stop()
	}

	return evaluateHazardsFromGraphWithOpts(repo, snap, syms, edges, opts), nil
}

func hazardsHasLocalAnalyzeFlag(args []string) bool {
	fs := flag.NewFlagSet("hazards", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	_ = fs.String("category", "", "")
	_ = fs.String("file-prefix", "", "")
	_ = fs.Int("limit", 0, "")
	_ = fs.String("snapshot", "", "")
	repoPath := fs.String("repo-path", "", "")
	analyzePath := fs.String("analyze", "", "")
	dirPath := fs.String("dir", "", "")
	if err := fs.Parse(args); err != nil {
		return false
	}
	return strings.TrimSpace(*repoPath) != "" || strings.TrimSpace(*analyzePath) != "" || strings.TrimSpace(*dirPath) != ""
}

func cmdHazards(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("hazards", flag.ExitOnError)
	category := fs.String("category", "", "filter by hazard category")
	filePrefix := fs.String("file-prefix", "", "filter by file path prefix")
	limit := fs.Int("limit", 0, "max hazards to return (0 = all)")
	snapshot := fs.String("snapshot", "", "override the current snapshot")
	repoPath := fs.String("repo-path", "", "analyze a local repository path directly without Spanner")
	analyzePath := fs.String("analyze", "", "alias for --repo-path: analyze a local repository path directly without Spanner")
	dirPath := fs.String("dir", "", "alias for --repo-path: analyze a local repository path directly without Spanner")
	_ = fs.Parse(args)

	localRepoPath := strings.TrimSpace(*repoPath)
	if localRepoPath == "" {
		localRepoPath = strings.TrimSpace(*analyzePath)
	}
	if localRepoPath == "" {
		localRepoPath = strings.TrimSpace(*dirPath)
	}

	opts := HazardQueryOpts{
		Category:   *category,
		FilePrefix: *filePrefix,
		Limit:      *limit,
		RepoPath:   localRepoPath,
	}
	if localRepoPath != "" {
		rep, err := doHazards(ctx, nil, "local", "working-tree", opts)
		if err != nil {
			return err
		}
		printHazardsReport(rep)
		return nil
	}

	sp, err := commandSpannerClient(ctx)
	if err != nil {
		return err
	}
	defer sp.Close()
	repo, snap, note, err := resolveQuerySnapshot(ctx, sp, *snapshot)
	if err != nil {
		return err
	}
	fmt.Println(note)
	rep, err := doHazards(ctx, sp, repo, snap, opts)
	if err != nil {
		return err
	}
	printHazardsReport(rep)
	return nil
}

func printHazardsReport(rep *HazardsReport) {
	fmt.Printf("hazards: repo=%s snapshot=%s total=%d\n", rep.Repo, rep.Snapshot, rep.Total)
	for _, h := range rep.Hazards {
		fmt.Printf("  [%-6s] %-28s %s (%s:%d)\n           %s\n",
			h.Severity, h.Category, h.FunctionFQN, h.FilePath, h.LineStart, h.Description)
	}
}
