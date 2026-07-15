package context

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// ErrCycle is returned when plugin produces/consumes declarations form a
// cycle, making a valid DAG impossible.
var ErrCycle = errors.New("context pipeline has a dependency cycle")

// Engine builds a DAG from plugin declarations and executes it. Independent
// branches run concurrently under a shared, decreasing budget; the slowest
// branch bounds tail latency rather than the sum of all plugins.
type Engine struct {
	plugins  []Plugin
	breakers *Breakers
	logger   *slog.Logger
	// lastGood caches the most recent successful artifacts per produced name,
	// for fail-open fallback when a plugin errors or its breaker is open.
	mu       sync.Mutex
	lastGood Artifacts
}

// NewEngine builds a DAG from plugins, rejecting cycles. breakers may be nil
// to disable per-plugin circuit breaking.
func NewEngine(plugins []Plugin, breakers *Breakers, logger *slog.Logger) (*Engine, error) {
	if breakers == nil {
		breakers = NewBreakers(nil)
	}
	e := &Engine{
		plugins:  plugins,
		breakers: breakers,
		logger:   logger,
		lastGood: Artifacts{},
	}
	if err := e.validateDAG(); err != nil {
		return nil, err
	}
	return e, nil
}

// validateDAG ensures produces/consumes declarations form an acyclic graph.
func (e *Engine) validateDAG() error {
	// Build producer map: artifact → plugin that produces it.
	producer := make(map[string]string)
	for _, p := range e.plugins {
		for _, art := range p.Produces() {
			if existing, ok := producer[art]; ok && existing != p.Name() {
				return fmt.Errorf("artifact %q produced by both %q and %q", art, existing, p.Name())
			}
			producer[art] = p.Name()
		}
	}
	// Edge: plugin → plugins it depends on (via consumed artifacts).
	adj := make(map[string][]string)
	for _, p := range e.plugins {
		for _, art := range p.Consumes() {
			prod, ok := producer[art]
			if !ok {
				return fmt.Errorf("plugin %q consumes unknown artifact %q", p.Name(), art)
			}
			adj[p.Name()] = append(adj[p.Name()], prod)
		}
	}
	// Cycle detection via DFS coloring.
	color := make(map[string]int) // 0=white, 1=gray, 2=black
	var visit func(name string) error
	visit = func(name string) error {
		color[name] = 1
		for _, dep := range adj[name] {
			switch color[dep] {
			case 1:
				return fmt.Errorf("%w: %q → %q", ErrCycle, name, dep)
			case 0:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[name] = 2
		return nil
	}
	for _, p := range e.plugins {
		if color[p.Name()] == 0 {
			if err := visit(p.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Run executes the DAG. It is fail-open: a plugin error or open breaker does
// not fail the request; the engine uses last-good artifacts (or empty) for
// that branch and continues. The shared budget is decremented as plugins run.
func (e *Engine) Run(ctx context.Context, req *model.Request) (Artifacts, error) {
	if len(e.plugins) == 0 {
		return Artifacts{}, nil
	}
	artifacts := Artifacts{}
	// Seed with last-good for fail-open fallback availability.
	e.mu.Lock()
	for k, v := range e.lastGood {
		artifacts[k] = v
	}
	e.mu.Unlock()

	// Topological execution: a plugin is ready when all its consumed artifacts
	// exist in the artifact set. We schedule in waves so independent plugins
	// run concurrently. The shared budget (remaining/deadline) and the artifact
	// set are mutated only under mu (writes) or via per-plugin snapshots (reads
	// inside a plugin), so concurrent plugins never race on shared state.
	remaining := req.Budget
	deadline := time.Now().Add(remaining)

	var mu sync.Mutex
	completed := make(map[string]bool, len(e.plugins))
	for round := 0; len(completed) < len(e.plugins); round++ {
		if round > len(e.plugins) {
			// safety: should be unreachable given acyclicity
			break
		}
		// Compute the ready set serially under mu (reads artifacts safely).
		mu.Lock()
		var ready []Plugin
		for _, p := range e.plugins {
			if completed[p.Name()] {
				continue
			}
			ok := true
			for _, c := range p.Consumes() {
				if _, has := artifacts[c]; !has {
					ok = false
					break
				}
			}
			if ok {
				ready = append(ready, p)
			}
		}
		// Snapshot the budget and a read-only copy of artifacts for this wave.
		// Plugins execute against the snapshot, so a sibling plugin writing its
		// outputs into the shared map cannot trigger a concurrent map read+write.
		curRemaining := remaining
		curDeadline := deadline
		artSnapshot := snapshotArtifacts(artifacts)
		mu.Unlock()

		if len(ready) == 0 {
			// No progress possible — remaining plugins have unsatisfied deps.
			break
		}
		var wg sync.WaitGroup
		for _, p := range ready {
			wg.Add(1)
			go func(p Plugin, art Artifacts, rem time.Duration, dl time.Time) {
				defer wg.Done()
				// Fail-open extends to panics: a buggy upper-layer plugin must
				// never crash the gateway. Recover, mark the plugin done (no
				// artifacts), and record a breaker failure so repeated panics
				// trip the breaker.
				defer func() {
					if r := recover(); r != nil {
						e.logger.Error("plugin panicked, using fallback",
							"plugin", p.Name(), "panic", r)
						mu.Lock()
						completed[p.Name()] = true
						e.breakers.Record(p.Name(), 0, false)
						mu.Unlock()
					}
				}()
				out, spent, ok := e.runPlugin(ctx, p, req, art, dl, rem)
				mu.Lock()
				defer mu.Unlock()
				completed[p.Name()] = true
				if ok {
					for k, v := range out {
						artifacts[k] = v
					}
					e.mu.Lock()
					for k, v := range out {
						e.lastGood[k] = v
					}
					e.mu.Unlock()
				}
				// decrement shared budget by what this plugin spent
				remaining -= spent
				if remaining < 0 {
					remaining = 0
				}
				deadline = time.Now().Add(remaining)
			}(p, artSnapshot, curRemaining, curDeadline)
		}
		wg.Wait()
	}
	// Write the budget the context pipeline actually consumed back to the
	// request, so the connector's doWithHeaderBudget races backend headers
	// against what's left — not the original full budget. Without this,
	// context plugins + backend headers can sum to ~2x the configured latency
	// class.
	req.Budget = remaining
	return artifacts, nil
}

// runPlugin executes one plugin with fail-open + breaker semantics. Returns
// the produced artifacts, the time spent, and whether it succeeded. The
// artifacts argument is a per-wave snapshot, safe for the plugin to read
// concurrently with siblings writing their own outputs.
func (e *Engine) runPlugin(ctx context.Context, p Plugin, req *model.Request, artifacts Artifacts, deadline time.Time, remaining time.Duration) (Artifacts, time.Duration, bool) {
	if e.breakers.IsOpen(p.Name()) {
		e.logger.Warn("plugin breaker open, using fallback", "plugin", p.Name())
		return Artifacts{}, 0, false
	}
	stageBudget := remaining
	if stageBudget <= 0 {
		// no budget left: fail-open, skip this plugin
		return Artifacts{}, 0, false
	}
	pctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	start := time.Now()
	out, err := p.Execute(pctx, req, artifacts, deadline, stageBudget)
	spent := time.Since(start)
	e.breakers.Record(p.Name(), spent, err == nil)
	if err != nil {
		e.logger.Warn("plugin failed, using fallback", "plugin", p.Name(), "error", err)
		return Artifacts{}, spent, false
	}
	return out, spent, true
}

// snapshotArtifacts returns a shallow copy of a, so callers can hand plugins a
// read-only view that is immune to concurrent writes into the original map.
func snapshotArtifacts(a Artifacts) Artifacts {
	out := make(Artifacts, len(a))
	for k, v := range a {
		out[k] = v
	}
	return out
}
