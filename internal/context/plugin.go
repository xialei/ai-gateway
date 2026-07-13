// Package context implements the Context Pipeline: a plugin DAG with shared
// budget, fail-open semantics, and per-plugin circuit breakers.
//
// Plugins declare the named artifacts they produce / consume; the engine
// builds a DAG from those declarations (detecting cycles), runs independent
// branches concurrently under a shared, decreasing budget, and falls back to
// last-good artifacts on failure so context enrichment never fails a user
// request.
package context

import (
	"context"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// Plugin is the contract every context extension implements. The three
// design contracts map onto this interface:
//   - Dependency declaration: Produces / Consumes form DAG edges.
//   - Timeout budget: Execute receives deadline + remaining budget as args;
//     the plugin must not exceed min(its own cap, remaining).
//   - Breaker state: the plugin reads its own breaker (via BreakerReader) to
//     decide whether to short-circuit to a fallback.
type Plugin interface {
	Name() string
	Produces() []string
	Consumes() []string
	// Execute runs the plugin. artifacts holds the named outputs of upstream
	// plugins (keyed by produced name). The plugin returns its produced
	// artifacts, which the engine merges into the shared artifact set.
	Execute(ctx context.Context, req *model.Request, artifacts Artifacts, deadline time.Time, remaining time.Duration) (Artifacts, error)
}

// Artifacts is the set of named outputs flowing through the DAG.
type Artifacts map[string]any

// BreakerReader lets a plugin read its own circuit-breaker state without
// owning state-management complexity. The engine injects a reader bound to
// the plugin's id.
type BreakerReader interface {
	// IsOpen reports whether the plugin's breaker is currently tripped.
	IsOpen(pluginID string) bool
}
