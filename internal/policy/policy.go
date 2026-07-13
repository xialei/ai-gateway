// Package policy implements the Policy Engine: model routing, latency-class
// budget injection, ACL, and the PII decision (allow / redact / block). All
// decisions are O(1) reads over precomputed config — no heavy compute on the
// sync path.
package policy

import (
	"errors"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

// ErrDenied is returned when ACL or PII policy refuses a request.
var ErrDenied = errors.New("request denied by policy")

// Engine applies policy to a request in place.
type Engine struct {
	cfg     config.PolicyConfig
	budgets config.LatencyBudgets
	// piiForExternal: when the route target is an external backend, PII is
	// redacted; internal (self-hosted vLLM/SGLang) backends are trusted.
	externalTargets map[string]struct{}
}

// New constructs an Engine. externalTargets lists route target labels whose
// backends are external (OpenAI/Anthropic) and thus subject to PII redaction.
func New(cfg config.PolicyConfig, budgets config.LatencyBudgets, externalTargets []string) *Engine {
	ext := make(map[string]struct{}, len(externalTargets))
	for _, t := range externalTargets {
		ext[t] = struct{}{}
	}
	return &Engine{cfg: cfg, budgets: budgets, externalTargets: ext}
}

// Apply resolves route target, latency class + budget, ACL, and PII decision
// on the request. It mutates req and returns nil on success.
func (e *Engine) Apply(req *model.Request) error {
	// Latency class: explicit per-request hint wins; else default.
	if req.LatencyClass == "" {
		req.LatencyClass = e.cfg.DefaultLatencyClass
	}
	budget, err := e.budgets.For(req.LatencyClass)
	if err != nil {
		return err
	}
	req.Budget = budget

	// Route target: explicit route for the model, else the model name itself.
	// The route target is the key matched against external_targets below —
	// so a model can be flagged external either via a named route
	// (routes: model → label, label in external_targets) or directly
	// (no route entry, model name itself in external_targets).
	if target, ok := e.cfg.Routes[req.Model]; ok {
		req.RouteTarget = target
	} else {
		req.RouteTarget = req.Model
	}

	// ACL: api key → allowed models. Empty list means "all models".
	if allowed, ok := e.cfg.ACL[req.APIKey]; ok && len(allowed) > 0 {
		if !contains(allowed, req.Model) {
			return ErrDenied
		}
	}

	// PII decision: a route target listed in external_targets is an external
	// backend (OpenAI/Anthropic) → redact before forwarding. Anything not
	// listed is treated as a trusted self-hosted backend (vLLM/SGLang) →
	// allow as-is. The default for an unknown target is therefore "trusted",
	// not "redact": redaction is opt-in per target, not the safe default.
	// Operators must explicitly list external targets to opt in.
	if _, ext := e.externalTargets[req.RouteTarget]; ext {
		req.PIIDecision = model.PIIRedact
	} else {
		req.PIIDecision = model.PIIAllow
	}
	return nil
}

// RemainingBudget returns min(perStageCap, remaining). Helpers use it to
// decrement the shared budget token as it flows downstream. A zero or negative
// perStageCap yields zero (no budget), NOT the full remaining budget — this is
// what makes the shared decreasing token actually bind downstream stages.
func RemainingBudget(req *model.Request, perStageCap time.Duration) time.Duration {
	if perStageCap < req.Budget {
		return perStageCap
	}
	return req.Budget
}

// SpendBudget deducts d from the request's remaining budget, floored at 0.
func SpendBudget(req *model.Request, d time.Duration) {
	req.Budget -= d
	if req.Budget < 0 {
		req.Budget = 0
	}
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
