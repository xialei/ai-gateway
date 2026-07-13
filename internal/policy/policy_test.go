package policy

import (
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

func budgets() config.LatencyBudgets {
	return config.LatencyBudgets{Strict: 500 * time.Millisecond, Normal: 2 * time.Second, Loose: 5 * time.Second}
}

func TestApplyDefaultsAndBudget(t *testing.T) {
	e := New(config.PolicyConfig{DefaultLatencyClass: model.LatencyNormal}, budgets(), nil)
	req := &model.Request{Model: "qwen2.5", APIKey: "k"}
	if err := e.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if req.LatencyClass != model.LatencyNormal {
		t.Errorf("latency class = %q", req.LatencyClass)
	}
	if req.Budget != 2*time.Second {
		t.Errorf("budget = %v", req.Budget)
	}
	if req.RouteTarget != "qwen2.5" {
		t.Errorf("route target = %q", req.RouteTarget)
	}
	if req.PIIDecision != model.PIIAllow {
		t.Errorf("pii decision = %q (internal should allow)", req.PIIDecision)
	}
}

func TestApplyExplicitLatencyClass(t *testing.T) {
	e := New(config.PolicyConfig{DefaultLatencyClass: model.LatencyNormal}, budgets(), nil)
	req := &model.Request{Model: "m", APIKey: "k", LatencyClass: model.LatencyStrict}
	_ = e.Apply(req)
	if req.Budget != 500*time.Millisecond {
		t.Errorf("strict budget = %v", req.Budget)
	}
}

func TestRouteTargetFromMap(t *testing.T) {
	e := New(config.PolicyConfig{
		DefaultLatencyClass: model.LatencyNormal,
		Routes:              map[string]string{"gpt-4o-mini": "group-a"},
	}, budgets(), []string{"group-a"})
	req := &model.Request{Model: "gpt-4o-mini", APIKey: "k"}
	_ = e.Apply(req)
	if req.RouteTarget != "group-a" {
		t.Errorf("route target = %q", req.RouteTarget)
	}
	if req.PIIDecision != model.PIIRedact {
		t.Errorf("external target should redact, got %q", req.PIIDecision)
	}
}

// TestExternalTargetViaModelName covers the boundary where no route entry
// exists, so the model name itself is the route target, and that model name is
// listed directly in external_targets. This is the "directly external" path
// (vs. TestRouteTargetFromMap, which routes through a named label).
func TestExternalTargetViaModelName(t *testing.T) {
	e := New(config.PolicyConfig{DefaultLatencyClass: model.LatencyNormal}, budgets(), []string{"gpt-4o"})
	req := &model.Request{Model: "gpt-4o", APIKey: "k"}
	if err := e.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if req.RouteTarget != "gpt-4o" {
		t.Errorf("route target = %q, want model name", req.RouteTarget)
	}
	if req.PIIDecision != model.PIIRedact {
		t.Errorf("model listed directly in external_targets should redact, got %q", req.PIIDecision)
	}
}

// TestUnlistedTargetDefaultsTrusted covers the default-trusted boundary: a
// target not present in external_targets is treated as a self-hosted backend
// and allowed as-is (NOT redacted). Redaction is opt-in per target.
func TestUnlistedTargetDefaultsTrusted(t *testing.T) {
	e := New(config.PolicyConfig{DefaultLatencyClass: model.LatencyNormal}, budgets(), []string{"some-other-target"})
	req := &model.Request{Model: "internal-vllm-model", APIKey: "k"}
	if err := e.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if req.RouteTarget != "internal-vllm-model" {
		t.Errorf("route target = %q", req.RouteTarget)
	}
	if req.PIIDecision != model.PIIAllow {
		t.Errorf("unlisted target should default to allow (trusted), got %q", req.PIIDecision)
	}
}

// TestRouteTargetNotInExternalTargets covers a named route whose label is NOT
// in external_targets: it is trusted even though it has a route entry.
func TestRouteTargetNotInExternalTargets(t *testing.T) {
	e := New(config.PolicyConfig{
		DefaultLatencyClass: model.LatencyNormal,
		Routes:              map[string]string{"qwen2.5-72b": "group-a"},
	}, budgets(), []string{"some-external-target"})
	req := &model.Request{Model: "qwen2.5-72b", APIKey: "k"}
	if err := e.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if req.RouteTarget != "group-a" {
		t.Errorf("route target = %q", req.RouteTarget)
	}
	if req.PIIDecision != model.PIIAllow {
		t.Errorf("named route not in external_targets should allow, got %q", req.PIIDecision)
	}
}

func TestACLDeny(t *testing.T) {
	e := New(config.PolicyConfig{
		DefaultLatencyClass: model.LatencyNormal,
		ACL:                 map[string][]string{"limited": {"allowed-model"}},
	}, budgets(), nil)
	req := &model.Request{Model: "forbidden", APIKey: "limited"}
	if err := e.Apply(req); err != ErrDenied {
		t.Fatalf("expected ErrDenied, got %v", err)
	}
}

func TestACLAllowAllWhenEmpty(t *testing.T) {
	e := New(config.PolicyConfig{
		DefaultLatencyClass: model.LatencyNormal,
		ACL:                 map[string][]string{"any": {}},
	}, budgets(), nil)
	req := &model.Request{Model: "whatever", APIKey: "any"}
	if err := e.Apply(req); err != nil {
		t.Fatalf("empty ACL list should allow all, got %v", err)
	}
}

func TestBudgetHelpers(t *testing.T) {
	req := &model.Request{Budget: 1 * time.Second}
	if got := RemainingBudget(req, 200*time.Millisecond); got != 200*time.Millisecond {
		t.Errorf("RemainingBudget cap = %v", got)
	}
	if got := RemainingBudget(req, 5*time.Second); got != 1*time.Second {
		t.Errorf("RemainingBudget floor = %v", got)
	}
	// A zero per-stage cap must yield zero, not the full budget — otherwise the
	// shared decreasing token does not bind downstream stages.
	if got := RemainingBudget(req, 0); got != 0 {
		t.Errorf("RemainingBudget(0) = %v, want 0", got)
	}
	SpendBudget(req, 700*time.Millisecond)
	if req.Budget != 300*time.Millisecond {
		t.Errorf("after spend budget = %v", req.Budget)
	}
	SpendBudget(req, 1*time.Second)
	if req.Budget != 0 {
		t.Errorf("budget should floor at 0, got %v", req.Budget)
	}
}
