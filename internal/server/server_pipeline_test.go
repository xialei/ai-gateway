package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
	"log/slog"
)

// minimalCfg builds a config that runs the in-process mock backend (opt-in via
// scheduler.mock) for end-to-end handler tests with no external services.
func minimalCfg(t *testing.T) *config.Config {
	t.Helper()
	c, err := config.Load("../../config.demo.yaml")
	if err != nil {
		t.Fatalf("load demo config: %v", err)
	}
	return c
}

// TestPIIRedactRequiredFailsClosedWhenRedactorNil is the regression for the
// silent PII leak: when an external target forces PIIDecision=Redact but the
// redactor is nil (PII disabled or pattern compile failed), handleChat must
// refuse the request (503) rather than forward the user's PII to the external
// provider.
func TestPIIRedactRequiredFailsClosedWhenRedactorNil(t *testing.T) {
	cfg := minimalCfg(t)
	// PII disabled → redactor is nil at construction. But policy still marks
	// the demo external target (external-demo) as PIIRedact.
	cfg.PII.Enabled = false
	// Force a model that routes to the external target in demo config.
	cfg.Policy.Routes["demo-model"] = "external-demo"
	cfg.Policy.ExternalTargets = []string{"external-demo"}

	srv := New(cfg, slog.Default())
	srv.Start(context.Background())
	defer srv.closeMock()

	body := strings.NewReader(`{"model":"demo-model","stream":false,"messages":[{"role":"user","content":"hi alice@example.com"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	req.Header.Set("Authorization", "Bearer sk-gateway-demo")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 (redaction required but unavailable), got %d (body=%q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "pii redaction required") {
		t.Errorf("unexpected body: %q", rec.Body.String())
	}
}

// TestMockListenerClosedOnShutdown is the regression for the FD/goroutine
// leak: the mock backend's listener must be stored and closed on shutdown so
// http.Serve returns and its goroutine exits.
func TestMockListenerClosedOnShutdown(t *testing.T) {
	cfg := minimalCfg(t)
	srv := New(cfg, slog.Default())
	srv.Start(context.Background())

	if srv.mockCloser == nil {
		t.Fatal("expected mockCloser to be set when scheduler.mock is true")
	}
	if err := srv.closeMock(); err != nil {
		t.Errorf("closeMock returned error: %v", err)
	}
	// A second close should error (listener already closed) — confirms the
	// first close actually closed it.
	if err := srv.closeMock(); err == nil {
		t.Error("expected error on double-close of mock listener")
	}
}

// keep imports referenced.
var _ = model.LatencyNormal
var _ time.Duration
