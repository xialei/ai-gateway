package connector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

func newTestConnector(t *testing.T) *Connector {
	t.Helper()
	return New(30*time.Second, config.ConnectorConfig{})
}

// TestHeaderBudgetTimesOut proves the per-request budget actually binds
// connect + first-byte: a backend that delays its response headers beyond the
// budget yields ErrHeaderTimeout (a failover-eligible failure), rather than
// the gateway waiting indefinitely. This is the regression for the global
// ResponseHeaderTimeout that did not vary per latency class.
func TestHeaderBudgetTimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay headers beyond the budget.
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestConnector(t)
	req := model.Request{
		ID:      "r1",
		Model:   "m",
		Stream:  true,
		Budget:  50 * time.Millisecond, // strict-class budget
		Messages: []model.Message{{Role: "user", Content: "hi"}},
	}
	_, _, _, err := c.Forward(context.Background(), srv.URL, req)
	if !errors.Is(err, ErrHeaderTimeout) {
		t.Fatalf("expected ErrHeaderTimeout, got %v", err)
	}
}

// TestStreamingTailExemptFromBudget proves the streaming tail is NOT bound by
// the budget: once headers arrive within budget, the body streams to
// completion even if that takes far longer than the budget. This is the
// "connect + first byte governed, streaming tail exempt" contract.
func TestStreamingTailExemptFromBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Headers arrive quickly...
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// ...then the body trickles in slowly, well past the budget.
		for i := 0; i < 3; i++ {
			io.WriteString(w, "chunk ")
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(60 * time.Millisecond)
		}
	}))
	defer srv.Close()

	c := newTestConnector(t)
	req := model.Request{
		ID:       "r2",
		Model:    "m",
		Stream:   true,
		Budget:   30 * time.Millisecond, // tiny budget, headers arrive faster
		Messages: []model.Message{{Role: "user", Content: "hi"}},
	}
	body, _, _, err := c.Forward(context.Background(), srv.URL, req)
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	defer body.Close()
	got, _ := io.ReadAll(body)
	if !strings.Contains(string(got), "chunk chunk chunk") {
		t.Errorf("streaming body truncated by budget; got %q", got)
	}
}

// TestForwardBackendError asserts a non-2xx backend surfaces as a
// *BackendError carrying the status (mapped to 502 by the server).
func TestForwardBackendError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"overloaded"}`)
	}))
	defer srv.Close()

	c := newTestConnector(t)
	req := model.Request{
		ID:       "r3",
		Model:    "m",
		Budget:   5 * time.Second,
		Messages: []model.Message{{Role: "user", Content: "hi"}},
	}
	_, _, _, err := c.Forward(context.Background(), srv.URL, req)
	var be *BackendError
	if !errors.As(err, &be) {
		t.Fatalf("expected *BackendError, got %v", err)
	}
	if be.Status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", be.Status, http.StatusServiceUnavailable)
	}
}
