package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/internal/server/mock"
)

// mockSource is an InstanceSource backed by an in-process mock backend. It is
// used when no real instances are configured so the gateway is runnable
// end-to-end and the scheduling path is exercised identically to production.
type mockSource struct {
	inst model.Instance
}

// newMockSource starts one in-process mock backend and returns a source over
// it. The mock listens on an ephemeral loopback port.
func newMockSource(logger *slog.Logger) *mockSource {
	m := mock.New()
	ln := newDisposableListener()
	go func() {
		_ = http.Serve(ln, m.Handler())
	}()
	addr := ln.Addr().String()
	logger.Info("no instances configured; started in-process mock backend", "addr", addr)
	return &mockSource{
		inst: model.Instance{
			ID:      "mock-0",
			BaseURL: "http://" + addr,
			Model:   "mock",
			Weight:  1,
			Healthy: true,
		},
	}
}

func (s *mockSource) List(ctx context.Context) ([]model.Instance, error) {
	return []model.Instance{s.inst}, nil
}
