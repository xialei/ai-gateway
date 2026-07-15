package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/internal/server/mock"
)

// mockSource is an InstanceSource backed by an in-process mock backend. It is
// opt-in (cfg.Scheduler.Mock) for demos / local development; the scheduling
// path is exercised identically to production.
type mockSource struct {
	inst model.Instance
	ln   net.Listener
}

// newMockSource starts one in-process mock backend and returns a source over
// it. The mock listens on an ephemeral loopback port. The returned source owns
// its listener; Close stops the serve goroutine and releases the file
// descriptor.
func newMockSource(logger *slog.Logger) *mockSource {
	m := mock.New()
	ln := newDisposableListener()
	go func() {
		_ = http.Serve(ln, m.Handler())
		// Serve returns only when ln is closed (via Close), so the goroutine
		// always exits on shutdown.
	}()
	addr := ln.Addr().String()
	logger.Info("started in-process mock backend", "addr", addr)
	return &mockSource{
		ln: ln,
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

// Close stops the mock backend's listener, which unblocks the http.Serve
// goroutine and releases the file descriptor.
func (s *mockSource) Close() error { return s.ln.Close() }
