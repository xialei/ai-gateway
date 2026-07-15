// Package server assembles the request pipeline and runs the HTTP server.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/lei.xia/ai-gateway/internal/access"
	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/connector"
	ctxpipe "github.com/lei.xia/ai-gateway/internal/context"
	"github.com/lei.xia/ai-gateway/internal/eventbus"
	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/internal/normalize"
	"github.com/lei.xia/ai-gateway/internal/pii"
	"github.com/lei.xia/ai-gateway/internal/policy"
	"github.com/lei.xia/ai-gateway/internal/scheduler"
)

// Server wires the core modules and serves inbound requests.
type Server struct {
	cfg        *config.Config
	logger     *slog.Logger
	access     *access.Access
	policy     *policy.Engine
	context    *ctxpipe.Engine
	redactor   *pii.Redactor
	piiStore   pii.MapStore
	piiEnabled bool
	bus        *eventbus.Bus
	connector  *connector.Connector
	registry   *scheduler.Registry
	scheduler  *scheduler.Scheduler
	http       *http.Server
	mockCloser io.Closer
}

// New constructs a Server from config. Modules are injected here as the
// pipeline is built out in later stages.
func New(cfg *config.Config, logger *slog.Logger) *Server {
	accessGate := access.New(cfg.Access)
	// No transport-level ResponseHeaderTimeout: a single global value would cap
	// every latency class at the same ceiling (e.g. a loose 5s request killed at
	// the normal 2s budget). Instead the per-request budget in
	// connector.doWithHeaderBudget governs connect + first-byte per class, with
	// the streaming tail exempt.
	conn := connector.New(cfg.Connector)

	// Membership source: static config. The in-process mock backend is opt-in
	// (cfg.Scheduler.Mock) for demos / local development — never inferred from
	// the absence of instances, since a production deploy can legitimately start
	// with zero instances while service discovery populates.
	var source scheduler.InstanceSource = scheduler.NewStaticSource(cfg.Instances)
	var mockCloser io.Closer
	if cfg.Scheduler.Mock {
		mockSrc := newMockSource(logger)
		source = mockSrc
		mockCloser = mockSrc
	}
	registry := scheduler.NewRegistry(source, cfg.Scheduler, logger)
	sched := scheduler.New(registry, cfg.Scheduler)
	sched.Rebuild()
	pol := policy.New(cfg.Policy, cfg.Latency, cfg.Policy.ExternalTargets)

	var ctxEngine *ctxpipe.Engine
	if cfg.Context.Enabled {
		// No built-in plugins: the engine + Plugin contract are the extension
		// point. Upper-layer plugins are registered here (appended to the
		// slice) when available. With none registered the engine is a no-op.
		breakers := ctxpipe.NewBreakers(ctxpipe.NewMemoryBreakerStore(0.5, 10*time.Second))
		ce, err := ctxpipe.NewEngine(nil, breakers, logger)
		if err != nil {
			logger.Error("context pipeline invalid, disabling", "error", err)
		} else {
			ctxEngine = ce
		}
	}

	var redactor *pii.Redactor
	var piiStore pii.MapStore
	if cfg.PII.Enabled {
		piiStore = pii.NewMemoryMapStore()
		patterns, err := pii.CompilePatterns(toRawPatterns(cfg.PII.Patterns))
		if err != nil {
			logger.Error("pii pattern compile failed, disabling PII", "error", err)
		} else {
			redactor = pii.NewRedactor(patterns, piiStore, cfg.PII.MapTTL)
		}
	}

	var bus *eventbus.Bus
	if cfg.EventBus.Enabled {
		// Default sink is NoOp: the bus stays enabled so the sync path's emit
		// remains non-blocking and pool-safe, but the default demo/mock
		// configuration does not emit four structured-log lines per request
		// (which dominates throughput when there is nothing to observe). An
		// upper layer wires a real Sink (Kafka→ClickHouse) by constructing the
		// bus itself; that path replaces this default.
		bus = eventbus.New(eventbus.NewNoOpSink(), cfg.EventBus.Workers, cfg.EventBus.BufferCap)
	}

	s := &Server{
		cfg:        cfg,
		logger:     logger,
		access:     accessGate,
		policy:     pol,
		context:    ctxEngine,
		redactor:   redactor,
		piiStore:   piiStore,
		piiEnabled: redactor != nil,
		bus:        bus,
		connector:  conn,
		registry:   registry,
		scheduler:  sched,
		mockCloser: mockCloser,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)
	s.http = &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           mux,
		ReadTimeout:       cfg.Server.ReadTimeout,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		WriteTimeout:      0, // streaming responses
	}
	return s
}

// Start begins background registry probing and the event bus.
func (s *Server) Start(ctx context.Context) {
	s.registry.Start(ctx)
	if s.bus != nil {
		s.bus.Start(ctx)
	}
}

// emit enqueues an async event if the bus is configured. fields MUST be
// obtained from s.fieldsN (or nil): once passed here, the bus owns the map
// and recycles it after the worker delivers the event (or on drop). Callers
// must not retain or mutate fields after this call.
func (s *Server) emit(t model.EventType, req model.Request, fields map[string]any) {
	if s.bus == nil {
		// No bus: the fieldsN helpers already returned nil in this case, so
		// there is nothing to release. This short-circuits the entire event
		// path (no AcquireFields, no channel) when the bus is disabled — the
		// common mock/demo configuration.
		return
	}
	s.bus.Emit(model.Event{
		Type:       t,
		RequestID:  req.ID,
		InstanceID: req.InstanceID,
		Model:      req.Model,
		Fields:     fields,
	})
}

// fields1/2/3 acquire a pooled fields map and populate it, but ONLY when the
// bus is enabled. When the bus is nil they return nil, so the hot path pays
// zero pool cost in the common mock/demo configuration. The caller hands the
// returned map to emit and must not touch it afterward.
func (s *Server) fields1(k string, v any) map[string]any {
	if s.bus == nil {
		return nil
	}
	f := eventbus.AcquireFields()
	f[k] = v
	return f
}

func (s *Server) fields2(k1 string, v1 any, k2 string, v2 any) map[string]any {
	if s.bus == nil {
		return nil
	}
	f := eventbus.AcquireFields()
	f[k1] = v1
	f[k2] = v2
	return f
}

func (s *Server) fields3(k1 string, v1 any, k2 string, v2 any, k3 string, v3 any) map[string]any {
	if s.bus == nil {
		return nil
	}
	f := eventbus.AcquireFields()
	f[k1] = v1
	f[k2] = v2
	f[k3] = v3
	return f
}

// streamCopyBuf returns a small reusable buffer for io.CopyBuffer on the
// streaming relay. A small buffer (2 KiB) is intentional: io.Copy's default
// 32 KiB buffer would accumulate SSE chunks until full before flushing, which
// delays the first token and inflates P99. 2 KiB fits a typical SSE chunk
// (one token delta ≈ tens of bytes) so each chunk flushes promptly while
// keeping the per-request allocation off the heap (sync.Pool).
func streamCopyBuf() []byte { return streamBufPool.Get().([]byte) }

var streamBufPool = sync.Pool{
	// A non-zero length is required: io.CopyBuffer panics on an empty buffer.
	// 2 KiB fits a typical SSE chunk (one token delta ≈ tens of bytes) so each
	// chunk flushes promptly; pooled to keep the per-request allocation off the
	// heap.
	New: func() any { return make([]byte, 2048) },
}

// releaseStreamCopyBuf returns a copy buffer to the pool. Callers using
// streamCopyBuf must defer this once io.CopyBuffer returns.
func releaseStreamCopyBuf(b []byte) { streamBufPool.Put(b) }

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/v1/chat/completions", s.handleChat)
}

// handleChat is the main pipeline entry. Stages are added incrementally;
// currently: access → normalize → schedule → forward (failover) → stream.
func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. Access
	key, err := s.access.Authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !s.access.AllowRateLimit(key) {
		writeError(w, http.StatusTooManyRequests, "rate limited")
		return
	}

	// 2. Normalize
	req, err := normalize.ChatRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.APIKey = key
	if req.ID == "" {
		req.ID = newRequestID()
	}

	// 3. Policy Engine: route target, latency budget, ACL, PII decision.
	if err := s.policy.Apply(&req); err != nil {
		writeError(w, http.StatusForbidden, "denied by policy")
		return
	}
	s.emit(model.EventRouteDecision, req, s.fields3(
		"route_target", req.RouteTarget,
		"latency_class", string(req.LatencyClass),
		"budget_ms", req.Budget.Milliseconds(),
	))
	// The budget bounds the Context Pipeline here, AND connect + first-byte in
	// the connector (Forward races the backend's headers against req.Budget).
	// The streaming tail is intentionally exempt: it runs under the client's
	// request context so a client disconnect cancels it but a long generation
	// is not truncated.
	budgetCtx, budgetCancel := context.WithTimeout(ctx, req.Budget)
	defer budgetCancel()

	// 4. Context Pipeline (plugin DAG, fail-open). Enrichment never fails the
	// request; on error it is skipped and last-good artifacts are used.
	if s.context != nil {
		artifacts, _ := s.context.Run(budgetCtx, &req)
		for k, v := range artifacts {
			req.Context[k] = v
		}
	}

	// 5. PII redaction (external backends only, per Policy Engine decision).
	// Detect → Replace (sentinel placeholders, id→entity in MapStore).
	// RedactMessages shares entity→id across the whole request, so the same
	// entity in multiple messages maps to one placeholder and one store entry.
	var piiIDs []string
	if req.PIIDecision == model.PIIRedact {
		// Fail closed: an external target the Policy Engine marked as needing
		// redaction must never be forwarded with PII intact. If the redactor is
		// nil (PII disabled in config, or pattern compilation failed at startup)
		// the safe action is to refuse the request, not to leak it.
		if s.redactor == nil {
			writeError(w, http.StatusServiceUnavailable, "pii redaction required but unavailable")
			return
		}
		piiIDs = s.redactor.RedactMessages(req.Messages)
	}
	// Release id→entity mappings when the request completes.
	if s.redactor != nil {
		defer s.redactor.Release(piiIDs)
	}
	s.emit(model.EventPIIResult, req, s.fields2(
		"decision", string(req.PIIDecision),
		"redactions", len(piiIDs),
	))

	// 6. Prefix Scheduler → pick instance with failover on connect failure.
	// Uses the client request context (not budgetCtx): the stream must
	// outlive the budget, tied only to client presence.
	body, contentType, inst, err := s.forwardWithFailover(ctx, req)
	if err != nil {
		var be *connector.BackendError
		if errors.As(err, &be) {
			writeError(w, http.StatusBadGateway, be.Error())
			return
		}
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer body.Close()
	// The picked instance's inflight counter was incremented by
	// forwardWithFailover; release it once streaming completes so the
	// overload signal (the registry's primary scheduling input) stays accurate.
	defer s.registry.DecInflight(inst.ID)

	// 7. Stream relay to client. If PII was redacted, wrap the body in a
	// restoring reader that reassembles placeholders split across chunks.
	req.InstanceID = inst.ID
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Gateway-Instance", inst.ID)
	flusher, _ := w.(http.Flusher)

	startRelay := time.Now()
	copyBuf := streamCopyBuf()
	defer releaseStreamCopyBuf(copyBuf)
	var reader io.Reader = body
	if req.PIIDecision == model.PIIRedact && s.piiStore != nil {
		restorer := pii.NewRestorer(s.piiStore, s.cfg.PII.BufferCap, s.logger)
		reader = newRestoringReader(body, restorer, s.logger)
		defer func() {
			restored, redacted, overflowed := restorer.Stats()
			s.logger.Info("pii restore stats",
				"request_id", req.ID, "restored", restored, "redacted", redacted, "overflowed", overflowed)
			s.emit(model.EventPIIResult, req, s.fields3(
				"restored", restored,
				"redacted", redacted,
				"overflowed", overflowed,
			))
		}()
	}
	// KV-cache hit feedback (closed loop): tap the relayed bytes to recover the
	// backend's final usage chunk and feed cached_tokens/prompt_tokens back to
	// the registry. This validates prefix affinity over time — a persistently-
	// low hit rate on an affinity target discounts it in scheduling. The tap is
	// zero-stall: it forwards bytes first, then parses what already passed.
	if req.Stream {
		reader = newCacheHitTap(reader, func(frac float64) {
			s.registry.ObserveCacheHit(inst.ID, frac)
		})
	}
	_, _ = io.CopyBuffer(&streamWriter{w: w, f: flusher}, reader, copyBuf)
	if flusher != nil {
		flusher.Flush()
	}
	s.emit(model.EventLatency, req, s.fields1("relay_ms", time.Since(startRelay).Milliseconds()))
	s.emit(model.EventTokenUsage, req, s.fields1("stream", req.Stream))
}

// forwardWithFailover picks a primary instance by prefix affinity and forwards.
// If the backend call fails before any response bytes are produced (connect /
// first-byte), it fails over to the next ring node. Once streaming has begun
// the response is committed and mid-stream failure is not retried (accepting
// the loss of affinity for this request, per the design boundary).
//
// On success the returned instance's inflight counter has been incremented;
// the caller MUST call s.registry.DecInflight(inst.ID) when streaming finishes
// (typically via defer) so the overload signal stays accurate.
func (s *Server) forwardWithFailover(ctx context.Context, req model.Request) (body io.ReadCloser, contentType string, inst model.Instance, err error) {
	tried := make([]string, 0, 3)
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var picked model.Instance
		var perr error
		if attempt == 0 {
			picked, perr = s.scheduler.Pick(req)
		} else {
			picked, perr = s.scheduler.FailoverFrom(req, tried)
		}
		if perr != nil {
			// Scheduler could not place the request (e.g. all candidates
			// exhausted). Return the scheduler error — not a stale backend
			// error from a previous attempt — so the caller maps the right
			// status code (503, not 502).
			if err == nil {
				err = perr
			} else {
				err = fmt.Errorf("failover exhausted: %w (last backend error: %v)", perr, err)
			}
			return nil, "", model.Instance{}, err
		}
		tried = append(tried, picked.ID)
		s.registry.IncInflight(picked.ID)
		start := time.Now()
		body, contentType, _, ferr := s.connector.Forward(ctx, picked.BaseURL, req)
		latency := time.Since(start)
		if ferr != nil {
			s.registry.DecInflight(picked.ID)
			s.registry.ObserveResult(picked.ID, latency, false)
			s.logger.Warn("backend connect failed, failing over",
				"request_id", req.ID, "instance", picked.ID, "error", ferr)
			err = ferr
			continue
		}
		s.registry.ObserveResult(picked.ID, latency, true)
		return body, contentType, picked, nil
	}
	// Rebuild the ring in case membership drifted since the last pick, then
	// surface the last error.
	s.scheduler.Rebuild()
	return nil, "", model.Instance{}, err
}

// Run starts the HTTP server. Blocks until ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	s.Start(ctx)
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("ai-gateway listening", "addr", s.cfg.Server.Addr)
		if s.cfg.Server.TLSCrt != "" && s.cfg.Server.TLSKey != "" {
			errCh <- s.http.ListenAndServeTLS(s.cfg.Server.TLSCrt, s.cfg.Server.TLSKey)
		} else {
			errCh <- s.http.ListenAndServe()
		}
	}()
	select {
	case <-ctx.Done():
		s.registry.Stop()
		if s.bus != nil {
			s.bus.Stop()
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.http.Shutdown(shutCtx)
		if s.bus != nil {
			s.bus.Drain()
		}
		_ = s.closeMock()
		return err
	case err := <-errCh:
		s.registry.Stop()
		if s.bus != nil {
			s.bus.Stop()
			s.bus.Drain()
		}
		_ = s.closeMock()
		return err
	}
}

// closeMock stops the in-process mock backend's listener if one was started,
// reaping its serve goroutine and file descriptor. No-op (returns nil) when no
// mock is in use.
func (s *Server) closeMock() error {
	if s.mockCloser != nil {
		return s.mockCloser.Close()
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Marshal via encoding/json so quotes / backslashes / control chars in msg
	// (which originate from err.Error(), including raw backend response bodies)
	// are escaped. Naive string concatenation would emit malformed JSON and let
	// a backend inject arbitrary fields.
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// streamWriter wraps an http.ResponseWriter + Flusher as an io.Writer for
// streaming relay.
type streamWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func (sw *streamWriter) Write(p []byte) (int, error) {
	n, err := sw.w.Write(p)
	if sw.f != nil {
		sw.f.Flush()
	}
	return n, err
}
