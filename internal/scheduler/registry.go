package scheduler

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/pkg/ewma"
)

// InstanceSource provides the membership view. Real deployments back this with
// K8s Endpoints / Consul / etcd / Nacos watches; the default implementation is
// a static config that never changes.
type InstanceSource interface {
	// List returns the current membership snapshot.
	List(ctx context.Context) ([]model.Instance, error)
}

// StaticSource serves a fixed instance list from config. It is the cold-start
// fallback when no service-discovery adapter is wired.
type StaticSource struct {
	instances []model.Instance
}

// NewStaticSource builds a StaticSource from config.
func NewStaticSource(cfgs []config.InstanceConfig) *StaticSource {
	out := make([]model.Instance, 0, len(cfgs))
	for _, c := range cfgs {
		w := c.Weight
		if w <= 0 {
			w = 1
		}
		out = append(out, model.Instance{
			ID:      c.ID,
			BaseURL: c.BaseURL,
			Model:   c.Model,
			Weight:  w,
			Healthy: true,
		})
	}
	return &StaticSource{instances: out}
}

func (s *StaticSource) List(ctx context.Context) ([]model.Instance, error) {
	out := make([]model.Instance, len(s.instances))
	copy(out, s.instances)
	return out, nil
}

// perInstance tracks live health/load signals for one instance.
type perInstance struct {
	inst        model.Instance
	latency     *ewma.EWMA
	errorRate   *ewma.EWMA
	broken      atomic.Bool  // circuit breaker: open when true
	brokenUntil atomic.Int64 // unix nano until which the breaker is open
	lastChange  time.Time    // for thrash suppression
	inflight    atomic.Int64
	metrics     atomic.Pointer[model.InstanceMetrics]
	// cacheHitRate is the EWMA of the per-request KV-cache hit fraction
	// (cached_tokens / prompt_tokens), fed back from the backend's final
	// usage chunk. It closes the affinity loop: the scheduler assumes same
	// prefix → same instance → cache hit, and this signal confirms or refutes
	// that assumption per instance over time.
	cacheHitRate *ewma.EWMA
}

// Registry tracks membership + health/load and is the single source of truth
// the scheduler reads. It owns the live ring view.
type Registry struct {
	source InstanceSource
	client *http.Client
	logger *slog.Logger
	cfg    config.SchedulerConfig

	mu    sync.RWMutex
	byID  map[string]*perInstance
	order []string // stable ordering for snapshot reads

	// generation is bumped on every membership or health transition so the
	// scheduler can detect that its ring is stale and rebuild. Reads are
	// lock-free (the scheduler polls this on the Pick path).
	generation atomic.Int64

	probeCancel context.CancelFunc
}

// NewRegistry constructs a Registry. Call Start to begin probing.
func NewRegistry(source InstanceSource, cfg config.SchedulerConfig, logger *slog.Logger) *Registry {
	r := &Registry{
		source: source,
		client: &http.Client{Timeout: 2 * time.Second},
		logger: logger,
		cfg:    cfg,
		byID:   make(map[string]*perInstance),
	}
	_ = r.refresh(context.Background()) // seed membership synchronously
	return r
}

// Generation returns a counter that changes whenever membership or any
// instance's health transitions. The scheduler compares it to the value it
// last rebuilt from to decide whether its ring is stale — avoiding the cost
// of rebuilding every Pick while still converging promptly when probing
// flips an instance unhealthy.
func (r *Registry) Generation() int64 {
	return r.generation.Load()
}

// Start launches background probing until ctx is canceled.
func (r *Registry) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r.probeCancel = cancel
	go r.probeLoop(ctx, r.cfg.HealthProbeEvery, r.probeHealth)
	go r.probeLoop(ctx, r.cfg.MetricsScrapeEvery, r.scrapeMetrics)
}

// Stop halts background probing.
func (r *Registry) Stop() {
	if r.probeCancel != nil {
		r.probeCancel()
	}
}

// Snapshot returns the current live instances (healthy, on the ring).
func (r *Registry) Snapshot() []model.Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]model.Instance, 0, len(r.order))
	for _, id := range r.order {
		pi := r.byID[id]
		if !pi.inst.Healthy {
			continue
		}
		inst := pi.inst
		inst.InFlight = int(pi.inflight.Load())
		inst.CacheHitRate = pi.cacheHitRate.Value()
		if mp := pi.metrics.Load(); mp != nil {
			inst.Metrics = *mp
		}
		out = append(out, inst)
	}
	return out
}

// Get returns the per-instance handle for id (ok=false if unknown).
func (r *Registry) Get(id string) (model.Instance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pi, ok := r.byID[id]
	if !ok || !pi.inst.Healthy {
		return model.Instance{}, false
	}
	inst := pi.inst
	inst.InFlight = int(pi.inflight.Load())
	inst.CacheHitRate = pi.cacheHitRate.Value()
	if mp := pi.metrics.Load(); mp != nil {
		inst.Metrics = *mp
	}
	return inst, true
}

// CandidateMetrics returns the live load signals for each id in ids, skipping
// unknown or unhealthy members. It takes the read lock once (not per id) so
// the load-aware scheduler can score its affinity candidates without
// contending the lock repeatedly on the hot path.
//
// The returned Instance values carry InFlight and Metrics (the GPU/cache/wait
// gauges scraped from /metrics); the scheduler uses these to pick, among the
// prefix-affinity neighbors, the instance least likely to queue or evict.
func (r *Registry) CandidateMetrics(ids []string) []model.Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]model.Instance, 0, len(ids))
	for _, id := range ids {
		pi, ok := r.byID[id]
		if !ok || !pi.inst.Healthy {
			continue
		}
		inst := pi.inst
		inst.InFlight = int(pi.inflight.Load())
		inst.CacheHitRate = pi.cacheHitRate.Value()
		if mp := pi.metrics.Load(); mp != nil {
			inst.Metrics = *mp
		}
		out = append(out, inst)
	}
	return out
}

// IncInflight / DecInflight track per-instance concurrency, the primary
// overload signal (zero-cost, most fresh).
func (r *Registry) IncInflight(id string) {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if ok {
		pi.inflight.Add(1)
	}
}

func (r *Registry) DecInflight(id string) {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if ok {
		pi.inflight.Add(-1)
	}
}

// ObserveResult feeds passive latency/error signals into an instance's
// breaker EWMA. ok=false marks the call a failure.
func (r *Registry) ObserveResult(id string, latency time.Duration, ok bool) {
	r.mu.RLock()
	pi, ok2 := r.byID[id]
	r.mu.RUnlock()
	if !ok2 {
		return
	}
	pi.latency.Update(latency.Seconds())
	failVal := 0.0
	if !ok {
		failVal = 1.0
	}
	pi.errorRate.Update(failVal)
	r.maybeOpenBreaker(pi)
}

// ObserveCacheHit feeds the per-request KV-cache hit fraction back into an
// instance's cacheHitRate EWMA. hitFraction is cached_tokens/prompt_tokens in
// [0,1]. This is the closed-loop signal that validates prefix affinity: a
// persistently-low hit rate on an instance that affinity keeps targeting means
// the prefix assumption is wrong for that workload, and the scheduler can
// discount it (see scheduler.score's cacheHitRate term).
//
// Called from the relay path after the backend's final usage chunk is parsed;
// best-effort and never blocks the stream.
func (r *Registry) ObserveCacheHit(id string, hitFraction float64) {
	if hitFraction < 0 {
		hitFraction = 0
	} else if hitFraction > 1 {
		hitFraction = 1
	}
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return
	}
	pi.cacheHitRate.Update(hitFraction)
}

// CacheHitRate returns the instance's smoothed KV-cache hit fraction in
// [0,1], or 0 if unknown. Used by the load-aware scheduler to discount
// affinity targets that rarely actually hit.
func (r *Registry) CacheHitRate(id string) float64 {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return 0
	}
	return pi.cacheHitRate.Value()
}

// IsBroken reports whether the instance's breaker is currently open.
func (r *Registry) IsBroken(id string) bool {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return true
	}
	if pi.broken.Load() {
		if time.Now().UnixNano() < pi.brokenUntil.Load() {
			return true
		}
		// open window elapsed → half-open: allow a probe through.
		pi.broken.Store(false)
	}
	return false
}

// maybeOpenBreaker trips the breaker when recent error rate exceeds the
// configured threshold. The breaker is independent of the liveness flag: a
// tripped breaker excludes the instance from scheduling (via brokenSet)
// without flipping Healthy, so the passive error signal and the active
// /health probe do not fight over membership state. Health transitions remain
// owned by probeHealth.
func (r *Registry) maybeOpenBreaker(pi *perInstance) {
	threshold := r.cfg.BreakerErrorThreshold
	if threshold <= 0 {
		threshold = 0.5
	}
	openFor := r.cfg.BreakerOpenFor
	if openFor <= 0 {
		openFor = 10 * time.Second
	}
	if pi.errorRate.Value() >= threshold {
		if !pi.broken.Load() {
			pi.broken.Store(true)
			pi.brokenUntil.Store(time.Now().Add(openFor).UnixNano())
			r.logger.Warn("instance breaker opened", "id", pi.inst.ID, "error_rate", pi.errorRate.Value())
		}
	}
}

func (r *Registry) markHealth(pi *perInstance, healthy bool, reason string) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if pi.inst.Healthy == healthy {
		pi.lastChange = now
		return
	}
	// thrash suppression: require min stable window before flipping state
	if now.Sub(pi.lastChange) < r.cfg.MinStableWindow {
		return
	}
	pi.inst.Healthy = healthy
	pi.lastChange = now
	r.generation.Add(1)
	id := pi.inst.ID
	if !healthy {
		r.logger.Warn("instance marked unhealthy", "id", id, "reason", reason)
	} else {
		r.logger.Info("instance marked healthy", "id", id, "reason", reason)
	}
}

// refresh pulls membership from the source and reconciles the local view.
func (r *Registry) refresh(ctx context.Context) error {
	insts, err := r.source.List(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[string]struct{}, len(insts))
	newOrder := make([]string, 0, len(insts))
	membershipChanged := false
	for _, inst := range insts {
		seen[inst.ID] = struct{}{}
		if pi, ok := r.byID[inst.ID]; ok {
			// preserve live health/load; update static fields only
			pi.inst.BaseURL = inst.BaseURL
			pi.inst.Model = inst.Model
			pi.inst.Weight = inst.Weight
			if !pi.inst.Healthy {
				// a freshly announced instance defaults to healthy unless
				// already known-unhealthy
			}
		} else {
			r.byID[inst.ID] = newPerInstance(inst)
			membershipChanged = true
		}
		newOrder = append(newOrder, inst.ID)
	}
	// drop removed members
	for id := range r.byID {
		if _, ok := seen[id]; !ok {
			delete(r.byID, id)
			membershipChanged = true
		}
	}
	r.order = newOrder
	if membershipChanged {
		r.generation.Add(1)
	}
	return nil
}

func newPerInstance(inst model.Instance) *perInstance {
	pi := &perInstance{
		inst:         inst,
		latency:      ewma.New(0.3),
		errorRate:    ewma.New(0.3),
		cacheHitRate: ewma.New(0.2),
	}
	pi.inst.Healthy = true
	pi.lastChange = time.Now()
	return pi
}

func (r *Registry) probeLoop(ctx context.Context, every time.Duration, fn func(context.Context)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// probeHealth hits /health for liveness.
func (r *Registry) probeHealth(ctx context.Context) {
	pis := r.snapshotPointers()
	for _, pi := range pis {
		base := pi.inst.BaseURL
		go func(pi *perInstance, base string) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/health", nil)
			resp, err := r.client.Do(req)
			if err != nil {
				r.markHealth(pi, false, "probe-error")
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode < 500 {
				r.markHealth(pi, true, "probe-ok")
			} else {
				r.markHealth(pi, false, "probe-status")
			}
		}(pi, base)
	}
}

// snapshotPointers returns a stable slice of per-instance handles under the
// read lock, so background probes do not access the byID map without
// synchronization (which would race with a future service-discovery refresh).
func (r *Registry) snapshotPointers() []*perInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*perInstance, 0, len(r.order))
	for _, id := range r.order {
		if pi, ok := r.byID[id]; ok {
			out = append(out, pi)
		}
	}
	return out
}

// scrapeMetrics pulls /metrics and extracts the vLLM/SGLang gauges used for
// affinity and overload judgment.
func (r *Registry) scrapeMetrics(ctx context.Context) {
	pis := r.snapshotPointers()
	for _, pi := range pis {
		base := pi.inst.BaseURL
		go func(pi *perInstance, base string) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/metrics", nil)
			resp, err := r.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			m := parseMetrics(string(body))
			pi.metrics.Store(&m)
		}(pi, base)
	}
}

var (
	reRunning    = regexp.MustCompile(`num_requests_running\s+(\d+)`)
	reWaiting    = regexp.MustCompile(`num_requests_waiting\s+(\d+)`)
	reCacheUsage = regexp.MustCompile(`gpu_cache_usage_perc\s+([0-9.]+)`)
	rePreempt    = regexp.MustCompile(`num_preemption\s+(\d+)`)
)

func parseMetrics(body string) model.InstanceMetrics {
	var m model.InstanceMetrics
	if mm := reRunning.FindStringSubmatch(body); len(mm) == 2 {
		m.RunningRequests, _ = strconv.Atoi(mm[1])
	}
	if mm := reWaiting.FindStringSubmatch(body); len(mm) == 2 {
		m.WaitingRequests, _ = strconv.Atoi(mm[1])
	}
	if mm := reCacheUsage.FindStringSubmatch(body); len(mm) == 2 {
		m.GPUCacheUsage, _ = strconv.ParseFloat(mm[1], 64)
	}
	if mm := rePreempt.FindStringSubmatch(body); len(mm) == 2 {
		n, _ := strconv.ParseInt(mm[1], 10, 64)
		m.Preemptions = n
	}
	return m
}
