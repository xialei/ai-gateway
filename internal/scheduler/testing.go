package scheduler

import (
	"context"
	"testing"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/pkg/ewma"
	"log/slog"
	"time"
)

// fakeSource returns a fixed membership for tests.
type fakeSource struct{ insts []model.Instance }

func (f *fakeSource) List(ctx context.Context) ([]model.Instance, error) {
	out := make([]model.Instance, len(f.insts))
	copy(out, f.insts)
	return out, nil
}

// newTestRegistry builds a Registry over a fake source without starting the
// probe loop, so tests are deterministic.
func newTestRegistry(t *testing.T, ids []string) *Registry {
	t.Helper()
	insts := make([]model.Instance, 0, len(ids))
	for _, id := range ids {
		insts = append(insts, model.Instance{
			ID:      id,
			BaseURL: "http://" + id + ".test",
			Model:   "test-model",
			Weight:  1,
			Healthy: true,
		})
	}
	return NewRegistry(&fakeSource{insts: insts}, testSchedulerCfg(), slog.Default())
}

func testSchedulerCfg() config.SchedulerConfig {
	return config.SchedulerConfig{
		VirtualNodes:             160,
		MinStableWindow:          1 * time.Millisecond,
		LoadAwareCandidates:      3,
		WaitingRequestsThreshold: 8,
	}
}

// setInflightForTest sets an instance's inflight counter directly. Test-only:
// the live path drives inflight via IncInflight/DecInflight, but load-aware
// scheduling tests need to seed a specific concurrency value without issuing
// real requests.
func (r *Registry) setInflightForTest(id string, n int) {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return
	}
	pi.inflight.Store(int64(n))
}

// setMetricsForTest seeds an instance's scraped /metrics gauges. Test-only:
// the live path populates these via scrapeMetrics; tests use this to drive
// load-aware selection deterministically.
func (r *Registry) setMetricsForTest(id string, m model.InstanceMetrics) {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return
	}
	mm := m
	pi.metrics.Store(&mm)
}

// markHealthForTest flips an instance's health the way probeHealth would, and
// is the test hook for the generation/rebuild loop: a health transition bumps
// the registry generation, which the scheduler observes on its next Pick and
// rebuilds the ring from the (now smaller) healthy snapshot.
func (r *Registry) markHealthForTest(id string, healthy bool) {
	r.mu.RLock()
	pi, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
		return
	}
	r.markHealth(pi, healthy, "test")
}

// keep imports referenced even when some test files are pruned.
var _ = ewma.New
