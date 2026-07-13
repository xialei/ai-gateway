package scheduler

import (
	"errors"
	"sync"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

// ErrNoInstance is returned when no healthy instance can serve a request.
var ErrNoInstance = errors.New("no healthy instance available")

// Scheduler picks an instance for a request using prefix-aware consistent
// hashing, with failover to the next ring node on failure.
type Scheduler struct {
	registry *Registry
	ring     *ring

	// loadAwareCandidates is how many prefix-affinity neighbors to consider
	// before picking. 1 reduces to pure prefix affinity.
	loadAwareCandidates int
	// waitingThreshold deprioritizes candidates whose num_requests_waiting is
	// at or above this value (their KV cache is under eviction pressure).
	waitingThreshold int

	mu          sync.RWMutex
	lastRebuild time.Time
	// lastGen is the registry generation the ring was last built from. Pick
	// compares it to the registry's current generation and rebuilds when they
	// diverge, so probe-driven health/membership changes converge into the
	// ring without rebuilding on every request.
	lastGen int64
}

// New constructs a Scheduler over a Registry.
func New(reg *Registry, cfg config.SchedulerConfig) *Scheduler {
	return &Scheduler{
		registry:            reg,
		ring:                newRing(cfg.VirtualNodes),
		loadAwareCandidates: cfg.LoadAwareCandidates,
		waitingThreshold:    cfg.WaitingRequestsThreshold,
	}
}

// Rebuild refreshes the ring from the registry snapshot. Idempotent and cheap;
// the registry's thrash suppression keeps membership churn from destroying
// affinity. Callers should invoke this after registry updates.
func (s *Scheduler) Rebuild() {
	snap := s.registry.Snapshot()
	weights := make(map[string]int, len(snap))
	for _, inst := range snap {
		weights[inst.ID] = inst.Weight
	}
	s.ring.SetInstances(weights)
	s.mu.Lock()
	s.lastRebuild = time.Now()
	s.lastGen = s.registry.Generation()
	s.mu.Unlock()
}

// Pick selects the primary instance for req by prefix affinity. It returns
// the instance and an exclude set pre-seeded with broken instances so that
// failover skips them.
//
// Load-aware selection: among the first LoadAwareCandidates prefix-affinity
// neighbors (the instances whose KV cache most likely holds this prompt's
// prefix), pick the one with the best live load signal — lowest inflight, and
// deprioritize any whose waiting-queue is at/above threshold. The first
// candidate (the pure affinity owner) wins ties, so when all candidates are
// equally loaded the request lands exactly where pure affinity would send it:
// no affinity regression under balanced load. With LoadAwareCandidates == 1
// this reduces to pure prefix affinity (the legacy behavior).
func (s *Scheduler) Pick(req model.Request) (model.Instance, error) {
	if s.ring.Empty() || s.stale() {
		s.Rebuild()
	}
	if s.ring.Empty() {
		return model.Instance{}, ErrNoInstance
	}
	key := PrefixKey(req)
	exclude := s.brokenSet()
	candIDs := s.ring.GetCandidates(key, exclude, s.candidateCount())
	if len(candIDs) == 0 {
		// all instances broken → fall back to the pure affinity target and
		// let the connector attempt (better than failing outright)
		id, ok := s.ring.Get(key)
		if !ok {
			return model.Instance{}, ErrNoInstance
		}
		inst, ok := s.registry.Get(id)
		if !ok {
			return model.Instance{}, ErrNoInstance
		}
		return inst, nil
	}
	cands := s.registry.CandidateMetrics(candIDs)
	if len(cands) == 0 {
		return model.Instance{}, ErrNoInstance
	}
	best := s.bestCandidate(cands)
	return best, nil
}

// candidateCount returns the configured candidate window, floored at 1.
func (s *Scheduler) candidateCount() int {
	if s.loadAwareCandidates < 1 {
		return 1
	}
	return s.loadAwareCandidates
}

// stale reports whether the registry's membership/health generation has moved
// past the one this ring was built from. Pick uses it to rebuild promptly when
// probing flips an instance (un)healthy or service discovery changes
// membership, without paying a rebuild on every request.
func (s *Scheduler) stale() bool {
	s.mu.RLock()
	last := s.lastGen
	s.mu.RUnlock()
	return s.registry.Generation() != last
}

// bestCandidate scores candidates in ring order and returns the least-loaded
// one. The first candidate wins all ties, so balanced load preserves pure
// affinity.
func (s *Scheduler) bestCandidate(cands []model.Instance) model.Instance {
	best := cands[0]
	bestScore := s.score(best)
	for _, c := range cands[1:] {
		sc := s.score(c)
		if sc < bestScore {
			best, bestScore = c, sc
		}
	}
	return best
}

// score ranks an instance by load; lower is better. It combines:
//   - waiting queue pressure: a candidate at/above the threshold is penalized
//     heavily (its KV cache is under eviction pressure, so the affinity win is
//     no longer worth the queue latency);
//   - inflight concurrency: the primary overload signal, most fresh;
//   - GPU cache usage: a fuller cache means less headroom for a new prefix;
//   - observed KV hit rate: a persistently-low hit rate discounts the affinity
//     assumption for this instance (the closed-loop feedback from ObserveCacheHit).
//
// The waiting penalty is an additive constant larger than realistic inflight
// sums, so a queued candidate only loses to another queued candidate or to a
// wildly overloaded non-queued one — never to a lightly loaded one. The hit-rate
// term is a small bucket (≤100) so it only breaks ties, never overrides inflight.
func (s *Scheduler) score(c model.Instance) int {
	score := c.InFlight
	if s.waitingThreshold > 0 && c.Metrics.WaitingRequests >= s.waitingThreshold {
		score += 1 << 20 // dominate inflight/cache terms
	}
	// Fold GPU cache usage (0.0–1.0) into a small integer bucket so it only
	// breaks ties between equally-inflight candidates, never overrides inflight.
	score += int(c.Metrics.GPUCacheUsage * 100)
	// Discount a persistently-low KV hit rate: a candidate whose observed hit
	// fraction is low is less likely to actually serve this prefix from cache,
	// so the affinity assumption is worth less. (1 - hitRate) * 100 keeps the
	// term in the same tie-breaking bucket as GPU cache usage.
	score += int((1 - c.CacheHitRate) * 100)
	return score
}

// FailoverFrom returns the next instance after each id in tried, excluding
// broken instances. Used when a picked instance fails mid-request.
func (s *Scheduler) FailoverFrom(req model.Request, tried []string) (model.Instance, error) {
	key := PrefixKey(req)
	exclude := s.brokenSet()
	for _, t := range tried {
		exclude[t] = struct{}{}
	}
	id, ok := s.ring.GetNext(key, exclude)
	if !ok {
		return model.Instance{}, ErrNoInstance
	}
	inst, ok := s.registry.Get(id)
	if !ok {
		return model.Instance{}, ErrNoInstance
	}
	return inst, nil
}

// brokenSet returns the set of currently-broken instance ids.
func (s *Scheduler) brokenSet() map[string]struct{} {
	snap := s.registry.Snapshot()
	out := make(map[string]struct{}, len(snap))
	for _, inst := range snap {
		if s.registry.IsBroken(inst.ID) {
			out[inst.ID] = struct{}{}
		}
	}
	return out
}
