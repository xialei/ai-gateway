package context

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lei.xia/ai-gateway/pkg/ewma"
)

// BreakerState is the circuit-breaker status for one plugin.
type BreakerState int

const (
	BreakerClosed   BreakerState = iota // healthy, traffic flows
	BreakerOpen                         // tripped, short-circuit to fallback
	BreakerHalfOpen                     // cooling, allow one probe
)

// BreakerStore persists breaker state so it is shared across gateway replicas.
// The default in-memory implementation lives in this package; a Redis adapter
// would satisfy this interface for multi-replica consistency.
type BreakerStore interface {
	// Record observes a plugin outcome and returns the resulting state.
	Record(pluginID string, latency time.Duration, ok bool) BreakerState
	// State returns the current state for a plugin.
	State(pluginID string) BreakerState
}

// Breakers is a per-plugin EWMA breaker set backed by a BreakerStore. It
// implements BreakerReader for plugin injection.
type Breakers struct {
	store BreakerStore
}

// NewBreakers wraps a store. If store is nil a noopBreakerStore is used
// (plugins never see an open state).
func NewBreakers(store BreakerStore) *Breakers {
	if store == nil {
		store = noopBreakerStore{}
	}
	return &Breakers{store: store}
}

func (b *Breakers) IsOpen(pluginID string) bool {
	return b.store.State(pluginID) == BreakerOpen
}

// Record reports a plugin outcome to the shared store.
func (b *Breakers) Record(pluginID string, latency time.Duration, ok bool) {
	b.store.Record(pluginID, latency, ok)
}

// noopBreakerStore never trips; for disabled-breaker or test use.
type noopBreakerStore struct{}

func (noopBreakerStore) Record(string, time.Duration, bool) BreakerState { return BreakerClosed }
func (noopBreakerStore) State(string) BreakerState                       { return BreakerClosed }

// memoryBreakerStore is the default in-process BreakerStore.
type memoryBreakerStore struct {
	mu      sync.Mutex
	plugins map[string]*memBreaker
	errThr  float64
	openFor time.Duration
}

type memBreaker struct {
	errRate   *ewma.EWMA
	state     atomic.Int32 // BreakerState
	openUntil atomic.Int64 // unix nano
}

// NewMemoryBreakerStore returns an in-process store. errThr is the recent
// error-rate fraction that trips the breaker; openFor is how long it stays
// open before half-open.
func NewMemoryBreakerStore(errThr float64, openFor time.Duration) BreakerStore {
	if errThr <= 0 {
		errThr = 0.5
	}
	if openFor <= 0 {
		openFor = 10 * time.Second
	}
	return &memoryBreakerStore{
		plugins: make(map[string]*memBreaker),
		errThr:  errThr,
		openFor: openFor,
	}
}

func (m *memoryBreakerStore) Record(pluginID string, latency time.Duration, ok bool) BreakerState {
	m.mu.Lock()
	b, exists := m.plugins[pluginID]
	if !exists {
		b = &memBreaker{errRate: ewma.New(0.3)}
		m.plugins[pluginID] = b
	}
	m.mu.Unlock()

	failVal := 0.0
	if !ok {
		failVal = 1.0
	}
	b.errRate.Update(failVal)

	// Transition to open if the error rate crosses threshold.
	if b.errRate.Value() >= m.errThr {
		b.state.Store(int32(BreakerOpen))
		b.openUntil.Store(time.Now().Add(m.openFor).UnixNano())
		return BreakerOpen
	}
	// Healthy observation closes a half-open breaker.
	b.state.Store(int32(BreakerClosed))
	return BreakerClosed
}

func (m *memoryBreakerStore) State(pluginID string) BreakerState {
	m.mu.Lock()
	b, exists := m.plugins[pluginID]
	m.mu.Unlock()
	if !exists {
		return BreakerClosed
	}
	if b.state.Load() == int32(BreakerOpen) {
		if time.Now().UnixNano() < b.openUntil.Load() {
			return BreakerOpen
		}
		// open window elapsed → half-open: allow a probe.
		b.state.Store(int32(BreakerHalfOpen))
		return BreakerHalfOpen
	}
	return BreakerState(b.state.Load())
}
