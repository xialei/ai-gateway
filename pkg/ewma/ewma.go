// Package ewma implements an exponentially-weighted moving average for
// latency and error-rate signals feeding the circuit breaker.
package ewma

import (
	"math"
	"sync"
	"time"
)

// EWMA tracks a moving average with a configurable decay half-life.
type EWMA struct {
	mu          sync.Mutex
	alpha       float64 // decay per update
	value       float64
	lastUpdate  time.Time
	initialized bool
}

// New returns an EWMA where alpha controls decay (higher = more recent-heavy).
// A common choice derived from half-life t½ on a sample period dt is
// alpha = 1 - exp(-ln2 * dt / t½).
func New(alpha float64) *EWMA {
	if alpha <= 0 {
		alpha = 0.3
	}
	if alpha > 1 {
		alpha = 1
	}
	return &EWMA{alpha: alpha}
}

// Update incorporates a new sample. dt since the last update is ignored
// (samples are assumed roughly periodic); for irregular spacing use
// UpdateWithTime.
func (e *EWMA) Update(sample float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.initialized {
		e.value = sample
		e.initialized = true
		e.lastUpdate = time.Now()
		return
	}
	e.value = e.alpha*sample + (1-e.alpha)*e.value
	e.lastUpdate = time.Now()
}

// UpdateWithTime applies decay proportional to elapsed time since the last
// update, suitable for irregular arrival rates.
func (e *EWMA) UpdateWithTime(sample float64, now time.Time, halfLife time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.initialized {
		e.value = sample
		e.initialized = true
		e.lastUpdate = now
		return
	}
	dt := now.Sub(e.lastUpdate).Seconds()
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-math.Ln2*dt/(halfLife.Seconds()))
	e.value = alpha*sample + (1-alpha)*e.value
	e.lastUpdate = now
}

// Value returns the current average.
func (e *EWMA) Value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.value
}

// Reset clears the average.
func (e *EWMA) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = 0
	e.initialized = false
}
