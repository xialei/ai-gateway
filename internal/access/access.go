// Package access handles TLS-adjacent concerns: API-key auth and per-key
// token-bucket rate limiting on the inbound edge.
package access

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
)

// reapInterval bounds how often stale per-key buckets are swept. Sweeping is
// piggybacked on AllowRateLimit calls (no background goroutine), so the
// gateway stays shutdown-safe without a Start/Stop lifecycle here.
const reapInterval = 5 * time.Minute

// staleBucketTTL is how long a bucket may sit unused before it is eligible for
// reaping. A bucket that has gone idle for this long has fully refilled (its
// tokens cap at burst), so dropping and recreating it on the next request is
// equivalent to keeping it — no rate state worth preserving remains.
const staleBucketTTL = 10 * time.Minute

// Access wraps auth and rate-limit checks.
type Access struct {
	keys map[string]struct{}
	rl   *rateLimiter // fallback bucket when no per-key identity is available

	// perKey holds one token bucket per API key so one key cannot starve
	// another. When the allow-list is empty (any key accepted) this map is
	// unbounded in the face of random keys, so stale buckets are reaped
	// lazily (see reap).
	perKeyMu sync.Mutex
	perKey   map[string]*keyBucket

	rate  float64
	burst int

	lastReap time.Time
}

// keyBucket wraps a rateLimiter with a last-used timestamp for reaping.
type keyBucket struct {
	rl      *rateLimiter
	lastUsed time.Time
}

// New builds an Access gate from config.
func New(cfg config.AccessConfig) *Access {
	keys := make(map[string]struct{}, len(cfg.APIKeys))
	for _, k := range cfg.APIKeys {
		keys[k] = struct{}{}
	}
	return &Access{
		keys:   keys,
		rl:     newRateLimiter(cfg.RateLimitPerSecond, cfg.RateLimitBurst),
		perKey: make(map[string]*keyBucket),
		rate:   cfg.RateLimitPerSecond,
		burst:  cfg.RateLimitBurst,
	}
}

// Authenticate extracts a bearer token and checks it against the allow-list.
// Returns the key on success.
func (a *Access) Authenticate(r *http.Request) (string, error) {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		return "", ErrUnauthorized
	}
	key := strings.TrimPrefix(auth, prefix)
	if len(a.keys) > 0 {
		if _, ok := a.keys[key]; !ok {
			return "", ErrUnauthorized
		}
	}
	return key, nil
}

// AllowRateLimit reports whether the caller identified by key is within its
// rate budget. Each key draws from its own token bucket, so one heavy key
// cannot consume the quota of another. When there is no key identity (empty
// key) the shared fallback bucket is used.
func (a *Access) AllowRateLimit(key string) bool {
	if key == "" {
		return a.rl.allow()
	}
	bucket := a.bucketFor(key)
	return bucket.allow()
}

// bucketFor returns the per-key bucket, creating it lazily. It also sweeps
// stale buckets on a coarse cadence so an open allow-list (any key accepted)
// cannot grow the map without bound under a key-spraying client.
func (a *Access) bucketFor(key string) *rateLimiter {
	a.perKeyMu.Lock()
	defer a.perKeyMu.Unlock()

	now := time.Now()
	if now.Sub(a.lastReap) >= reapInterval {
		a.reapLocked(now)
		a.lastReap = now
	}

	kb, ok := a.perKey[key]
	if !ok {
		kb = &keyBucket{rl: newRateLimiter(a.rate, a.burst)}
		a.perKey[key] = kb
	}
	kb.lastUsed = now
	return kb.rl
}

// reapLocked removes buckets unused for longer than staleBucketTTL. A bucket
// idle that long has fully refilled, so dropping it loses no rate state.
// Caller must hold perKeyMu.
func (a *Access) reapLocked(now time.Time) {
	for k, kb := range a.perKey {
		if now.Sub(kb.lastUsed) >= staleBucketTTL {
			delete(a.perKey, k)
		}
	}
}

// errType is a sentinel error type.
type sentinel string

func (s sentinel) Error() string { return string(s) }

const ErrUnauthorized sentinel = "unauthorized"

// rateLimiter is a token bucket. For multi-replica fairness the limiter would
// be backed by a shared store; that is left to an adapter.
type rateLimiter struct {
	mu         sync.Mutex
	rate       float64 // tokens per second
	burst      float64
	tokens     float64
	lastUpdate time.Time
	// unlimited is set when rate <= 0: a zero rate would add elapsed*0 tokens
	// on each call (never refilling), so the bucket would allow exactly one
	// request then block all subsequent traffic. Treating rate<=0 as unlimited
	// avoids bricking the gateway when rate limiting is left unconfigured.
	unlimited bool
}

func newRateLimiter(rate float64, burst int) *rateLimiter {
	if rate <= 0 {
		return &rateLimiter{unlimited: true}
	}
	if burst <= 0 {
		burst = 1
	}
	return &rateLimiter{
		rate:       rate,
		burst:      float64(burst),
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

func (rl *rateLimiter) allow() bool {
	if rl.unlimited {
		return true
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(rl.lastUpdate).Seconds()
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.burst {
		rl.tokens = rl.burst
	}
	rl.lastUpdate = now
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}
