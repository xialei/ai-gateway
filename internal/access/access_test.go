package access

import (
	"net/http"
	"testing"

	"github.com/lei.xia/ai-gateway/internal/config"
)

// makeReq builds a GET with a Bearer Authorization header for auth tests.
func makeReq(key string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer "+key)
	return r
}

// TestPerKeyIsolation proves the core per-key contract: a key that exhausts
// its own bucket must NOT block a different key. This is the regression for
// the global-bucket bug, where one heavy key starved every other key.
func TestPerKeyIsolation(t *testing.T) {
	a := New(config.AccessConfig{
		APIKeys:             []string{"heavy", "light"},
		RateLimitPerSecond:  1,
		RateLimitBurst:      3,
	})
	// Drain the "heavy" key's entire burst.
	for i := 0; i < 3; i++ {
		if !a.AllowRateLimit("heavy") {
			t.Fatalf("heavy should be allowed within burst, denied at %d", i)
		}
	}
	if a.AllowRateLimit("heavy") {
		t.Error("heavy should be rate-limited after exhausting its burst")
	}
	// A different key has its own bucket and must still be allowed.
	if !a.AllowRateLimit("light") {
		t.Error("light must not be starved by heavy's traffic (per-key isolation)")
	}
}

// TestEmptyKeyFallsBackToSharedBucket covers the no-identity path (no key
// extracted): all such callers share one bucket, preserving the legacy
// behavior for the unauthenticated-demo case.
func TestEmptyKeyFallsBackToSharedBucket(t *testing.T) {
	a := New(config.AccessConfig{
		RateLimitPerSecond: 1,
		RateLimitBurst:     2,
	})
	if !a.AllowRateLimit("") {
		t.Fatal("first empty-key call should be allowed")
	}
	if !a.AllowRateLimit("") {
		t.Fatal("second empty-key call should be allowed")
	}
	if a.AllowRateLimit("") {
		t.Error("third empty-key call should be rate-limited by the shared bucket")
	}
}

// TestAuthenticateRejectsUnknownKey guards the allow-list boundary.
func TestAuthenticateRejectsUnknownKey(t *testing.T) {
	a := New(config.AccessConfig{APIKeys: []string{"good"}})
	if _, err := a.Authenticate(makeReq("good")); err != nil {
		t.Errorf("known key rejected: %v", err)
	}
	if _, err := a.Authenticate(makeReq("bad")); err == nil {
		t.Error("unknown key should be rejected")
	}
}

// TestRateLimiterUnlimitedWhenRateZero is the regression for the rate-limiter
// deadlock: a zero rate (config left the field unset) used to make the bucket
// allow exactly one request then 429 every subsequent call forever, because
// elapsed*0 adds no tokens. A zero rate must now mean "unlimited" so an
// unconfigured limiter never bricks traffic.
func TestRateLimiterUnlimitedWhenRateZero(t *testing.T) {
	a := New(config.AccessConfig{APIKeys: []string{"k"}}) // rate + burst both 0
	for i := 0; i < 1000; i++ {
		if !a.AllowRateLimit("k") {
			t.Fatalf("call %d denied under zero-rate (unlimited) limiter", i)
		}
	}
	// Empty-key fallback path must be unlimited too.
	for i := 0; i < 1000; i++ {
		if !a.AllowRateLimit("") {
			t.Fatalf("empty-key call %d denied under zero-rate limiter", i)
		}
	}
}
