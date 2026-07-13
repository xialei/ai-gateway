package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

func TestRingDistribution(t *testing.T) {
	r := newRing(160)
	r.SetInstances(map[string]int{"a": 1, "b": 1, "c": 1, "d": 1})
	if r.Empty() {
		t.Fatal("ring should not be empty")
	}
	counts := map[string]int{}
	const N = 10000
	for i := 0; i < N; i++ {
		id, ok := r.Get(keyStr(i))
		if !ok {
			t.Fatal("expected a hit")
		}
		counts[id]++
	}
	// With 4 equal-weight instances, each should hold ~25% ±5%.
	for id, c := range counts {
		frac := float64(c) / N
		if frac < 0.20 || frac > 0.30 {
			t.Errorf("instance %s got %.2f (expected ~0.25)", id, frac)
		}
	}
}

func TestRingWeightSkew(t *testing.T) {
	r := newRing(160)
	r.SetInstances(map[string]int{"a": 3, "b": 1})
	counts := map[string]int{}
	const N = 10000
	for i := 0; i < N; i++ {
		id, _ := r.Get(keyStr(i))
		counts[id]++
	}
	aFrac := float64(counts["a"]) / N
	// weight 3:1 → a should get ~75%.
	if aFrac < 0.68 || aFrac > 0.82 {
		t.Errorf("weighted instance a got %.2f (expected ~0.75)", aFrac)
	}
}

func TestRingRemovalMigratesOnlyOwned(t *testing.T) {
	r := newRing(160)
	r.SetInstances(map[string]int{"a": 1, "b": 1, "c": 1})
	owners := make(map[string]string, 1000)
	for i := 0; i < 1000; i++ {
		k := keyStr(i)
		id, _ := r.Get(k)
		owners[k] = id
	}
	// Remove b; keys previously owned by a or c must NOT move.
	r.SetInstances(map[string]int{"a": 1, "c": 1})
	movedAorC := 0
	for k, old := range owners {
		now, _ := r.Get(k)
		if old == "b" {
			continue // expected to migrate
		}
		if now != old {
			movedAorC++
		}
	}
	if movedAorC > 20 { // small jitter from vnode collisions is acceptable
		t.Errorf("removing b migrated %d keys owned by a/c", movedAorC)
	}
}

func TestRingGetNextSkipsExcluded(t *testing.T) {
	r := newRing(160)
	r.SetInstances(map[string]int{"a": 1, "b": 1, "c": 1})
	id, ok := r.GetNext("some-key", map[string]struct{}{})
	if !ok {
		t.Fatal("expected a hit with no excludes")
	}
	if id == "" {
		t.Fatal("expected non-empty id")
	}
	// Exclude every member → no hit.
	_, ok = r.GetNext("some-key", map[string]struct{}{"a": {}, "b": {}, "c": {}})
	if ok {
		t.Fatal("expected no hit when all excluded")
	}
}

func TestPrefixKeyStability(t *testing.T) {
	// Same leading prompt → same affinity key, regardless of trailing turn.
	a := model.Request{Messages: []model.Message{
		{Role: "system", Content: "You are a helpful assistant."},
		{Role: "user", Content: "first question"},
	}}
	b := model.Request{Messages: []model.Message{
		{Role: "system", Content: "You are a helpful assistant."},
		{Role: "user", Content: "second question"},
	}}
	if PrefixKey(a) != PrefixKey(b) {
		t.Error("prefix key should be stable across trailing user turns")
	}
}

func TestPrefixKeyDiffersBySystem(t *testing.T) {
	a := model.Request{Messages: []model.Message{
		{Role: "system", Content: "System A"},
		{Role: "user", Content: "q"},
	}}
	b := model.Request{Messages: []model.Message{
		{Role: "system", Content: "System B"},
		{Role: "user", Content: "q"},
	}}
	if PrefixKey(a) == PrefixKey(b) {
		t.Error("different system prompts should yield different prefix keys")
	}
}

func TestSchedulerPickAndFailover(t *testing.T) {
	reg := newTestRegistry(t, []string{"a", "b", "c"})
	sched := New(reg, testSchedulerCfg())
	sched.Rebuild()

	req := model.Request{Messages: []model.Message{
		{Role: "system", Content: "S"},
		{Role: "user", Content: "q"},
	}}
	primary, err := sched.Pick(req)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	// Same prefix → stable primary.
	again, _ := sched.Pick(req)
	if again.ID != primary.ID {
		t.Errorf("Pick not stable: %s then %s", primary.ID, again.ID)
	}
	// Failover skips the tried instance.
	next, err := sched.FailoverFrom(req, []string{primary.ID})
	if err != nil {
		t.Fatalf("FailoverFrom: %v", err)
	}
	if next.ID == primary.ID {
		t.Error("failover returned the same instance")
	}
}

func TestSchedulerNoInstances(t *testing.T) {
	reg := newTestRegistry(t, nil)
	sched := New(reg, testSchedulerCfg())
	sched.Rebuild()
	_, err := sched.Pick(model.Request{Messages: []model.Message{{Role: "user", Content: "q"}}})
	if err != ErrNoInstance {
		t.Fatalf("expected ErrNoInstance, got %v", err)
	}
}

func TestSchedulerFailoverExcludesAll(t *testing.T) {
	reg := newTestRegistry(t, []string{"a", "b"})
	sched := New(reg, testSchedulerCfg())
	sched.Rebuild()
	req := model.Request{Messages: []model.Message{{Role: "user", Content: "q"}}}
	_, err := sched.FailoverFrom(req, []string{"a", "b"})
	if err != ErrNoInstance {
		t.Fatalf("expected ErrNoInstance when all tried, got %v", err)
	}
}

func TestParseMetrics(t *testing.T) {
	body := `# HELP num_requests_running
num_requests_running 3
num_requests_waiting 1
gpu_cache_usage_perc 0.42
num_preemption 5
`
	m := parseMetrics(body)
	if m.RunningRequests != 3 || m.WaitingRequests != 1 || m.GPUCacheUsage != 0.42 || m.Preemptions != 5 {
		t.Errorf("parsed metrics wrong: %+v", m)
	}
}

// TestSchedulerPickIsLoadAware proves the InstanceMetrics (scraped from
// /metrics) actually drive selection: among the prefix-affinity candidates,
// the scheduler must prefer the one with the lowest load signal, instead of
// blindly returning the pure-affinity owner. This is the inference-aware
// differentiation over a plain L7 gateway.
func TestSchedulerPickIsLoadAware(t *testing.T) {
	reg := newTestRegistry(t, []string{"a", "b", "c"})
	sched := New(reg, config.SchedulerConfig{
		VirtualNodes:             160,
		MinStableWindow:          1 * time.Millisecond,
		LoadAwareCandidates:      3,
		WaitingRequestsThreshold: 8,
	})
	sched.Rebuild()

	req := model.Request{Messages: []model.Message{
		{Role: "system", Content: "S"},
		{Role: "user", Content: "q"},
	}}
	// First pick the natural affinity owner under zero load (all metrics zero
	// → equal scores → first candidate wins).
	owner, err := sched.Pick(req)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	// Pin the owner as the most loaded candidate (high inflight + a saturated
	// waiting queue). The scheduler must then route to a different candidate.
	reg.setInflightForTest(owner.ID, 50)
	reg.setMetricsForTest(owner.ID, model.InstanceMetrics{WaitingRequests: 16, GPUCacheUsage: 0.99})

	picked, err := sched.Pick(req)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if picked.ID == owner.ID {
		t.Errorf("load-aware Pick returned the overloaded affinity owner %s "+
			"instead of a lighter neighbor", owner.ID)
	}
}

// TestSchedulerPickFallsBackToAffinityUnderBalancedLoad asserts the
// affinity-preserving property: when all candidates are equally (un)loaded, the
// pure-affinity owner wins, so KV-cache affinity is not fragmented under
// balanced load. This is the regression guard for the load-aware change.
func TestSchedulerPickFallsBackToAffinityUnderBalancedLoad(t *testing.T) {
	reg := newTestRegistry(t, []string{"a", "b", "c"})
	sched := New(reg, config.SchedulerConfig{
		VirtualNodes:             160,
		MinStableWindow:          1 * time.Millisecond,
		LoadAwareCandidates:      3,
		WaitingRequestsThreshold: 8,
	})
	sched.Rebuild()
	// Give every instance identical load so scores tie.
	for _, id := range []string{"a", "b", "c"} {
		reg.setInflightForTest(id, 5)
		reg.setMetricsForTest(id, model.InstanceMetrics{GPUCacheUsage: 0.3})
	}
	req := model.Request{Messages: []model.Message{
		{Role: "system", Content: "balanced"},
		{Role: "user", Content: "q"},
	}}
	first, _ := sched.Pick(req)
	for i := 0; i < 20; i++ {
		got, _ := sched.Pick(req)
		if got.ID != first.ID {
			t.Fatalf("affinity not preserved under balanced load: first=%s then=%s",
				first.ID, got.ID)
		}
	}
}

// keyStr is a deterministic key generator for distribution tests.
func keyStr(i int) string {
	const digits = "abcdefghijklmnopqrstuvwxyz0123456789"
	var b [6]byte
	for j := 0; j < 6; j++ {
		b[j] = digits[(i>>(j*4))%len(digits)]
	}
	return string(b[:])
}

// TestHashKeyIsDeterministic pins the cross-process stability contract: a
// given key must always hash to the same value. This is what makes prefix
// affinity survive a gateway restart (KV-cache affinity lives on the
// instance that owns the key's ring position). Regression for the
// maphash.MakeSeed bug, where the seed was random per process.
func TestHashKeyIsDeterministic(t *testing.T) {
	// Known SHA-1 (top 8 bytes) outputs for fixed inputs. If these change,
	// every running gateway would re-shard its ring relative to a previous
	// build — a silent affinity regression — so a change here MUST be
	// deliberate.
	cases := map[string]uint64{
		"":           0xda39a3ee5e6b4b0d,
		"a":          0x86f7e437faa5a7fc,
		"system:S\n": 0xa763166584bf3012, // representative prefix key
	}
	for in, want := range cases {
		if got := hashKey(in); got != want {
			t.Errorf("hashKey(%q) = %#x, want %#x (affinity would re-shard)", in, got, want)
		}
	}
}

// TestRingRebuildsOnMembershipChange proves the scheduler's ring converges
// after service discovery changes membership, without an explicit Rebuild
// call: refresh bumps the registry generation, and the next Pick detects the
// stale generation and rebuilds. Regression for the bug where the ring was
// built once at startup and never updated, so a removed instance kept
// receiving affinity until every failover attempt failed.
func TestRingRebuildsOnMembershipChange(t *testing.T) {
	src := &fakeSource{insts: []model.Instance{
		{ID: "a", BaseURL: "http://a.test", Model: "m", Weight: 1, Healthy: true},
		{ID: "b", BaseURL: "http://b.test", Model: "m", Weight: 1, Healthy: true},
	}}
	reg := NewRegistry(src, testSchedulerCfg(), slog.Default())
	sched := New(reg, testSchedulerCfg())
	sched.Rebuild()

	req := model.Request{Messages: []model.Message{
		{Role: "system", Content: "S"},
		{Role: "user", Content: "q"},
	}}
	owner, err := sched.Pick(req)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	// Remove the owner from membership and refresh; the generation must bump.
	keep := make([]model.Instance, 0, 1)
	for _, in := range src.insts {
		if in.ID != owner.ID {
			keep = append(keep, in)
		}
	}
	src.insts = keep
	if err := reg.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	genBefore := reg.Generation()

	// The next Pick must rebuild the ring (stale generation) and therefore
	// never return the removed owner.
	picked, err := sched.Pick(req)
	if err != nil {
		t.Fatalf("Pick after membership change: %v", err)
	}
	if picked.ID == owner.ID {
		t.Errorf("ring not rebuilt: removed instance %s still picked", owner.ID)
	}
	if reg.Generation() != genBefore {
		t.Errorf("generation changed during Pick without a membership event")
	}
}
