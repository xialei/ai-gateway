package context

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
	"github.com/lei.xia/ai-gateway/pkg/ewma"
	"log/slog"
)

// recordingPlugin records execution order and timing for DAG tests.
type recordingPlugin struct {
	name     string
	produces []string
	consumes []string
	delay    time.Duration
	fail     bool
	execCnt  atomic.Int32
	startT   atomic.Int64 // unix nano of start
}

func (p *recordingPlugin) Name() string       { return p.name }
func (p *recordingPlugin) Produces() []string { return p.produces }
func (p *recordingPlugin) Consumes() []string { return p.consumes }
func (p *recordingPlugin) Execute(ctx context.Context, req *model.Request, art Artifacts, deadline time.Time, remaining time.Duration) (Artifacts, error) {
	p.execCnt.Add(1)
	p.startT.Store(time.Now().UnixNano())
	// Read consumed artifacts so the concurrent-map-safety test exercises the
	// read path while sibling plugins write their outputs.
	for _, c := range p.consumes {
		_ = art[c]
	}
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
		}
	}
	if p.fail {
		return nil, errors.New("planned failure")
	}
	return Artifacts{p.produces[0]: p.name + "-out"}, nil
}

func newReq(budget time.Duration) *model.Request {
	return &model.Request{
		Budget:   budget,
		Messages: []model.Message{{Role: "user", Content: "q"}},
	}
}

func TestDAGIndependentBranchesRunConcurrently(t *testing.T) {
	// Two independent producers (a, b) both feeding consumer c. If a and b
	// ran serially, total would be ~2*delay; concurrency makes it ~delay.
	a := &recordingPlugin{name: "a", produces: []string{"a_out"}, delay: 60 * time.Millisecond}
	b := &recordingPlugin{name: "b", produces: []string{"b_out"}, delay: 60 * time.Millisecond}
	c := &recordingPlugin{name: "c", produces: []string{"c_out"}, consumes: []string{"a_out", "b_out"}}

	e, err := NewEngine([]Plugin{a, b, c}, NewBreakers(NewMemoryBreakerStore(0.5, time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	start := time.Now()
	out, err := e.Run(context.Background(), newReq(2*time.Second))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("independent branches ran serially: elapsed=%v", elapsed)
	}
	if got := out["c_out"]; got != "c-out" {
		t.Errorf("c_out = %v", got)
	}
}

func TestDAGCycleRejected(t *testing.T) {
	// a produces a_out consumes b_out; b produces b_out consumes a_out.
	a := &recordingPlugin{name: "a", produces: []string{"a_out"}, consumes: []string{"b_out"}}
	b := &recordingPlugin{name: "b", produces: []string{"b_out"}, consumes: []string{"a_out"}}
	_, err := NewEngine([]Plugin{a, b}, nil, slog.Default())
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

func TestDAGUnknownConsumerRejected(t *testing.T) {
	p := &recordingPlugin{name: "p", produces: []string{"p_out"}, consumes: []string{"missing"}}
	_, err := NewEngine([]Plugin{p}, nil, slog.Default())
	if err == nil {
		t.Fatal("expected error for unknown consumed artifact")
	}
}

func TestDAGDuplicateProducerRejected(t *testing.T) {
	a := &recordingPlugin{name: "a", produces: []string{"dup"}}
	b := &recordingPlugin{name: "b", produces: []string{"dup"}}
	_, err := NewEngine([]Plugin{a, b}, nil, slog.Default())
	if err == nil {
		t.Fatal("expected error for duplicate produced artifact")
	}
}

func TestFailOpenUsesLastGood(t *testing.T) {
	// First run: a succeeds and caches lastGood. Second run: a fails; engine
	// should still return a's last-good artifact (fail-open).
	a := &recordingPlugin{name: "a", produces: []string{"a_out"}}
	e, err := NewEngine([]Plugin{a}, NewBreakers(NewMemoryBreakerStore(0.9, time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	req1 := newReq(time.Second)
	out1, _ := e.Run(context.Background(), req1)
	if out1["a_out"] != "a-out" {
		t.Fatalf("first run missing a_out: %v", out1)
	}
	// flip a to fail and run again
	a.fail = true
	out2, _ := e.Run(context.Background(), newReq(time.Second))
	if out2["a_out"] != "a-out" {
		t.Errorf("fail-open should keep last-good a_out, got %v", out2["a_out"])
	}
}

func TestBudgetZeroSkipsPlugin(t *testing.T) {
	// With zero budget, plugins are skipped (fail-open) rather than executing.
	a := &recordingPlugin{name: "a", produces: []string{"a_out"}}
	e, err := NewEngine([]Plugin{a}, NewBreakers(NewMemoryBreakerStore(0.5, time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	_, _ = e.Run(context.Background(), newReq(0))
	if a.execCnt.Load() != 0 {
		t.Errorf("zero budget should skip plugin, got %d executions", a.execCnt.Load())
	}
}

func TestBreakerOpensOnError(t *testing.T) {
	store := NewMemoryBreakerStore(0.4, time.Second) // trip at 40% errors
	breakers := NewBreakers(store)
	a := &recordingPlugin{name: "a", produces: []string{"a_out"}, fail: true}
	e, err := NewEngine([]Plugin{a}, breakers, slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// Run several times to push the error-rate EWMA over threshold.
	for i := 0; i < 5; i++ {
		_, _ = e.Run(context.Background(), newReq(time.Second))
	}
	if store.State("a") != BreakerOpen {
		t.Errorf("expected breaker open after repeated failures, got %v", store.State("a"))
	}
}

// TestDAGConcurrentMapSafety stresses the engine with many wide waves of
// plugins that read shared artifacts while siblings write outputs. Before the
// per-wave artifact snapshot, this triggered a fatal "concurrent map read and
// map write" panic. Run with -race to also catch data races on the budget.
func TestDAGConcurrentMapSafety(t *testing.T) {
	// 8 independent producers in one wave, each producing a distinct artifact
	// and reading the seeded last-good map; then a fan-in consumer.
	plugins := []Plugin{}
	for i := 0; i < 8; i++ {
		// each producer reads a pre-existing artifact "seed" (we seed it via a
		// leading producer) to exercise concurrent reads during sibling writes.
		p := &recordingPlugin{
			name:     "p" + itoa(i),
			produces: []string{"p" + itoa(i) + "_out"},
			consumes: []string{"seed"},
			delay:    5 * time.Millisecond,
		}
		plugins = append(plugins, p)
	}
	plugins = append([]Plugin{&recordingPlugin{name: "seed", produces: []string{"seed"}}}, plugins...)

	e, err := NewEngine(plugins, NewBreakers(NewMemoryBreakerStore(0.5, time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// Repeated runs increase the chance of overlapping read/write.
	for run := 0; run < 50; run++ {
		out, err := e.Run(context.Background(), newReq(2*time.Second))
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		for i := 0; i < 8; i++ {
			if got := out["p"+itoa(i)+"_out"]; got != "p"+itoa(i)+"-out" {
				t.Fatalf("run %d: missing p%d_out (got %v)", run, i, got)
			}
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// keep import referenced.
var _ = ewma.New
var _ sync.Mutex

// TestEngineWritesBackRemainingBudget is the regression for the budget-not-
// written-back bug: Run used to decrement a local `remaining` as plugins
// consumed time but never assign it back to req.Budget, so the connector raced
// backend headers against the ORIGINAL full budget (context plugins + backend
// headers could sum to ~2x the configured latency class). After Run,
// req.Budget must reflect the time the context pipeline actually consumed.
func TestEngineWritesBackRemainingBudget(t *testing.T) {
	p := &recordingPlugin{name: "slow", produces: []string{"slow_out"}, delay: 100 * time.Millisecond}
	e, err := NewEngine([]Plugin{p}, NewBreakers(NewMemoryBreakerStore(0.5, 10*time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	req := newReq(500 * time.Millisecond)
	_, _ = e.Run(context.Background(), req)
	// The plugin spent ~100ms; the remaining budget written back must be
	// strictly less than the original 500ms.
	if req.Budget >= 500*time.Millisecond {
		t.Errorf("req.Budget not decremented: got %v (plugin should have consumed ~100ms)", req.Budget)
	}
	if req.Budget < 350*time.Millisecond || req.Budget > 500*time.Millisecond {
		// sanity band: should be roughly 400ms ± timing slop
		t.Logf("note: req.Budget=%v", req.Budget)
	}
}

// TestEnginePluginPanicFailsOpen is the regression for the no-recover crash: a
// panicking plugin used to propagate and kill the gateway process, violating
// the documented fail-open contract. The engine must recover the panic, mark
// the plugin failed, and return (last-good/empty) artifacts without crashing.
func TestEnginePluginPanicFailsOpen(t *testing.T) {
	panicker := &panicPlugin{name: "boom", produces: []string{"boom_out"}}
	e, err := NewEngine([]Plugin{panicker}, NewBreakers(NewMemoryBreakerStore(0.5, 10*time.Second)), slog.Default())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	req := newReq(500 * time.Millisecond)
	// Must not panic (which would fail the test process).
	out, runErr := e.Run(context.Background(), req)
	if runErr != nil {
		t.Fatalf("Run returned error: %v", runErr)
	}
	if _, ok := out["boom_out"]; ok {
		t.Error("panicking plugin should not have produced artifacts")
	}
}

type panicPlugin struct {
	name     string
	produces []string
	consumes []string
}

func (p *panicPlugin) Name() string       { return p.name }
func (p *panicPlugin) Produces() []string { return p.produces }
func (p *panicPlugin) Consumes() []string { return p.consumes }
func (p *panicPlugin) Execute(ctx context.Context, req *model.Request, art Artifacts, deadline time.Time, remaining time.Duration) (Artifacts, error) {
	panic("simulated plugin bug")
}
