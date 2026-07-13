package eventbus

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// countingSink records every emitted event. It deep-copies Fields because the
// bus recycles Fields maps back to the pool after Emit returns; without a copy
// the recorded reference would be overwritten by the next AcquireFields.
type countingSink struct {
	mu    sync.Mutex
	count atomic.Int32
	seen  []model.Event
}

func (c *countingSink) Emit(e model.Event) error {
	c.count.Add(1)
	if e.Fields != nil {
		cp := make(map[string]any, len(e.Fields))
		for k, v := range e.Fields {
			cp[k] = v
		}
		e.Fields = cp
	}
	c.mu.Lock()
	c.seen = append(c.seen, e)
	c.mu.Unlock()
	return nil
}

func TestEmitDeliversAsync(t *testing.T) {
	sink := &countingSink{}
	bus := New(sink, 2, 256)
	bus.Start(context.Background())
	defer bus.Stop()

	for i := 0; i < 100; i++ {
		bus.Emit(model.Event{Type: model.EventAudit, RequestID: "r"})
	}
	// async delivery: wait for drain
	bus.Drain()
	if got := sink.count.Load(); got != 100 {
		t.Errorf("expected 100 delivered, got %d", got)
	}
}

func TestEmitNonBlockingUnderOverflow(t *testing.T) {
	sink := &countingSink{}
	// No Start → no workers → buffer fills, Emit must drop not block.
	bus := New(sink, 1, 4)
	// flood well beyond capacity; must return promptly
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			bus.Emit(model.Event{Type: model.EventAudit})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Emit blocked under overflow instead of dropping")
	}
}

func TestLogSinkDoesNotPanic(t *testing.T) {
	bus := New(nil, 1, 4)
	bus.Start(context.Background())
	defer bus.Stop()
	f := AcquireFields()
	f["p99"] = "12ms"
	bus.Emit(model.Event{Type: model.EventLatency, RequestID: "r", Fields: f})
	bus.Drain()
}

// Emit after Drain must not panic on a closed channel. Regression: the prior
// implementation closed the channel in Drain, so a late Emit crashed.
func TestEmitAfterDrainDoesNotPanic(t *testing.T) {
	bus := New(nil, 1, 4)
	bus.Start(context.Background())
	bus.Emit(model.Event{Type: model.EventAudit})
	bus.Drain()
	// These must be no-ops, not panics.
	for i := 0; i < 10; i++ {
		bus.Emit(model.Event{Type: model.EventAudit})
	}
}
