// Package eventbus implements the Async Event Bus. The sync request path only
// emits Events onto a buffered channel; worker goroutines drain them and
// dispatch to sinks (default: structured logging). This keeps audit, metrics,
// tracing, and usage statistics off the hot path.
package eventbus

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// fieldsPool reuses map[string]any for Event.Fields across the emit→drain
// cycle. A hot request emits several events (route_decision, pii_result,
// latency, token_usage), each with a small fields map; pooling avoids
// per-event map allocation.
//
// Safety contract: a pooled map is acquired by the sync path, filled, and
// passed to Emit. From that point the caller MUST NOT touch it again. The
// channel serializes ownership to exactly one worker, which reads it inside
// sink.Emit and then returns it via ReleaseFields. The drop paths inside Emit
// (buffer full / bus closed) also return it. This guarantees no map is ever
// read and written concurrently.
var fieldsPool = sync.Pool{
	New: func() any { return make(map[string]any, 4) },
}

// AcquireFields returns a cleared map[string]any from the pool, ready to fill.
// The caller fills it and passes it to Bus.Emit inside an Event; once passed,
// the caller must not retain or mutate it.
func AcquireFields() map[string]any {
	m := fieldsPool.Get().(map[string]any)
	for k := range m {
		delete(m, k)
	}
	return m
}

// ReleaseFields returns a map to the pool. It is called by the bus after a
// worker has consumed an event (inside Emit) and on the drop paths. Callers
// that acquire via AcquireFields should NOT call this themselves — Emit owns
// the lifecycle once the Event is handed over.
func ReleaseFields(m map[string]any) {
	fieldsPool.Put(m)
}

// Sink consumes one event at a time. A Kafka→ClickHouse adapter would satisfy
// this interface; the default sink writes structured log lines.
type Sink interface {
	Emit(event model.Event) error
}

// LogSink writes events as structured log lines.
type LogSink struct {
	logger *slog.Logger
}

// NewLogSink returns a Sink that writes to logger.
func NewLogSink(logger *slog.Logger) *LogSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSink{logger: logger}
}

func (s *LogSink) Emit(e model.Event) error {
	s.logger.Info("event",
		"type", string(e.Type),
		"request_id", e.RequestID,
		"instance_id", e.InstanceID,
		"model", e.Model,
		"fields", e.Fields)
	return nil
}

// NoOpSink discards every event. It is the cold-path default when no external
// sink (Kafka→ClickHouse adapter) is wired: the bus stays enabled so the sync
// path's emit is still non-blocking and pool-safe, but no log line is written
// per event. In the default demo/mock configuration this keeps structured
// logging from emitting four lines per request, which dominates throughput
// when there is nothing to observe.
type NoOpSink struct{}

// NewNoOpSink returns a Sink that discards events.
func NewNoOpSink() *NoOpSink { return &NoOpSink{} }

func (NoOpSink) Emit(model.Event) error { return nil }

// Bus is the async event bus. Emit is non-blocking (best-effort); events are
// drained by worker goroutines and delivered to the configured Sink.
type Bus struct {
	sink    Sink
	workers int
	ch      chan model.Event
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	started bool
	closed  atomic.Bool
	mu      sync.Mutex
}

// New constructs a Bus with the given worker count and buffer capacity.
// If sink is nil a LogSink over slog.Default is used. Start must be called
// before Emit to launch drain workers.
func New(sink Sink, workers, bufferCap int) *Bus {
	if sink == nil {
		sink = NewLogSink(slog.Default())
	}
	if workers <= 0 {
		workers = 2
	}
	if bufferCap <= 0 {
		bufferCap = 1024
	}
	return &Bus{sink: sink, workers: workers, ch: make(chan model.Event, bufferCap)}
}

// Start launches the drain workers. Safe to call once.
func (b *Bus) Start(ctx context.Context) {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return
	}
	b.started = true
	ctx, b.cancel = context.WithCancel(ctx)
	b.mu.Unlock()

	for i := 0; i < b.workers; i++ {
		b.wg.Add(1)
		go b.run(ctx)
	}
}

func (b *Bus) run(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Context canceled (Stop/Drain): drain any buffered events that
			// were enqueued before the closed flag, then exit.
			for {
				select {
				case ev := <-b.ch:
					_ = b.sink.Emit(ev)
					releaseEventFields(ev)
				default:
					return
				}
			}
		case ev, ok := <-b.ch:
			if !ok {
				return
			}
			_ = b.sink.Emit(ev)
			releaseEventFields(ev)
		}
	}
}

// releaseEventFields returns the event's fields map to the pool. Pooling is
// opt-in: events whose Fields came from AcquireFields are recycled; nil or
// non-pooled maps are left for the GC. The bus treats ALL non-nil Fields as
// pooled per the package contract (callers passing Events to Emit must obtain
// Fields via AcquireFields or leave it nil). This keeps the worker path a
// single branch with no per-event type tag.
func releaseEventFields(ev model.Event) {
	if ev.Fields != nil {
		ReleaseFields(ev.Fields)
	}
}

// Stop signals workers to exit. It does NOT wait for queued events to flush;
// call Drain for that.
func (b *Bus) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
}

// Drain blocks until all queued events are delivered and workers exit. The
// channel is NOT closed (a closed channel would make concurrent Emit panic);
// instead the `closed` flag makes subsequent Emit calls drop silently, and
// workers exit via the canceled context after draining the buffered events.
func (b *Bus) Drain() {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return
	}
	b.started = false
	b.closed.Store(true)
	b.mu.Unlock()
	// Cancel the worker context so workers stop selecting on the channel once
	// they've drained buffered events.
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
}

// Emit enqueues an event. It is non-blocking: if the buffer is full (or the
// bus has been drained) the event is dropped rather than stalling the sync
// path. Dropping protects latency at the cost of audit completeness.
//
// Fields ownership: once an Event is passed to Emit, the bus owns its Fields
// map. Callers MUST NOT retain or mutate Fields after Emit. On delivery the
// worker recycles the map; on drop (buffer full / closed) Emit recycles it
// here. Callers should obtain Fields via AcquireFields (or leave it nil).
//
// Emit is safe to call after Drain: the closed flag short-circuits the send,
// and the channel is never closed, so there is no send-on-closed-channel panic.
func (b *Bus) Emit(ev model.Event) {
	if b.closed.Load() {
		releaseEventFields(ev)
		return
	}
	select {
	case b.ch <- ev:
	default:
		// buffer full → drop to protect the request path. Recycle the fields
		// map so the drop path does not leak pooled maps.
		releaseEventFields(ev)
	}
}
