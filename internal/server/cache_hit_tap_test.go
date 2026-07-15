package server

import (
	"io"
	"strings"
	"testing"
)

// TestCacheHitTapExtractsUsage proves the tap recovers the KV-cache hit
// fraction from a realistic OpenAI-shaped SSE stream split across Read calls,
// without buffering or delaying the bytes forwarded to the caller.
func TestCacheHitTapExtractsUsage(t *testing.T) {
	// A minimal stream: one content chunk, then the final chunk carrying usage
	// with cached_tokens, then [DONE].
	stream := strings.Join([]string{
		`data: {"id":"x","choices":[{"delta":{"content":"hi"}}]}`,
		``,
		`data: {"id":"x","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"prompt_tokens_details":{"cached_tokens":75}}}`,
		``,
		`data: [DONE]`,
		``,
		"",
	}, "\n")

	var got float64
	var saw int64
	tap := newCacheHitTap(strings.NewReader(stream), func(frac float64) {
		got = frac
		saw++
	})

	// Read 7 bytes at a time to force splits across data-line boundaries and
	// within the usage payload itself.
	buf := make([]byte, 7)
	var out strings.Builder
	for {
		n, err := tap.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	}
	if saw != 1 {
		t.Errorf("expected onHit called once, got %d", saw)
	}
	if got != 0.75 {
		t.Errorf("hit fraction: got %v, want 0.75", got)
	}
	// The tap must have forwarded every byte unchanged (zero-stall passthrough).
	if out.String() != stream {
		t.Errorf("stream not passed through unchanged:\n got: %q\nwant: %q", out.String(), stream)
	}
}

// TestCacheHitTapNoUsageNeverCalls asserts a stream without a usage chunk
// simply never fires onHit (the common non-caching or non-reporting backend).
func TestCacheHitTapNoUsageNeverCalls(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"x","choices":[{"delta":{"content":"hi"}}]}`,
		``,
		`data: [DONE]`,
		``,
		"",
	}, "\n")
	called := false
	tap := newCacheHitTap(strings.NewReader(stream), func(float64) { called = true })
	io.Copy(io.Discard, tap)
	if called {
		t.Error("onHit should not fire for a stream with no usage chunk")
	}
}

// TestCacheHitTapHandlesZeroCached asserts the math is safe when cached_tokens
// is 0 (a cache miss) — the fraction is 0, reported exactly once.
func TestCacheHitTapHandlesZeroCached(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":50,"prompt_tokens_details":{"cached_tokens":0}}}`,
		``,
		`data: [DONE]`,
		``,
		"",
	}, "\n")
	var got float64 = -1
	tap := newCacheHitTap(strings.NewReader(stream), func(frac float64) { got = frac })
	io.Copy(io.Discard, tap)
	if got != 0 {
		t.Errorf("expected 0 hit fraction on full miss, got %v", got)
	}
}

// TestCacheHitTapCapsRunawayLine is the regression for the unbounded-line
// OOM: a backend that streams bytes without a newline used to grow t.line
// without bound. The tap must cap the held line and give up parsing (the relay
// itself is unaffected — every byte still reaches the caller).
func TestCacheHitTapCapsRunawayLine(t *testing.T) {
	// 2 MiB with no newline — well past maxTapLine (1 MiB).
	blob := strings.Repeat("a", 2*1024*1024)
	var saw int64
	tap := newCacheHitTap(strings.NewReader(blob), func(frac float64) { saw++ })

	n, err := io.Copy(io.Discard, tap)
	if err != nil {
		t.Fatalf("relay failed: %v", err)
	}
	if n != int64(len(blob)) {
		t.Errorf("relay dropped bytes: copied %d want %d", n, len(blob))
	}
	if saw != 0 {
		t.Errorf("tap should not have reported a hit on a newline-less blob")
	}
	if !tap.done {
		t.Error("tap should have stopped parsing after the line cap")
	}
}

