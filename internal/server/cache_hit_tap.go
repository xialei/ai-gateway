package server

import (
	"encoding/json"
	"io"
	"strings"
)

// maxTapLine bounds the held partial line. A single SSE data line should never
// approach this (usage chunks are tiny); if a backend streams bytes without a
// newline for this long, the tap gives up parsing rather than growing without
// bound. The relay itself is unaffected — bytes still flow to the client.
const maxTapLine = 1 << 20 // 1 MiB

// cacheHitTap is a zero-stall pass-through io.Reader that inspects the SSE
// stream relayed to the client to recover the backend's final usage chunk,
// from which it extracts the KV-cache hit fraction (cached_tokens /
// prompt_tokens). The fraction is reported via onHit so the registry can
// update its per-instance cacheHitRate EWMA — the closed loop that validates
// prefix affinity.
//
// It MUST NOT delay the stream: bytes are forwarded to the caller as soon as
// they arrive. Parsing is done on the same bytes after they have been handed
// off; the only state held is a line buffer for the SSE frame currently being
// assembled (bounded by maxTapLine).
//
// The tap tolerates non-SSE or malformed bodies: if no usage chunk is seen,
// or the body is not OpenAI-shaped SSE, onHit is simply never called.
type cacheHitTap struct {
	src   io.Reader
	onHit func(hitFraction float64)

	// line accumulates the current SSE data line across Read calls. A data
	// line is emitted when a newline is seen; a frame is complete on a blank
	// line. Only the final usage frame matters, so partial frames are held
	// until completed (bounded by maxTapLine).
	line []byte
	// sawDone after the OpenAI [DONE] sentinel: no further parsing needed.
	done bool
}

// newCacheHitTap wraps src. onHit is invoked at most once, with the recovered
// hit fraction in [0,1], when the final usage chunk is parsed.
func newCacheHitTap(src io.Reader, onHit func(float64)) *cacheHitTap {
	return &cacheHitTap{src: src, onHit: onHit}
}

// Read forwards bytes from the source to p and opportunistically parses any
// complete SSE data lines observed in those bytes.
func (t *cacheHitTap) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 {
		t.parse(p[:n])
	}
	return n, err
}

// parse scans the just-forwarded bytes for complete SSE data lines and, on the
// final usage payload, reports the cache-hit fraction. The bytes in b have
// already been delivered to the client, so parsing is best-effort and never
// affects the relay. A single trailing line without a newline is held in
// t.line until the next Read completes it.
func (t *cacheHitTap) parse(b []byte) {
	if t.done {
		return
	}
	for i := 0; i < len(b); {
		nl := indexByte(b[i:], '\n')
		if nl < 0 {
			// No newline in the remainder: hold it for the next Read. Cap the
			// held line so a malformed/adversarial backend streaming bytes
			// without a newline cannot grow it without bound (OOM). On cap
			// overflow the tap gives up parsing this stream (best-effort); the
			// relay itself is unaffected.
			t.line = append(t.line, b[i:]...)
			if len(t.line) > maxTapLine {
				t.line = t.line[:0]
				t.done = true
				return
			}
			return
		}
		line := b[i : i+nl]
		i += nl + 1
		// Combine with any held partial line from the previous Read.
		if len(t.line) > 0 {
			line = append(t.line, line...)
			t.line = t.line[:0]
		}
		trimmed := strings.TrimRight(string(line), "\r")
		if t.handleLine(trimmed) {
			return // done: usage parsed or [DONE] seen
		}
	}
}

// handleLine processes one SSE line. Returns true if parsing should stop
// (usage recovered, or [DONE] terminator seen).
func (t *cacheHitTap) handleLine(line string) bool {
	if line == "[DONE]" {
		t.done = true
		return true
	}
	const prefix = "data:"
	if !strings.HasPrefix(line, prefix) {
		return false
	}
	payload := strings.TrimSpace(line[len(prefix):])
	if payload == "" || payload == "[DONE]" {
		if payload == "[DONE]" {
			t.done = true
			return true
		}
		return false
	}
	if frac, ok := extractCacheHitFraction(payload); ok {
		if t.onHit != nil {
			t.onHit(frac)
		}
		t.done = true
		return true
	}
	return false
}

// indexByte returns the index of the first occurrence of c in b, or -1.
// Inlined to avoid pulling bytes.IndexByte through a separate call site.
func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

// extractCacheHitFraction parses one SSE data payload (an OpenAI chat
// completion chunk JSON) and returns cached_tokens/prompt_tokens if the chunk
// carries a usage object with prompt token details. Returns ok=false for any
// chunk without usage (the common case — only the final chunk before [DONE]
// carries usage).
func extractCacheHitFraction(payload string) (float64, bool) {
	// Cheap pre-filter: only chunks containing "usage" can carry the field.
	// This avoids a full JSON decode on every token delta.
	if !strings.Contains(payload, "usage") {
		return 0, false
	}
	var u struct {
		Usage *struct {
			PromptTokens int `json:"prompt_tokens"`
			// PromptTokensDetails is OpenAI's object for cached/verified tokens.
			// vLLM/SGLang mirror this shape for KV-cache hits.
			PromptTokensDetails *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(payload), &u); err != nil {
		return 0, false
	}
	if u.Usage == nil || u.Usage.PromptTokens <= 0 || u.Usage.PromptTokensDetails == nil {
		return 0, false
	}
	return float64(u.Usage.PromptTokensDetails.CachedTokens) / float64(u.Usage.PromptTokens), true
}
