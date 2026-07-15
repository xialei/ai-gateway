package pii

import (
	"log/slog"
	"strings"
)

// jsonNUL is the 6-character JSON escape sequence a backend's JSON encoder
// emits for a raw NUL byte inside an SSE data payload. A sentinel byte \x00
// placed in a request is echoed back inside a JSON string value as the six
// characters backslash-u-0-0-0-0. The restorer normalizes this back to \x00
// so the sentinel state machine can recognize it. (If the backend streams raw
// bytes outside JSON, \x00 passes through unchanged and is matched directly.)
const jsonNUL = "\\u0000"

// RestoreOutcome describes how a restored chunk was produced.
type RestoreOutcome int

const (
	// OutcomeRestored: a placeholder was successfully reassembled and replaced.
	OutcomeRestored RestoreOutcome = iota
	// OutcomeRedacted: placeholder TTL expired or mapping missing → [REDACTED].
	OutcomeRedacted
	// OutcomeOverflowed: buffer cap exceeded → recovery aborted for this stream.
	OutcomeOverflowed
)

// Restorer is the per-stream state machine that reassembles sentinel
// placeholders split across chunk boundaries and replaces them with the
// original entity (or [REDACTED] on TTL expiry).
//
// Design (from spec):
//   - Only flush to the client when the buffer contains a complete sentinel
//     pair OR a prefix that cannot be the start of a sentinel.
//   - Buffer is bounded; overflow aborts recovery (already-restored kept,
//     unrecovered → [REDACTED]).
//   - TTL-expired mappings → [REDACTED] literal passthrough, stream continues.
type Restorer struct {
	store     MapStore
	logger    *slog.Logger
	bufferCap int

	buf        strings.Builder // hold buffer: bytes not yet safe to flush
	overflowed bool
	// seenSentinel tracks whether any sentinel placeholder has been observed
	// in this stream yet. Until the first sentinel appears the stream is
	// indistinguishable from a non-redacted one, so there is nothing to
	// reassemble — holding bytes only delays the first token. Once true, the
	// trailing-prefix hold logic (needed to stitch sentinels split across
	// chunks) engages normally.
	seenSentinel bool
	// rawBuf holds a trailing slice that may be a partial <NUL> escape, so the
	// 6-character sequence split across chunks is reassembled before NUL
	// normalization and sentinel scanning.
	rawBuf strings.Builder
	// stats for audit/observability
	restored int
	redacted int
}

// NewRestorer builds a per-stream restorer.
func NewRestorer(store MapStore, bufferCap int, logger *slog.Logger) *Restorer {
	if bufferCap <= 0 {
		bufferCap = 64 * 1024
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Restorer{store: store, bufferCap: bufferCap, logger: logger}
}

// Feed ingests one chunk of the response stream and returns the bytes safe to
// flush to the client. It performs sentinel reassembly across chunk boundaries,
// including the JSON-escaped form of the NUL sentinel byte.
func (rs *Restorer) Feed(chunk string) string {
	// Reassemble any partial <NUL> escape carried over from the previous chunk,
	// then normalize complete escapes to \x00 so the sentinel scanner matches.
	combined := rs.rawBuf.String() + chunk
	rs.rawBuf.Reset()
	normalized := rs.normalizeJSONNUL(combined)

	if rs.overflowed {
		// Recovery aborted: pass through literally (placeholders already seen
		// were restored; new ones degrade to [REDACTED] via the normal path,
		// but we no longer buffer across chunks).
		return rs.scanNoBuffer(normalized)
	}

	rs.buf.WriteString(normalized)
	if rs.buf.Len() > rs.bufferCap {
		// Fallback tier 3: buffer overflow. Flush what we have with best-effort
		// replacement, then switch to no-buffer passthrough.
		rs.overflowed = true
		rs.logger.Warn("pii restore buffer overflow, degrading", "buffer_cap", rs.bufferCap)
		flushed := rs.processBuffer(true)
		rs.buf.Reset()
		return flushed
	}
	return rs.processBuffer(false)
}

// normalizeJSONNUL replaces complete <NUL> sequences with \x00 and holds back any
// trailing prefix of <NUL> that might continue in the next chunk.
func (rs *Restorer) normalizeJSONNUL(s string) string {
	for {
		i := strings.Index(s, jsonNUL)
		if i < 0 {
			break
		}
		s = s[:i] + "\x00" + s[i+len(jsonNUL):]
	}
	maxHold := len(jsonNUL) - 1
	if maxHold > 0 && len(s) > 0 {
		for n := maxHold; n > 0; n-- {
			if n > len(s) {
				continue
			}
			if s[len(s)-n:] == jsonNUL[:n] {
				rs.rawBuf.WriteString(s[len(s)-n:])
				s = s[:len(s)-n]
				break
			}
		}
	}
	return s
}

// Flush returns any remaining buffered content, called at stream end.
func (rs *Restorer) Flush() string {
	// Drain any held raw escape prefix: at stream end it cannot complete, so
	// emit it literally (it is not a valid sentinel).
	tail := rs.rawBuf.String()
	rs.rawBuf.Reset()
	if tail != "" {
		rs.buf.WriteString(tail)
	}
	if rs.overflowed {
		out := rs.scanNoBuffer(rs.buf.String())
		rs.buf.Reset()
		return out
	}
	out := rs.processBuffer(true)
	rs.buf.Reset()
	return out
}

// Stats reports restoration counts for audit.
func (rs *Restorer) Stats() (restored, redacted int, overflowed bool) {
	return rs.restored, rs.redacted, rs.overflowed
}

// processBuffer scans the hold buffer, replacing complete sentinels and
// flushing bytes that are safe (not a sentinel prefix). If flushTail is true
// (stream end or overflow), everything is emitted.
func (rs *Restorer) processBuffer(flushTail bool) string {
	s := rs.buf.String()
	rs.buf.Reset()

	var out strings.Builder
	out.Grow(len(s))

	for len(s) > 0 {
		startIdx := strings.Index(s, sentinelStart)
		if startIdx < 0 {
			// No sentinel start in the remainder. The tail may still be a
			// prefix of a future sentinel start (e.g. a lone \x00 that could
			// continue to \x00PH:...), so compute how much to hold.
			hold := trailingPrefixLen(s)
			if hold == 0 || flushTail {
				if out.Len() == 0 {
					// Fast path: nothing to hold (hold==0), or stream end so the
					// trailing prefix cannot complete and is emitted as-is — AND
					// no prior iteration wrote restored entities into out.
					// Return s directly, skipping the out builder entirely (the
					// builder path would Grow+WriteString a full copy). s is
					// freshly produced by normalizeJSONNUL / buf.String(), so
					// returning it shares no live caller state.
					//
					// This is also the TTFT path: a first chunk that contains
					// no sentinel-start prefix (the common early-stream case)
					// flushes immediately rather than waiting for the next
					// chunk, so the first token is not delayed.
					return s
				}
				out.WriteString(s)
				return out.String()
			}
			// The tail is a sentinel-start prefix; hold it until the next chunk.
			out.WriteString(s[:len(s)-hold])
			rs.buf.WriteString(s[len(s)-hold:])
			return out.String()
		}
		// A sentinel is present in this remainder. Record that sentinels are
		// in play for this stream; subsequent chunks then know cross-chunk
		// reassembly is meaningful (rather than a stream that never redacted).
		rs.seenSentinel = true
		// flush everything before the sentinel start
		out.WriteString(s[:startIdx])
		rest := s[startIdx:]

		// find the closing sentinel
		endIdx := strings.Index(rest[len(sentinelStart):], sentinelEnd)
		if endIdx < 0 {
			// incomplete sentinel: hold it all unless flushTail.
			if flushTail {
				// Stream ended mid-sentinel → the id cannot be resolved, so
				// degrade the sentinel-start prefix to [REDACTED]. Then continue
				// scanning the remainder (mirrors scanNoBuffer) so trailing
				// content after the partial sentinel is preserved, not dropped.
				out.WriteString("[REDACTED]")
				rs.redacted++
				s = rest[len(sentinelStart):]
				continue
			}
			rs.buf.WriteString(rest)
			return out.String()
		}
		// complete sentinel: rest[:len(sentinelStart)+endIdx+1]
		full := rest[:len(sentinelStart)+endIdx+1]
		id, err := DecodePlaceholder(full)
		if err != nil {
			// malformed → flush literally
			out.WriteString(full)
			s = rest[len(full):]
			continue
		}
		entity, ok := rs.store.Get(id)
		if ok {
			out.WriteString(entity)
			rs.restored++
		} else {
			// Fallback tier 2: TTL expired / mapping missing.
			out.WriteString("[REDACTED]")
			rs.redacted++
		}
		s = rest[len(full):]
	}
	return out.String()
}

// scanNoBuffer does best-effort replacement without holding a cross-chunk
// buffer. Used after overflow: only complete sentinels within this chunk are
// replaced; partial ones are emitted with their tail degraded to [REDACTED].
func (rs *Restorer) scanNoBuffer(s string) string {
	var out strings.Builder
	out.Grow(len(s))
	for len(s) > 0 {
		startIdx := strings.Index(s, sentinelStart)
		if startIdx < 0 {
			// No sentinel start. A trailing prefix of sentinelStart (e.g. a
			// stray NUL) is NOT a placeholder — pass it through literally
			// rather than degrading to [REDACTED]. We are in the overflow
			// sacrifice path; restoration is best-effort, not speculative.
			out.WriteString(s)
			return out.String()
		}
		out.WriteString(s[:startIdx])
		rest := s[startIdx:]
		endIdx := strings.Index(rest[len(sentinelStart):], sentinelEnd)
		if endIdx < 0 {
			// Incomplete sentinel in no-buffer mode: emit [REDACTED] for the
			// sentinel-start prefix and continue scanning the remainder
			// (rather than dropping it), so content after a malformed start
			// is preserved.
			out.WriteString("[REDACTED]")
			rs.redacted++
			s = rest[len(sentinelStart):]
			continue
		}
		full := rest[:len(sentinelStart)+endIdx+1]
		id, err := DecodePlaceholder(full)
		if err != nil {
			out.WriteString(full)
			s = rest[len(full):]
			continue
		}
		entity, ok := rs.store.Get(id)
		if ok {
			out.WriteString(entity)
			rs.restored++
		} else {
			out.WriteString("[REDACTED]")
			rs.redacted++
		}
		s = rest[len(full):]
	}
	return out.String()
}

// trailingPrefixLen returns the length of the longest suffix of s that is a
// prefix of sentinelStart. Used to decide how much to hold across chunks.
func trailingPrefixLen(s string) int {
	for n := len(sentinelStart); n > 0; n-- {
		if n > len(s) {
			continue
		}
		if s[len(s)-n:] == sentinelStart[:n] {
			return n
		}
	}
	return 0
}
