package server

import (
	"io"
	"log/slog"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/pii"
)

// toRawPatterns converts config patterns to pii.RawPattern.
func toRawPatterns(ps []config.PIIPattern) []pii.RawPattern {
	out := make([]pii.RawPattern, len(ps))
	for i, p := range ps {
		out[i] = pii.RawPattern{Name: p.Name, Pattern: p.Pattern}
	}
	return out
}

// restoringReader wraps an io.Reader of a response stream and pipes each read
// chunk through a Restorer, emitting the restored bytes. Because restoration
// is NOT length-preserving (a placeholder collapses to a longer or shorter
// entity, or expands to [REDACTED]), the reader maintains an internal buffer
// of restored bytes and serves Read calls from it, refilling from the source
// as needed. This is the bridge between the SSE body and the per-stream
// restoration state machine.
//
// It honors the io.Reader contract: a non-zero byte count is returned together
// with any error, so callers (io.Copy) drain restored bytes before stopping on
// a source error.
type restoringReader struct {
	src      io.Reader
	restorer *pii.Restorer
	logger   *slog.Logger
	// buf holds restored bytes not yet consumed by the caller. Sliced, not
	// grown, after each refill: Feed returns a string whose bytes we keep in
	// a backing slice and serve incrementally.
	buf []byte
	// scratch is a reusable read buffer for source reads, avoiding a per-Read
	// allocation. It is sized to match a typical SSE chunk; larger chunks are
	// handled by the source returning more bytes than the scratch length on a
	// single Read call, which io.Reader permits.
	scratch [8192]byte
	// eof is set when the source returned io.EOF.
	eof bool
	// pendingErr holds a non-EOF source error to report after buffered bytes
	// are delivered. Cleared once surfaced.
	pendingErr error
}

func newRestoringReader(src io.Reader, r *pii.Restorer, logger *slog.Logger) *restoringReader {
	if logger == nil {
		logger = slog.Default()
	}
	return &restoringReader{src: src, restorer: r, logger: logger}
}

func (rr *restoringReader) Read(p []byte) (int, error) {
	// Refill only when we have no buffered bytes to serve.
	if len(rr.buf) == 0 && !rr.eof && rr.pendingErr == nil {
		rr.refill()
	}
	if len(rr.buf) == 0 {
		// Nothing to deliver: surface a pending error or EOF.
		if rr.pendingErr != nil {
			err := rr.pendingErr
			rr.pendingErr = nil
			return 0, err
		}
		return 0, io.EOF
	}
	n := copy(p, rr.buf)
	rr.buf = rr.buf[n:]
	// If a source error is pending, report it alongside the final bytes.
	if len(rr.buf) == 0 && rr.pendingErr != nil {
		err := rr.pendingErr
		rr.pendingErr = nil
		return n, err
	}
	return n, nil
}

// refill reads one chunk from the source and feeds it through the restorer,
// appending restored bytes to buf. On io.EOF it flushes the restorer's hold
// buffer; a non-EOF error is stored in pendingErr for later surfacing.
func (rr *restoringReader) refill() {
	n, err := rr.src.Read(rr.scratch[:])
	if n > 0 {
		// Feed takes a string; the conversion copies scratch[:n] into a new
		// string. This is the one unavoidable copy across the string-based
		// Restorer boundary. The returned restored bytes are appended directly
		// to buf (no intermediate allocation when cap suffices).
		restored := rr.restorer.Feed(string(rr.scratch[:n]))
		rr.buf = append(rr.buf, restored...)
	}
	if err != nil {
		if err == io.EOF {
			rr.eof = true
			tail := rr.restorer.Flush()
			if tail != "" {
				rr.buf = append(rr.buf, tail...)
			}
			return
		}
		// Non-EOF error: keep it to report after buffered bytes drain.
		rr.pendingErr = err
	}
}
