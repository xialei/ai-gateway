package server

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/pii"
)

// errorReader returns data then a non-EOF error, modeling a backend stream
// that fails mid-response. The restoringReader must deliver the restored
// bytes it already buffered BEFORE surfacing the error.
type errorReader struct {
	data string
	err  error
	read int
}

func (e *errorReader) Read(p []byte) (int, error) {
	if e.read >= len(e.data) {
		return 0, e.err
	}
	n := copy(p, e.data[e.read:])
	e.read += n
	if e.read >= len(e.data) {
		return n, e.err // final bytes + error together
	}
	return n, nil
}

func TestRestoringReaderDeliversBufferedBeforeError(t *testing.T) {
	store := pii.NewMemoryMapStore()
	store.Put("abc", "alice@example.com", time.Minute)
	placeholder := "\x00PH:abc\x00"
	// Stream: prefix + sentinel + trailing content, then a read error.
	src := &errorReader{data: "hello " + placeholder + " world", err: errors.New("upstream reset")}

	restorer := pii.NewRestorer(store, 64*1024, nil)
	rr := newRestoringReader(src, restorer, nil)

	var got strings.Builder
	_, err := io.Copy(&got, rr)
	if err == nil {
		t.Fatal("expected the upstream error to propagate after buffered bytes")
	}
	if !strings.Contains(got.String(), "alice@example.com") {
		t.Errorf("lost restored bytes before error: got %q", got.String())
	}
	if !strings.Contains(got.String(), "world") {
		t.Errorf("lost trailing content before error: got %q", got.String())
	}
}
