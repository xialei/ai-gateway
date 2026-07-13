package pii

import (
	"strings"
	"testing"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
)

func TestRedactAndRestoreRoundtrip(t *testing.T) {
	store := NewMemoryMapStore()
	patterns := mustPatterns(t, []RawPattern{
		{Name: "email", Pattern: `[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`},
	})
	r := NewRedactor(patterns, store, time.Minute)

	text := "Contact me at alice@example.com please."
	redacted, ids := r.Redact(text)
	if len(ids) == 0 {
		t.Fatal("expected at least one entity id")
	}
	if strings.Contains(redacted, "alice@example.com") {
		t.Error("redacted text still contains the entity")
	}
	if !strings.Contains(redacted, sentinelStart) {
		t.Error("redacted text should contain a sentinel placeholder")
	}

	// Restore in one shot.
	rs := NewRestorer(store, 64*1024, nil)
	out := rs.Feed(redacted)
	out += rs.Flush()
	if out != text {
		t.Errorf("roundtrip mismatch:\n got: %q\nwant: %q", out, text)
	}
	restored, _, _ := rs.Stats()
	if restored != 1 {
		t.Errorf("expected 1 restored, got %d", restored)
	}
}

func TestRestoreAcrossChunkBoundary(t *testing.T) {
	store := NewMemoryMapStore()
	store.Put("abc", "secret@example.com", time.Minute)

	placeholder := EncodePlaceholder("abc")
	// Split the placeholder at several points; each split must still restore.
	splits := []int{1, 3, 5, len(sentinelStart), len(sentinelStart) + 1, len(placeholder) - 1}
	for _, sp := range splits {
		if sp <= 0 || sp >= len(placeholder) {
			continue
		}
		chunks := []string{"prefix ", placeholder[:sp], placeholder[sp:], " suffix"}
		rs := NewRestorer(store, 64*1024, nil)
		var sb strings.Builder
		for _, c := range chunks {
			sb.WriteString(rs.Feed(c))
		}
		sb.WriteString(rs.Flush())
		got := sb.String()
		want := "prefix secret@example.com suffix"
		if got != want {
			t.Errorf("split@%d:\n got: %q\nwant: %q", sp, got, want)
		}
	}
}

func TestRestoreMultipleEntitiesInOneChunk(t *testing.T) {
	store := NewMemoryMapStore()
	store.Put("a", "alice@example.com", time.Minute)
	store.Put("b", "bob@example.com", time.Minute)
	text := "to=" + EncodePlaceholder("a") + " from=" + EncodePlaceholder("b")
	rs := NewRestorer(store, 64*1024, nil)
	out := rs.Feed(text) + rs.Flush()
	want := "to=alice@example.com from=bob@example.com"
	if out != want {
		t.Errorf("got %q want %q", out, want)
	}
}

func TestRestoreTTLExpiredDegradesToRedacted(t *testing.T) {
	store := NewMemoryMapStore()
	// Put with effectively-zero TTL so it's expired by the time we read.
	store.Put("gone", "secret@example.com", -1*time.Second)

	text := "hello " + EncodePlaceholder("gone") + " world"
	rs := NewRestorer(store, 64*1024, nil)
	out := rs.Feed(text) + rs.Flush()
	want := "hello [REDACTED] world"
	if out != want {
		t.Errorf("got %q want %q", out, want)
	}
	_, redacted, _ := rs.Stats()
	if redacted != 1 {
		t.Errorf("expected 1 redacted, got %d", redacted)
	}
}

func TestRestoreBufferOverflow(t *testing.T) {
	store := NewMemoryMapStore()
	store.Put("abc", "entity", time.Minute)
	ph := EncodePlaceholder("abc")

	// Tiny buffer cap so a large chunk overflows.
	rs := NewRestorer(store, 8, nil)
	// First feed a chunk larger than cap → overflow.
	out := rs.Feed("prefix " + ph + " padding-padding-padding")
	// After overflow, already-restored portion is emitted; rest degrades.
	flushed := out + rs.Flush()
	if !strings.Contains(flushed, "entity") {
		t.Errorf("overflow path should still emit restored entity, got %q", flushed)
	}
	_, _, overflowed := rs.Stats()
	if !overflowed {
		t.Error("expected overflow flag set")
	}
}

func TestRestoreNonSentinelNULPassesThrough(t *testing.T) {
	// A stray NUL not forming a sentinel must pass through untouched.
	store := NewMemoryMapStore()
	rs := NewRestorer(store, 64*1024, nil)
	out := rs.Feed("a\x00b\x00c") + rs.Flush()
	if out != "a\x00b\x00c" {
		t.Errorf("got %q", out)
	}
}

func TestRestoreStrayNULInOverflowMode(t *testing.T) {
	// In overflow (no-buffer) mode a stray NUL that is not a sentinel must
	// pass through literally, NOT be degraded to [REDACTED]. Regression for
	// the scanNoBuffer trailing-prefix path.
	store := NewMemoryMapStore()
	rs := NewRestorer(store, 4, nil) // tiny cap → immediate overflow
	out := rs.Feed("abc\x00def\x00ghi") + rs.Flush()
	if out != "abc\x00def\x00ghi" {
		t.Errorf("got %q, want literal passthrough", out)
	}
}

func TestRedactEqualEntitiesShareID(t *testing.T) {
	store := NewMemoryMapStore()
	patterns := mustPatterns(t, []RawPattern{
		{Name: "email", Pattern: `[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`},
	})
	r := NewRedactor(patterns, store, time.Minute)
	text := "a@example.com and a@example.com"
	redacted, ids := r.Redact(text)
	if len(ids) != 1 {
		t.Errorf("expected 1 shared id, got %d (%v)", len(ids), ids)
	}
	// both placeholders identical
	if strings.Count(redacted, ids[0]) != 2 {
		t.Errorf("expected the same id twice, redacted=%q", redacted)
	}
}

// TestRedactMessagesDedupesAcrossMessages covers request-level dedup: the same
// entity appearing in multiple messages maps to ONE id and ONE store entry,
// not one per message. This is the regression for the old per-message loop in
// the server, which generated distinct ids for equal cross-message entities.
func TestRedactMessagesDedupesAcrossMessages(t *testing.T) {
	store := NewMemoryMapStore()
	patterns := mustPatterns(t, []RawPattern{
		{Name: "email", Pattern: `[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`},
	})
	r := NewRedactor(patterns, store, time.Minute)

	msgs := []model.Message{
		{Role: "system", Content: "Notify alice@example.com of updates."},
		{Role: "user", Content: "Also CC alice@example.com and bob@example.com."},
	}
	ids := r.RedactMessages(msgs)
	// Two distinct entities across the whole request → exactly 2 ids, NOT 3.
	if len(ids) != 2 {
		t.Errorf("expected 2 deduped ids (alice + bob), got %d (%v)", len(ids), ids)
	}
	// alice appears in both messages but with the SAME placeholder.
	aliceID := ids[0]
	if strings.Count(msgs[0].Content, aliceID) != 1 {
		t.Errorf("system message should contain alice's id once: %q", msgs[0].Content)
	}
	if strings.Count(msgs[1].Content, aliceID) != 1 {
		t.Errorf("user message should reuse alice's id: %q", msgs[1].Content)
	}
	// store should hold exactly 2 entries (one per distinct entity).
	if got := len(store.(*memoryMapStore).data); got != 2 {
		t.Errorf("expected 2 store entries, got %d", got)
	}
	// No raw entity leaks in redacted content.
	for _, m := range msgs {
		if strings.Contains(m.Content, "alice@example.com") || strings.Contains(m.Content, "bob@example.com") {
			t.Errorf("entity leaked in redacted message: %q", m.Content)
		}
	}
}

func TestCompilePatternsError(t *testing.T) {
	_, err := CompilePatterns([]RawPattern{{Name: "bad", Pattern: "("}})
	if err == nil {
		t.Fatal("expected compile error for bad regex")
	}
}

func TestIsPlaceholderPrefix(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"\x00", true},
		{"\x00P", true},
		{"\x00PH:", true},
		{"x", false},
		{"hello", false},
	}
	for _, c := range cases {
		if got := IsPlaceholderPrefix(c.in); got != c.want {
			t.Errorf("IsPlaceholderPrefix(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

func TestRestoreJSONEscapedSentinel(t *testing.T) {
	// A backend JSON-encoder escapes the sentinel NUL byte as the 6-char
	// sequence  inside an SSE data payload. The restorer must still
	// recognize and restore it.
	store := NewMemoryMapStore()
	store.Put("abc", "alice@example.com", time.Minute)
	// Simulate the JSON-escaped form: literal backslash-u-0-0-0-0 around the id.
	escaped := "hello " + jsonNUL + "PH:abc" + jsonNUL + " world"
	rs := NewRestorer(store, 64*1024, nil)
	out := rs.Feed(escaped) + rs.Flush()
	want := "hello alice@example.com world"
	if out != want {
		t.Errorf("got %q want %q", out, want)
	}
	restored, _, _ := rs.Stats()
	if restored != 1 {
		t.Errorf("expected 1 restored, got %d", restored)
	}
}

func TestRestoreJSONEscapedSentinelSplitAcrossChunks(t *testing.T) {
	store := NewMemoryMapStore()
	store.Put("abc", "alice@example.com", time.Minute)
	escaped := "hello " + jsonNUL + "PH:abc" + jsonNUL + " world"
	// Split the 6-char escape sequence itself across chunks.
	rs := NewRestorer(store, 64*1024, nil)
	mid := strings.Index(escaped, jsonNUL) + 3 // split inside ""
	var sb strings.Builder
	sb.WriteString(rs.Feed(escaped[:mid]))
	sb.WriteString(rs.Feed(escaped[mid:]))
	sb.WriteString(rs.Flush())
	if got := sb.String(); got != "hello alice@example.com world" {
		t.Errorf("got %q", got)
	}
}

func mustPatterns(t *testing.T, raw []RawPattern) []Pattern {
	t.Helper()
	p, err := CompilePatterns(raw)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return p
}
