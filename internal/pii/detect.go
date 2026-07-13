package pii

import (
	"regexp"
	"time"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// Pattern is one detection rule.
type Pattern struct {
	Name    string
	Pattern *regexp.Regexp
}

// CompilePatterns compiles raw pattern strings into Pattern values.
func CompilePatterns(raw []RawPattern) ([]Pattern, error) {
	out := make([]Pattern, 0, len(raw))
	for _, r := range raw {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			return nil, &PatternError{Name: r.Name, Err: err}
		}
		out = append(out, Pattern{Name: r.Name, Pattern: re})
	}
	return out, nil
}

// RawPattern is the uncompiled form from config.
type RawPattern struct {
	Name    string
	Pattern string
}

// PatternError reports a compile failure.
type PatternError struct {
	Name string
	Err  error
}

func (e *PatternError) Error() string { return "pattern " + e.Name + ": " + e.Err.Error() }

// Redactor detects entities in a request body and replaces them with sentinel
// placeholders, persisting id → entity mappings in the MapStore.
type Redactor struct {
	patterns []Pattern
	store    MapStore
	ttl      time.Duration
}

// NewRedactor builds a Redactor.
func NewRedactor(patterns []Pattern, store MapStore, ttl time.Duration) *Redactor {
	return &Redactor{patterns: patterns, store: store, ttl: ttl}
}

// Redact scans text, replacing each detected entity with a sentinel
// placeholder. Equal entities within one call map to the same id. Returns the
// redacted text and the set of ids created/refreshed (for cleanup on request
// completion).
func (r *Redactor) Redact(text string) (string, []string) {
	if len(r.patterns) == 0 {
		return text, nil
	}
	entityToID := map[string]string{}
	var ids []string
	counter := 0
	out := r.redactText(text, entityToID, &ids, &counter)
	return out, ids
}

// RedactMessages redacts the Content of each message in place, sharing entity
// → id mappings across the whole request so the same entity appearing in
// multiple messages (e.g. an email in both the system and user message) maps
// to ONE placeholder and ONE store entry, not one per message. Returns the
// deduplicated list of ids created/refreshed, for cleanup on request completion.
func (r *Redactor) RedactMessages(messages []model.Message) []string {
	if len(r.patterns) == 0 {
		return nil
	}
	entityToID := map[string]string{}
	var ids []string
	counter := 0
	for i := range messages {
		messages[i].Content = r.redactText(messages[i].Content, entityToID, &ids, &counter)
	}
	return ids
}

// redactText is the shared scan loop. entityToID and counter are shared across
// calls when invoked from RedactMessages, so equal entities reuse one id and
// one store Put across the entire request.
func (r *Redactor) redactText(text string, entityToID map[string]string, ids *[]string, counter *int) string {
	out := text
	for _, p := range r.patterns {
		// ReplaceFunc lets us map equal entities to equal ids deterministically.
		out = p.Pattern.ReplaceAllStringFunc(out, func(match string) string {
			if id, ok := entityToID[match]; ok {
				return EncodePlaceholder(id)
			}
			id := GenerateID(match, *counter)
			*counter++
			entityToID[match] = id
			r.store.Put(id, match, r.ttl)
			*ids = append(*ids, id)
			return EncodePlaceholder(id)
		})
	}
	return out
}

// Release deletes the mappings for the given ids. Called when the request
// completes, per the design ("请求结束立即删除映射").
func (r *Redactor) Release(ids []string) {
	for _, id := range ids {
		r.store.Delete(id)
	}
}
