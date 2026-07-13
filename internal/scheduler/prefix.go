package scheduler

import (
	"strings"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// maxPrefixChars bounds how much of the prompt feeds the affinity key. KV
// caches are prefix caches; only the leading prompt matters for hit rate, and
// a bounded key keeps hashing O(1)-ish.
const maxPrefixChars = 4096

// PrefixKey derives the affinity key from a request's messages. It normalizes
// (role + content of the leading messages, lowercased, whitespace-trimmed) so
// trivial formatting differences do not fragment affinity.
//
// Only the prefix is hashed: KV-cache hit rate depends on the leading prompt,
// not the trailing user turn, so the key intentionally excludes the last
// message to keep requests with the same system/instruction prefix together.
func PrefixKey(req model.Request) string {
	var sb strings.Builder
	remaining := maxPrefixChars
	// Use all but the final message as the prefix anchor when there's more
	// than one message; a lone message is used in full.
	last := len(req.Messages) - 1
	if last < 0 {
		last = 0
	}
	for i, m := range req.Messages {
		if i == last && len(req.Messages) > 1 {
			break
		}
		sb.WriteString(strings.ToLower(strings.TrimSpace(m.Role)))
		sb.WriteString(":")
		sb.WriteString(strings.ToLower(strings.TrimSpace(m.Content)))
		sb.WriteString("\n")
		remaining -= len(m.Role) + len(m.Content) + 2
		if remaining <= 0 {
			break
		}
	}
	return sb.String()
}
