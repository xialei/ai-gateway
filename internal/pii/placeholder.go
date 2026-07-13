// Package pii implements external-model redaction: Detect → Replace (with
// sentinel placeholders) → Forward → Restore. The streaming restore path is a
// bounded per-stream state machine that reassembles placeholders split across
// token chunk boundaries, with three fallback tiers.
package pii

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel framing. The leading \x00 NUL byte does not occur in normal model
// token streams, making accidental matches effectively impossible. The id is
// a short hex string keyed to the stored original entity.
//
//	\x00PH:<id>\x00
const (
	sentinelStart = "\x00PH:"
	sentinelEnd   = "\x00"
)

// EncodePlaceholder returns the sentinel placeholder for id.
func EncodePlaceholder(id string) string {
	return sentinelStart + id + sentinelEnd
}

// ErrInvalidPlaceholder is returned by DecodePlaceholder for malformed input.
var ErrInvalidPlaceholder = errors.New("invalid placeholder")

// DecodePlaceholder extracts the id from a complete sentinel placeholder, or
// returns ErrInvalidPlaceholder if the string is not a complete placeholder.
func DecodePlaceholder(s string) (string, error) {
	if !strings.HasPrefix(s, sentinelStart) || !strings.HasSuffix(s, sentinelEnd) {
		return "", ErrInvalidPlaceholder
	}
	inner := s[len(sentinelStart) : len(s)-len(sentinelEnd)]
	if inner == "" {
		return "", ErrInvalidPlaceholder
	}
	return inner, nil
}

// IsPlaceholderPrefix reports whether s is a (possibly partial) prefix of a
// sentinel placeholder. Used by the restore state machine to decide whether to
// buffer and wait vs flush immediately.
func IsPlaceholderPrefix(s string) bool {
	// Walk the sentinel start sequence; s is a prefix if it matches up to its
	// length. We also accept partial matches of the trailing sentinel.
	if strings.HasPrefix(s, sentinelStart) {
		return true // starts correctly, may be mid-id or mid-trailing-NUL
	}
	// check partial match against sentinelStart
	for i := 1; i <= len(s) && i <= len(sentinelStart); i++ {
		if s == sentinelStart[:i] {
			return true
		}
	}
	return false
}

// GenerateID returns a short hex id for an entity. Deterministic-per-call via
// a counter + entity hash; uniqueness within a request's TTL window is all
// that's required.
func GenerateID(entity string, counter int) string {
	return fmt.Sprintf("%x", hashEntity(entity)^uint64(counter)+uint64(counter)*2654435761)
}

// hashEntity is a stable FNV-1a hash; avoids importing crypto for a non-
// security-sensitive id.
func hashEntity(s string) uint64 {
	const offset = 1469598103934665603
	const prime = 1099511628211
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
