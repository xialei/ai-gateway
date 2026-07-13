// Package sse provides helpers for parsing and forwarding Server-Sent Events
// in the OpenAI streaming format (data: {...}\n\n, terminated by data: [DONE]).
package sse

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// Event is one SSE frame.
type Event struct {
	Data string
}

// Done is the OpenAI stream terminator.
const Done = "[DONE]"

// Scanner iterates SSE frames from a reader line by line.
type Scanner struct {
	r *bufio.Reader
}

// NewScanner returns a Scanner over r. SSE frames are delimited by a blank
// line; OpenAI uses \n\n.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

// Next returns the next SSE data payload. It returns io.EOF when the stream
// ends. Multi-line data fields are joined with "\n".
func (s *Scanner) Next() (string, error) {
	var dataLines []string
	for {
		line, err := s.r.ReadString('\n')
		if len(line) == 0 && err != nil {
			if len(dataLines) > 0 {
				return strings.Join(dataLines, "\n"), nil
			}
			return "", err
		}
		trimmed := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(trimmed, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			dataLines = append(dataLines, payload)
		case trimmed == "":
			if len(dataLines) > 0 {
				return strings.Join(dataLines, "\n"), nil
			}
			// blank line with no data: skip
		case strings.HasPrefix(trimmed, ":"):
			// comment / heartbeat
		case hasAnyPrefix(trimmed, "event:", "id:", "retry:"):
			// named SSE fields OpenAI doesn't use; ignore
		default:
			// unknown line; tolerate by ignoring
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(dataLines) > 0 {
				return strings.Join(dataLines, "\n"), nil
			}
			return "", err
		}
	}
}

// IsDone reports whether payload is the OpenAI stream terminator.
func IsDone(payload string) bool {
	return payload == Done
}

// hasAnyPrefix reports whether s begins with any of the given prefixes.
func hasAnyPrefix(s string, prefixes ...string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
