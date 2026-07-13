// Package normalize converts inbound OpenAI-compatible HTTP requests into
// the internal model.Request representation.
package normalize

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// openAIChatRequest mirrors the subset of the OpenAI chat completions
// payload the gateway cares about. Unknown fields are ignored.
type openAIChatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	Temperature *float64  `json:"temperature,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	// LatencyClass is a gateway extension hint (header overrides in ApplyHeaders).
}

// Message is the JSON form of a chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

// ChatRequest parses an inbound chat-completions request body.
func ChatRequest(r *http.Request) (model.Request, error) {
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "json") {
		// tolerate absent header; body still JSON
	}
	var body openAIChatRequest
	dec := json.NewDecoder(r.Body)
	// Unknown fields are ignored rather than rejected: OpenAI-compatible
	// backends (and their proxies) emit extra/optional fields, and rejecting
	// them would break compatibility. Validation is limited to the fields the
	// gateway actually depends on (model, messages) below. This also keeps the
	// decode hot path free of per-field map lookups.
	if err := dec.Decode(&body); err != nil {
		return model.Request{}, fmt.Errorf("decode chat request: %w", err)
	}
	if body.Model == "" {
		return model.Request{}, fmt.Errorf("missing model")
	}
	if len(body.Messages) == 0 {
		return model.Request{}, fmt.Errorf("missing messages")
	}
	msgs := make([]model.Message, len(body.Messages))
	for i, m := range body.Messages {
		if m.Role == "" {
			return model.Request{}, fmt.Errorf("message %d missing role", i)
		}
		msgs[i] = model.Message{Role: m.Role, Content: m.Content, Name: m.Name}
	}
	req := model.Request{
		Model:       body.Model,
		Messages:    msgs,
		Stream:      body.Stream,
		Temperature: body.Temperature,
		MaxTokens:   body.MaxTokens,
		Context:     map[string]any{},
	}
	ApplyHeaders(&req, r)
	return req, nil
}

// ApplyHeaders overlays gateway-specific headers onto a request.
//   - X-Gateway-Latency-Class: strict|normal|loose
//   - X-Request-Id: correlation id
//   - Authorization: Bearer <api key> → APIKey
func ApplyHeaders(req *model.Request, r *http.Request) {
	if v := r.Header.Get("X-Gateway-Latency-Class"); v != "" {
		req.LatencyClass = model.LatencyClass(v)
	}
	if v := r.Header.Get("X-Request-Id"); v != "" {
		req.ID = v
	}
	if v := r.Header.Get("Authorization"); v != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(v, prefix) {
			req.APIKey = strings.TrimPrefix(v, prefix)
		}
	}
}
