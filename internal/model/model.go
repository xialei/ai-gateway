// Package model holds shared types that flow across gateway modules.
package model

import "time"

// LatencyClass governs the link budget injected by the Policy Engine.
type LatencyClass string

const (
	LatencyStrict LatencyClass = "strict"
	LatencyNormal LatencyClass = "normal"
	LatencyLoose  LatencyClass = "loose"
)

// PIIDecision is the Policy Engine's verdict for an outgoing request.
type PIIDecision string

const (
	PIIAllow  PIIDecision = "allow"  // forward as-is
	PIIRedact PIIDecision = "redact" // detect → replace → restore on stream
	PIIBlock  PIIDecision = "block"  // refuse to forward
)

// Instance is one inference backend member.
type Instance struct {
	ID       string
	BaseURL  string
	Weight   int
	Model    string
	Healthy  bool
	InFlight int
	Metrics  InstanceMetrics
	// CacheHitRate is the gateway-side EWMA of the per-request KV-cache hit
	// fraction observed for this instance (cached_tokens/prompt_tokens, fed
	// back from the backend's final usage chunk). Unlike Metrics (scraped from
	// the backend /metrics endpoint), this is a gateway-observed signal that
	// closes the prefix-affinity loop. Range [0,1]; 0 when no feedback yet.
	CacheHitRate float64
}

// InstanceMetrics are scraped from the backend /metrics endpoint.
type InstanceMetrics struct {
	RunningRequests int
	WaitingRequests int
	GPUCacheUsage   float64
	Preemptions     int64
}

// Request is the normalized internal representation of an inbound call.
type Request struct {
	ID           string
	APIKey       string
	Model        string
	Messages     []Message
	Stream       bool
	Temperature  *float64
	MaxTokens    *int
	LatencyClass LatencyClass
	// Budget is the remaining link budget, decremented as it flows downstream.
	Budget time.Duration
	// RouteTarget is decided by the Policy Engine.
	RouteTarget string
	PIIDecision PIIDecision
	// InstanceID is chosen by the Prefix Scheduler.
	InstanceID string
	// Context carries named artifacts produced by Context Pipeline plugins.
	Context map[string]any
}

// Message is one chat message in OpenAI-compatible form. The JSON tags make
// Message directly serializable to the OpenAI wire format, so the connector
// can marshal []Message without rebuilding a parallel openAIMessage slice on
// every request (an object-copy the hot path does not need).
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

// EventType enumerates async events emitted from the sync path.
type EventType string

const (
	EventRouteDecision EventType = "route_decision"
	EventPIIResult     EventType = "pii_result"
	EventTokenUsage    EventType = "token_usage"
	EventLatency       EventType = "latency"
	EventAudit         EventType = "audit"
)

// Event is the unit sent to the Async Event Bus.
type Event struct {
	Type       EventType
	RequestID  string
	InstanceID string
	Model      string
	At         time.Time
	Fields     map[string]any
}
