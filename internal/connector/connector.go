// Package connector implements the OpenAI-compatible Model Connector. It
// serializes a model.Request back to the OpenAI chat-completions wire format,
// forwards it to a backend instance, and returns the response body for
// streaming relay.
package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lei.xia/ai-gateway/internal/config"
	"github.com/lei.xia/ai-gateway/internal/model"
)

// Connector owns the HTTP client used to reach backends.
type Connector struct {
	client *http.Client
}

// New constructs a Connector. The client is tuned for streaming:
//   - No client-side Timeout (it would cap the entire streaming response).
//   - No transport-level ResponseHeaderTimeout: a single global value would cap
//     every latency class at the same ceiling (e.g. a loose 5s request killed
//     at the normal 2s budget). Instead the per-request budget in
//     doWithHeaderBudget governs connect + first-byte per class, with the
//     streaming tail exempt. The request's own context (the caller's r.Context)
//     still cancels the stream if the client disconnects.
//   - A generous per-host idle pool so a warm instance rarely re-handshakes,
//     and a soft MaxConnsPerHost for backpressure — the connection-oriented
//     resource model (a connection is the unit, not a request).
func New(cc config.ConnectorConfig) *Connector {
	if cc.MaxIdleConns <= 0 {
		cc.MaxIdleConns = 1024
	}
	if cc.MaxIdleConnsPerHost <= 0 {
		cc.MaxIdleConnsPerHost = 128
	}
	if cc.IdleConnTimeout <= 0 {
		cc.IdleConnTimeout = 90 * time.Second
	}
	return &Connector{
		client: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				MaxIdleConns:        cc.MaxIdleConns,
				MaxIdleConnsPerHost: cc.MaxIdleConnsPerHost,
				// 0 means unlimited; only set when configured so the default
				// path does not impose an unintended cap.
				MaxConnsPerHost:    cc.MaxConnsPerHost,
				IdleConnTimeout:    cc.IdleConnTimeout,
				ForceAttemptHTTP2:  cc.ForceAttemptHTTP2,
			},
		},
	}
}

// Forward sends req to baseURL and returns the raw response body reader plus
// the content type. The caller MUST close the returned reader.
//
// ctx is attached to the outbound request; it should be the caller's request
// context (so a client disconnect cancels the stream). req.Budget bounds ONLY
// connect + first-byte: it races client.Do against a timer, and a budget
// elapsed before headers arrive yields ErrHeaderTimeout. Once headers arrive
// the timer is stopped and the body streams under ctx alone — so a long
// generation is not truncated by the budget (the streaming tail is exempt),
// while a slow-to-respond backend still fails fast under a strict latency
// class. This is the per-request budget that the transport-level
// ResponseHeaderTimeout (a single global value) could not provide.
func (c *Connector) Forward(ctx context.Context, baseURL string, req model.Request) (io.ReadCloser, string, *http.Header, error) {
	// A fresh buffer per request (not pooled). Pooling the request-body buffer
	// is unsafe: on a header-budget timeout doWithHeaderBudget returns while
	// the in-flight client.Do goroutine is still reading the request body from
	// bytes.NewReader(payload). Returning such a buffer to a sync.Pool would
	// let another Forward reset/overwrite it mid-read — a data race corrupting
	// the outbound body. The GC handles a per-request buffer correctly.
	var buf bytes.Buffer
	if err := encode(&buf, req); err != nil {
		return nil, "", nil, err
	}
	payload := buf.Bytes()
	url := strings.TrimRight(baseURL, "/") + "/v1/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, "", nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("X-Request-Id", req.ID)

	resp, err := c.doWithHeaderBudget(ctx, httpReq, req.Budget)
	if err != nil {
		return nil, "", nil, fmt.Errorf("backend call: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, "", nil, &BackendError{Status: resp.StatusCode, Body: string(body)}
	}
	return resp.Body, resp.Header.Get("Content-Type"), &resp.Header, nil
}

// ErrHeaderTimeout is returned when the backend does not produce response
// headers within the request's budget. It is a connect/first-byte failure, so
// the caller treats it as retryable failover (not a committed-stream error).
var ErrHeaderTimeout = errors.New("backend header timeout exceeded budget")

// doWithHeaderBudget runs client.Do in a goroutine and races it against the
// request budget. If headers arrive before the budget elapses, the response is
// returned and the body continues to stream under ctx (the parent context),
// unbounded by the budget — implementing the "connect + first byte governed,
// streaming tail exempt" contract that a transport-level ResponseHeaderTimeout
// (one global value for all latency classes) cannot.
//
// The outbound request carries ctx, NOT a budget-derived context: that way a
// budget timeout does not cancel the body. Instead the budget is enforced by
// the select below, which returns ErrHeaderTimeout and abandons the in-flight
// Do (its ctx-derived cancellation follows from the caller closing the
// response or the client disconnecting).
func (c *Connector) doWithHeaderBudget(ctx context.Context, httpReq *http.Request, budget time.Duration) (*http.Response, error) {
	if budget <= 0 {
		// No per-request budget: there is no transport-level ceiling either
		// (New intentionally omits ResponseHeaderTimeout so one latency class
		// is not capped at another's budget). Use a generous default so a
		// misconfigured request still fails fast rather than hanging forever.
		budget = 30 * time.Second
	}
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := c.client.Do(httpReq)
		done <- result{resp, err}
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.resp, r.err
	case <-timer.C:
		// Headers did not arrive within budget. The in-flight Do is still
		// running against ctx (the caller's request context), so it is reaped
		// when that context is canceled at request end — not leaked forever.
		// If the backend eventually responds after the budget, drain and close
		// the orphaned body so its connection returns to the pool.
		go func() {
			if r := <-done; r.resp != nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(r.resp.Body, 4096))
				r.resp.Body.Close()
			}
		}()
		return nil, ErrHeaderTimeout
	}
}

// BackendError carries a non-2xx backend response.
type BackendError struct {
	Status int
	Body   string
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend status %d: %s", e.Status, e.Body)
}

// openAIChatRequest is the wire-format payload sent to backends. Its Messages
// field is []model.Message directly — model.Message carries the OpenAI JSON
// tags, so the connector marshals the request without rebuilding a parallel
// message slice (one fewer allocation + copy per request).
type openAIChatRequest struct {
	Model       string          `json:"model"`
	Messages    []model.Message `json:"messages"`
	Stream      bool            `json:"stream"`
	Temperature *float64        `json:"temperature,omitempty"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
}

// encode writes the OpenAI chat-completions payload for req into buf. Encoding
// into a pooled buffer avoids the internal grow+alloc that json.Marshal would
// perform for each request on the hot path.
func encode(buf *bytes.Buffer, req model.Request) error {
	out := openAIChatRequest{
		Model:       req.Model,
		Messages:    req.Messages,
		Stream:      req.Stream,
		Temperature: req.Temperature,
		MaxTokens:   req.MaxTokens,
	}
	enc := json.NewEncoder(buf)
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("encode chat request: %w", err)
	}
	// json.Encoder.Encode appends a trailing newline; trim it so the body
	// matches the canonical OpenAI payload exactly.
	if b := buf.Bytes(); len(b) > 0 && b[len(b)-1] == '\n' {
		buf.Truncate(len(b) - 1)
	}
	return nil
}
