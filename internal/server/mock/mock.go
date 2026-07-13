// Package mock provides an in-process OpenAI-compatible streaming backend so
// the gateway can run end-to-end without a real vLLM/SGLang instance.
package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// chatRequest is the subset of the OpenAI payload the mock reads.
type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chunk mirrors the OpenAI streaming delta payload.
type chunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
}

type choice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Server is a mock OpenAI-compatible backend.
type Server struct {
	mu  sync.Mutex
	req int
}

// New returns a mock backend.
func New() *Server { return &Server{} }

// Handler returns an http.Handler implementing /v1/chat/completions.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, s.metrics())
	})
	mux.HandleFunc("/v1/chat/completions", s.chat)
	return mux
}

func (s *Server) metrics() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join([]string{
		"# HELP num_requests_running",
		"num_requests_running 0",
		"# HELP num_requests_waiting",
		"num_requests_waiting 0",
		"# HELP gpu_cache_usage_perc",
		"gpu_cache_usage_perc 0.1",
	}, "\n")
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := echoReply(req)

	if !req.Stream {
		w.Header().Set("Content-Type", "application/json")
		finish := "stop"
		resp := map[string]any{
			"id":      fmt.Sprintf("chatcmpl-mock-%d", s.nextReq()),
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   req.Model,
			"choices": []map[string]any{
				{
					"index":         0,
					"message":       map[string]any{"role": "assistant", "content": reply},
					"finish_reason": finish,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	id := fmt.Sprintf("chatcmpl-mock-%d", s.nextReq())
	// role frame
	writeChunk(w, flusher, chunk{
		ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: req.Model,
		Choices: []choice{{Index: 0, Delta: delta{Role: "assistant"}}},
	})
	// content frames, a few tokens
	words := strings.Fields(reply)
	for _, word := range words {
		writeChunk(w, flusher, chunk{
			ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: req.Model,
			Choices: []choice{{Index: 0, Delta: delta{Content: word + " "}}},
		})
	}
	// finish frame
	finish := "stop"
	writeChunk(w, flusher, chunk{
		ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: req.Model,
		Choices: []choice{{Index: 0, FinishReason: &finish}},
	})
	fmt.Fprint(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

func writeChunk(w http.ResponseWriter, flusher http.Flusher, c chunk) {
	b, _ := json.Marshal(c)
	fmt.Fprintf(w, "data: %s\n\n", b)
	if flusher != nil {
		flusher.Flush()
	}
}

func (s *Server) nextReq() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.req++
	return s.req
}

func echoReply(req chatRequest) string {
	var lastUser string
	for _, m := range req.Messages {
		if m.Role == "user" {
			lastUser = m.Content
		}
	}
	if lastUser == "" {
		return "Hello from the mock backend."
	}
	return "Echo from mock backend: " + lastUser
}
