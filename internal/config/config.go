// Package config loads gateway configuration and resolves latency budgets.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/lei.xia/ai-gateway/internal/model"
)

// Config is the root gateway configuration.
type Config struct {
	Server    ServerConfig     `yaml:"server"`
	Access    AccessConfig     `yaml:"access"`
	Latency   LatencyBudgets   `yaml:"latency"`
	Scheduler SchedulerConfig  `yaml:"scheduler"`
	Policy    PolicyConfig     `yaml:"policy"`
	Context   ContextConfig    `yaml:"context"`
	PII       PIIConfig        `yaml:"pii"`
	EventBus  EventBusConfig   `yaml:"eventbus"`
	Connector ConnectorConfig  `yaml:"connector"`
	Instances []InstanceConfig `yaml:"instances"`
}

type ServerConfig struct {
	Addr         string        `yaml:"addr"`
	TLSCrt       string        `yaml:"tls_crt"`
	TLSKey       string        `yaml:"tls_key"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	// ReadHeaderTimeout bounds how long the server waits for request headers.
	// It is the primary slowloris guard and protects the accept loop from
	// clients that drip-feed header bytes. Unlike ReadTimeout it does not cap
	// the streaming body or response, so long generations are unaffected.
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
}

// ConnectorConfig tunes the outbound HTTP transport used to reach backends.
// The defaults favor streaming latency stability over raw throughput: more
// idle conns per host (so a warm instance rarely re-handshakes), a soft
// MaxConnsPerHost for backpressure, and HTTP/2 when the backend supports it.
type ConnectorConfig struct {
	MaxIdleConns        int           `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost     int           `yaml:"max_conns_per_host"`
	IdleConnTimeout     time.Duration `yaml:"idle_conn_timeout"`
	// ForceAttemptHTTP2 enables HTTP/2 negotiation (h2c or ALPN) to backends
	// behind an HTTP/2-capable front. Most vLLM/SGLang deployments sit behind
	// such a proxy; HTTP/2 multiplexes many concurrent streams over one conn,
	// which is the connection-oriented resource model the gateway targets.
	ForceAttemptHTTP2 bool `yaml:"force_attempt_http2"`
}

type AccessConfig struct {
	APIKeys []string `yaml:"api_keys"`
	// Requests per second per key.
	RateLimitPerSecond float64 `yaml:"rate_limit_per_second"`
	RateLimitBurst     int     `yaml:"rate_limit_burst"`
}

// LatencyBudgets maps each latency class to a total link budget.
type LatencyBudgets struct {
	Strict time.Duration `yaml:"strict"`
	Normal time.Duration `yaml:"normal"`
	Loose  time.Duration `yaml:"loose"`
}

func (b LatencyBudgets) For(class model.LatencyClass) (time.Duration, error) {
	switch class {
	case model.LatencyStrict:
		return b.Strict, nil
	case model.LatencyNormal:
		return b.Normal, nil
	case model.LatencyLoose:
		return b.Loose, nil
	default:
		return 0, fmt.Errorf("unknown latency class %q", class)
	}
}

type SchedulerConfig struct {
	VirtualNodes       int           `yaml:"virtual_nodes"`
	HealthProbeEvery   time.Duration `yaml:"health_probe_every"`
	MetricsScrapeEvery time.Duration `yaml:"metrics_scrape_every"`
	// MinStableWindow suppresses ring thrash on membership churn.
	MinStableWindow time.Duration `yaml:"min_stable_window"`
	// LoadAwareCandidates is how many prefix-affinity neighbors the scheduler
	// considers before picking. 1 = pure prefix affinity (the legacy behavior,
	// maximize KV-cache hit at the cost of ignoring load). Larger values trade
	// a little KV-affinity for load spreading: among the N nearest ring
	// neighbors, pick the one with the best live GPU/cache/wait signal. This
	// is the inference-aware upgrade over a plain L7 gateway.
	LoadAwareCandidates int `yaml:"load_aware_candidates"`
	// WaitingRequestsThreshold is the soft cap on a candidate's
	// num_requests_waiting: a candidate at or above this value is deprioritized
	// (its KV cache is likely about to be evicted under queue pressure, so the
	// affinity benefit is no longer worth the queue latency). 0 disables the
	// threshold (never deprioritize on waiting count).
	WaitingRequestsThreshold int `yaml:"waiting_requests_threshold"`
	// BreakerErrorThreshold is the recent-failure fraction (from the per-instance
	// error-rate EWMA) that trips the passive circuit breaker, excluding an
	// instance from scheduling for BreakerOpenFor without flipping its health
	// flag. Default 0.5. Set higher to make the breaker tolerate transient
	// failover bursts (e.g. 3 retries all failing should not brand a healthy
	// instance broken); set lower to trip faster on sustained errors.
	BreakerErrorThreshold float64 `yaml:"breaker_error_threshold"`
	// BreakerOpenFor is how long a tripped breaker stays open before half-open.
	BreakerOpenFor time.Duration `yaml:"breaker_open_for"`
}

type PolicyConfig struct {
	DefaultLatencyClass model.LatencyClass `yaml:"default_latency_class"`
	// Routes maps model name → instance group / backend target label.
	Routes map[string]string `yaml:"routes"`
	// ACL: api key → allowed models (empty = all).
	ACL map[string][]string `yaml:"acl"`
	// ExternalTargets lists route target labels whose backends are external
	// (OpenAI/Anthropic) and thus subject to PII redaction.
	ExternalTargets []string `yaml:"external_targets"`
}

// ContextConfig toggles the Context Pipeline. The engine and Plugin contract
// are the extension point; no plugins ship built-in (Memory/RAG/Rewrite/
// Summary are implemented by the upper layer and registered in
// buildContextPlugins). Plugin enable/timeout flags belong to that upper-layer
// wiring, not to the gateway core, so they are not modeled here.
type ContextConfig struct {
	Enabled bool `yaml:"enabled"`
}

type PIIConfig struct {
	Enabled   bool          `yaml:"enabled"`
	MapTTL    time.Duration `yaml:"map_ttl"`
	BufferCap int           `yaml:"buffer_cap"`
	Patterns  []PIIPattern  `yaml:"patterns"`
}

type PIIPattern struct {
	Name    string `yaml:"name"`
	Pattern string `yaml:"pattern"`
}

type EventBusConfig struct {
	Enabled   bool `yaml:"enabled"`
	Workers   int  `yaml:"workers"`
	BufferCap int  `yaml:"buffer_cap"`
}

type InstanceConfig struct {
	ID      string `yaml:"id"`
	BaseURL string `yaml:"base_url"`
	Model   string `yaml:"model"`
	Weight  int    `yaml:"weight"`
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.Latency.Strict == 0 {
		c.Latency.Strict = 500 * time.Millisecond
	}
	if c.Latency.Normal == 0 {
		c.Latency.Normal = 2 * time.Second
	}
	if c.Latency.Loose == 0 {
		c.Latency.Loose = 5 * time.Second
	}
	if c.Policy.DefaultLatencyClass == "" {
		c.Policy.DefaultLatencyClass = model.LatencyNormal
	}
	if c.Scheduler.VirtualNodes == 0 {
		c.Scheduler.VirtualNodes = 160
	}
	if c.Scheduler.HealthProbeEvery == 0 {
		c.Scheduler.HealthProbeEvery = 3 * time.Second
	}
	if c.Scheduler.MetricsScrapeEvery == 0 {
		c.Scheduler.MetricsScrapeEvery = 5 * time.Second
	}
	if c.Scheduler.MinStableWindow == 0 {
		c.Scheduler.MinStableWindow = 2 * time.Second
	}
	if c.Scheduler.LoadAwareCandidates == 0 {
		// Default to 3: the primary affinity owner plus two nearest neighbors.
		// This spreads load across the small set most likely to share the KV
		// prefix without fragmenting affinity across the whole ring.
		c.Scheduler.LoadAwareCandidates = 3
	}
	if c.Scheduler.WaitingRequestsThreshold == 0 {
		// A vLLM instance with this many queued requests is under sustained
		// pressure; deprioritize it unless every candidate is in the same state.
		c.Scheduler.WaitingRequestsThreshold = 8
	}
	if c.Scheduler.BreakerErrorThreshold == 0 {
		c.Scheduler.BreakerErrorThreshold = 0.5
	}
	if c.Scheduler.BreakerOpenFor == 0 {
		c.Scheduler.BreakerOpenFor = 10 * time.Second
	}
	if c.PII.Enabled {
		if c.PII.MapTTL == 0 {
			c.PII.MapTTL = 5 * time.Minute
		}
		if c.PII.BufferCap == 0 {
			c.PII.BufferCap = 64 * 1024
		}
	}
	if c.EventBus.Enabled {
		if c.EventBus.Workers == 0 {
			c.EventBus.Workers = 2
		}
		if c.EventBus.BufferCap == 0 {
			c.EventBus.BufferCap = 1024
		}
	}
	if c.Connector.MaxIdleConns == 0 {
		c.Connector.MaxIdleConns = 1024
	}
	if c.Connector.MaxIdleConnsPerHost == 0 {
		c.Connector.MaxIdleConnsPerHost = 128
	}
	if c.Connector.MaxConnsPerHost == 0 {
		c.Connector.MaxConnsPerHost = 512
	}
	if c.Connector.IdleConnTimeout == 0 {
		c.Connector.IdleConnTimeout = 90 * time.Second
	}
	c.Connector.ForceAttemptHTTP2 = true
	if c.Server.ReadHeaderTimeout == 0 {
		// Bound header reads at 10s by default; the streaming tail is still
		// exempt (ReadTimeout/WriteTimeout do not apply once headers are read).
		c.Server.ReadHeaderTimeout = 10 * time.Second
	}
}
