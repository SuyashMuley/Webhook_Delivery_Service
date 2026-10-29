// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"webhookdelivery/internal/retry"
	"webhookdelivery/internal/secretbox"
)

type Config struct {
	Env             string // dev | prod
	DatabaseURL     string
	HTTPAddr        string
	LogLevel        string // debug | info | warn | error
	LogFormat       string // json | text
	ShutdownTimeout time.Duration

	// Worker settings.
	WorkerBatchSize   int           // max deliveries claimed per poll
	WorkerConcurrency int           // max deliveries in flight per worker
	PollInterval      time.Duration // sleep when the queue is empty (fallback when WorkerListen is off)
	WorkerListen      bool          // wake idle workers via Postgres LISTEN/NOTIFY when events arrive
	DeliveryTimeout   time.Duration // total time allowed for one HTTP delivery
	LeaseDuration     time.Duration // how long a claim is held before it can be reclaimed
	TenantSlice       int           // max deliveries taken from one tenant per claim (fairness)
	ReaperInterval    time.Duration // how often expired leases are reclaimed

	// Retry policy.
	MaxAttempts   int             // attempts per delivery before it goes to the DLQ
	RetrySchedule []time.Duration // nominal wait after the 1st, 2nd, ... failed attempt
	RetryAfterMax time.Duration   // upper bound on honoring a receiver's Retry-After

	// Circuit breaker.
	BreakerThreshold int           // consecutive failures that open an endpoint's circuit
	BreakerBaseOpen  time.Duration // first open window; doubles each re-open
	BreakerMaxOpen   time.Duration
	AutoDisableAfter time.Duration // continuous failure time after which an endpoint is disabled (0 = never)

	APIRequestTimeout time.Duration // deadline for one API request, database calls included

	// Operations.
	MetricsAddr        string        // listen address of the Prometheus /metrics server ("" disables it)
	OTLPEndpoint       string        // OTLP/HTTP trace collector, e.g. http://jaeger:4318 ("" = no export)
	TraceSampleRatio   float64       // fraction of new traces sampled (0..1)
	DrainDelay         time.Duration // how long readiness fails before the listener closes on shutdown
	WorkerDrainTimeout time.Duration // how long a worker waits for in-flight deliveries on shutdown
	StatsInterval      time.Duration // how often queue state is sampled for metrics

	// Retention (0 disables a category; PURGE_INTERVAL=0 disables the job).
	PurgeInterval   time.Duration
	RetainSucceeded time.Duration
	RetainDead      time.Duration
	RetainEvents    time.Duration

	// Security.
	AllowPrivateTargets bool   // permit http:// endpoint URLs and private/loopback destinations (dev only)
	SecretKeys          string // SECRET_ENCRYPTION_KEYS: "id:base64key[,id:base64key...]", first is active
	DBRole              string // database role to run as ("" = stay as the login user)
}

// Keyring builds the key-encryption keyring for endpoint secrets. Development
// falls back to a fixed, public key so the stack runs with no setup; production
// refuses to start without real keys (see load).
func (c Config) Keyring() (*secretbox.Keyring, error) {
	if c.SecretKeys == "" {
		return secretbox.DevKeyring(), nil
	}
	return secretbox.ParseKeyring(c.SecretKeys)
}

// Load reads configuration from the environment. defaultHTTPAddr and
// defaultDBRole let each binary pick its own listen address and restricted
// database role without needing separate variables.
func Load(defaultHTTPAddr, defaultDBRole string) (Config, error) {
	return load(os.Getenv, defaultHTTPAddr, defaultDBRole)
}

func load(get func(string) string, defaultHTTPAddr, defaultDBRole string) (Config, error) {
	var errs []error
	c := Config{
		Env:         or(get("APP_ENV"), "dev"),
		DatabaseURL: get("DATABASE_URL"),
		HTTPAddr:    or(get("HTTP_ADDR"), defaultHTTPAddr),
		LogLevel:    or(get("LOG_LEVEL"), "info"),
		LogFormat:   get("LOG_FORMAT"),

		ShutdownTimeout:   duration(get, "SHUTDOWN_TIMEOUT", "30s", &errs),
		WorkerBatchSize:   integer(get, "WORKER_BATCH_SIZE", 20, &errs),
		WorkerConcurrency: integer(get, "WORKER_CONCURRENCY", 50, &errs),
		PollInterval:      duration(get, "POLL_INTERVAL", "500ms", &errs),
		DeliveryTimeout:   duration(get, "DELIVERY_TIMEOUT", "10s", &errs),
		LeaseDuration:     duration(get, "LEASE_DURATION", "60s", &errs),
		TenantSlice:       integer(get, "WORKER_TENANT_SLICE", 5, &errs),
		ReaperInterval:    duration(get, "REAPER_INTERVAL", "10s", &errs),

		MaxAttempts:   integer(get, "MAX_ATTEMPTS", 8, &errs),
		RetryAfterMax: duration(get, "RETRY_AFTER_MAX", "6h", &errs),
		RetrySchedule: schedule(get, "RETRY_SCHEDULE", retry.DefaultSchedule, &errs),

		BreakerThreshold: integer(get, "BREAKER_THRESHOLD", 20, &errs),
		BreakerBaseOpen:  duration(get, "BREAKER_BASE_OPEN", "30s", &errs),
		BreakerMaxOpen:   duration(get, "BREAKER_MAX_OPEN", "1h", &errs),
		AutoDisableAfter: duration(get, "AUTO_DISABLE_AFTER", "72h", &errs),

		MetricsAddr:        or(get("METRICS_ADDR"), ":9090"),
		OTLPEndpoint:       strings.TrimRight(strings.TrimSpace(get("OTEL_EXPORTER_OTLP_ENDPOINT")), "/"),
		DrainDelay:         duration(get, "SHUTDOWN_DRAIN_DELAY", "3s", &errs),
		WorkerDrainTimeout: duration(get, "WORKER_DRAIN_TIMEOUT", "20s", &errs),
		StatsInterval:      duration(get, "STATS_INTERVAL", "15s", &errs),
		APIRequestTimeout:  duration(get, "API_REQUEST_TIMEOUT", "5s", &errs),

		PurgeInterval:   duration(get, "PURGE_INTERVAL", "5m", &errs),
		RetainSucceeded: duration(get, "RETAIN_SUCCEEDED", "168h", &errs),
		RetainDead:      duration(get, "RETAIN_DEAD", "720h", &errs),
		RetainEvents:    duration(get, "RETAIN_EVENTS", "168h", &errs),

		SecretKeys: strings.TrimSpace(get("SECRET_ENCRYPTION_KEYS")),
		DBRole:     or(get("DB_ROLE"), defaultDBRole),
	}
	if c.DBRole == "none" {
		c.DBRole = ""
	}
	c.TraceSampleRatio = ratio(get, "OTEL_TRACES_SAMPLER_ARG", map[bool]float64{true: 1, false: 0.1}[c.Env == "dev"], &errs)
	c.WorkerListen = boolean(get, "WORKER_LISTEN", true, &errs)
	c.AllowPrivateTargets = boolean(get, "ALLOW_PRIVATE_TARGETS", c.Env == "dev", &errs)
	if c.LogFormat == "" {
		c.LogFormat = "text"
		if c.Env == "prod" {
			c.LogFormat = "json"
		}
	}

	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.Env != "dev" && c.Env != "prod" {
		errs = append(errs, fmt.Errorf("APP_ENV must be dev or prod, got %q", c.Env))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or text, got %q", c.LogFormat))
	}
	if c.WorkerBatchSize < 1 || c.WorkerConcurrency < 1 {
		errs = append(errs, errors.New("WORKER_BATCH_SIZE and WORKER_CONCURRENCY must be >= 1"))
	}
	if c.TenantSlice < 1 || c.MaxAttempts < 1 {
		errs = append(errs, errors.New("WORKER_TENANT_SLICE and MAX_ATTEMPTS must be >= 1"))
	}
	if c.BreakerThreshold < 1 || c.BreakerBaseOpen <= 0 || c.BreakerMaxOpen < c.BreakerBaseOpen {
		errs = append(errs, errors.New("BREAKER_THRESHOLD must be >= 1 and BREAKER_MAX_OPEN >= BREAKER_BASE_OPEN > 0"))
	}
	if c.Env == "prod" {
		if c.SecretKeys == "" {
			errs = append(errs, errors.New("SECRET_ENCRYPTION_KEYS is required when APP_ENV=prod (generate one with: admin gen-secret-key <id>)"))
		}
	}
	if c.SecretKeys != "" {
		if _, err := secretbox.ParseKeyring(c.SecretKeys); err != nil {
			errs = append(errs, fmt.Errorf("SECRET_ENCRYPTION_KEYS: %w", err))
		}
	}
	if c.LeaseDuration <= c.DeliveryTimeout {
		errs = append(errs, errors.New("LEASE_DURATION must be longer than DELIVERY_TIMEOUT"))
	}

	return c, errors.Join(errs...)
}

func duration(get func(string) string, key, def string, errs *[]error) time.Duration {
	d, err := time.ParseDuration(or(get(key), def))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
	}
	return d
}

// schedule parses a comma-separated list of positive durations, e.g. "30s,5m,1h".
func schedule(get func(string) string, key string, def []time.Duration, errs *[]error) []time.Duration {
	v := get(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	var out []time.Duration
	for part := range strings.SplitSeq(v, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || d <= 0 {
			*errs = append(*errs, fmt.Errorf("%s: %q is not a positive duration", key, part))
			continue
		}
		out = append(out, d)
	}
	return out
}

// ratio parses a fraction in [0, 1].
func ratio(get func(string) string, key string, def float64, errs *[]error) float64 {
	v := strings.TrimSpace(get(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 || f > 1 {
		*errs = append(*errs, fmt.Errorf("%s must be a number between 0 and 1, got %q", key, v))
		return def
	}
	return f
}

func boolean(get func(string) string, key string, def bool, errs *[]error) bool {
	v := strings.TrimSpace(get(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
	}
	return b
}

func integer(get func(string) string, key string, def int, errs *[]error) int {
	v := get(key)
	if strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func or(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
