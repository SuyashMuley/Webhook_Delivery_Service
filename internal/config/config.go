// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
	PollInterval      time.Duration // sleep when the queue is empty
	DeliveryTimeout   time.Duration // total time allowed for one HTTP delivery
	LeaseDuration     time.Duration // how long a claim is held before it can be reclaimed
}

// Load reads configuration from the environment. defaultHTTPAddr lets each
// binary pick its own listen address without needing separate variables.
func Load(defaultHTTPAddr string) (Config, error) {
	return load(os.Getenv, defaultHTTPAddr)
}

func load(get func(string) string, defaultHTTPAddr string) (Config, error) {
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
	}
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
